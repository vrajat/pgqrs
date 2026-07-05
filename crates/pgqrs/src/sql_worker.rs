use crate::{QueueMessage, Store};
use serde_json::Value;
use sqlparser::ast::{Expr, Value as SqlValue, VisitMut, VisitorMut};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::Duration;

pub async fn run(
    dsn: String,
    schema: String,
    queues: Vec<String>,
    interval_ms: u64,
    worker_name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting pgqrs sql-worker daemon...");
    println!("Worker Name: {}", worker_name);
    println!("Schema: {}", schema);
    println!("Queues: {:?}", queues);
    println!("Poll Interval: {}ms", interval_ms);

    let config = crate::Config::from_dsn_with_schema(&dsn, &schema)?;
    let store = crate::connect_with_config(&config).await?;

    // Spawn a polling task for each queue
    for queue in queues {
        let store = store.clone();
        let queue_name = queue.clone();
        let worker_name = worker_name.clone();

        tokio::spawn(async move {
            println!(
                "Registering worker '{}' for queue '{}'",
                worker_name, queue_name
            );
            let mut consumer = None;
            for attempt in 1..=60 {
                match store.consumer(&queue_name, &worker_name).await {
                    Ok(c) => {
                        consumer = Some(c);
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to register consumer for queue {} (attempt {}/60): {}",
                            queue_name, attempt, e
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }

            let consumer = match consumer {
                Some(c) => c,
                None => {
                    eprintln!("Giving up registering consumer for queue {}", queue_name);
                    return;
                }
            };

            let handler = workflow_handler(&store);

            let result = crate::dequeue()
                .worker(&consumer)
                .batch(1)
                .poll_interval(Duration::from_millis(interval_ms))
                .handle(handler)
                .poll(&store)
                .await;

            if let Err(e) = result {
                eprintln!("Error in dequeue poll loop for queue {}: {}", queue_name, e);
            }
        });
    }

    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    println!("pgqrs sql-worker daemon is running. Press Ctrl+C to stop.");

    tokio::select! {
        _ = &mut sigint => {
            println!("Received shutdown signal. Stopping pgqrs sql-worker...");
        }
    }

    Ok(())
}

pub fn workflow_handler(
    store: &Store,
) -> impl Fn(QueueMessage) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send>>
       + Send
       + Sync
       + Clone
       + 'static {
    let store = store.clone();
    crate::workflow_handler(store.clone(), move |run: crate::Run, input: Value| {
        let store = store.clone();
        Box::pin(async move { execute_steps(&store, run, input).await })
    })
}

struct PlaceholderRewriter {
    input: Value,
    cumulative_outputs: Value,
    bindings: Vec<Value>,
    name_to_index: HashMap<String, usize>,
    error: Option<String>,
}

impl VisitorMut for PlaceholderRewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(ref mut val_with_span) = expr {
            if let SqlValue::Placeholder(ref name) = val_with_span.value {
                let name_clean = name.trim_start_matches(':').to_string();

                let index = match self.name_to_index.get(&name_clean) {
                    Some(&idx) => idx,
                    None => {
                        let val = if name_clean == "input" {
                            self.input.clone()
                        } else {
                            // Check prior step outputs
                            if let Some(outputs_map) = self.cumulative_outputs.as_object() {
                                if let Some(out) = outputs_map.get(&name_clean) {
                                    out.clone()
                                } else {
                                    self.error = Some(format!(
                                        "Placeholder '{}' is not a valid step output or input.",
                                        name_clean
                                    ));
                                    return ControlFlow::Break(());
                                }
                            } else {
                                self.error = Some(format!(
                                    "Placeholder '{}' is not a valid step output or input.",
                                    name_clean
                                ));
                                return ControlFlow::Break(());
                            }
                        };
                        self.bindings.push(val);
                        let idx = self.bindings.len();
                        self.name_to_index.insert(name_clean.clone(), idx);
                        idx
                    }
                };

                val_with_span.value = SqlValue::Placeholder(format!("${}", index));
            }
        }
        ControlFlow::Continue(())
    }
}

async fn execute_steps(store: &Store, run: crate::Run, input: Value) -> crate::Result<Value> {
    let workflow_id = run.record().workflow_id;

    // Load Steps: Load all step definitions from pgqrs_workflow_steps_def ordered by position ASC
    #[derive(sqlx::FromRow, Clone)]
    struct SqlStepDefinition {
        step_name: String,
        statement: String,
        #[allow(dead_code)]
        position: i32,
    }

    let step_defs = sqlx::query_as::<_, SqlStepDefinition>(
        "SELECT step_name, statement, position FROM pgqrs_workflow_steps_def WHERE workflow_id = $1 ORDER BY position ASC"
    )
    .bind(workflow_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| crate::Error::QueryFailed {
        query: "SELECT FROM pgqrs_workflow_steps_def".into(),
        source: Box::new(e),
        context: format!("Failed to load step definitions for workflow {}", workflow_id),
    })?;

    let mut cumulative_outputs = Value::Object(serde_json::Map::new());

    for step_def in step_defs {
        let step_output =
            crate::workflow::workflow_step(&run, &step_def.step_name, {
                let store = store.clone();
                let step_def = step_def.clone();
                let cumulative_outputs = cumulative_outputs.clone();
                let input = input.clone();
                move || {
                    let store = store.clone();
                    let step_def = step_def.clone();
                    let cumulative_outputs = cumulative_outputs.clone();
                    let input = input.clone();
                    async move {
                        // 1. Parse SQL statement
                        let dialect = PostgreSqlDialect {};
                        let mut statements = Parser::parse_sql(&dialect, &step_def.statement)
                            .map_err(|e| crate::Error::ValidationFailed {
                                reason: format!("Failed to parse SQL statement: {}", e),
                            })?;

                        if statements.is_empty() {
                            return Err(crate::Error::ValidationFailed {
                                reason: "SQL statement is empty".to_string(),
                            });
                        }

                        // 2. Rewrite placeholders and collect bindings
                        let mut rewriter = PlaceholderRewriter {
                            input,
                            cumulative_outputs,
                            bindings: Vec::new(),
                            name_to_index: HashMap::new(),
                            error: None,
                        };

                        for stmt in &mut statements {
                            let _ = stmt.visit(&mut rewriter);
                        }

                        if let Some(err) = rewriter.error {
                            return Err(crate::Error::ValidationFailed { reason: err });
                        }

                        let rewritten_sql = statements
                            .iter()
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                            .join("; ");

                        // Check if it's a select query or has returning clause
                        let stmt = &statements[0];
                        let is_query = matches!(stmt, sqlparser::ast::Statement::Query(_));
                        let has_returning = match stmt {
                            sqlparser::ast::Statement::Insert(insert) => {
                                insert.returning.as_ref().is_some_and(|r| !r.is_empty())
                            }
                            sqlparser::ast::Statement::Update(update) => {
                                update.returning.as_ref().is_some_and(|r| !r.is_empty())
                            }
                            sqlparser::ast::Statement::Delete(delete) => {
                                delete.returning.as_ref().is_some_and(|r| !r.is_empty())
                            }
                            _ => false,
                        };

                        // Start a new database transaction
                        let mut tx =
                            store
                                .pool()
                                .begin()
                                .await
                                .map_err(|e| crate::Error::QueryFailed {
                                    query: "BEGIN TRANSACTION".into(),
                                    source: Box::new(e),
                                    context: "Failed to begin transaction for workflow step".into(),
                                })?;

                        let step_output = if is_query || has_returning {
                            // Wrap the query in Postgres JSON conversion to offload datatype serialization
                            let wrapped_sql = format!(
                                "WITH t AS ( {} ) SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM t",
                                rewritten_sql
                            );

                            let mut query = sqlx::query_scalar::<_, Value>(&wrapped_sql);
                            for val in rewriter.bindings {
                                query = query.bind(val);
                            }

                            let res: Value = query.fetch_one(&mut *tx).await.map_err(|e| {
                                crate::Error::QueryFailed {
                                    query: wrapped_sql.clone(),
                                    source: Box::new(e),
                                    context: format!(
                                        "Step '{}' failed during execution",
                                        step_def.step_name
                                    ),
                                }
                            })?;
                            res
                        } else {
                            // Execute DML statement directly without wrapping
                            let mut query = sqlx::query(&rewritten_sql);
                            for val in rewriter.bindings {
                                query = query.bind(val);
                            }

                            query.execute(&mut *tx).await.map_err(|e| {
                                crate::Error::QueryFailed {
                                    query: rewritten_sql.clone(),
                                    source: Box::new(e),
                                    context: format!(
                                        "Step '{}' failed during execution",
                                        step_def.step_name
                                    ),
                                }
                            })?;
                            Value::Array(vec![])
                        };

                        tx.commit().await.map_err(|e| crate::Error::QueryFailed {
                            query: "COMMIT TRANSACTION".into(),
                            source: Box::new(e),
                            context: format!(
                                "Failed to commit transaction for step '{}'",
                                step_def.step_name
                            ),
                        })?;

                        Ok(step_output)
                    }
                }
            })
            .await?;

        // Add the output to the cumulative outputs map
        if let Value::Object(ref mut map) = cumulative_outputs {
            map.insert(step_def.step_name.clone(), step_output);
        }
    }

    Ok(cumulative_outputs)
}
