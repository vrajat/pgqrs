use crate::error::Result;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::types::QueueMessage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use turso::{Database, Value as TursoValue};

const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const MAX_BATCH_SIZE: usize = 100;

const GET_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = ?;
"#;

const LIST_ALL_MESSAGES: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    ORDER BY enqueued_at DESC;
"#;

const DELETE_MESSAGE_BY_ID: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = ?;
"#;

const LIST_MESSAGES_BY_QUEUE: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE queue_id = ?
    ORDER BY enqueued_at DESC
    LIMIT 1000;
"#;

const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = ?
    WHERE id = ?;
"#;

#[derive(Debug, Clone)]
pub struct TursoMessageTable {
    db: Arc<Database>,
}

impl TursoMessageTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn map_row(row: &turso::Row) -> Result<QueueMessage> {
        let id: i64 = row.get(0)?;
        let queue_id: i64 = row.get(1)?;

        let payload_str: String = row.get(2)?;
        let payload: JsonValue = serde_json::from_str(&payload_str)?;

        let vt_str: Option<String> = row.get(3)?;
        let vt = match vt_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let enqueued_at_str: String = row.get(4)?;
        let enqueued_at = parse_turso_timestamp(&enqueued_at_str)?;

        let read_ct: i32 = row.get(5)?;

        let dequeued_at_str: Option<String> = row.get(6)?;
        let dequeued_at = match dequeued_at_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let producer_worker_id: Option<i64> = row.get(7)?;
        let consumer_worker_id: Option<i64> = row.get(8)?;

        Ok(QueueMessage {
            id,
            queue_id,
            payload,
            vt: vt.unwrap_or(enqueued_at),
            enqueued_at,
            read_ct,
            dequeued_at,
            producer_worker_id,
            consumer_worker_id,
        })
    }

    pub async fn delete_owned(&self, id: i64, worker_id: i64) -> Result<u64> {
        let rows = crate::store::turso::query(
            "DELETE FROM pgqrs_messages WHERE id = ? AND consumer_worker_id = ?",
        )
        .bind(id)
        .bind(worker_id)
        .execute(&self.db)
        .await?;
        Ok(rows)
    }

    pub async fn delete_many_owned(&self, ids: &[i64], worker_id: i64) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        if ids.len() > MAX_BATCH_SIZE {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!("Batch size {} exceeds limit {}", ids.len(), MAX_BATCH_SIZE),
            });
        }

        let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "DELETE FROM pgqrs_messages WHERE id IN ({}) AND consumer_worker_id = ? RETURNING id",
            placeholders.join(", ")
        );

        let mut query = crate::store::turso::query(&sql);
        for id in ids {
            query = query.bind(*id);
        }
        query = query.bind(worker_id);

        let rows = query.fetch_all(&self.db).await?;

        let mut deleted_ids = HashSet::new();
        for row in rows {
            let id: i64 = row.get(0)?;
            deleted_ids.insert(id);
        }

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            results.push(deleted_ids.contains(id));
        }
        Ok(results)
    }
}

#[async_trait]
impl crate::store::MessageTable for TursoMessageTable {
    async fn insert(&self, data: crate::types::NewMessage) -> Result<QueueMessage> {
        let payload_str = data.payload.to_string();
        let enqueued_at_str = format_turso_timestamp(&data.enqueued_at);
        let vt_str = format_turso_timestamp(&data.vt);

        let row = crate::store::turso::query(INSERT_MESSAGE)
            .bind(TursoValue::Integer(data.queue_id))
            .bind(TursoValue::Text(payload_str))
            .bind(TursoValue::Integer(data.read_ct as i64))
            .bind(TursoValue::Text(enqueued_at_str))
            .bind(TursoValue::Text(vt_str))
            .bind(match data.producer_worker_id {
                Some(id) => TursoValue::Integer(id),
                None => TursoValue::Null,
            })
            .bind(match data.consumer_worker_id {
                Some(id) => TursoValue::Integer(id),
                None => TursoValue::Null,
            })
            .fetch_one(&self.db)
            .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        let row = crate::store::turso::query(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.db)
            .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        let rows = crate::store::turso::query(LIST_ALL_MESSAGES)
            .fetch_all(&self.db)
            .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_messages")
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let rows = crate::store::turso::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(rows)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        let rows = crate::store::turso::query(LIST_MESSAGES_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.db)
            .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[JsonValue],
        params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        if payloads.is_empty() {
            return Ok(vec![]);
        }

        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Begin batch".into(),
            })?;

        let mut ids = Vec::with_capacity(payloads.len());
        let enqueued_at_str = format_turso_timestamp(&params.enqueued_at);
        let vt_str = format_turso_timestamp(&params.vt);

        for payload in payloads {
            // We can check if loop fails, we should ROLLBACK ideally.
            let res = async {
                crate::store::turso::query(
                    r#"
                    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                    "#,
                )
                .bind(TursoValue::Integer(queue_id))
                .bind(TursoValue::Text(payload.to_string()))
                .bind(TursoValue::Integer(params.read_ct as i64))
                .bind(TursoValue::Text(enqueued_at_str.as_str().to_string()))
                .bind(TursoValue::Text(vt_str.as_str().to_string()))
                .bind(match params.producer_worker_id {
                    Some(id) => TursoValue::Integer(id),
                    None => TursoValue::Null,
                })
                .bind(match params.consumer_worker_id {
                    Some(id) => TursoValue::Integer(id),
                    None => TursoValue::Null,
                })
                .fetch_one_on_connection(&conn)
                .await
             }.await;

            match res {
                Ok(row) => {
                    let id: i64 = row.get(0)?;
                    ids.push(id);
                }
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(e);
                }
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Commit batch".into(),
            })?;

        Ok(ids)
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        if ids.len() > MAX_BATCH_SIZE {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!("Batch size {} exceeds limit {}", ids.len(), MAX_BATCH_SIZE),
            });
        }

        let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id IN ({}) ORDER BY id",
            placeholders.join(", ")
        );

        let mut query = crate::store::turso::query(&sql);
        for id in ids {
            query = query.bind(*id);
        }

        let rows = query.fetch_all(&self.db).await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn update_visibility_timeout(&self, id: i64, vt: DateTime<Utc>) -> Result<u64> {
        let vt_str = format_turso_timestamp(&vt);
        let rows = crate::store::turso::query(UPDATE_MESSAGE_VT)
            .bind(vt_str)
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(rows)
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        let sql = r#"
            UPDATE pgqrs_messages
            SET vt = datetime(vt, '+' || ? || ' seconds')
            WHERE id = ? AND consumer_worker_id = ?;
        "#;

        let rows = crate::store::turso::query(sql)
            .bind(additional_seconds as i32)
            .bind(id)
            .bind(worker_id)
            .execute(&self.db)
            .await?;

        Ok(rows)
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;
        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Begin extend batch".into(),
            })?;

        let mut extended_set = HashSet::new();
        let sql = r#"
            UPDATE pgqrs_messages
            SET vt = datetime(vt, '+' || ? || ' seconds')
            WHERE id = ? AND consumer_worker_id = ?
        "#;

        for id in message_ids {
            let res = async {
                crate::store::turso::query(sql)
                    .bind(additional_seconds as i32)
                    .bind(*id)
                    .bind(worker_id)
                    .execute_on_connection(&conn)
                    .await
            }
            .await;

            match res {
                Ok(rows) => {
                    if rows > 0 {
                        extended_set.insert(*id);
                    }
                }
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(e);
                }
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Commit extend batch".into(),
            })?;

        let result = message_ids
            .iter()
            .map(|id| extended_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;
        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Begin release batch".into(),
            })?;

        let mut released_set = HashSet::new();
        let sql = r#"
            UPDATE pgqrs_messages
            SET vt = datetime('now'), consumer_worker_id = NULL
            WHERE id = ? AND consumer_worker_id = ?
        "#;

        for id in message_ids {
            let res = async {
                crate::store::turso::query(sql)
                    .bind(*id)
                    .bind(worker_id)
                    .execute_on_connection(&conn)
                    .await
            }
            .await;

            match res {
                Ok(rows) => {
                    if rows > 0 {
                        released_set.insert(*id);
                    }
                }
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(e);
                }
            }
        }

        conn.execute("COMMIT", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COMMIT".into(),
                source: Box::new(e),
                context: "Commit release batch".into(),
            })?;

        let result = message_ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending_filtered(queue_id, None).await
    }

    async fn count_pending_filtered(&self, queue_id: i64, worker_id: Option<i64>) -> Result<i64> {
        let count: i64 = match worker_id {
            Some(wid) => {
                crate::store::turso::query_scalar(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = ? AND consumer_worker_id = ?
                    "#,
                )
                .bind(queue_id)
                .bind(wid)
                .fetch_one(&self.db)
                .await
            }
            None => {
                crate::store::turso::query_scalar(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = ? AND (vt IS NULL OR vt <= datetime('now')) AND consumer_worker_id IS NULL
                    "#,
                )
                .bind(queue_id)
                .fetch_one(&self.db)
                .await
            }
        }?; // Error already mapped

        Ok(count)
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(ids.len());

        if ids.is_empty() {
            return Ok(vec![]);
        }

        for &id in ids {
            let rows = crate::store::turso::query(DELETE_MESSAGE_BY_ID)
                .bind(id)
                .execute(&self.db)
                .await?;
            results.push(rows > 0);
        }

        Ok(results)
    }
}
