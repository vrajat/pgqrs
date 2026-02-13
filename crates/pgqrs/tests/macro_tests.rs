use pgqrs::{pgqrs_step, pgqrs_workflow, Run, StepGuardExt, Store};

use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

#[pgqrs_step]
async fn step_one(ctx: &mut (impl Run + ?Sized), _input: &str) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: "step1_done".to_string(),
    })
}

#[pgqrs_step]
#[allow(unused_variables)] // Test attribute forwarding (should not warn about arg2)
async fn step_multi_args(
    ctx: &mut (impl Run + ?Sized),
    arg1: &str,
    arg2: i32,
) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: format!("multi: {}", arg1),
    })
}

#[pgqrs_step]
async fn step_side_effect(
    _ctx: &mut (impl Run + ?Sized),
    _input: &str,
) -> anyhow::Result<TestData> {
    // This step returns a value that we will manually tamper with in the DB
    // to prove that the second execution returns the DB value, not this value.
    Ok(TestData {
        msg: "original_value".to_string(),
    })
}

#[pgqrs_step]
async fn step_fail(ctx: &mut (impl Run + ?Sized), _input: &str) -> anyhow::Result<TestData> {
    anyhow::bail!("step failed intentionally")
}

#[pgqrs_workflow]
async fn my_workflow(ctx: &mut (impl Run + ?Sized), input: &TestData) -> anyhow::Result<TestData> {
    // Step 1
    let s1 = step_one(ctx, "input").await?;

    // Step with multiple args
    let s2 = step_multi_args(ctx, "arg", 42).await?;

    // Return combined result
    Ok(TestData {
        msg: format!("{}, {}, {}", input.msg, s1.msg, s2.msg),
    })
}

#[pgqrs_workflow]
async fn workflow_with_failing_step(
    ctx: &mut (impl Run + ?Sized),
    _input: &TestData,
) -> anyhow::Result<TestData> {
    let _ = step_fail(ctx, "fail").await?;
    Ok(TestData {
        msg: "should not happen".to_string(),
    })
}

#[pgqrs_workflow]
async fn workflow_fail_at_end(
    ctx: &mut (impl Run + ?Sized),
    _input: &TestData,
) -> anyhow::Result<TestData> {
    anyhow::bail!("workflow failed intentionally")
}

#[tokio::test]
async fn test_macro_suite() -> anyhow::Result<()> {
    // Setup
    let store = common::create_store("pgqrs_workflow_test").await;

    // --- CASE 0: Creation State (Pending) ---
    {
        let input = TestData {
            msg: "pending_check".to_string(),
        };
        pgqrs::workflow().name("pending_wf").create(&store).await?;
        let run = pgqrs::workflow()
            .name("pending_wf")
            .trigger(&input)?
            .run(&store)
            .await?;

        // Verify status is PENDING immediately after creation
        let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
        assert_eq!(
            record.status,
            pgqrs::WorkflowStatus::Pending,
            "Run should be PENDING upon creation"
        );
    }

    // --- CASE 1: Successful Workflow (with multi-arg step) ---
    {
        let input = TestData {
            msg: "start".to_string(),
        };
        pgqrs::workflow().name("my_workflow").create(&store).await?;
        let mut my_wf_run = pgqrs::workflow()
            .name("my_workflow")
            .trigger(&input)?
            .run(&store)
            .await?;

        let res = my_workflow(&mut my_wf_run, &input).await?;
        assert_eq!(res.msg, "start, step1_done, multi: arg");

        // Verify persisting SUCCESS
        let record = pgqrs::tables(&store)
            .workflow_runs()
            .get(my_wf_run.id())
            .await?;
        assert_eq!(record.status, pgqrs::WorkflowStatus::Success);
        let db_output: TestData = serde_json::from_value(record.output.unwrap())?;
        assert_eq!(db_output.msg, "start, step1_done, multi: arg");
    }

    // --- CASE 2: Step Idempotency (Retry logic) ---
    {
        // 1. Create context
        let input = TestData {
            msg: "idempotency".to_string(),
        };
        pgqrs::workflow()
            .name("idempotency_wf")
            .create(&store)
            .await?;
        let mut idem_wf_run = pgqrs::workflow()
            .name("idempotency_wf")
            .trigger(&input)?
            .run(&store)
            .await?;

        // 2. Run step first time -> Success
        let res1 = step_side_effect(&mut idem_wf_run, "run1").await?;
        assert_eq!(res1.msg, "original_value");

        // 3. Manually TAMPER with the step output in the database
        // This proves that the next call reads from DB instead of running function logic
        let tampered_json = serde_json::json!({ "msg": "tampered_value" });
        let tampered_json_sql = tampered_json.to_string().replace('\'', "''");
        let step_col = "step_id";
        let update_sql = format!(
            "UPDATE pgqrs_workflow_steps SET output = '{}' WHERE run_id = {} AND {} = 'step_side_effect'",
            tampered_json_sql, idem_wf_run.id(), step_col
        );
        store.execute_raw(&update_sql).await?;

        // 4. Run step second time -> Should return TAMPERED value
        let res2 = step_side_effect(&mut idem_wf_run, "run2").await?;
        assert_eq!(
            res2.msg, "tampered_value",
            "Step should have returned cached (tampered) result from DB"
        );
    }

    // --- CASE 3: Step Failure ---
    {
        let input = TestData {
            msg: "fail_step".to_string(),
        };
        pgqrs::workflow()
            .name("workflow_with_failing_step")
            .create(&store)
            .await?;
        let mut wf_failing_step_run = pgqrs::workflow()
            .name("workflow_with_failing_step")
            .trigger(&input)?
            .run(&store)
            .await?;

        let res = workflow_with_failing_step(&mut wf_failing_step_run, &input).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "step failed intentionally");

        // Verify persistence
        let record = pgqrs::tables(&store)
            .workflow_runs()
            .get(wf_failing_step_run.id())
            .await?;
        assert_eq!(record.status, pgqrs::WorkflowStatus::Error);
        let error_val = record.error.expect("Should have error");
        let error_str = error_val.as_str().expect("Error should be string");
        assert!(error_str.contains("step failed intentionally"));
    }

    // --- CASE 4: Workflow Failure ---
    {
        let input = TestData {
            msg: "fail_wf".to_string(),
        };
        pgqrs::workflow()
            .name("workflow_fail_at_end")
            .create(&store)
            .await?;
        let mut wf_fail_run = pgqrs::workflow()
            .name("workflow_fail_at_end")
            .trigger(&input)?
            .run(&store)
            .await?;

        let res = workflow_fail_at_end(&mut wf_fail_run, &input).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "workflow failed intentionally"
        );

        // Verify persistence
        let record = pgqrs::tables(&store)
            .workflow_runs()
            .get(wf_fail_run.id())
            .await?;
        assert_eq!(record.status, pgqrs::WorkflowStatus::Error);
        let error_val = record.error.expect("Should have error");
        let error_str = error_val.as_str().expect("Error should be string");
        assert_eq!(error_str, "workflow failed intentionally");
    }

    Ok(())
}
