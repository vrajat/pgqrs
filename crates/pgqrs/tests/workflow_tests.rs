use pgqrs::workflow::{StepGuard, StepResult};
use pgqrs::{Admin, Config, Workflow};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod common;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

#[tokio::test]
async fn test_workflow_lifecycle() -> anyhow::Result<()> {
    let schema = "workflow_test";
    let dsn = common::get_postgres_dsn(Some(schema)).await;
    let config = Config::from_dsn_with_schema(&dsn, schema)?;

    let admin = Admin::new(&config).await?;
    admin.install().await?;

    let pool = admin.pool.clone();
    let workflow_id = Uuid::new_v4();
    let workflow = Workflow::new(pool.clone(), workflow_id);

    // Start workflow
    let input = TestData {
        msg: "start".to_string(),
    };
    workflow.start("test_wf", &input).await?;

    // Step 1: Run
    let step1_id = "step1";
    let step_res = StepGuard::acquire::<TestData>(&pool, workflow_id, step1_id).await?;

    match step_res {
        StepResult::Execute(guard) => {
            let output = TestData {
                msg: "step1_done".to_string(),
            };
            guard.success(output).await?;
        }
        StepResult::Skipped(_) => panic!("Step 1 should execute first time"),
    }

    // Step 1: Rerun (should skip)
    let step_res = StepGuard::acquire::<TestData>(&pool, workflow_id, step1_id).await?;
    match step_res {
        StepResult::Skipped(val) => {
            assert_eq!(val.msg, "step1_done");
        }
        StepResult::Execute(_) => panic!("Step 1 should skip on rerun"),
    }

    // Step 2: Drop (Panic simulation)
    let step2_id = "step2";
    let step_res = StepGuard::acquire::<TestData>(&pool, workflow_id, step2_id).await?;
    match step_res {
        StepResult::Execute(guard) => {
            // Explicitly drop without calling success/fail
            drop(guard);
        }
        StepResult::Skipped(_) => panic!("Step 2 should execute"),
    }

    // Allow async drop to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Step 2: Rerun (should be ERROR state because of drop)
    let step_res = StepGuard::acquire::<TestData>(&pool, workflow_id, step2_id).await;
    assert!(
        step_res.is_err(),
        "Step 2 should be in terminal ERROR state after drop"
    );

    // Finish Workflow
    workflow
        .success(TestData {
            msg: "done".to_string(),
        })
        .await?;

    // Restart Workflow (should adhere to SUCCESS terminal state)
    // Currently start() on SUCCESS behaves like update (idempotent success or keeps success).
    // Our refactor returns Ok(()) but updates nothing if already SUCCESS.
    workflow.start("test_wf", &input).await?;

    // Verify Workflow Failure Logic
    let wf_fail_id = Uuid::new_v4();
    let wf_fail = Workflow::new(pool.clone(), wf_fail_id);
    wf_fail.start("fail_wf", &input).await?;
    wf_fail
        .fail(TestData {
            msg: "failed".to_string(),
        })
        .await?;

    // Restart Failed Workflow (should fail because ERROR is terminal)
    let res = wf_fail.start("fail_wf", &input).await;
    assert!(
        res.is_err(),
        "Workflow start should fail if currently ERROR"
    );

    Ok(())
}
