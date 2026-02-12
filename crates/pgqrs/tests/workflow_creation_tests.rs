use pgqrs::store::AnyStore;
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestParams {
    message: String,
    count: i32,
}

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_workflow_creation_test").await
}

#[tokio::test]
async fn test_create_workflow_creates_workflow_and_queue() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "test_create_workflow_basic";

    // Create workflow definition via builder (new API)
    pgqrs::workflow().name(workflow_name).create(&store).await?;

    // Trigger a run
    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_id = pgqrs::workflow()
        .name(workflow_name)
        .trigger(&input)?
        .execute(&store)
        .await?;
    assert!(run_id > 0, "run_id should be positive");

    // Verify workflow run record exists and matches name
    let record = pgqrs::tables(&store).workflow_runs().get(run_id).await?;
    assert_eq!(record.id, run_id);
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}

#[tokio::test]
async fn test_create_workflow_is_not_idempotent() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "test_duplicate_workflow_create";

    // Definition creation is strict by name
    pgqrs::workflow().name(workflow_name).create(&store).await?;

    let err = pgqrs::workflow().name(workflow_name).create(&store).await;
    assert!(err.is_err(), "Expected duplicate workflow create to fail");

    match err.unwrap_err() {
        pgqrs::Error::WorkflowAlreadyExists { name } => {
            assert_eq!(name, workflow_name);
        }
        other => panic!("Expected WorkflowAlreadyExists, got: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_workflow_get_missing_returns_error() -> anyhow::Result<()> {
    let store = create_store().await;

    // With the new API we address workflows by ID.
    // Pick a very large ID and assert it is not found.
    let missing_id = i64::MAX;
    let result = pgqrs::tables(&store).workflow_runs().get(missing_id).await;
    assert!(result.is_err(), "expected missing workflow to be an error");

    Ok(())
}

#[tokio::test]
async fn test_create_multiple_workflows() -> anyhow::Result<()> {
    let store = create_store().await;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    pgqrs::workflow().name("test_wf_1").create(&store).await?;
    pgqrs::workflow().name("test_wf_2").create(&store).await?;
    pgqrs::workflow().name("test_wf_3").create(&store).await?;

    let run_1 = pgqrs::workflow()
        .name("test_wf_1")
        .trigger(&input)?
        .execute(&store)
        .await?;
    let run_2 = pgqrs::workflow()
        .name("test_wf_2")
        .trigger(&input)?
        .execute(&store)
        .await?;
    let run_3 = pgqrs::workflow()
        .name("test_wf_3")
        .trigger(&input)?
        .execute(&store)
        .await?;

    assert_ne!(run_1, run_2);
    assert_ne!(run_2, run_3);
    assert_ne!(run_1, run_3);

    let rec1 = pgqrs::tables(&store).workflow_runs().get(run_1).await?;
    let rec2 = pgqrs::tables(&store).workflow_runs().get(run_2).await?;
    let rec3 = pgqrs::tables(&store).workflow_runs().get(run_3).await?;

    assert_eq!(rec1.workflow_name, "test_wf_1");
    assert_eq!(rec2.workflow_name, "test_wf_2");
    assert_eq!(rec3.workflow_name, "test_wf_3");

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_unicode_name() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "テストワークフロー_🎉";

    pgqrs::workflow().name(workflow_name).create(&store).await?;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_id = pgqrs::workflow()
        .name(workflow_name)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run_id).await?;
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_long_name() -> anyhow::Result<()> {
    let store = create_store().await;
    // Create a name close to the 255 char limit
    let workflow_name = "a".repeat(250);

    pgqrs::workflow()
        .name(&workflow_name)
        .create(&store)
        .await?;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_id = pgqrs::workflow()
        .name(&workflow_name)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run_id).await?;
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}
