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

    // Create workflow via builder (new API)
    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };
    let workflow = pgqrs::workflow()
        .name(workflow_name)
        .arg(&input)?
        .create(&store)
        .await?;
    let run_id = workflow.id();
    assert!(run_id > 0, "run_id should be positive");

    // Verify workflow run record exists and matches name
    let record = pgqrs::tables(&store).workflow_runs().get(run_id).await?;
    assert_eq!(record.id, run_id);
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}

#[tokio::test]
async fn test_create_workflow_is_idempotent() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "test_idempotent_workflow";

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    // Workflows are not name-idempotent in the new API: each create() makes a new record.
    // So we validate both exist and have the expected name.
    let wf1 = pgqrs::workflow()
        .name(workflow_name)
        .arg(&input)?
        .create(&store)
        .await?;
    let wf2 = pgqrs::workflow()
        .name(workflow_name)
        .arg(&input)?
        .create(&store)
        .await?;

    assert_ne!(wf1.id(), wf2.id(), "workflow ids should be unique");

    let rec1 = pgqrs::tables(&store).workflow_runs().get(wf1.id()).await?;
    let rec2 = pgqrs::tables(&store).workflow_runs().get(wf2.id()).await?;

    assert_eq!(rec1.workflow_name, workflow_name);
    assert_eq!(rec2.workflow_name, workflow_name);

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

    let wf1 = pgqrs::workflow()
        .name("test_wf_1")
        .arg(&input)?
        .create(&store)
        .await?;
    let wf2 = pgqrs::workflow()
        .name("test_wf_2")
        .arg(&input)?
        .create(&store)
        .await?;
    let wf3 = pgqrs::workflow()
        .name("test_wf_3")
        .arg(&input)?
        .create(&store)
        .await?;

    assert_ne!(wf1.id(), wf2.id());
    assert_ne!(wf2.id(), wf3.id());
    assert_ne!(wf1.id(), wf3.id());

    let rec1 = pgqrs::tables(&store).workflow_runs().get(wf1.id()).await?;
    let rec2 = pgqrs::tables(&store).workflow_runs().get(wf2.id()).await?;
    let rec3 = pgqrs::tables(&store).workflow_runs().get(wf3.id()).await?;

    assert_eq!(rec1.workflow_name, "test_wf_1");
    assert_eq!(rec2.workflow_name, "test_wf_2");
    assert_eq!(rec3.workflow_name, "test_wf_3");

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_unicode_name() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "テストワークフロー_🎉";

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let workflow = pgqrs::workflow()
        .name(workflow_name)
        .arg(&input)?
        .create(&store)
        .await?;

    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(workflow.id())
        .await?;
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_long_name() -> anyhow::Result<()> {
    let store = create_store().await;
    // Create a name close to the 255 char limit
    let workflow_name = "a".repeat(250);

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let workflow = pgqrs::workflow()
        .name(&workflow_name)
        .arg(&input)?
        .create(&store)
        .await?;

    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(workflow.id())
        .await?;
    assert_eq!(record.workflow_name, workflow_name);

    Ok(())
}
