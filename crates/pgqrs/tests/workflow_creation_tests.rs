use pgqrs::pgqrs_workflow;
use pgqrs::store::AnyStore;
use pgqrs::Run;
use serde::{Deserialize, Serialize};

mod common;

#[pgqrs_workflow(name = "test_duplicate_workflow_create")]
async fn test_duplicate_workflow_create_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "test_wf_1")]
async fn test_wf_1_wf(_run: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "test_wf_2")]
async fn test_wf_2_wf(_run: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "test_wf_3")]
async fn test_wf_3_wf(_run: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "テストワークフロー_🎉")]
async fn unicode_wf(_run: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestParams {
    message: String,
    count: i32,
}

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_workflow_creation_test").await
}

#[tokio::test]
async fn test_create_workflow_is_not_idempotent() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "test_duplicate_workflow_create";

    // Definition creation is strict by name
    pgqrs::workflow()
        .name(test_duplicate_workflow_create_wf)
        .create(&store)
        .await?;

    let err = pgqrs::workflow()
        .name(test_duplicate_workflow_create_wf)
        .create(&store)
        .await;
    assert!(err.is_err(), "Expected duplicate workflow create to fail");

    match err {
        Err(pgqrs::Error::WorkflowAlreadyExists { name }) => {
            assert_eq!(name, workflow_name);
        }
        _ => panic!("Expected WorkflowAlreadyExists"),
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

    let wf_1 = pgqrs::workflow().name(test_wf_1_wf).create(&store).await?;
    let wf_2 = pgqrs::workflow().name(test_wf_2_wf).create(&store).await?;
    let wf_3 = pgqrs::workflow().name(test_wf_3_wf).create(&store).await?;

    let run_1_msg = pgqrs::workflow()
        .name(test_wf_1_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;
    let run_2_msg = pgqrs::workflow()
        .name(test_wf_2_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;
    let run_3_msg = pgqrs::workflow()
        .name(test_wf_3_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    assert_ne!(run_1_msg.id, run_2_msg.id);
    assert_ne!(run_2_msg.id, run_3_msg.id);
    assert_ne!(run_1_msg.id, run_3_msg.id);

    // Create handles to force RunRecord creation
    let run_1 = pgqrs::run()
        .message(run_1_msg)
        .store(&store)
        .execute()
        .await?;
    let run_2 = pgqrs::run()
        .message(run_2_msg)
        .store(&store)
        .execute()
        .await?;
    let run_3 = pgqrs::run()
        .message(run_3_msg)
        .store(&store)
        .execute()
        .await?;

    let rec1 = pgqrs::tables(&store)
        .workflow_runs()
        .get(run_1.id())
        .await?;
    let rec2 = pgqrs::tables(&store)
        .workflow_runs()
        .get(run_2.id())
        .await?;
    let rec3 = pgqrs::tables(&store)
        .workflow_runs()
        .get(run_3.id())
        .await?;

    assert_eq!(rec1.workflow_id, wf_1.id);
    assert_eq!(rec2.workflow_id, wf_2.id);
    assert_eq!(rec3.workflow_id, wf_3.id);

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_unicode_name() -> anyhow::Result<()> {
    let store = create_store().await;

    let wf = pgqrs::workflow().name(unicode_wf).create(&store).await?;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_msg = pgqrs::workflow()
        .name(unicode_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;
    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(record.workflow_id, wf.id);

    Ok(())
}
