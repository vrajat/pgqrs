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
async fn test_create_workflow_is_not_idempotent() -> anyhow::Result<()> {
    let store = create_store().await;
    let workflow_name = "test_duplicate_workflow_create";

    // Definition creation is strict by name
    pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let err = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .create()
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

    let wf_1 = pgqrs::workflow()
        .name("test_wf_1")
        .store(&store)
        .create()
        .await?;
    let wf_2 = pgqrs::workflow()
        .name("test_wf_2")
        .store(&store)
        .create()
        .await?;
    let wf_3 = pgqrs::workflow()
        .name("test_wf_3")
        .store(&store)
        .create()
        .await?;

    let run_1_msg = pgqrs::workflow()
        .name("test_wf_1")
        .store(&store)
        .trigger(&input)?
        .execute()
        .await?;
    let run_2_msg = pgqrs::workflow()
        .name("test_wf_2")
        .store(&store)
        .trigger(&input)?
        .execute()
        .await?;
    let run_3_msg = pgqrs::workflow()
        .name("test_wf_3")
        .store(&store)
        .trigger(&input)?
        .execute()
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
    let workflow_name = "テストワークフロー_🎉";

    let wf = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_msg = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&input)?
        .execute()
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

#[tokio::test]
async fn test_workflow_insert_requires_and_persists_queue_id() -> anyhow::Result<()> {
    let store = create_store().await;

    // Arrange: create a backing queue and capture id
    let queue = pgqrs::tables(&store)
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "wf_insert_queue".to_string(),
        })
        .await?;

    // Act: insert workflow definition via the generic table API
    let inserted = pgqrs::tables(&store)
        .workflows()
        .insert(pgqrs::types::NewWorkflowRecord {
            name: "wf_insert_workflow".to_string(),
            queue_id: queue.id,
        })
        .await?;

    // Assert: queue_id round-trips
    assert_eq!(inserted.name, "wf_insert_workflow");
    assert_eq!(inserted.queue_id, queue.id);

    let fetched = pgqrs::tables(&store).workflows().get(inserted.id).await?;
    assert_eq!(fetched.name, inserted.name);
    assert_eq!(fetched.queue_id, queue.id);

    Ok(())
}

#[tokio::test]
async fn test_workflow_with_long_name() -> anyhow::Result<()> {
    let store = create_store().await;
    // Create a name close to the 255 char limit
    let workflow_name = "a".repeat(250);

    let wf = pgqrs::workflow()
        .name(&workflow_name)
        .store(&store)
        .create()
        .await?;

    let input = TestParams {
        message: "hello".to_string(),
        count: 1,
    };

    let run_msg = pgqrs::workflow()
        .name(&workflow_name)
        .store(&store)
        .trigger(&input)?
        .execute()
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
