use pgqrs::store::AnyStore;

mod common;

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_workflow_table_insert_test").await
}

#[tokio::test]
async fn test_workflow_insert_requires_and_persists_queue_id() -> anyhow::Result<()> {
    let store = create_store().await;

    // Arrange: create a backing queue and capture id
    let queue = pgqrs::tables(&store)
        .queues()
        .insert(pgqrs::types::NewQueue {
            queue_name: "wf_insert_queue".to_string(),
        })
        .await?;

    // Act: insert workflow definition via the generic table API
    let inserted = pgqrs::tables(&store)
        .workflows()
        .insert(pgqrs::types::NewWorkflow {
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
