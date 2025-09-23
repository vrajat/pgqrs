use serde_json::json;

mod common;

#[tokio::test]
async fn verify() {
    let admin = common::get_pgqrs_client().await;
    // Verify should succeed
    assert!(admin.verify().is_ok());
}

#[tokio::test]
async fn test_create_queue() {
    let admin = common::get_pgqrs_client().await;
    let queue = admin.create_queue(&"test_create_queue".to_string()).await;
    let queue_list = admin.list_queues().await;
    assert!(queue.is_ok());
    assert!(queue_list.is_ok());
    let queue = queue.unwrap();
    let queue_list = queue_list.unwrap();
    assert!(queue_list.iter().any(|q| q.queue_name == queue.queue_name));

    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_send_message() {
    let admin = common::get_pgqrs_client().await;
    let queue = admin.create_queue(&"test_send_message".to_string()).await;
    assert!(queue.is_ok());
    let queue = queue.unwrap();
    let payload = json!({
        "k": "v"
    });
    assert!(queue.enqueue(&payload).await.is_ok());
    assert!(queue.pending_count().await.unwrap() == 1);
    let read_messages = queue.read(1).await;
    assert!(read_messages.is_ok());
    let read_messages = read_messages.unwrap();
    assert!(read_messages.len() == 1);
    assert!(read_messages[0].message == payload);
    let dequeued_message = queue.dequeue(read_messages[0].msg_id).await;
    assert!(dequeued_message.is_ok());
    let dequeued_message = dequeued_message.unwrap();
    assert!(dequeued_message.msg_id == read_messages[0].msg_id);
    assert!(queue.pending_count().await.unwrap() == 0);
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}
