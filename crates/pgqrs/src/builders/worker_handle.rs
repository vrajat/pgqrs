use crate::store::Store;

/// Create a handle for an existing worker
///
/// This provides a way to interact with any worker (Producer, Consumer, Admin)
/// via the [`crate::Worker`] trait using just its ID.
pub async fn worker_handle<S: Store>(
    store: &S,
    worker_id: i64,
) -> crate::error::Result<Box<dyn crate::Worker>> {
    store.worker(worker_id).await
}
