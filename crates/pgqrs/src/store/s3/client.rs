use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// Object payload and associated ETag/revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
}

/// Minimal object-store API required by S3Store sync logic.
#[async_trait]
pub trait ObjectStoreClient: Send + Sync + 'static {
    async fn get_object(&self, key: &str) -> Result<ObjectData>;
    async fn put_object_if_match(
        &self,
        key: &str,
        bytes: &[u8],
        expected_etag: Option<&str>,
    ) -> Result<String>;
}

#[derive(Clone, Debug)]
struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
}

/// Deterministic in-memory object store for tests.
#[derive(Clone, Debug, Default)]
pub struct InMemoryObjectStore {
    objects: Arc<RwLock<HashMap<String, StoredObject>>>,
    revisions: Arc<AtomicU64>,
    force_timeout_once: Arc<AtomicBool>,
    force_conflict_once: Arc<AtomicBool>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inject a transient timeout error for the next write operation.
    pub fn fail_next_put_timeout(&self) {
        self.force_timeout_once.store(true, Ordering::SeqCst);
    }

    /// Inject a conflict for the next write operation.
    pub fn fail_next_put_conflict(&self) {
        self.force_conflict_once.store(true, Ordering::SeqCst);
    }

    fn next_etag(&self) -> String {
        let rev = self.revisions.fetch_add(1, Ordering::SeqCst) + 1;
        format!("v{rev}")
    }
}

#[async_trait]
impl ObjectStoreClient for InMemoryObjectStore {
    async fn get_object(&self, key: &str) -> Result<ObjectData> {
        let guard = self.objects.read().await;
        let obj = guard.get(key).ok_or_else(|| Error::NotFound {
            entity: "object".to_string(),
            id: key.to_string(),
        })?;
        Ok(ObjectData {
            bytes: obj.bytes.clone(),
            etag: Some(obj.etag.clone()),
        })
    }

    async fn put_object_if_match(
        &self,
        key: &str,
        bytes: &[u8],
        expected_etag: Option<&str>,
    ) -> Result<String> {
        if self.force_timeout_once.swap(false, Ordering::SeqCst) {
            return Err(Error::Timeout {
                operation: format!("put_object_if_match({key})"),
            });
        }
        if self.force_conflict_once.swap(false, Ordering::SeqCst) {
            return Err(Error::Conflict {
                message: format!("CAS mismatch for key '{key}'"),
            });
        }

        let mut guard = self.objects.write().await;
        if let Some(existing) = guard.get(key) {
            match expected_etag {
                Some(etag) if etag == existing.etag => {}
                _ => {
                    return Err(Error::Conflict {
                        message: format!(
                            "CAS mismatch for key '{key}'. expected={expected_etag:?}, actual={}",
                            existing.etag
                        ),
                    });
                }
            }
        } else if expected_etag.is_some() {
            return Err(Error::Conflict {
                message: format!("CAS mismatch for key '{key}' (object missing)"),
            });
        }

        let etag = self.next_etag();
        guard.insert(
            key.to_string(),
            StoredObject {
                bytes: bytes.to_vec(),
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryObjectStore, ObjectStoreClient};
    use crate::error::Error;

    #[tokio::test]
    async fn put_get_round_trip() {
        let store = InMemoryObjectStore::new();
        let etag = store
            .put_object_if_match("queue.sqlite", b"hello", None)
            .await
            .expect("put should succeed");
        let obj = store
            .get_object("queue.sqlite")
            .await
            .expect("get should work");
        assert_eq!(obj.bytes, b"hello");
        assert_eq!(obj.etag.as_deref(), Some(etag.as_str()));
    }

    #[tokio::test]
    async fn stale_etag_returns_conflict() {
        let store = InMemoryObjectStore::new();
        let etag = store
            .put_object_if_match("queue.sqlite", b"v1", None)
            .await
            .expect("initial put");
        let _etag2 = store
            .put_object_if_match("queue.sqlite", b"v2", Some(&etag))
            .await
            .expect("second put");

        let err = store
            .put_object_if_match("queue.sqlite", b"v3", Some(&etag))
            .await
            .expect_err("stale etag should conflict");
        assert!(matches!(err, Error::Conflict { .. }));
    }

    #[tokio::test]
    async fn injected_timeout_returns_timeout() {
        let store = InMemoryObjectStore::new();
        store.fail_next_put_timeout();
        let err = store
            .put_object_if_match("queue.sqlite", b"x", None)
            .await
            .expect_err("forced timeout should fail");
        assert!(matches!(err, Error::Timeout { .. }));
    }
}
