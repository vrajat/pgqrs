use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, Notify};

use crate::error::{Error, Result};

/// Tracks local mutation sequence numbers and flush progress.
#[derive(Debug, Default)]
pub struct SyncCoordinator {
    current_seq: AtomicU64,
    last_flushed_seq: AtomicU64,
    failed_seq: AtomicU64,
    notify: Notify,
}

impl SyncCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve the next local mutation sequence.
    pub fn next_sequence(&self) -> u64 {
        self.current_seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn current_sequence(&self) -> u64 {
        self.current_seq.load(Ordering::SeqCst)
    }

    pub fn last_flushed_sequence(&self) -> u64 {
        self.last_flushed_seq.load(Ordering::SeqCst)
    }

    pub fn failed_sequence(&self) -> u64 {
        self.failed_seq.load(Ordering::SeqCst)
    }

    /// Mark all sequences up to `flushed_seq` as durable.
    pub fn mark_flushed(&self, flushed_seq: u64) {
        // Monotonic max update to avoid stale writes rolling progress back.
        let _ = self
            .last_flushed_seq
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                Some(prev.max(flushed_seq))
            });
        self.notify.notify_waiters();
    }

    /// Mark all sequences up to `failed_seq` as failed due to conflict.
    pub fn mark_conflict(&self, failed_seq: u64) {
        let _ = self
            .failed_seq
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                Some(prev.max(failed_seq))
            });
        self.notify.notify_waiters();
    }

    /// Wait until target sequence is durable or timeout is hit.
    pub async fn wait_until_flushed(&self, target_seq: u64, timeout: Duration) -> Result<()> {
        if self.last_flushed_sequence() >= target_seq {
            return Ok(());
        }
        if self.failed_sequence() >= target_seq {
            return Err(Error::Conflict {
                message: format!("flush sequence {} failed due to CAS conflict", target_seq),
            });
        }

        tokio::time::timeout(timeout, async {
            loop {
                if self.last_flushed_sequence() >= target_seq {
                    return;
                }
                if self.failed_sequence() >= target_seq {
                    return;
                }
                self.notify.notified().await;
            }
        })
        .await
        .map_err(|_| Error::Timeout {
            operation: format!("wait_until_flushed(seq={target_seq})"),
        })?;

        if self.failed_sequence() >= target_seq {
            return Err(Error::Conflict {
                message: format!("flush sequence {} failed due to CAS conflict", target_seq),
            });
        }

        Ok(())
    }
}

/// Non-blocking wake sender for sync loop.
#[derive(Debug, Clone)]
pub struct SyncWakeSender {
    tx: mpsc::Sender<()>,
}

impl SyncWakeSender {
    pub fn new(tx: mpsc::Sender<()>) -> Self {
        Self { tx }
    }

    /// Best-effort wake; drops if channel is full/closed.
    pub fn wake(&self) {
        let _ = self.tx.try_send(());
    }
}

/// Build a wake channel used by the sync task.
pub fn wake_channel(buffer: usize) -> (SyncWakeSender, mpsc::Receiver<()>) {
    let (tx, rx) = mpsc::channel(buffer.max(1));
    (SyncWakeSender::new(tx), rx)
}

/// Generic interval-or-wakeup loop used by S3 sync task.
pub async fn run_sync_loop<F, Fut>(
    mut wake_rx: mpsc::Receiver<()>,
    interval: Duration,
    max_backoff: Duration,
    mut flush_once: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                flush_with_backoff(&mut flush_once, max_backoff).await;
            }
            msg = wake_rx.recv() => {
                if msg.is_none() {
                    break;
                }
                flush_with_backoff(&mut flush_once, max_backoff).await;
            }
        }
    }
}

async fn flush_with_backoff<F, Fut>(flush_once: &mut F, max_backoff: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff = Duration::from_millis(50);

    loop {
        match flush_once().await {
            Ok(()) => return,
            Err(err) if is_retryable_sync_error(&err) => {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff.max(backoff));
            }
            Err(_) => return,
        }
    }
}

fn is_retryable_sync_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Timeout { .. }
            | Error::ConnectionFailed { .. }
            | Error::PoolExhausted { .. }
            | Error::Internal { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::{run_sync_loop, wake_channel, SyncCoordinator};
    use crate::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_until_flushed_unblocks_after_mark() {
        let c = Arc::new(SyncCoordinator::new());
        let target = c.next_sequence();
        let c2 = c.clone();

        let waiter = tokio::spawn(async move {
            c2.wait_until_flushed(target, Duration::from_secs(1))
                .await
                .expect("wait should succeed");
        });

        tokio::time::sleep(Duration::from_millis(30)).await;
        c.mark_flushed(target);
        waiter.await.expect("wait task should complete");
    }

    #[tokio::test]
    async fn wait_until_flushed_times_out() {
        let c = SyncCoordinator::new();
        let target = c.next_sequence();
        let err = c
            .wait_until_flushed(target, Duration::from_millis(20))
            .await
            .expect_err("expected timeout");
        assert!(matches!(err, Error::Timeout { .. }));
    }

    #[tokio::test]
    async fn wait_until_flushed_returns_conflict_after_mark_conflict() {
        let c = Arc::new(SyncCoordinator::new());
        let target = c.next_sequence();
        let c2 = c.clone();

        let waiter = tokio::spawn(async move {
            c2.wait_until_flushed(target, Duration::from_secs(1))
                .await
                .expect_err("wait should fail with conflict")
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        c.mark_conflict(target);
        let err = waiter.await.expect("wait task should complete");
        assert!(matches!(err, Error::Conflict { .. }));
    }

    #[tokio::test]
    async fn run_sync_loop_flushes_on_wake_and_interval() {
        let (wake, rx) = wake_channel(4);
        let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let calls_ref = calls.clone();

        let loop_task = tokio::spawn(async move {
            run_sync_loop(
                rx,
                Duration::from_millis(50),
                Duration::from_millis(200),
                || {
                    let calls_ref = calls_ref.clone();
                    async move {
                        calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await;
        });

        // explicit wake
        wake.wake();
        tokio::time::sleep(Duration::from_millis(70)).await;
        // interval should also fire at least once
        tokio::time::sleep(Duration::from_millis(70)).await;

        drop(wake);
        loop_task.await.expect("loop should exit cleanly");

        let n = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert!(n >= 2, "expected >=2 flushes, got {}", n);
    }

    #[tokio::test]
    async fn run_sync_loop_retries_with_backoff_for_timeouts() {
        let (wake, rx) = wake_channel(4);
        let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let calls_ref = calls.clone();

        let loop_task = tokio::spawn(async move {
            run_sync_loop(
                rx,
                Duration::from_secs(60),
                Duration::from_millis(60),
                || {
                    let calls_ref = calls_ref.clone();
                    async move {
                        let n = calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        if n < 3 {
                            return Err(Error::Timeout {
                                operation: "test transient".to_string(),
                            });
                        }
                        Ok(())
                    }
                },
            )
            .await;
        });

        wake.wake();
        tokio::time::sleep(Duration::from_millis(220)).await;

        drop(wake);
        loop_task.await.expect("loop should exit cleanly");

        let n = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert!(n >= 3, "expected retries before success, got {}", n);
    }
}
