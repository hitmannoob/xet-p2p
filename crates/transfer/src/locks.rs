//! Destination locking: transfers into overlapping trees run one at a time; disjoint ones run
//! concurrently.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::error::{Error, Result};

#[derive(Default)]
pub struct DestLocks {
    active: Mutex<Vec<PathBuf>>,
    released: Notify,
}

pub struct DestGuard {
    locks: Arc<DestLocks>,
    path: PathBuf,
}

impl DestLocks {
    /// Waits up to `timeout` until no active transfer's destination contains, or is contained by,
    /// `path` (a root-relative path), then claims it.
    pub async fn acquire(self: &Arc<Self>, path: &Path, timeout: Duration) -> Result<DestGuard> {
        let wait = async {
            loop {
                // Register for wake-up before checking, so a release in between is not missed.
                let released = self.released.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                {
                    let mut active = self.active.lock().unwrap();
                    if !active
                        .iter()
                        .any(|a| a.starts_with(path) || path.starts_with(a))
                    {
                        active.push(path.to_owned());
                        return DestGuard {
                            locks: self.clone(),
                            path: path.to_owned(),
                        };
                    }
                }
                released.await;
            }
        };
        tokio::time::timeout(timeout, wait).await.map_err(|_| {
            Error::Protocol(format!(
                "destination {} busy: another transfer into an overlapping path is still running",
                path.display()
            ))
        })
    }
}

impl Drop for DestGuard {
    fn drop(&mut self) {
        let mut active = self.locks.active.lock().unwrap();
        if let Some(i) = active.iter().position(|p| *p == self.path) {
            active.swap_remove(i);
        }
        drop(active);
        self.locks.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn overlapping_waits_disjoint_proceeds() {
        let locks = Arc::new(DestLocks::default());
        let t = Duration::from_millis(50);
        let a = locks.acquire(Path::new("p"), t).await.unwrap();
        let _b = locks.acquire(Path::new("q"), t).await.unwrap();
        let _c = locks.acquire(Path::new("pq"), t).await.unwrap(); // not a path prefix
        assert!(locks.acquire(Path::new("p/sub"), t).await.is_err());
        assert!(locks.acquire(Path::new("p"), t).await.is_err());

        let waiter = tokio::spawn({
            let locks = locks.clone();
            async move {
                locks
                    .acquire(Path::new("p/sub"), Duration::from_secs(5))
                    .await
                    .is_ok()
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(a);
        assert!(waiter.await.unwrap());
    }
}
