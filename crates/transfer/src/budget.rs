//! A byte budget for objects held in memory, shared by every task on one side of a transfer.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Budget granularity.
const UNIT: u64 = 64 * 1024;

#[derive(Clone)]
pub struct MemoryBudget {
    sem: Arc<Semaphore>,
    units: u32,
}

impl MemoryBudget {
    pub fn new(bytes: u64) -> Self {
        let units = bytes.div_ceil(UNIT).clamp(1, u32::MAX as u64) as u32;
        Self {
            sem: Arc::new(Semaphore::new(units as usize)),
            units,
        }
    }

    /// Waits until `len` bytes fit in the budget. An object larger than the whole budget waits
    /// for the entire budget, so at most one such object is in memory at a time.
    pub async fn reserve(&self, len: u64) -> OwnedSemaphorePermit {
        let want = len.div_ceil(UNIT).clamp(1, self.units as u64) as u32;
        self.sem
            .clone()
            .acquire_many_owned(want)
            .await
            .expect("budget semaphore is never closed")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn bounds_concurrent_reservations() {
        let b = MemoryBudget::new(10 * UNIT);
        let first = b.reserve(6 * UNIT).await;
        // 6 + 6 > 10: the second must wait for the first.
        let blocked = tokio::time::timeout(Duration::from_millis(30), b.reserve(6 * UNIT)).await;
        assert!(blocked.is_err());
        drop(first);
        let _second = b.reserve(6 * UNIT).await;
        // Oversized objects are clamped to the whole budget rather than deadlocking.
        let small = MemoryBudget::new(UNIT);
        let _huge = small.reserve(100 * UNIT).await;
    }
}
