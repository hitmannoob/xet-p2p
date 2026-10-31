//! A token-bucket rate limiter for outgoing payload bytes.

use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

/// Largest burst, and the granularity payload writes are split into.
pub const SLICE: usize = 256 * 1024;

pub struct RateLimiter {
    bytes_per_sec: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: f64) -> Self {
        assert!(bytes_per_sec > 0.0);
        Self {
            bytes_per_sec,
            state: Mutex::new((SLICE as f64, Instant::now())),
        }
    }

    /// Waits until `n` (≤ [`SLICE`]) bytes may be sent. Shared by all streams of a push, so the
    /// limit applies to their sum.
    pub async fn take(&self, n: usize) {
        let n = n as f64;
        loop {
            let wait = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                let refill = now.duration_since(st.1).as_secs_f64() * self.bytes_per_sec;
                st.0 = (st.0 + refill).min(SLICE as f64);
                st.1 = now;
                if st.0 >= n {
                    st.0 -= n;
                    return;
                }
                (n - st.0) / self.bytes_per_sec
            };
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_throughput() {
        let r = RateLimiter::new(4.0 * SLICE as f64); // 1 MiB/s
        let start = Instant::now();
        for _ in 0..8 {
            r.take(SLICE).await; // 2 MiB total; first slice is the burst
        }
        let secs = start.elapsed().as_secs_f64();
        assert!((1.6..2.6).contains(&secs), "{secs}");
    }
}
