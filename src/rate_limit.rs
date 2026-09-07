use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// Per-upstream token-bucket rate limiter stub.
#[derive(Clone)]
pub struct UpstreamRateLimiter {
    inner: Arc<RateLimiter<governor::state::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>>,
    label: &'static str,
}

impl UpstreamRateLimiter {
    pub fn new(label: &'static str, requests_per_second: u32) -> Self {
        let rps = requests_per_second.max(1);
        let quota = Quota::with_period(Duration::from_secs(1))
            .expect("valid quota")
            .allow_burst(NonZeroU32::new(rps).unwrap_or(nonzero!(1u32)));

        Self {
            inner: Arc::new(RateLimiter::direct(quota)),
            label,
        }
    }

    /// Returns true if the request is allowed under the rate limit.
    pub fn try_acquire(&self) -> bool {
        match self.inner.check() {
            Ok(()) => true,
            Err(_) => {
                tracing::debug!(upstream = self.label, "rate limit exceeded (stub)");
                false
            }
        }
    }

    pub fn label(&self) -> &'static str {
        self.label
    }
}
