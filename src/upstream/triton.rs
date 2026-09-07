use crate::config::Config;
use crate::rate_limit::UpstreamRateLimiter;
use crate::upstream::UpstreamStatus;

pub struct TritonGrpcUpstream {
    enabled: bool,
    dry_run: bool,
    grpc_url: Option<String>,
    rate_limit_rps: u32,
    rate_limiter: UpstreamRateLimiter,
}

impl TritonGrpcUpstream {
    pub fn new(config: &Config) -> Self {
        Self {
            enabled: config.enable_triton_grpc,
            dry_run: config.dry_run,
            grpc_url: config.triton_grpc_url.clone(),
            rate_limit_rps: config.triton_rate_limit_rps,
            rate_limiter: UpstreamRateLimiter::new("triton_grpc", config.triton_rate_limit_rps),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn rate_limiter(&self) -> &UpstreamRateLimiter {
        &self.rate_limiter
    }

    pub fn status(&self) -> UpstreamStatus {
        UpstreamStatus {
            name: "triton_grpc",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && self.grpc_url.is_some(),
            mode: if self.dry_run {
                "stub/dry-run"
            } else {
                "stub/grpc"
            },
            rate_limit_rps: Some(self.rate_limit_rps),
        }
    }

    pub async fn poll_stub(&self) {
        if !self.rate_limiter.try_acquire() {
            return;
        }
        tracing::trace!(
            upstream = "triton_grpc",
            grpc = self.grpc_url.is_some(),
            "gRPC upstream stub poll (rate-limited, no connection in MVP)"
        );
    }
}
