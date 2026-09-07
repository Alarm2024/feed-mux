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
        if config.enable_triton_grpc && !config.dry_run && config.triton_grpc_url.is_none() {
            tracing::error!(
                upstream = "triton_grpc",
                "Triton gRPC enabled but TRITON_GRPC_URL is not set; refusing to connect"
            );
        }

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
        let has_url = self.grpc_url.is_some();
        let mode = if self.dry_run {
            "stub/dry-run"
        } else if !has_url {
            "error/missing-grpc-url"
        } else {
            "live/grpc"
        };

        UpstreamStatus {
            name: "triton_grpc",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && has_url,
            mode,
            rate_limit_rps: Some(self.rate_limit_rps),
        }
    }

    pub async fn poll_stub(&self) {
        if !self.enabled {
            return;
        }

        if self.dry_run {
            if !self.rate_limiter.try_acquire() {
                return;
            }
            tracing::trace!(
                upstream = "triton_grpc",
                grpc = self.grpc_url.is_some(),
                "Triton gRPC stub poll (DRY_RUN=true, no connection)"
            );
            return;
        }

        if self.grpc_url.is_none() {
            return;
        }

        if !self.rate_limiter.try_acquire() {
            return;
        }

        tracing::trace!(
            upstream = "triton_grpc",
            "Triton gRPC live poll (gRPC client not yet implemented)"
        );
    }
}
