use crate::config::Config;
use crate::rate_limit::UpstreamRateLimiter;
use crate::upstream::UpstreamStatus;

pub struct TitanWsUpstream {
    enabled: bool,
    dry_run: bool,
    ws_url: Option<String>,
    rate_limit_rps: u32,
    rate_limiter: UpstreamRateLimiter,
}

impl TitanWsUpstream {
    pub fn new(config: &Config) -> Self {
        Self {
            enabled: config.enable_titan_ws,
            dry_run: config.dry_run,
            ws_url: config.titan_ws_url.clone(),
            rate_limit_rps: config.titan_rate_limit_rps,
            rate_limiter: UpstreamRateLimiter::new("titan_ws", config.titan_rate_limit_rps),
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
            name: "titan_ws",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && self.ws_url.is_some(),
            mode: if self.dry_run {
                "stub/dry-run"
            } else {
                "stub/ws"
            },
            rate_limit_rps: Some(self.rate_limit_rps),
        }
    }

    pub async fn poll_stub(&self) {
        if !self.rate_limiter.try_acquire() {
            return;
        }
        tracing::trace!(
            upstream = "titan_ws",
            ws = self.ws_url.is_some(),
            "WebSocket upstream stub poll (rate-limited, no connection in MVP)"
        );
    }
}
