use crate::config::Config;
use crate::upstream::UpstreamStatus;

pub struct HeliusUpstream {
    enabled: bool,
    dry_run: bool,
    rpc_url: Option<String>,
}

impl HeliusUpstream {
    pub fn new(config: &Config) -> Self {
        Self {
            enabled: config.enable_helius,
            dry_run: config.dry_run,
            rpc_url: config.helius_rpc_url.clone(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn status(&self) -> UpstreamStatus {
        UpstreamStatus {
            name: "helius",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && self.rpc_url.is_some(),
            mode: if self.dry_run { "stub/dry-run" } else { "stub/backup" },
            rate_limit_rps: None,
        }
    }

    pub async fn poll_stub(&self) {
        tracing::trace!(
            upstream = "helius",
            rpc = self.rpc_url.is_some(),
            "backup upstream stub poll (no connection in MVP)"
        );
    }
}
