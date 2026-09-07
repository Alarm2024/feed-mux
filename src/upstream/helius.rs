use crate::config::Config;
use crate::upstream::UpstreamStatus;

pub struct HeliusUpstream {
    enabled: bool,
    dry_run: bool,
    rpc_url: Option<String>,
}

impl HeliusUpstream {
    pub fn new(config: &Config) -> Self {
        if config.enable_helius && !config.dry_run && config.helius_rpc_url.is_none() {
            tracing::error!(
                upstream = "helius",
                "Helius backup enabled but HELIUS_RPC_URL is not set; refusing to connect"
            );
        }

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
        let has_url = self.rpc_url.is_some();
        let mode = if self.dry_run {
            "stub/dry-run"
        } else if !has_url {
            "error/missing-rpc-url"
        } else {
            "live/backup"
        };

        UpstreamStatus {
            name: "helius",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && has_url,
            mode,
            rate_limit_rps: None,
        }
    }

    pub async fn poll_stub(&self) {
        if !self.enabled {
            return;
        }

        if self.dry_run {
            tracing::trace!(
                upstream = "helius",
                rpc = self.rpc_url.is_some(),
                "Helius backup stub poll (DRY_RUN=true, no connection)"
            );
            return;
        }

        if self.rpc_url.is_none() {
            return;
        }

        tracing::trace!(
            upstream = "helius",
            "Helius backup live poll (RPC client not yet implemented)"
        );
    }
}
