use crate::config::Config;
use crate::upstream::UpstreamStatus;

pub struct ChainstackUpstream {
    enabled: bool,
    dry_run: bool,
    rpc_url: Option<String>,
    ws_url: Option<String>,
}

impl ChainstackUpstream {
    pub fn new(config: &Config) -> Self {
        Self {
            enabled: config.enable_chainstack,
            dry_run: config.dry_run,
            rpc_url: config.chainstack_rpc_url.clone(),
            ws_url: config.chainstack_ws_url.clone(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn status(&self) -> UpstreamStatus {
        UpstreamStatus {
            name: "chainstack",
            enabled: self.enabled,
            connected: self.enabled && !self.dry_run && self.rpc_url.is_some(),
            mode: if self.dry_run { "stub/dry-run" } else { "stub/live" },
            rate_limit_rps: None,
        }
    }

    pub async fn poll_stub(&self) {
        tracing::trace!(
            upstream = "chainstack",
            rpc = self.rpc_url.is_some(),
            ws = self.ws_url.is_some(),
            "upstream stub poll (no connection in MVP)"
        );
    }
}
