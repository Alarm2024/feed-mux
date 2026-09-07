pub mod chainstack;
pub mod helius;
pub mod titan;
pub mod triton;

use crate::config::Config;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamStatus {
    pub name: &'static str,
    pub enabled: bool,
    pub connected: bool,
    pub mode: &'static str,
    pub rate_limit_rps: Option<u32>,
}

pub struct UpstreamHub {
    pub chainstack: chainstack::ChainstackUpstream,
    pub helius: helius::HeliusUpstream,
    pub triton: triton::TritonGrpcUpstream,
    pub titan: titan::TitanWsUpstream,
}

impl UpstreamHub {
    pub fn from_config(config: &Config) -> Self {
        Self {
            chainstack: chainstack::ChainstackUpstream::new(config),
            helius: helius::HeliusUpstream::new(config),
            triton: triton::TritonGrpcUpstream::new(config),
            titan: titan::TitanWsUpstream::new(config),
        }
    }

    pub fn statuses(&self) -> Vec<UpstreamStatus> {
        vec![
            self.chainstack.status(),
            self.helius.status(),
            self.triton.status(),
            self.titan.status(),
        ]
    }

    /// Poll all enabled upstream stubs (no-op in dry-run for live upstreams).
    pub async fn poll_stubs(&self) {
        if self.chainstack.is_enabled() {
            self.chainstack.poll_stub().await;
        }
        if self.helius.is_enabled() {
            self.helius.poll_stub().await;
        }
        if self.triton.is_enabled() {
            self.triton.poll_stub().await;
        }
        if self.titan.is_enabled() {
            self.titan.poll_stub().await;
        }
    }

    /// Start live upstream background tasks (only when DRY_RUN=false and configured).
    pub fn spawn_live(&self, fanout: crate::redis_fanout::RedisFanout) {
        self.titan.spawn_live(fanout);
    }
}
