pub mod chainstack;
pub mod helius;
pub mod titan;
pub mod triton;
pub mod triton_shred;

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
    pub triton_shred: triton_shred::TritonShredUpstream,
    pub titan: titan::TitanWsUpstream,
}

impl UpstreamHub {
    pub fn from_config(config: &Config) -> Self {
        Self {
            chainstack: chainstack::ChainstackUpstream::new(config),
            helius: helius::HeliusUpstream::new(config),
            triton: triton::TritonGrpcUpstream::new(config),
            triton_shred: triton_shred::TritonShredUpstream::new(config),
            titan: titan::TitanWsUpstream::new(config),
        }
    }

    pub fn shred_stats(&self) -> std::sync::Arc<triton_shred::ShredStats> {
        self.triton_shred.stats()
    }

    pub fn statuses(&self) -> Vec<UpstreamStatus> {
        vec![
            self.chainstack.status(),
            self.helius.status(),
            self.triton.status(),
            self.triton_shred.status(),
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
        if self.triton_shred.is_enabled() {
            self.triton_shred.poll_stub().await;
        }
        if self.titan.is_enabled() {
            self.titan.poll_stub().await;
        }
    }

    /// Start live upstream background tasks (only when DRY_RUN=false and configured).
    pub fn spawn_live(
        &self,
        config: &Config,
        fanout: crate::redis_fanout::RedisFanout,
        titan_local_relay: Option<crate::titan_local::TitanLocalRelay>,
        triton_local_relay: Option<crate::triton_local::TritonLocalRelay>,
    ) {
        self.triton
            .spawn_live(fanout.clone(), triton_local_relay);
        if !config.shred_watch_vaults.is_empty() {
            let vaults =
                crate::shred::VaultWatchSet::from_pubkeys(&config.shred_watch_vaults);
            self.triton_shred.spawn_live(
                fanout.clone(),
                vaults,
                config.shred_hit_ttl_secs,
                config.shred_udp_prefix_skip,
            );
        }
        self.titan.spawn_live(fanout, titan_local_relay);
    }
}
