use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;

use crate::config::Config;
use crate::redis_fanout::{FeedPayload, RedisFanout};
use crate::shred::{
    parse_data_shred, ShredReassembler, VaultWatchSet, MAX_DATAGRAM_SIZE,
};
use crate::upstream::UpstreamStatus;

const KEEP_SHRED_PORT: u16 = 8002;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShredLiveState {
    Disabled,
    DryRunStub,
    MissingVaults,
    KeepPortConflict,
    InvalidBind,
    Ready,
}

#[derive(Debug, Default)]
pub struct ShredStatsSnapshot {
    pub shreds: u64,
    pub txs_deshredded: u64,
    pub vault_hits: u64,
    pub last_ms: u64,
    pub last_hit_ms: u64,
    pub last_age_ms: u64,
    pub bound: bool,
}

#[derive(Debug)]
pub struct ShredStats {
    shreds: AtomicU64,
    txs_deshredded: AtomicU64,
    vault_hits: AtomicU64,
    last_ms: AtomicU64,
    last_hit_ms: AtomicU64,
    bound: AtomicBool,
}

impl ShredStats {
    pub fn new() -> Self {
        Self {
            shreds: AtomicU64::new(0),
            txs_deshredded: AtomicU64::new(0),
            vault_hits: AtomicU64::new(0),
            last_ms: AtomicU64::new(0),
            last_hit_ms: AtomicU64::new(0),
            bound: AtomicBool::new(false),
        }
    }

    pub fn record_shred(&self, ts: u64) {
        self.shreds.fetch_add(1, Ordering::Relaxed);
        self.last_ms.store(ts, Ordering::Relaxed);
    }

    pub fn record_deshred(&self, tx_count: u64) {
        self.txs_deshredded
            .fetch_add(tx_count, Ordering::Relaxed);
    }

    pub fn record_vault_hit(&self, ts: u64) {
        self.vault_hits.fetch_add(1, Ordering::Relaxed);
        self.last_hit_ms.store(ts, Ordering::Relaxed);
    }

    pub fn set_bound(&self, bound: bool) {
        self.bound.store(bound, Ordering::Relaxed);
    }

    pub fn snapshot(&self, now_ms: u64) -> ShredStatsSnapshot {
        let last_ms = self.last_ms.load(Ordering::Relaxed);
        ShredStatsSnapshot {
            shreds: self.shreds.load(Ordering::Relaxed),
            txs_deshredded: self.txs_deshredded.load(Ordering::Relaxed),
            vault_hits: self.vault_hits.load(Ordering::Relaxed),
            last_ms,
            last_hit_ms: self.last_hit_ms.load(Ordering::Relaxed),
            last_age_ms: if last_ms == 0 {
                0
            } else {
                now_ms.saturating_sub(last_ms)
            },
            bound: self.bound.load(Ordering::Relaxed),
        }
    }
}

pub struct TritonShredUpstream {
    enabled: bool,
    dry_run: bool,
    bind_addr: String,
    live_state: ShredLiveState,
    stats: Arc<ShredStats>,
}

impl TritonShredUpstream {
    pub fn new(config: &Config) -> Self {
        let live_state = Self::resolve_live_state(config);

        if config.enable_triton_shred && !config.dry_run {
            match live_state {
                ShredLiveState::MissingVaults => {
                    tracing::error!(
                        upstream = "triton_shred_udp",
                        "Triton UDP shreds enabled but SHRED_WATCH_VAULTS is empty; refusing to bind"
                    );
                }
                ShredLiveState::KeepPortConflict => {
                    tracing::error!(
                        upstream = "triton_shred_udp",
                        bind = %config.shred_bind,
                        "SHRED_BIND must not use port 8002 (KEEP arb-bot owns that socket forever)"
                    );
                }
                ShredLiveState::InvalidBind => {
                    tracing::error!(
                        upstream = "triton_shred_udp",
                        bind = %config.shred_bind,
                        "invalid SHRED_BIND address"
                    );
                }
                ShredLiveState::Ready => {
                    tracing::info!(
                        upstream = "triton_shred_udp",
                        bind = %config.shred_bind,
                        vaults = config.shred_watch_vaults.len(),
                        "Triton UDP shred listener configured (Bot 350 dry eyes — separate bind from KEEP :8002)"
                    );
                }
                _ => {}
            }
        }

        Self {
            enabled: config.enable_triton_shred,
            dry_run: config.dry_run,
            bind_addr: config.shred_bind.clone(),
            live_state,
            stats: Arc::new(ShredStats::new()),
        }
    }

    fn resolve_live_state(config: &Config) -> ShredLiveState {
        if !config.enable_triton_shred {
            return ShredLiveState::Disabled;
        }
        if config.dry_run {
            return ShredLiveState::DryRunStub;
        }
        if config.shred_watch_vaults.is_empty() {
            return ShredLiveState::MissingVaults;
        }
        if shred_bind_conflicts_with_keep(&config.shred_bind) {
            return ShredLiveState::KeepPortConflict;
        }
        if config.shred_bind.parse::<SocketAddr>().is_err() {
            return ShredLiveState::InvalidBind;
        }
        ShredLiveState::Ready
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn stats(&self) -> Arc<ShredStats> {
        Arc::clone(&self.stats)
    }

    pub fn status(&self) -> UpstreamStatus {
        let (connected, mode) = match self.live_state {
            ShredLiveState::Disabled => (false, "disabled"),
            ShredLiveState::DryRunStub => (false, "stub/dry-run"),
            ShredLiveState::MissingVaults => (false, "error/missing-vaults"),
            ShredLiveState::KeepPortConflict => (false, "error/keep-port-8002-conflict"),
            ShredLiveState::InvalidBind => (false, "error/invalid-bind"),
            ShredLiveState::Ready => (
                self.stats.bound.load(Ordering::Relaxed),
                if self.stats.bound.load(Ordering::Relaxed) {
                    "live/udp"
                } else {
                    "live/binding"
                },
            ),
        };

        UpstreamStatus {
            name: "triton_shred_udp",
            enabled: self.enabled,
            connected,
            mode,
            rate_limit_rps: None,
        }
    }

    pub async fn poll_stub(&self) {
        if !self.enabled || !self.dry_run {
            return;
        }
        tracing::trace!(
            upstream = "triton_shred_udp",
            bind = %self.bind_addr,
            "Triton UDP shred stub poll (DRY_RUN=true, no bind)"
        );
    }

    pub fn spawn_live(
        &self,
        fanout: RedisFanout,
        vaults: VaultWatchSet,
        hit_ttl_secs: u64,
        udp_prefix_skip: usize,
    ) {
        if self.live_state != ShredLiveState::Ready {
            return;
        }

        let bind_addr = self.bind_addr.clone();
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            run_udp_loop(
                bind_addr,
                fanout,
                vaults,
                stats,
                hit_ttl_secs,
                udp_prefix_skip,
            )
            .await;
        });
    }
}

fn shred_bind_conflicts_with_keep(bind: &str) -> bool {
    bind.parse::<SocketAddr>()
        .ok()
        .is_some_and(|addr| addr.port() == KEEP_SHRED_PORT)
}

async fn run_udp_loop(
    bind_addr: String,
    fanout: RedisFanout,
    vaults: VaultWatchSet,
    stats: Arc<ShredStats>,
    hit_ttl_secs: u64,
    udp_prefix_skip: usize,
) {
    let socket = match UdpSocket::bind(&bind_addr).await {
        Ok(sock) => sock,
        Err(e) => {
            tracing::error!(
                upstream = "triton_shred_udp",
                bind = %bind_addr,
                error = %e,
                "failed to bind UDP shred socket (KEEP stays on :8002 — mux needs its own SHRED_BIND)"
            );
            fanout.set_shred_upstream_up(false).await;
            stats.set_bound(false);
            return;
        }
    };

    stats.set_bound(true);
    fanout.reset_shred_state_at_boot().await;
    fanout.set_shred_upstream_up(true).await;
    tracing::info!(
        upstream = "triton_shred_udp",
        bind = %bind_addr,
        vaults = vaults.len(),
        "Triton UDP shred listener bound (duplicate Triton destination — not KEEP :8002)"
    );

    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
    let mut reassembler = ShredReassembler::new(256);

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, src)) => {
                let start = udp_prefix_skip.min(size);
                let packet = &buf[start..size];
                let ts = crate::redis_fanout::now_ms();
                stats.record_shred(ts);
                fanout.record_shred_frame(ts).await;

                let Some(shred) = parse_data_shred(packet) else {
                    continue;
                };

                let Some(batch) = reassembler.push(shred) else {
                    continue;
                };

                let tx_count = vaults.count_transactions_estimate(&batch.bytes);
                stats.record_deshred(tx_count);
                fanout.record_shred_deshred(tx_count, ts).await;

                let hits = vaults.scan_hits(&batch.bytes);
                for hit in hits {
                    stats.record_vault_hit(ts);
                    fanout
                        .publish_shred_hit(
                            &hit,
                            batch.slot,
                            batch.fec_set_index,
                            &src.to_string(),
                            hit_ttl_secs,
                            ts,
                        )
                        .await;

                    let wake = FeedPayload {
                        event: "shred.vault_hit".to_string(),
                        source: "triton_shred_udp".to_string(),
                        ts: chrono::Utc::now().to_rfc3339(),
                        data: Some(serde_json::json!({
                            "schema": "mux.shred.wake.v1",
                            "vault": hit.vault_b58,
                            "slot": batch.slot,
                            "fec_set_index": batch.fec_set_index,
                            "src": src.to_string(),
                        })),
                    };
                    if let Err(e) = fanout.publish(&wake).await {
                        tracing::warn!(
                            upstream = "triton_shred_udp",
                            error = %e,
                            vault = %hit.vault_b58,
                            "failed to fan-out shred wake"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    upstream = "triton_shred_udp",
                    error = %e,
                    "UDP shred recv error"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_keep_port_8002() {
        assert!(shred_bind_conflicts_with_keep("0.0.0.0:8002"));
        assert!(!shred_bind_conflicts_with_keep("0.0.0.0:8003"));
    }

    #[test]
    fn missing_vaults_is_error_state() {
        let config = Config {
            bind_addr: "127.0.0.1:8787".to_string(),
            dry_run: false,
            redis_url: None,
            redis_channel: "feed:350".to_string(),
            enable_chainstack: false,
            chainstack_rpc_url: None,
            chainstack_ws_url: None,
            enable_helius: false,
            helius_rpc_url: None,
            enable_triton_grpc: false,
            triton_grpc_url: None,
            triton_grpc_token: None,
            triton_rate_limit_rps: 25,
            triton_local_bind: "127.0.0.1:19000".to_string(),
            enable_titan_ws: false,
            titan_ws_url: None,
            titan_wallet_pubkey: None,
            titan_rate_limit_rps: 15,
            titan_local_bind: "127.0.0.1:19001".to_string(),
            titan_hunt_size_lamports: None,
            titan_hop1_ttl_secs: 2,
            mock_publish_interval_secs: 0,
            enable_triton_shred: true,
            shred_bind: "0.0.0.0:8003".to_string(),
            shred_watch_vaults: Vec::new(),
            shred_hit_ttl_secs: 2,
            shred_udp_prefix_skip: 0,
        };

        let upstream = TritonShredUpstream::new(&config);
        assert_eq!(upstream.status().mode, "error/missing-vaults");
    }
}
