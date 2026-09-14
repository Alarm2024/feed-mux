use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestPing, SubscribeUpdate,
};
use crate::config::Config;
use crate::rate_limit::UpstreamRateLimiter;
use crate::redis_fanout::{FeedPayload, RedisFanout};
use crate::triton_local::TritonLocalRelay;
use crate::upstream::UpstreamStatus;
use crate::ws_reconnect::{
    classify_grpc_stream_error, grpc_error_summary, log_grpc_reconnect, GrpcStreamFailure,
    ReconnectBackoff,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TritonLiveState {
    Disabled,
    DryRunStub,
    MissingGrpcUrl,
    Ready,
}

pub struct TritonGrpcUpstream {
    enabled: bool,
    dry_run: bool,
    grpc_url: Option<String>,
    grpc_token: Option<String>,
    rate_limit_rps: u32,
    rate_limiter: UpstreamRateLimiter,
    live_state: TritonLiveState,
    connected: Arc<AtomicBool>,
    auth_blocked: Arc<AtomicBool>,
}

impl TritonGrpcUpstream {
    pub fn new(config: &Config) -> Self {
        let live_state = Self::resolve_live_state(config);

        if config.enable_triton_grpc && !config.dry_run {
            match live_state {
                TritonLiveState::MissingGrpcUrl => {
                    tracing::error!(
                        upstream = "triton_grpc",
                        "Triton gRPC enabled but TRITON_GRPC_URL is not set; refusing to connect"
                    );
                }
                TritonLiveState::Ready => {
                    if config
                        .triton_grpc_token
                        .as_ref()
                        .is_none_or(|s| s.trim().is_empty())
                    {
                        tracing::warn!(
                            upstream = "triton_grpc",
                            "TRITON_GRPC_TOKEN is not set; Triton may reject the stream with 403"
                        );
                    }
                    tracing::info!(
                        upstream = "triton_grpc",
                        rate_limit_rps = config.triton_rate_limit_rps,
                        "Triton gRPC live mode configured (Bot 350 local bind — never KEEP)"
                    );
                }
                _ => {}
            }
        }

        Self {
            enabled: config.enable_triton_grpc,
            dry_run: config.dry_run,
            grpc_url: config.triton_grpc_url.clone(),
            grpc_token: config.triton_grpc_token.clone(),
            rate_limit_rps: config.triton_rate_limit_rps,
            rate_limiter: UpstreamRateLimiter::new("triton_grpc", config.triton_rate_limit_rps),
            live_state,
            connected: Arc::new(AtomicBool::new(false)),
            auth_blocked: Arc::new(AtomicBool::new(false)),
        }
    }

    fn resolve_live_state(config: &Config) -> TritonLiveState {
        if !config.enable_triton_grpc {
            return TritonLiveState::Disabled;
        }
        if config.dry_run {
            return TritonLiveState::DryRunStub;
        }
        if config.triton_grpc_url.as_ref().is_none_or(|s| s.trim().is_empty()) {
            return TritonLiveState::MissingGrpcUrl;
        }
        TritonLiveState::Ready
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn rate_limiter(&self) -> &UpstreamRateLimiter {
        &self.rate_limiter
    }

    pub fn status(&self) -> UpstreamStatus {
        if self.auth_blocked.load(Ordering::Relaxed) {
            return UpstreamStatus {
                name: "triton_grpc",
                enabled: self.enabled,
                connected: false,
                mode: "error/auth-denied",
                rate_limit_rps: Some(self.rate_limit_rps),
            };
        }

        let (connected, mode) = match self.live_state {
            TritonLiveState::Disabled => (false, "disabled"),
            TritonLiveState::DryRunStub => (false, "stub/dry-run"),
            TritonLiveState::MissingGrpcUrl => (false, "error/missing-grpc-url"),
            TritonLiveState::Ready => (
                self.connected.load(Ordering::Relaxed),
                if self.connected.load(Ordering::Relaxed) {
                    "live/grpc"
                } else {
                    "live/connecting"
                },
            ),
        };

        UpstreamStatus {
            name: "triton_grpc",
            enabled: self.enabled,
            connected,
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

        if self.live_state != TritonLiveState::Ready {
            return;
        }

        if !self.rate_limiter.try_acquire() {
            return;
        }

        tracing::trace!(
            upstream = "triton_grpc",
            connected = self.connected.load(Ordering::Relaxed),
            auth_blocked = self.auth_blocked.load(Ordering::Relaxed),
            "Triton gRPC live poll (handled by background task)"
        );
    }

    pub fn spawn_live(&self, fanout: RedisFanout, local_relay: Option<TritonLocalRelay>) {
        if self.live_state != TritonLiveState::Ready {
            return;
        }

        let grpc_url = self
            .grpc_url
            .clone()
            .expect("ready state implies grpc url");
        let grpc_token = self.grpc_token.clone();
        let connected = Arc::clone(&self.connected);
        let auth_blocked = Arc::clone(&self.auth_blocked);
        let rate_limiter = self.rate_limiter.clone();

        tokio::spawn(async move {
            run_live_loop(
                grpc_url,
                grpc_token,
                connected,
                auth_blocked,
                rate_limiter,
                fanout,
                local_relay,
            )
            .await;
        });
    }
}

async fn run_live_loop(
    grpc_url: String,
    grpc_token: Option<String>,
    connected: Arc<AtomicBool>,
    auth_blocked: Arc<AtomicBool>,
    rate_limiter: UpstreamRateLimiter,
    fanout: RedisFanout,
    local_relay: Option<TritonLocalRelay>,
) {
    let mut backoff = ReconnectBackoff::new(Duration::from_secs(5), Duration::from_secs(30));

    loop {
        mark_triton_disconnected(&connected, &fanout, local_relay.as_ref()).await;

        match run_session(
            &grpc_url,
            grpc_token.as_deref(),
            &connected,
            &rate_limiter,
            &fanout,
            local_relay.as_ref(),
        )
        .await
        {
            Ok(received_data) => {
                if received_data {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                log_grpc_reconnect("triton_grpc", delay, None);
                mark_triton_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                backoff.wait().await;
            }
            Err(e) => {
                mark_triton_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                match classify_grpc_stream_error(&e) {
                    GrpcStreamFailure::AuthDenied => {
                        auth_blocked.store(true, Ordering::Relaxed);
                        tracing::error!(
                            upstream = "triton_grpc",
                            reason = %grpc_error_summary(&e),
                            "Triton gRPC access denied — check TRITON_GRPC_TOKEN and plan; upstream halted (no reconnect until restart)"
                        );
                        break;
                    }
                    GrpcStreamFailure::Retryable => {
                        let delay = backoff.next_delay();
                        log_grpc_reconnect(
                            "triton_grpc",
                            delay,
                            Some(&grpc_error_summary(&e)),
                        );
                        backoff.wait().await;
                    }
                }
            }
        }
    }
}

fn normalize_grpc_endpoint(url: &str) -> Result<String, String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("empty Triton gRPC URL".to_string());
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

async fn run_session(
    grpc_url: &str,
    grpc_token: Option<&str>,
    connected: &Arc<AtomicBool>,
    rate_limiter: &UpstreamRateLimiter,
    fanout: &RedisFanout,
    local_relay: Option<&TritonLocalRelay>,
) -> Result<bool, String> {
    if !rate_limiter.try_acquire() {
        return Err("rate limit exceeded for Triton subscribe".to_string());
    }

    let endpoint = normalize_grpc_endpoint(grpc_url)?;

    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.clone())
        .map_err(|e| format!("invalid Triton gRPC URL: {e}"))?;

    if endpoint.starts_with("https://") {
        builder = builder
            .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())
            .map_err(|e| format!("Triton TLS config failed: {e}"))?;
    }

    if let Some(token) = grpc_token.filter(|t| !t.trim().is_empty()) {
        builder = builder
            .x_token(Some(token.trim()))
            .map_err(|e| format!("Triton x-token config failed: {e}"))?;
    }

    let mut client = builder
        .connect()
        .await
        .map_err(|e| format!("Triton gRPC connect failed: {e}"))?;

    let mut slots = HashMap::new();
    slots.insert(
        "feed_mux".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            ..Default::default()
        },
    );

    let request = SubscribeRequest {
        slots,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| format!("Triton subscribe failed: {e}"))?;

    connected.store(true, Ordering::Relaxed);
    fanout.set_triton_upstream_up(true).await;
    if let Some(relay) = local_relay {
        relay.set_upstream_connected(true);
    }
    tracing::info!(upstream = "triton_grpc", "Triton gRPC connected");

    let session_start = Instant::now();
    let mut received_data = false;
    let mut ping_id: i32 = 1;

    while let Some(update) = stream.next().await {
        match update {
            Ok(msg) => {
                received_data = true;
                if let Some(relay) = local_relay {
                    relay.publish_update(msg.clone());
                }
                handle_triton_update(&msg, fanout).await;

                if let Some(subscribe_update::UpdateOneof::Ping(_)) = msg.update_oneof.as_ref() {
                    if rate_limiter.try_acquire() {
                        let ping = SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: ping_id }),
                            ..Default::default()
                        };
                        ping_id = ping_id.wrapping_add(1);
                        if let Err(e) = subscribe_tx.send(ping).await {
                            return Err(format!("Triton ping send failed: {e}"));
                        }
                    }
                }
            }
            Err(e) => return Err(format!("Triton stream error: {e}")),
        }
    }

    let _ = session_start;
    mark_triton_disconnected(connected, fanout, local_relay).await;
    Ok(received_data)
}

async fn mark_triton_disconnected(
    connected: &Arc<AtomicBool>,
    fanout: &RedisFanout,
    local_relay: Option<&TritonLocalRelay>,
) {
    connected.store(false, Ordering::Relaxed);
    fanout.set_triton_upstream_up(false).await;
    if let Some(relay) = local_relay {
        relay.set_upstream_connected(false);
    }
}

async fn handle_triton_update(update: &SubscribeUpdate, fanout: &RedisFanout) {
    fanout.record_triton_frame().await;

    let payload = FeedPayload {
        event: "triton.slot".to_string(),
        source: "triton_grpc".to_string(),
        ts: chrono::Utc::now().to_rfc3339(),
        data: Some(serde_json::json!({
            "kind": "SubscribeUpdate",
            "has_slot": update.update_oneof.as_ref().map(|u| matches!(u, subscribe_update::UpdateOneof::Slot(_))).unwrap_or(false),
        })),
    };

    if let Err(e) = fanout.publish(&payload).await {
        tracing::warn!(upstream = "triton_grpc", error = %e, "failed to fan-out Triton update");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn live_config(token: Option<&str>) -> Config {
        Config {
            bind_addr: "127.0.0.1:8787".to_string(),
            dry_run: false,
            redis_url: None,
            redis_channel: "feed:350".to_string(),
            enable_chainstack: false,
            chainstack_rpc_url: None,
            chainstack_ws_url: None,
            enable_helius: false,
            helius_rpc_url: None,
            enable_triton_grpc: true,
            triton_grpc_url: Some("grpc.example.test".to_string()),
            triton_grpc_token: token.map(|s| s.to_string()),
            triton_rate_limit_rps: 25,
            triton_local_bind: "127.0.0.1:19000".to_string(),
            enable_titan_ws: false,
            titan_ws_url: None,
            titan_wallet_pubkey: None,
            titan_rate_limit_rps: 15,
            titan_local_bind: "127.0.0.1:19001".to_string(),
            titan_hunt_size_lamports: None,
            titan_hop1_ttl_secs: 2,
            enable_triton_shred: false,
            shred_bind: "0.0.0.0:8003".to_string(),
            shred_watch_vaults: Vec::new(),
            shred_hit_ttl_secs: 2,
            shred_udp_prefix_skip: 0,
            mock_publish_interval_secs: 0,
        }
    }

    #[test]
    fn normalize_grpc_endpoint_adds_https_scheme() {
        assert_eq!(
            normalize_grpc_endpoint("grpc.example.test").unwrap(),
            "https://grpc.example.test"
        );
        assert_eq!(
            normalize_grpc_endpoint("https://grpc.example.test").unwrap(),
            "https://grpc.example.test"
        );
    }

    #[test]
    fn auth_blocked_status_is_honest() {
        let upstream = TritonGrpcUpstream::new(&live_config(Some("token")));
        upstream.auth_blocked.store(true, Ordering::Relaxed);
        let status = upstream.status();
        assert_eq!(status.mode, "error/auth-denied");
        assert!(!status.connected);
    }
}
