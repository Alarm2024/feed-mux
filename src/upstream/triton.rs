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
use crate::ws_reconnect::{log_ws_reconnect, ReconnectBackoff, WsDisconnectKind};

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
        let rate_limiter = self.rate_limiter.clone();

        tokio::spawn(async move {
            run_live_loop(
                grpc_url,
                grpc_token,
                connected,
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
                log_ws_reconnect("triton_grpc", WsDisconnectKind::AbruptClose, delay, None);
                mark_triton_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                backoff.wait().await;
            }
            Err(e) => {
                mark_triton_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                let delay = backoff.next_delay();
                log_ws_reconnect(
                    "triton_grpc",
                    WsDisconnectKind::TransportError,
                    delay,
                    Some(&e),
                );
                backoff.wait().await;
            }
        }
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

    let mut builder = GeyserGrpcClient::build_from_shared(grpc_url.to_string())
        .map_err(|e| format!("invalid Triton gRPC URL: {e}"))?;

    if grpc_url.starts_with("https://") {
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
