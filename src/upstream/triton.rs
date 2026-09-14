use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, ReconnectConfig};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterSlots,
};

use crate::config::Config;
use crate::rate_limit::UpstreamRateLimiter;
use crate::redis_fanout::RedisFanout;
use crate::triton_local::TritonLocalProbe;
use crate::upstream::UpstreamStatus;

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
                        "Triton Yellowstone gRPC live mode configured (Bot 350 only — never KEEP)"
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
        if config.triton_grpc_url.is_none() {
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
        if !self.enabled || self.live_state == TritonLiveState::Ready {
            return;
        }
        if self.dry_run && self.rate_limiter.try_acquire() {
            tracing::trace!(upstream = "triton_grpc", "Triton gRPC stub poll (DRY_RUN=true)");
        }
    }

    pub fn spawn_live(&self, fanout: RedisFanout, local_probe: TritonLocalProbe) {
        if self.live_state != TritonLiveState::Ready {
            return;
        }

        let grpc_url = self.grpc_url.clone().expect("ready implies url");
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
                local_probe,
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
    local_probe: TritonLocalProbe,
) {
    loop {
        mark_triton_disconnected(&connected, &fanout, &local_probe).await;

        match run_session(
            &grpc_url,
            grpc_token.as_deref(),
            &connected,
            &rate_limiter,
            &fanout,
            &local_probe,
        )
        .await
        {
            Ok(()) => {
                tracing::warn!(upstream = "triton_grpc", "Triton gRPC stream ended; reconnecting in 5s");
            }
            Err(e) => {
                tracing::warn!(
                    upstream = "triton_grpc",
                    error = %e,
                    "Triton gRPC session failed; reconnecting in 5s"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_session(
    grpc_url: &str,
    grpc_token: Option<&str>,
    connected: &Arc<AtomicBool>,
    rate_limiter: &UpstreamRateLimiter,
    fanout: &RedisFanout,
    local_probe: &TritonLocalProbe,
) -> Result<(), String> {
    if !rate_limiter.try_acquire() {
        tracing::debug!(upstream = "triton_grpc", "rate limit exceeded for subscribe");
    }

    let endpoint = normalize_grpc_endpoint(grpc_url)?;
    let use_tls = endpoint.starts_with("https://");

    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.clone())
        .map_err(|e| format!("invalid gRPC endpoint: {e}"))?
        .x_token(grpc_token.map(|s| s.to_string()))
        .map_err(|e| format!("invalid x-token: {e}"))?
        .set_reconnect_config(ReconnectConfig::default());

    if use_tls {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| format!("tls config: {e}"))?;
    }

    let mut client = builder
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    let mut slots = HashMap::new();
    slots.insert(
        "mux".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    let request = SubscribeRequest {
        slots,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(request)
        .await
        .map_err(|e| format!("subscribe failed: {e}"))?;

    connected.store(true, Ordering::Relaxed);
    fanout.set_triton_upstream_up(true).await;
    local_probe.set_upstream_connected(true);
    tracing::info!(upstream = "triton_grpc", "Triton Yellowstone gRPC subscribed (slots)");

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                if matches!(update.update_oneof, Some(UpdateOneof::Ping(_))) {
                    continue;
                }
                fanout.record_triton_frame().await;
            }
            Err(e) => return Err(format!("stream error: {e}")),
        }
    }

    Ok(())
}

fn normalize_grpc_endpoint(url: &str) -> Result<String, String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("empty gRPC URL".to_string());
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

async fn mark_triton_disconnected(
    connected: &Arc<AtomicBool>,
    fanout: &RedisFanout,
    local_probe: &TritonLocalProbe,
) {
    connected.store(false, Ordering::Relaxed);
    fanout.set_triton_upstream_up(false).await;
    local_probe.set_upstream_connected(false);
}
