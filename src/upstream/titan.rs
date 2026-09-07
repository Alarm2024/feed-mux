use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{parse_wallet_pubkey, Config};
use crate::rate_limit::UpstreamRateLimiter;
use crate::redis_fanout::{FeedPayload, RedisFanout};
use crate::upstream::UpstreamStatus;

const TITAN_WS_PROTOCOL: &str = "v1.api.titan.ag";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TitanLiveState {
    Disabled,
    DryRunStub,
    MissingWsUrl,
    MissingWalletPubkey,
    InvalidWalletPubkey,
    Ready,
}

pub struct TitanWsUpstream {
    enabled: bool,
    dry_run: bool,
    ws_url: Option<String>,
    wallet_pubkey: Option<String>,
    wallet_pubkey_bytes: Option<[u8; 32]>,
    live_state: TitanLiveState,
    rate_limit_rps: u32,
    rate_limiter: UpstreamRateLimiter,
    connected: Arc<AtomicBool>,
}

impl TitanWsUpstream {
    pub fn new(config: &Config) -> Self {
        let live_state = Self::resolve_live_state(config);
        let wallet_pubkey_bytes = config
            .titan_wallet_pubkey
            .as_deref()
            .and_then(|value| parse_wallet_pubkey(value).ok());

        if config.enable_titan_ws && !config.dry_run {
            match live_state {
                TitanLiveState::MissingWsUrl => {
                    tracing::error!(
                        upstream = "titan_ws",
                        "Titan WebSocket enabled but TITAN_WS_URL is not set; refusing to connect"
                    );
                }
                TitanLiveState::MissingWalletPubkey => {
                    tracing::error!(
                        upstream = "titan_ws",
                        "Titan WebSocket enabled but TITAN_WALLET_PUBKEY is not set; \
                         refusing to connect (wallet pubkey required for quote subscribe/compile)"
                    );
                }
                TitanLiveState::InvalidWalletPubkey => {
                    tracing::error!(
                        upstream = "titan_ws",
                        "Titan WebSocket enabled but TITAN_WALLET_PUBKEY is invalid; \
                         refusing to connect (wallet pubkey required for quote subscribe/compile)"
                    );
                }
                TitanLiveState::Ready => {
                    tracing::info!(
                        upstream = "titan_ws",
                        rate_limit_rps = config.titan_rate_limit_rps,
                        "Titan WebSocket live mode configured (Bot 350 wallet pubkey only — never KEEP)"
                    );
                }
                _ => {}
            }
        }

        Self {
            enabled: config.enable_titan_ws,
            dry_run: config.dry_run,
            ws_url: config.titan_ws_url.clone(),
            wallet_pubkey: config.titan_wallet_pubkey.clone(),
            wallet_pubkey_bytes,
            live_state,
            rate_limit_rps: config.titan_rate_limit_rps,
            rate_limiter: UpstreamRateLimiter::new("titan_ws", config.titan_rate_limit_rps),
            connected: Arc::new(AtomicBool::new(false)),
        }
    }

    fn resolve_live_state(config: &Config) -> TitanLiveState {
        if !config.enable_titan_ws {
            return TitanLiveState::Disabled;
        }
        if config.dry_run {
            return TitanLiveState::DryRunStub;
        }
        if config.titan_ws_url.is_none() {
            return TitanLiveState::MissingWsUrl;
        }
        match config.titan_wallet_pubkey.as_deref() {
            None => TitanLiveState::MissingWalletPubkey,
            Some(value) if value.trim().is_empty() => TitanLiveState::MissingWalletPubkey,
            Some(value) => {
                if parse_wallet_pubkey(value).is_ok() {
                    TitanLiveState::Ready
                } else {
                    TitanLiveState::InvalidWalletPubkey
                }
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn rate_limiter(&self) -> &UpstreamRateLimiter {
        &self.rate_limiter
    }

    pub fn status(&self) -> UpstreamStatus {
        let (connected, mode) = match self.live_state {
            TitanLiveState::Disabled => (false, "disabled"),
            TitanLiveState::DryRunStub => (false, "stub/dry-run"),
            TitanLiveState::MissingWsUrl => (false, "error/missing-ws-url"),
            TitanLiveState::MissingWalletPubkey | TitanLiveState::InvalidWalletPubkey => {
                (false, "error/wallet-pubkey-required")
            }
            TitanLiveState::Ready => (
                self.connected.load(Ordering::Relaxed),
                if self.connected.load(Ordering::Relaxed) {
                    "live/ws"
                } else {
                    "live/connecting"
                },
            ),
        };

        UpstreamStatus {
            name: "titan_ws",
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
                upstream = "titan_ws",
                ws = self.ws_url.is_some(),
                wallet = self.wallet_pubkey.is_some(),
                "Titan WebSocket stub poll (DRY_RUN=true, no connection)"
            );
            return;
        }

        if self.live_state != TitanLiveState::Ready {
            return;
        }

        if !self.rate_limiter.try_acquire() {
            return;
        }

        tracing::trace!(
            upstream = "titan_ws",
            connected = self.connected.load(Ordering::Relaxed),
            "Titan WebSocket live poll (handled by background task)"
        );
    }

    pub fn spawn_live(&self, fanout: RedisFanout) {
        if self.live_state != TitanLiveState::Ready {
            return;
        }

        let ws_url = self
            .ws_url
            .clone()
            .expect("ready state implies ws url");
        let wallet_pubkey = self
            .wallet_pubkey_bytes
            .expect("ready state implies wallet pubkey bytes");
        let connected = Arc::clone(&self.connected);
        let rate_limiter = self.rate_limiter.clone();

        tokio::spawn(async move {
            run_live_loop(ws_url, wallet_pubkey, connected, rate_limiter, fanout).await;
        });
    }
}

async fn run_live_loop(
    ws_url: String,
    wallet_pubkey: [u8; 32],
    connected: Arc<AtomicBool>,
    rate_limiter: UpstreamRateLimiter,
    fanout: RedisFanout,
) {
    loop {
        connected.store(false, Ordering::Relaxed);
        match run_session(&ws_url, wallet_pubkey, &connected, &rate_limiter, &fanout).await {
            Ok(()) => {
                tracing::warn!(upstream = "titan_ws", "Titan WebSocket session ended; reconnecting");
            }
            Err(e) => {
                tracing::warn!(
                    upstream = "titan_ws",
                    error = %e,
                    "Titan WebSocket session failed; reconnecting in 5s"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_session(
    ws_url: &str,
    wallet_pubkey: [u8; 32],
    connected: &Arc<AtomicBool>,
    rate_limiter: &UpstreamRateLimiter,
    fanout: &RedisFanout,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = ws_url.into_client_request()?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", TITAN_WS_PROTOCOL.parse()?);

    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    connected.store(true, Ordering::Relaxed);
    tracing::info!(upstream = "titan_ws", "Titan WebSocket connected");

    if !rate_limiter.try_acquire() {
        tracing::debug!(upstream = "titan_ws", "rate limit exceeded for GetInfo");
    } else {
        let get_info = encode_client_request(1, ClientRequestData::GetInfo(GetInfoRequest {}))?;
        write.send(Message::Binary(get_info)).await?;
    }

    if !rate_limiter.try_acquire() {
        tracing::debug!(
            upstream = "titan_ws",
            "rate limit exceeded for NewSwapQuoteStream"
        );
    } else {
        let input_mint = parse_wallet_pubkey(SOL_MINT)?;
        let output_mint = parse_wallet_pubkey(USDC_MINT)?;
        let subscribe = encode_client_request(
            2,
            ClientRequestData::NewSwapQuoteStream(NewSwapQuoteStreamRequest {
                swap: SwapParams {
                    input_mint,
                    output_mint,
                    amount: 1_000_000_000,
                    slippage_bps: Some(50),
                },
                transaction: TransactionParams {
                    user_public_key: wallet_pubkey,
                },
            }),
        )?;
        write
            .send(Message::Binary(subscribe))
            .await?;
        tracing::info!(
            upstream = "titan_ws",
            "Titan NewSwapQuoteStream subscribed with configured wallet pubkey"
        );
    }

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => handle_server_message(&data, fanout).await,
            Ok(Message::Ping(payload)) => {
                write.send(Message::Pong(payload)).await?;
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => return Err(e.into()),
        }
    }

    connected.store(false, Ordering::Relaxed);
    Ok(())
}

async fn handle_server_message(data: &[u8], fanout: &RedisFanout) {
    let value = match rmpv::decode::read_value(&mut std::io::Cursor::new(data)) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(upstream = "titan_ws", error = %e, "failed to decode Titan message");
            return;
        }
    };

    if message_contains_key(&value, "StreamData") {
        let payload = FeedPayload {
            event: "titan.quote".to_string(),
            source: "titan_ws".to_string(),
            ts: chrono::Utc::now().to_rfc3339(),
            data: Some(serde_json::json!({
                "kind": "StreamData",
                "note": "quote stream update (raw msgpack not forwarded)"
            })),
        };
        if let Err(e) = fanout.publish(&payload).await {
            tracing::warn!(upstream = "titan_ws", error = %e, "failed to fan-out Titan quote");
        }
    } else if message_contains_key(&value, "Error") {
        tracing::warn!(
            upstream = "titan_ws",
            message = ?value,
            "Titan WebSocket RPC error"
        );
    } else if message_contains_key(&value, "Response") {
        tracing::debug!(upstream = "titan_ws", "Titan WebSocket RPC response received");
    }
}

fn message_contains_key(value: &rmpv::Value, key: &str) -> bool {
    match value {
        rmpv::Value::Map(map) => map.iter().any(|(k, _)| {
            k.as_str().map(|s| s == key).unwrap_or(false)
        }),
        _ => false,
    }
}

fn encode_client_request(id: u64, data: ClientRequestData) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(&ClientRequest { id, data })
}

#[derive(Serialize)]
struct ClientRequest {
    id: u64,
    data: ClientRequestData,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
enum ClientRequestData {
    GetInfo(GetInfoRequest),
    NewSwapQuoteStream(NewSwapQuoteStreamRequest),
}

#[derive(Serialize)]
struct GetInfoRequest {}

#[derive(Serialize)]
struct NewSwapQuoteStreamRequest {
    swap: SwapParams,
    transaction: TransactionParams,
}

#[derive(Serialize)]
struct SwapParams {
    #[serde(rename = "inputMint")]
    input_mint: [u8; 32],
    #[serde(rename = "outputMint")]
    output_mint: [u8; 32],
    amount: u64,
    #[serde(rename = "slippageBps")]
    slippage_bps: Option<u16>,
}

#[derive(Serialize)]
struct TransactionParams {
    #[serde(rename = "userPublicKey")]
    user_public_key: [u8; 32],
}
