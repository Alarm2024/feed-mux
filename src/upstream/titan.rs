use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{parse_wallet_pubkey, Config};
use crate::rate_limit::UpstreamRateLimiter;
use crate::redis_fanout::{now_ms, FeedPayload, RedisFanout, TitanMessageOutcome};
use crate::titan_board::{
    parse_board_sizes, parse_stream_data_quotes, Hop1QuoteDelivery, SizeBoard, SizeBoardEntry,
    USDC_MINT, SOL_MINT,
};
use crate::titan_local::TitanLocalRelay;
use crate::upstream::UpstreamStatus;
use crate::ws_reconnect::{
    classify_ws_error, close_ws_write, log_ws_reconnect, should_reset_backoff, ReconnectBackoff,
    WsDisconnectKind,
};

pub const TITAN_WS_PROTOCOL: &str = "v1.api.titan.ag";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TitanLiveState {
    Disabled,
    DryRunStub,
    MissingWsUrl,
    MissingWalletPubkey,
    InvalidWalletPubkey,
    Ready,
}

struct SessionOutcome {
    kind: WsDisconnectKind,
    received_data: bool,
    connected_for: Duration,
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
    board_sizes_lamports: Vec<u64>,
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
            board_sizes_lamports: parse_board_sizes(config.titan_board_sizes_lamports.as_deref()),
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

    pub fn spawn_live(&self, fanout: RedisFanout, local_relay: Option<TitanLocalRelay>) {
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
        let board_sizes = self.board_sizes_lamports.clone();

        tokio::spawn(async move {
            run_live_loop(
                ws_url,
                wallet_pubkey,
                connected,
                rate_limiter,
                board_sizes,
                fanout,
                local_relay,
            )
            .await;
        });
    }
}

async fn run_live_loop(
    ws_url: String,
    wallet_pubkey: [u8; 32],
    connected: Arc<AtomicBool>,
    rate_limiter: UpstreamRateLimiter,
    board_sizes: Vec<u64>,
    fanout: RedisFanout,
    local_relay: Option<TitanLocalRelay>,
) {
    let mut backoff = ReconnectBackoff::new(Duration::from_secs(5), Duration::from_secs(30));

    loop {
        mark_titan_disconnected(&connected, &fanout, local_relay.as_ref()).await;

        match run_session(
            &ws_url,
            wallet_pubkey,
            &connected,
            &rate_limiter,
            &board_sizes,
            &fanout,
            local_relay.as_ref(),
        )
        .await
        {
            Ok(outcome) => {
                if should_reset_backoff(outcome.received_data, outcome.connected_for) {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                log_ws_reconnect("titan_ws", outcome.kind, delay, None);
                mark_titan_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                backoff.wait().await;
            }
            Err(e) => {
                mark_titan_disconnected(&connected, &fanout, local_relay.as_ref()).await;
                let kind = classify_ws_error(&e);
                let detail = if kind == WsDisconnectKind::TransportError {
                    Some(crate::ws_reconnect::ws_error_summary(&e))
                } else {
                    None
                };
                let delay = backoff.next_delay();
                log_ws_reconnect("titan_ws", kind, delay, detail.as_deref());
                backoff.wait().await;
            }
        }
    }
}

async fn run_session(
    ws_url: &str,
    wallet_pubkey: [u8; 32],
    connected: &Arc<AtomicBool>,
    rate_limiter: &UpstreamRateLimiter,
    board_sizes: &[u64],
    fanout: &RedisFanout,
    local_relay: Option<&TitanLocalRelay>,
) -> Result<SessionOutcome, WsError> {
    let session_start = Instant::now();
    let mut received_data = false;

    let mut request = ws_url
        .into_client_request()
        .map_err(|e| WsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", TITAN_WS_PROTOCOL.parse().map_err(|e| {
            WsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
        })?);

    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    connected.store(true, Ordering::Relaxed);
    fanout.set_titan_upstream_up(true).await;
    if let Some(relay) = local_relay {
        relay.set_upstream_connected(true);
    }
    tracing::info!(upstream = "titan_ws", "Titan WebSocket connected");

    if !rate_limiter.try_acquire() {
        tracing::debug!(upstream = "titan_ws", "rate limit exceeded for GetInfo");
    } else {
        let get_info = encode_client_request(1, ClientRequestData::GetInfo(GetInfoRequest {}))
            .map_err(session_encode_error)?;
        write.send(Message::Binary(get_info)).await?;
    }

    let input_mint = parse_wallet_pubkey(SOL_MINT).expect("SOL mint constant");
    let output_mint = parse_wallet_pubkey(USDC_MINT).expect("USDC mint constant");
    let size_board_state = Arc::new(Mutex::new(SizeBoard::new(now_ms())));

    let mut request_id: u64 = 2;
    for size in board_sizes {
        if !rate_limiter.try_acquire() {
            tracing::debug!(
                upstream = "titan_ws",
                size_lamports = size,
                "rate limit exceeded for NewSwapQuoteStream"
            );
            continue;
        }
        let subscribe = encode_client_request(
            request_id,
            ClientRequestData::NewSwapQuoteStream(NewSwapQuoteStreamRequest {
                swap: SwapParams {
                    input_mint,
                    output_mint,
                    amount: *size,
                    slippage_bps: Some(50),
                },
                transaction: TransactionParams {
                    user_public_key: wallet_pubkey,
                },
            }),
        )
        .map_err(session_encode_error)?;
        write.send(Message::Binary(subscribe)).await?;
        tracing::info!(
            upstream = "titan_ws",
            size_lamports = size,
            "Titan NewSwapQuoteStream subscribed for board size"
        );
        request_id += 1;
    }

    let mut disconnect_kind = WsDisconnectKind::AbruptClose;

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                received_data = true;
                if let Some(relay) = local_relay {
                    relay.publish_frame(data.clone());
                }
                handle_server_message(&data, fanout, board_sizes, Arc::clone(&size_board_state))
                    .await;
            }
            Ok(Message::Ping(payload)) => {
                if let Err(e) = write.send(Message::Pong(payload)).await {
                    disconnect_kind = classify_ws_error(&e);
                    if disconnect_kind == WsDisconnectKind::TransportError {
                        mark_titan_disconnected(connected, fanout, local_relay).await;
                        return Err(e);
                    }
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                disconnect_kind = WsDisconnectKind::CleanClose;
                break;
            }
            Ok(_) => {}
            Err(e) => {
                disconnect_kind = classify_ws_error(&e);
                if disconnect_kind == WsDisconnectKind::TransportError {
                    mark_titan_disconnected(connected, fanout, local_relay).await;
                    return Err(e);
                }
                break;
            }
        }
    }

    close_ws_write(&mut write).await;
    mark_titan_disconnected(connected, fanout, local_relay).await;
    Ok(SessionOutcome {
        kind: disconnect_kind,
        received_data,
        connected_for: session_start.elapsed(),
    })
}

fn session_encode_error(err: rmp_serde::encode::Error) -> WsError {
    WsError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        err.to_string(),
    ))
}

async fn mark_titan_disconnected(
    connected: &Arc<AtomicBool>,
    fanout: &RedisFanout,
    local_relay: Option<&TitanLocalRelay>,
) {
    connected.store(false, Ordering::Relaxed);
    fanout.set_titan_upstream_up(false).await;
    if let Some(relay) = local_relay {
        relay.set_upstream_connected(false);
    }
}

async fn handle_server_message(
    data: &[u8],
    fanout: &RedisFanout,
    board_sizes: &[u64],
    size_board_state: Arc<Mutex<SizeBoard>>,
) {
    let value = match rmpv::decode::read_value(&mut std::io::Cursor::new(data)) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(upstream = "titan_ws", error = %e, "failed to decode Titan message");
            fanout.record_titan_decode_error().await;
            return;
        }
    };

    if message_contains_key(&value, "StreamData") {
        let quote_ms = now_ms();
        let parsed = parse_stream_data_quotes(data);
        let size_lamports = parsed
            .as_ref()
            .and_then(|q| q.amount)
            .or_else(|| board_sizes.first().copied())
            .unwrap_or(0);

        let (age_ms, board) = {
            let mut board = size_board_state.lock().expect("size board lock");
            let age_ms = board
                .sizes
                .iter()
                .find(|e| e.size_lamports == size_lamports)
                .map(|e| quote_ms.saturating_sub(e.hop1_last_ms))
                .unwrap_or(0);
            board.upsert(
                SizeBoardEntry {
                    size_lamports,
                    hop1_age_ms: age_ms,
                    hop1_last_ms: quote_ms,
                },
                quote_ms,
            );
            (age_ms, board.clone())
        };

        let hop1 = Hop1QuoteDelivery {
            size_lamports,
            input_mint: parsed
                .as_ref()
                .and_then(|q| q.input_mint.clone())
                .unwrap_or_else(|| SOL_MINT.to_string()),
            output_mint: parsed
                .as_ref()
                .and_then(|q| q.output_mint.clone())
                .unwrap_or_else(|| USDC_MINT.to_string()),
            hop: 1,
            out_amount: parsed.as_ref().and_then(|q| q.out_amount),
            provider: parsed.as_ref().and_then(|q| q.provider.clone()),
            quote_ms,
            age_ms,
            source: "titan_ws".to_string(),
        };
        fanout.publish_hop1_quote(&hop1, &board).await;

        let payload = FeedPayload {
            event: "titan.hop1.quote".to_string(),
            source: "titan_ws".to_string(),
            ts: chrono::Utc::now().to_rfc3339(),
            data: Some(serde_json::json!({
                "kind": "StreamData",
                "hop": 1,
                "size_lamports": size_lamports,
                "out_amount": hop1.out_amount,
                "provider": hop1.provider,
                "redis_key": crate::titan_board::hop1_redis_key(size_lamports),
            })),
        };
        match fanout.publish(&payload).await {
            Ok(_) => {
                fanout
                    .record_titan_message(TitanMessageOutcome::QuotePublished)
                    .await;
            }
            Err(e) => {
                tracing::warn!(upstream = "titan_ws", error = %e, "failed to fan-out Titan quote");
                fanout
                    .record_titan_message(TitanMessageOutcome::RpcError)
                    .await;
            }
        }
    } else if message_contains_key(&value, "Error") {
        fanout
            .record_titan_message(TitanMessageOutcome::RpcError)
            .await;
        tracing::warn!(
            upstream = "titan_ws",
            message = ?value,
            "Titan WebSocket RPC error"
        );
    } else if message_contains_key(&value, "Response") {
        tracing::debug!(upstream = "titan_ws", "Titan WebSocket RPC response received");
    } else {
        fanout
            .record_titan_message(TitanMessageOutcome::Unhandled)
            .await;
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
