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
use crate::redis_fanout::RedisFanout;
use crate::titan_local::TitanLocalRelay;
use crate::titan_quote::{
    handle_titan_server_message, mint_pair_label, parse_hunt_sizes, publish_size_board_snapshot,
    TitanMessageKind, TitanSizeBoard, TitanStreamRegistry, USDC_MINT, SOL_MINT,
};
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

struct TitanSessionContext {
    stream_registry: Arc<Mutex<TitanStreamRegistry>>,
    size_board: Arc<Mutex<TitanSizeBoard>>,
    hop1_ttl_secs: u64,
    hunt_sizes: Vec<u64>,
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
    hunt_sizes: Vec<u64>,
    hop1_ttl_secs: u64,
}

impl TitanWsUpstream {
    pub fn new(config: &Config) -> Self {
        let live_state = Self::resolve_live_state(config);
        let wallet_pubkey_bytes = config
            .titan_wallet_pubkey
            .as_deref()
            .and_then(|value| parse_wallet_pubkey(value).ok());
        let hunt_sizes = parse_hunt_sizes(config.titan_hunt_size_lamports.as_deref());

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
                        hunt_sizes = hunt_sizes.len(),
                        hop1_ttl_secs = config.titan_hop1_ttl_secs,
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
            hunt_sizes,
            hop1_ttl_secs: config.titan_hop1_ttl_secs,
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
        let session_ctx = TitanSessionContext {
            stream_registry: Arc::new(Mutex::new(TitanStreamRegistry::default())),
            size_board: Arc::new(Mutex::new(TitanSizeBoard::default())),
            hop1_ttl_secs: self.hop1_ttl_secs,
            hunt_sizes: self.hunt_sizes.clone(),
        };

        tokio::spawn(async move {
            run_live_loop(
                ws_url,
                wallet_pubkey,
                connected,
                rate_limiter,
                fanout,
                local_relay,
                session_ctx,
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
    fanout: RedisFanout,
    local_relay: Option<TitanLocalRelay>,
    session_ctx: TitanSessionContext,
) {
    let mut backoff = ReconnectBackoff::new(Duration::from_secs(5), Duration::from_secs(30));

    loop {
        mark_titan_disconnected(&connected, &fanout, local_relay.as_ref()).await;

        match run_session(
            &ws_url,
            wallet_pubkey,
            &connected,
            &rate_limiter,
            &fanout,
            local_relay.as_ref(),
            &session_ctx,
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
    fanout: &RedisFanout,
    local_relay: Option<&TitanLocalRelay>,
    session_ctx: &TitanSessionContext,
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

    {
        session_ctx
            .stream_registry
            .lock()
            .expect("stream registry lock")
            .clear();
    }
    {
        let mut board = session_ctx
            .size_board
            .lock()
            .expect("size board lock");
        board.clear();
        board.init_hunt_sizes(&session_ctx.hunt_sizes);
    }

    if !rate_limiter.try_acquire() {
        tracing::debug!(upstream = "titan_ws", "rate limit exceeded for GetInfo");
    } else {
        let get_info = encode_client_request(1, ClientRequestData::GetInfo(GetInfoRequest {}))
            .map_err(session_encode_error)?;
        write.send(Message::Binary(get_info)).await?;
    }

    subscribe_hunt_streams(
        &mut write,
        wallet_pubkey,
        rate_limiter,
        session_ctx,
    )
    .await?;

    {
        let (pair, base, mid) = mint_pair_label(SOL_MINT, USDC_MINT);
        publish_size_board_snapshot(
            fanout,
            &session_ctx.size_board,
            &pair,
            &base,
            &mid,
        )
        .await;
    }

    let mut disconnect_kind = WsDisconnectKind::AbruptClose;

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                received_data = true;
                if let Some(relay) = local_relay {
                    relay.publish_frame(data.clone());
                }
                let kind = handle_titan_server_message(
                    &data,
                    fanout,
                    &session_ctx.stream_registry,
                    &session_ctx.size_board,
                    session_ctx.hop1_ttl_secs,
                )
                .await;
                if kind == TitanMessageKind::Hop1Published {
                    tracing::trace!(upstream = "titan_ws", "hop-1 quote published to redis");
                }
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

async fn subscribe_hunt_streams(
    write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    wallet_pubkey: [u8; 32],
    rate_limiter: &UpstreamRateLimiter,
    session_ctx: &TitanSessionContext,
) -> Result<(), WsError> {
    let input_mint = parse_wallet_pubkey(SOL_MINT).expect("SOL mint constant");
    let output_mint = parse_wallet_pubkey(USDC_MINT).expect("USDC mint constant");

    for (idx, amount) in session_ctx.hunt_sizes.iter().enumerate() {
        let request_id = (idx as u64) + 2;
        if !rate_limiter.try_acquire() {
            tracing::debug!(
                upstream = "titan_ws",
                request_id,
                amount,
                "rate limit exceeded for NewSwapQuoteStream"
            );
            session_ctx
                .size_board
                .lock()
                .expect("size board lock")
                .mark_unsubscribed(*amount);
            continue;
        }

        session_ctx
            .stream_registry
            .lock()
            .expect("stream registry lock")
            .register_pending_request(request_id, *amount);

        let subscribe = encode_client_request(
            request_id,
            ClientRequestData::NewSwapQuoteStream(NewSwapQuoteStreamRequest {
                swap: SwapParams {
                    input_mint,
                    output_mint,
                    amount: *amount,
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
            request_id,
            amount_lamports = amount,
            "Titan NewSwapQuoteStream subscribed for hunt size"
        );
    }

    Ok(())
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
