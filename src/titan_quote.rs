//! Titan StreamData parsing and hop-1 quote extraction for Bot 350 hunt.
//!
//! Redis contract (see README "Titan Redis keys"):
//! - `mux:titan:size_board` — aggregate per-size hop-1 ages (KEEP-like pin board)
//! - `mux:titan:hop1:<BASE>-<MID>:<size_lamports>` — individual hop-1 quote rows
//! - `mux:titan:hop1_served` — counter incremented on each hop-1 row write
//!
//! Hop-1 row freshness: rows carry `ts_ms` only (no frozen `hop1_age_ms`). Bot 350
//! consumers must derive age from `ts_ms` plus the hop-1 key TTL (`TITAN_HOP1_TTL_SECS`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::config::parse_wallet_pubkey;
use crate::redis_fanout::{now_ms, RedisFanout};

pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

pub const DEFAULT_HUNT_SIZES_LAMPORTS: &[u64] = &[
    250_000_000,    // 0.25 SOL
    500_000_000,    // 0.5 SOL
    1_000_000_000,  // 1 SOL
    2_500_000_000,  // 2.5 SOL (live pin)
    5_000_000_000,  // 5 SOL
];

/// Tracks Titan request/stream id → subscribed input amount (lamports).
#[derive(Default)]
pub struct TitanStreamRegistry {
    request_to_size: HashMap<u64, u64>,
    stream_to_size: HashMap<u32, u64>,
}

impl TitanStreamRegistry {
    pub fn clear(&mut self) {
        self.request_to_size.clear();
        self.stream_to_size.clear();
    }

    pub fn register_pending_request(&mut self, request_id: u64, size_lamports: u64) {
        self.request_to_size.insert(request_id, size_lamports);
    }

    pub fn confirm_stream(&mut self, request_id: u64, stream_id: u32) {
        if let Some(size) = self.request_to_size.get(&request_id).copied() {
            self.stream_to_size.insert(stream_id, size);
        }
    }

    pub fn size_for_stream(&self, stream_id: u32) -> Option<u64> {
        self.stream_to_size.get(&stream_id).copied()
    }

    fn size_for_pending_request(&self, request_id: u64) -> Option<u64> {
        self.request_to_size.get(&request_id).copied()
    }
}

/// In-memory aggregate for `mux:titan:size_board`.
#[derive(Default)]
pub struct TitanSizeBoard {
    pins: HashMap<u64, SizeBoardPin>,
    hunt_sizes: Vec<u64>,
    unsubscribed: HashMap<u64, ()>,
}

impl TitanSizeBoard {
    pub fn clear(&mut self) {
        self.pins.clear();
        self.hunt_sizes.clear();
        self.unsubscribed.clear();
    }

    pub fn init_hunt_sizes(&mut self, sizes: &[u64]) {
        self.hunt_sizes = sizes.to_vec();
    }

    pub fn mark_unsubscribed(&mut self, size_lamports: u64) {
        self.unsubscribed.insert(size_lamports, ());
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SizeBoardPin {
    pub size_lamports: u64,
    pub hop1_age_ms: u64,
    pub hop1_fresh_ms: u64,
    pub provider: String,
    pub venue_label: String,
    pub route_in_amount: u64,
    pub route_out_amount: u64,
    /// `"unsubscribed"` when a hunt rung was skipped (e.g. rate limit); omitted when live.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TitanSizeBoardDoc {
    pub schema: &'static str,
    pub pair: String,
    pub base: String,
    pub mid: String,
    pub updated_ms: u64,
    pub pins: Vec<SizeBoardPin>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TitanHop1Row {
    pub schema: &'static str,
    pub pair: String,
    pub base: String,
    pub mid: String,
    pub size_lamports: u64,
    /// Publish timestamp — Bot 350 derives hop-1 age from this plus key TTL.
    pub ts_ms: u64,
    pub provider: String,
    pub route_in_amount: u64,
    pub route_out_amount: u64,
    pub slippage_bps: u16,
    pub quote_id: Option<String>,
    pub stream_id: u32,
    pub stream_seq: u32,
    pub hop1: Hop1Leg,
}

#[derive(Debug, Clone, Serialize)]
pub struct Hop1Leg {
    pub label: String,
    pub amm_key: String,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: u64,
    pub out_amount: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_slot: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ParsedHop1Quote {
    pub pair: String,
    pub base: String,
    pub mid: String,
    pub size_lamports: u64,
    pub provider: String,
    pub route_in_amount: u64,
    pub route_out_amount: u64,
    pub slippage_bps: u16,
    pub quote_id: Option<String>,
    pub stream_id: u32,
    pub stream_seq: u32,
    pub hop1: Hop1Leg,
}

pub fn parse_hunt_sizes(raw: Option<&str>) -> Vec<u64> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_HUNT_SIZES_LAMPORTS.to_vec();
    };
    let mut sizes: Vec<u64> = raw
        .split(',')
        .filter_map(|part| part.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .collect();
    sizes.sort_unstable();
    sizes.dedup();
    if sizes.is_empty() {
        DEFAULT_HUNT_SIZES_LAMPORTS.to_vec()
    } else {
        sizes
    }
}

pub fn mint_pair_label(input_mint: &str, output_mint: &str) -> (String, String, String) {
    if input_mint == SOL_MINT && output_mint == USDC_MINT {
        ("SOL-USDC".to_string(), "SOL".to_string(), "USDC".to_string())
    } else if input_mint == USDC_MINT && output_mint == SOL_MINT {
        ("USDC-SOL".to_string(), "USDC".to_string(), "SOL".to_string())
    } else {
        let pair = format!("{}-{}", short_mint(input_mint), short_mint(output_mint));
        (pair.clone(), short_mint(input_mint), short_mint(output_mint))
    }
}

fn short_mint(mint: &str) -> String {
    if mint.len() <= 8 {
        mint.to_string()
    } else {
        format!("{}…", &mint[..4])
    }
}

pub async fn handle_titan_server_message(
    data: &[u8],
    fanout: &RedisFanout,
    stream_registry: &Arc<Mutex<TitanStreamRegistry>>,
    size_board: &Arc<Mutex<TitanSizeBoard>>,
    hop1_ttl_secs: u64,
) -> TitanMessageKind {
    let value = match rmpv::decode::read_value(&mut std::io::Cursor::new(data)) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(upstream = "titan_ws", error = %e, "failed to decode Titan message");
            fanout.record_titan_decode_error().await;
            return TitanMessageKind::DecodeError;
        }
    };

    if map_contains_key(&value, "StreamData") {
        let stream_data = map_get(&value, "StreamData");
        match parse_stream_data_hop1(stream_data, stream_registry) {
            Some(quote) => {
                publish_hop1_quote(fanout, size_board, quote, hop1_ttl_secs).await;
                TitanMessageKind::Hop1Published
            }
            None => {
                fanout
                    .record_titan_message(crate::redis_fanout::TitanMessageOutcome::Unhandled)
                    .await;
                TitanMessageKind::StreamDataNoHop1
            }
        }
    } else if map_contains_key(&value, "Response") {
        register_stream_from_response(&value, stream_registry);
        TitanMessageKind::Response
    } else if map_contains_key(&value, "Error") {
        fanout
            .record_titan_message(crate::redis_fanout::TitanMessageOutcome::RpcError)
            .await;
        tracing::warn!(upstream = "titan_ws", message = ?value, "Titan WebSocket RPC error");
        TitanMessageKind::Error
    } else {
        fanout
            .record_titan_message(crate::redis_fanout::TitanMessageOutcome::Unhandled)
            .await;
        TitanMessageKind::Unhandled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TitanMessageKind {
    Hop1Published,
    StreamDataNoHop1,
    DecodeError,
    Response,
    Error,
    Unhandled,
}

fn register_stream_from_response(value: &rmpv::Value, registry: &Arc<Mutex<TitanStreamRegistry>>) {
    let Some(response) = map_get(value, "Response") else {
        return;
    };
    let Some(request_id) = map_get(response, "requestId").and_then(value_as_u64) else {
        return;
    };
    let Some(data) = map_get(response, "data") else {
        return;
    };
    let Some(stream) = map_get(response, "stream")
        .or_else(|| map_get(response, "StreamStart"))
        .or_else(|| map_get(data, "stream"))
        .or_else(|| map_get(data, "StreamStart"))
    else {
        return;
    };
    let Some(stream_id) = map_get(stream, "id").and_then(value_as_u32) else {
        return;
    };
    let mut guard = registry.lock().expect("stream registry lock");
    let size_lamports = guard.size_for_pending_request(request_id);
    guard.confirm_stream(request_id, stream_id);
    tracing::debug!(
        upstream = "titan_ws",
        stream_id,
        size_lamports = ?size_lamports,
        request_id,
        "registered Titan quote stream"
    );
}

fn parse_stream_data_hop1(
    stream_data: Option<&rmpv::Value>,
    registry: &Arc<Mutex<TitanStreamRegistry>>,
) -> Option<ParsedHop1Quote> {
    let stream_data = stream_data?;
    let stream_id = map_get(stream_data, "id").and_then(value_as_u32)?;
    let stream_seq = map_get(stream_data, "seq").and_then(value_as_u32).unwrap_or(0);
    let payload = map_get(stream_data, "payload")?;
    let swap_quotes = map_get(payload, "SwapQuotes")?;
    let input_mint = map_get(swap_quotes, "inputMint")
        .and_then(value_as_pubkey_base58)?;
    let output_mint = map_get(swap_quotes, "outputMint")
        .and_then(value_as_pubkey_base58)?;
    let amount = map_get(swap_quotes, "amount")
        .and_then(value_as_u64)
        .unwrap_or(0);
    let quote_id = map_get(swap_quotes, "id").and_then(value_as_string);
    let quotes_map = map_get(swap_quotes, "quotes")?;
    let (provider, route) = select_best_route(quotes_map, swap_quotes)?;
    let size_lamports = registry
        .lock()
        .expect("stream registry lock")
        .size_for_stream(stream_id)
        .unwrap_or(amount);
    let (pair, base, mid) = mint_pair_label(&input_mint, &output_mint);
    let hop1 = extract_hop1_leg(route)?;
    Some(ParsedHop1Quote {
        pair,
        base,
        mid,
        size_lamports,
        provider,
        route_in_amount: map_get(route, "inAmount")
            .and_then(value_as_u64)
            .unwrap_or(amount),
        route_out_amount: map_get(route, "outAmount").and_then(value_as_u64)?,
        slippage_bps: map_get(route, "slippageBps")
            .and_then(value_as_u64)
            .map(|v| v as u16)
            .unwrap_or(0),
        quote_id,
        stream_id,
        stream_seq,
        hop1,
    })
}

fn select_best_route<'a>(
    quotes_map: &'a rmpv::Value,
    swap_quotes: &'a rmpv::Value,
) -> Option<(String, &'a rmpv::Value)> {
    let entries = value_as_map(quotes_map)?;
    if entries.is_empty() {
        return None;
    }

    if let Some(metadata) = map_get(swap_quotes, "metadata") {
        if let Some(winner) = map_get(metadata, "ExpectedWinner").and_then(value_as_string) {
            if let Some((_, route)) = entries.iter().find(|(k, _)| map_key_str(k) == Some(winner.as_str())) {
                return Some((winner, route));
            }
        }
    }

    entries
        .iter()
        .filter_map(|(k, route)| {
            let provider = map_key_str(k)?.to_string();
            let out = map_get(route, "outAmount").and_then(value_as_u64)?;
            Some((provider, route, out))
        })
        .max_by_key(|(_, _, out)| *out)
        .map(|(provider, route, _)| (provider, route))
}

fn extract_hop1_leg(route: &rmpv::Value) -> Option<Hop1Leg> {
    if let Some(steps) = map_get(route, "steps").and_then(value_as_array) {
        if let Some(first) = steps.first() {
            return Some(Hop1Leg {
                label: map_get(first, "label")
                    .and_then(value_as_string)
                    .unwrap_or_else(|| "unknown".to_string()),
                amm_key: map_get(first, "ammKey")
                    .and_then(value_as_pubkey_base58)
                    .unwrap_or_default(),
                input_mint: map_get(first, "inputMint")
                    .and_then(value_as_pubkey_base58)
                    .unwrap_or_default(),
                output_mint: map_get(first, "outputMint")
                    .and_then(value_as_pubkey_base58)
                    .unwrap_or_default(),
                in_amount: map_get(first, "inAmount").and_then(value_as_u64).unwrap_or(0),
                out_amount: map_get(first, "outAmount").and_then(value_as_u64).unwrap_or(0),
                context_slot: map_get(first, "contextSlot").and_then(value_as_u64),
            });
        }
    }

    // Direct route — whole route is hop-1.
    Some(Hop1Leg {
        label: "direct".to_string(),
        amm_key: String::new(),
        input_mint: String::new(),
        output_mint: String::new(),
        in_amount: map_get(route, "inAmount").and_then(value_as_u64).unwrap_or(0),
        out_amount: map_get(route, "outAmount").and_then(value_as_u64).unwrap_or(0),
        context_slot: map_get(route, "contextSlot").and_then(value_as_u64),
    })
}

async fn publish_hop1_quote(
    fanout: &RedisFanout,
    size_board: &Arc<Mutex<TitanSizeBoard>>,
    quote: ParsedHop1Quote,
    hop1_ttl_secs: u64,
) {
    let ts_ms = now_ms();
    let row = TitanHop1Row {
        schema: "mux.titan.hop1.v1",
        pair: quote.pair.clone(),
        base: quote.base.clone(),
        mid: quote.mid.clone(),
        size_lamports: quote.size_lamports,
        ts_ms,
        provider: quote.provider.clone(),
        route_in_amount: quote.route_in_amount,
        route_out_amount: quote.route_out_amount,
        slippage_bps: quote.slippage_bps,
        quote_id: quote.quote_id.clone(),
        stream_id: quote.stream_id,
        stream_seq: quote.stream_seq,
        hop1: quote.hop1.clone(),
    };

    {
        let mut board = size_board.lock().expect("size board lock");
        board.unsubscribed.remove(&quote.size_lamports);
        board.pins.insert(
            quote.size_lamports,
            SizeBoardPin {
                size_lamports: quote.size_lamports,
                hop1_age_ms: 0,
                hop1_fresh_ms: ts_ms,
                provider: quote.provider.clone(),
                venue_label: quote.hop1.label.clone(),
                route_in_amount: quote.route_in_amount,
                route_out_amount: quote.route_out_amount,
                status: None,
            },
        );
    }

    fanout
        .publish_titan_hop1(&row, &quote.pair, quote.size_lamports, hop1_ttl_secs)
        .await;
    fanout
        .publish_titan_size_board(&build_size_board_doc(size_board, &quote))
        .await;
    fanout
        .record_titan_message(crate::redis_fanout::TitanMessageOutcome::QuotePublished)
        .await;
}

fn build_size_board_doc(
    size_board: &Arc<Mutex<TitanSizeBoard>>,
    quote: &ParsedHop1Quote,
) -> TitanSizeBoardDoc {
    build_size_board_doc_with_pair(
        size_board,
        &quote.pair,
        &quote.base,
        &quote.mid,
    )
}

pub fn build_size_board_doc_with_pair(
    size_board: &Arc<Mutex<TitanSizeBoard>>,
    pair: &str,
    base: &str,
    mid: &str,
) -> TitanSizeBoardDoc {
    let board = size_board.lock().expect("size board lock");
    let now = now_ms();
    let mut pins: Vec<SizeBoardPin> = board.pins.values().cloned().collect();
    for pin in &mut pins {
        pin.hop1_age_ms = now.saturating_sub(pin.hop1_fresh_ms);
    }
    for &size_lamports in &board.hunt_sizes {
        if board.pins.contains_key(&size_lamports) {
            continue;
        }
        let status = if board.unsubscribed.contains_key(&size_lamports) {
            Some("unsubscribed".to_string())
        } else {
            None
        };
        pins.push(SizeBoardPin {
            size_lamports,
            hop1_age_ms: 0,
            hop1_fresh_ms: 0,
            provider: String::new(),
            venue_label: String::new(),
            route_in_amount: 0,
            route_out_amount: 0,
            status,
        });
    }
    pins.sort_by_key(|p| p.size_lamports);
    TitanSizeBoardDoc {
        schema: "mux.titan.size_board.v1",
        pair: pair.to_string(),
        base: base.to_string(),
        mid: mid.to_string(),
        updated_ms: now,
        pins,
    }
}

pub async fn publish_size_board_snapshot(
    fanout: &RedisFanout,
    size_board: &Arc<Mutex<TitanSizeBoard>>,
    pair: &str,
    base: &str,
    mid: &str,
) {
    let doc = build_size_board_doc_with_pair(size_board, pair, base, mid);
    fanout.publish_titan_size_board(&doc).await;
}

fn map_contains_key(value: &rmpv::Value, key: &str) -> bool {
    value_as_map(value)
        .map(|map| map.iter().any(|(k, _)| map_key_str(k) == Some(key)))
        .unwrap_or(false)
}

fn map_get<'a>(value: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    value_as_map(value)?
        .iter()
        .find(|(k, _)| map_key_str(k) == Some(key))
        .map(|(_, v)| v)
}

fn map_key_str(key: &rmpv::Value) -> Option<&str> {
    key.as_str().map(|s| s.as_ref())
}

fn value_as_map(value: &rmpv::Value) -> Option<&[(rmpv::Value, rmpv::Value)]> {
    match value {
        rmpv::Value::Map(map) => Some(map.as_slice()),
        _ => None,
    }
}

fn value_as_array(value: &rmpv::Value) -> Option<&[rmpv::Value]> {
    match value {
        rmpv::Value::Array(arr) => Some(arr.as_slice()),
        _ => None,
    }
}

fn value_as_u64(value: &rmpv::Value) -> Option<u64> {
    match value {
        rmpv::Value::Integer(i) => i.as_u64(),
        _ => None,
    }
}

fn value_as_u32(value: &rmpv::Value) -> Option<u32> {
    value_as_u64(value).and_then(|v| u32::try_from(v).ok())
}

fn value_as_string(value: &rmpv::Value) -> Option<String> {
    match value {
        rmpv::Value::String(s) => s.as_str().map(|s| s.to_string()),
        _ => None,
    }
}

fn value_as_pubkey_base58(value: &rmpv::Value) -> Option<String> {
    pubkey_bytes(value).map(|bytes| bs58::encode(bytes).into_string())
}

fn pubkey_bytes(value: &rmpv::Value) -> Option<[u8; 32]> {
    match value {
        rmpv::Value::Binary(bytes) if bytes.len() == 32 => {
            bytes.clone().try_into().ok()
        }
        rmpv::Value::Array(items) if items.len() == 32 => {
            let mut out = [0u8; 32];
            for (idx, item) in items.iter().enumerate() {
                out[idx] = u8::try_from(value_as_u64(item)?).ok()?;
            }
            Some(out)
        }
        rmpv::Value::String(s) => {
            let text = s.as_str()?.trim();
            parse_wallet_pubkey(text).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestStreamData {
        #[serde(rename = "StreamData")]
        stream_data: TestStreamBody,
    }

    #[derive(Serialize)]
    struct TestStreamBody {
        id: u32,
        seq: u32,
        payload: TestPayload,
    }

    #[derive(Serialize)]
    struct TestPayload {
        #[serde(rename = "SwapQuotes")]
        swap_quotes: TestSwapQuotes,
    }

    #[derive(Serialize)]
    struct TestSwapQuotes {
        id: String,
        #[serde(rename = "inputMint")]
        input_mint: [u8; 32],
        #[serde(rename = "outputMint")]
        output_mint: [u8; 32],
        amount: u64,
        quotes: HashMap<String, TestRoute>,
    }

    #[derive(Serialize)]
    struct TestRoute {
        #[serde(rename = "inAmount")]
        in_amount: u64,
        #[serde(rename = "outAmount")]
        out_amount: u64,
        #[serde(rename = "slippageBps")]
        slippage_bps: u16,
        steps: Vec<TestStep>,
    }

    #[derive(Serialize)]
    struct TestStep {
        #[serde(rename = "ammKey")]
        amm_key: [u8; 32],
        label: String,
        #[serde(rename = "inputMint")]
        input_mint: [u8; 32],
        #[serde(rename = "outputMint")]
        output_mint: [u8; 32],
        #[serde(rename = "inAmount")]
        in_amount: u64,
        #[serde(rename = "outAmount")]
        out_amount: u64,
    }

    fn sol_mint_bytes() -> [u8; 32] {
        parse_wallet_pubkey(SOL_MINT).unwrap()
    }

    fn usdc_mint_bytes() -> [u8; 32] {
        parse_wallet_pubkey(USDC_MINT).unwrap()
    }

    #[test]
    fn parse_hunt_sizes_defaults_and_dedupes() {
        assert_eq!(parse_hunt_sizes(None).len(), DEFAULT_HUNT_SIZES_LAMPORTS.len());
        let sizes = parse_hunt_sizes(Some("1000000000,250000000,1000000000"));
        assert_eq!(sizes, vec![250_000_000, 1_000_000_000]);
    }

    #[test]
    fn extracts_hop1_from_stream_data_fixture() {
        let mut quotes = HashMap::new();
        quotes.insert(
            "test_provider".to_string(),
            TestRoute {
                in_amount: 1_000_000_000,
                out_amount: 150_000_000,
                slippage_bps: 50,
                steps: vec![TestStep {
                    amm_key: [7u8; 32],
                    label: "Raydium AMM".to_string(),
                    input_mint: sol_mint_bytes(),
                    output_mint: usdc_mint_bytes(),
                    in_amount: 1_000_000_000,
                    out_amount: 150_000_000,
                }],
            },
        );

        let msg = TestStreamData {
            stream_data: TestStreamBody {
                id: 42,
                seq: 7,
                payload: TestPayload {
                    swap_quotes: TestSwapQuotes {
                        id: "q-1".to_string(),
                        input_mint: sol_mint_bytes(),
                        output_mint: usdc_mint_bytes(),
                        amount: 1_000_000_000,
                        quotes,
                    },
                },
            },
        };

        let buf = rmp_serde::to_vec_named(&msg).unwrap();

        let registry = Arc::new(Mutex::new(TitanStreamRegistry::default()));
        {
            let mut reg = registry.lock().unwrap();
            reg.register_pending_request(2, 1_000_000_000);
            reg.confirm_stream(2, 42);
        }

        let decoded = rmpv::decode::read_value(&mut std::io::Cursor::new(&buf)).unwrap();
        assert!(
            map_contains_key(&decoded, "StreamData"),
            "fixture must contain StreamData key, got {decoded:?}"
        );
        let stream_data = map_get(&decoded, "StreamData");
        let payload = stream_data.and_then(|sd| map_get(sd, "payload"));
        let swap_quotes = payload.and_then(|p| map_get(p, "SwapQuotes"));
        let quotes_map = swap_quotes.and_then(|sq| map_get(sq, "quotes"));
        assert!(quotes_map.is_some(), "fixture must contain quotes map");

        let parsed = parse_stream_data_hop1(stream_data, &registry).expect("hop-1 quote");

        assert_eq!(parsed.pair, "SOL-USDC");
        assert_eq!(parsed.provider, "test_provider");
        assert_eq!(parsed.hop1.label, "Raydium AMM");
        assert_eq!(parsed.route_out_amount, 150_000_000);
    }
}
