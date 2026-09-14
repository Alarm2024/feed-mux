use feed_mux::redis_fanout::{mux_keys, now_ms, RedisFanout};
use feed_mux::titan_quote::{
    handle_titan_server_message, Hop1Leg, TitanHop1Row, TitanSizeBoard, TitanStreamRegistry,
};
use redis::AsyncCommands;
use serde::Serialize;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::Once;

static SKIP_NOTE: Once = Once::new();

async fn redis_available(url: &str) -> bool {
    let Ok(client) = redis::Client::open(url) else {
        return false;
    };
    let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
        return false;
    };
    redis::cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .map(|p| p == "PONG")
        .unwrap_or(false)
}

fn test_redis_url() -> String {
    std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://:changeme-local-only@127.0.0.1:6379/0".to_string())
}

async fn require_redis() -> Option<(String, redis::aio::MultiplexedConnection)> {
    let url = test_redis_url();
    if !redis_available(&url).await {
        SKIP_NOTE.call_once(|| {
            eprintln!(
                "note: optional hop-1 Redis integration tests skipped (REDIS_URL unreachable)"
            );
        });
        return None;
    }
    let client = redis::Client::open(url.as_str()).ok()?;
    let conn = client.get_multiplexed_async_connection().await.ok()?;
    Some((url, conn))
}

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

fn sol_mint() -> [u8; 32] {
    feed_mux::config::parse_wallet_pubkey(feed_mux::titan_quote::SOL_MINT).unwrap()
}

fn usdc_mint() -> [u8; 32] {
    feed_mux::config::parse_wallet_pubkey(feed_mux::titan_quote::USDC_MINT).unwrap()
}

fn sample_stream_msg() -> Vec<u8> {
    let mut quotes = HashMap::new();
    quotes.insert(
        "fixture_provider".to_string(),
        TestRoute {
            in_amount: 2_500_000_000,
            out_amount: 375_000_000,
            slippage_bps: 50,
            steps: vec![TestStep {
                amm_key: [9u8; 32],
                label: "Orca Whirlpool".to_string(),
                input_mint: sol_mint(),
                output_mint: usdc_mint(),
                in_amount: 2_500_000_000,
                out_amount: 375_000_000,
            }],
        },
    );

    let msg = TestStreamData {
        stream_data: TestStreamBody {
            id: 11,
            seq: 3,
            payload: TestPayload {
                swap_quotes: TestSwapQuotes {
                    id: "fixture-q".to_string(),
                    input_mint: sol_mint(),
                    output_mint: usdc_mint(),
                    amount: 2_500_000_000,
                    quotes,
                },
            },
        },
    };

    rmp_serde::to_vec_named(&msg).unwrap()
}

#[tokio::test]
#[serial]
async fn hop1_publish_sets_size_board_and_hop1_served() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:hop1:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;
    fanout.reset_titan_state_at_boot().await;

    let registry = Arc::new(Mutex::new(TitanStreamRegistry::default()));
    {
        let mut reg = registry.lock().unwrap();
        reg.register_pending_request(2, 2_500_000_000);
        reg.confirm_stream(2, 11);
    }
    let size_board = Arc::new(Mutex::new(TitanSizeBoard::default()));

    let kind = handle_titan_server_message(
        &sample_stream_msg(),
        &fanout,
        &registry,
        &size_board,
        2,
    )
    .await;
    assert_eq!(kind, feed_mux::titan_quote::TitanMessageKind::Hop1Published);

    let hop1_served: i64 = conn.get(mux_keys::TITAN_HOP1_SERVED).await.unwrap();
    assert_eq!(hop1_served, 1);

    let served: String = conn.get(mux_keys::TITAN_SERVED).await.unwrap();
    assert_eq!(served, "0", "proxy served must stay separate from hop-1");

    let board: String = conn.get(mux_keys::TITAN_SIZE_BOARD).await.unwrap();
    assert!(board.contains("mux.titan.size_board.v1"));
    assert!(board.contains("2500000000"));
    assert!(board.contains("Orca Whirlpool"));

    let row_key = mux_keys::hop1_row_key("SOL-USDC", 2_500_000_000);
    let row: String = conn.get(&row_key).await.unwrap();
    assert!(row.contains("fixture_provider") || row.contains("Orca Whirlpool"));
    assert!(row.contains("mux.titan.hop1.v1"));
}

#[test]
fn hop1_row_json_shape_for_350_hunt() {
    let row = TitanHop1Row {
        schema: "mux.titan.hop1.v1",
        pair: "SOL-USDC".to_string(),
        base: "SOL".to_string(),
        mid: "USDC".to_string(),
        size_lamports: 2_500_000_000,
        ts_ms: 1_700_000_000_000,
        provider: "fixture_provider".to_string(),
        route_in_amount: 2_500_000_000,
        route_out_amount: 375_000_000,
        slippage_bps: 50,
        quote_id: Some("fixture-q".to_string()),
        stream_id: 11,
        stream_seq: 3,
        hop1: Hop1Leg {
            label: "Orca Whirlpool".to_string(),
            amm_key: bs58::encode([9u8; 32]).into_string(),
            input_mint: feed_mux::titan_quote::SOL_MINT.to_string(),
            output_mint: feed_mux::titan_quote::USDC_MINT.to_string(),
            in_amount: 2_500_000_000,
            out_amount: 375_000_000,
            context_slot: None,
        },
    };

    let json = serde_json::to_string(&row).unwrap();
    assert!(json.contains("\"size_lamports\":2500000000"));
    assert!(json.contains("\"pair\":\"SOL-USDC\""));
    assert!(json.contains("\"schema\":\"mux.titan.hop1.v1\""));
    assert!(
        !json.contains("hop1_age_ms"),
        "hop-1 row must not freeze age; Bot 350 uses ts_ms + TTL"
    );
}

#[tokio::test]
#[serial]
async fn unparseable_stream_data_does_not_write_hop1_key() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:bad:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;
    fanout.reset_titan_state_at_boot().await;

    let registry = Arc::new(Mutex::new(TitanStreamRegistry::default()));
    let size_board = Arc::new(Mutex::new(TitanSizeBoard::default()));

    #[derive(Serialize)]
    struct BadStreamData {
        #[serde(rename = "StreamData")]
        stream_data: BadStreamBody,
    }
    #[derive(Serialize)]
    struct BadStreamBody {
        id: u32,
        seq: u32,
        payload: BadPayload,
    }
    #[derive(Serialize)]
    struct BadPayload {
        #[serde(rename = "NotSwapQuotes")]
        junk: (),
    }

    // Valid msgpack but not a parseable SwapQuotes hop-1 frame.
    let garbage = rmp_serde::to_vec_named(&BadStreamData {
        stream_data: BadStreamBody {
            id: 1,
            seq: 0,
            payload: BadPayload { junk: () },
        },
    })
    .unwrap();

    let kind = handle_titan_server_message(
        &garbage,
        &fanout,
        &registry,
        &size_board,
        2,
    )
    .await;
    assert_eq!(kind, feed_mux::titan_quote::TitanMessageKind::StreamDataNoHop1);

    let hop1_served: i64 = conn.get(mux_keys::TITAN_HOP1_SERVED).await.unwrap();
    assert_eq!(hop1_served, 0);

    let row_key = mux_keys::hop1_row_key("SOL-USDC", 2_500_000_000);
    let row: Option<String> = conn.get(&row_key).await.unwrap();
    assert!(row.is_none(), "unparseable StreamData must not write hop1 key");
}

#[tokio::test]
#[serial]
async fn hop1_row_has_ttl() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:ttl:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;
    fanout.reset_titan_state_at_boot().await;

    let registry = Arc::new(Mutex::new(TitanStreamRegistry::default()));
    {
        let mut reg = registry.lock().unwrap();
        reg.register_pending_request(2, 2_500_000_000);
        reg.confirm_stream(2, 11);
    }
    let size_board = Arc::new(Mutex::new(TitanSizeBoard::default()));

    let ttl_secs = 3_u64;
    handle_titan_server_message(
        &sample_stream_msg(),
        &fanout,
        &registry,
        &size_board,
        ttl_secs,
    )
    .await;

    let row_key = mux_keys::hop1_row_key("SOL-USDC", 2_500_000_000);
    let ttl: i64 = redis::cmd("TTL")
        .arg(&row_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        ttl > 0 && ttl <= ttl_secs as i64,
        "hop1 key must have set_ex TTL, got {ttl}"
    );
}
