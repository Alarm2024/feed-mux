use feed_mux::redis_fanout::{mux_keys, now_ms, RedisFanout, TitanMessageOutcome};
use feed_mux::titan_board::{hop1_redis_key, Hop1QuoteDelivery, SizeBoard, SizeBoardEntry, SOL_MINT, USDC_MINT};
use redis::AsyncCommands;
use serial_test::serial;
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
                "note: optional mux Redis integration tests skipped (REDIS_URL unreachable); \
                 cargo test still passes — run `docker compose up redis` to exercise live coverage"
            );
        });
        return None;
    }
    let client = redis::Client::open(url.as_str()).ok()?;
    let conn = client.get_multiplexed_async_connection().await.ok()?;
    Some((url, conn))
}

#[tokio::test]
#[serial]
async fn boot_reset_zeroes_stale_counters_before_heartbeat() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:boot:{suffix}");
    let fanout = RedisFanout::connect(Some(url.clone()), channel, false).await;

    let stale_ts = "9999999999999";
    let _: () = conn.set(mux_keys::TITAN_FRAMES, "4621").await.unwrap();
    let _: () = conn.set(mux_keys::TITAN_LAST_FRAME_MS, stale_ts).await.unwrap();
    let _: () = conn.set(mux_keys::TITAN_FRESHEST_MS, stale_ts).await.unwrap();
    let _: () = conn.set(mux_keys::META_TITAN_UP, "1").await.unwrap();
    let _: () = conn.set(mux_keys::META_TRITON_UP, "1").await.unwrap();
    let _: () = conn.set(mux_keys::TITAN_PAIRS_LIVE, "1").await.unwrap();

    fanout.reset_mux_state_at_boot().await;

    let frames: String = conn.get(mux_keys::TITAN_FRAMES).await.unwrap();
    let last_frame_ms: String = conn.get(mux_keys::TITAN_LAST_FRAME_MS).await.unwrap();
    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    let triton_up: String = conn.get(mux_keys::META_TRITON_UP).await.unwrap();
    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert_eq!(frames, "0");
    assert_eq!(last_frame_ms, "0");
    assert_eq!(titan_up, "0");
    assert_eq!(triton_up, "0");
    assert_eq!(pairs_live, "0");

    fanout.heartbeat().await;
    let heartbeat: String = conn.get(mux_keys::META_HEARTBEAT_MS).await.unwrap();
    assert!(heartbeat.parse::<u64>().unwrap() > 0);
    let frames_after: String = conn.get(mux_keys::TITAN_FRAMES).await.unwrap();
    assert_eq!(frames_after, "0");
}

#[tokio::test]
#[serial]
async fn connect_alone_does_not_set_pairs_live() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:connect:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;

    fanout.reset_mux_state_at_boot().await;
    fanout.set_titan_upstream_up(true).await;

    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    assert_eq!(titan_up, "1");

    let pairs_live: Option<String> = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert!(
        pairs_live.as_deref() == Some("0"),
        "handshake must not claim live pairs; got {pairs_live:?}"
    );
}

#[tokio::test]
#[serial]
async fn quote_success_increments_decoded_not_served() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:quote:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;

    fanout.reset_mux_state_at_boot().await;
    fanout
        .record_titan_message(TitanMessageOutcome::QuotePublished)
        .await;

    let frames: i64 = conn.get(mux_keys::TITAN_FRAMES).await.unwrap();
    let decoded: i64 = conn.get(mux_keys::TITAN_DECODED).await.unwrap();
    let served: String = conn.get(mux_keys::TITAN_SERVED).await.unwrap();
    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();

    assert_eq!(frames, 1);
    assert_eq!(decoded, 1);
    assert_eq!(served, "0", "served must not mirror decode count");
    assert_eq!(pairs_live, "1");
}

#[tokio::test]
#[serial]
async fn live_mux_metrics_disconnect_clears_titan_up_and_pairs_live() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:disconnect:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;

    fanout.reset_mux_state_at_boot().await;
    fanout
        .record_titan_message(TitanMessageOutcome::QuotePublished)
        .await;
    fanout.set_titan_upstream_up(false).await;

    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert_eq!(titan_up, "0");
    assert_eq!(pairs_live, "0");
}

#[tokio::test]
#[serial]
async fn triton_up_and_frames_track_upstream_liveness() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:triton:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;

    fanout.reset_mux_state_at_boot().await;
    fanout.set_triton_upstream_up(true).await;
    fanout.record_triton_frame().await;

    let triton_up: String = conn.get(mux_keys::META_TRITON_UP).await.unwrap();
    let frames: i64 = conn.get(mux_keys::TRITON_FRAMES).await.unwrap();
    let last_ms: String = conn.get(mux_keys::TRITON_LAST_FRAME_MS).await.unwrap();
    assert_eq!(triton_up, "1");
    assert_eq!(frames, 1);
    assert!(last_ms.parse::<u64>().unwrap() > 0);

    fanout.set_triton_upstream_up(false).await;
    let triton_up_after: String = conn.get(mux_keys::META_TRITON_UP).await.unwrap();
    assert_eq!(triton_up_after, "0");
}

#[tokio::test]
#[serial]
async fn hop1_quote_and_size_board_are_published() {
    let Some((url, mut conn)) = require_redis().await else {
        return;
    };

    let suffix = now_ms();
    let channel = format!("feed:350:test:hop1:{suffix}");
    let fanout = RedisFanout::connect(Some(url), channel, false).await;
    fanout.reset_mux_state_at_boot().await;

    let size = 2_500_000_035_u64;
    let quote_ms = now_ms();
    let mut board = SizeBoard::new(quote_ms);
    board.upsert(
        SizeBoardEntry {
            size_lamports: size,
            hop1_age_ms: 0,
            hop1_last_ms: quote_ms,
        },
        quote_ms,
    );
    let hop1 = Hop1QuoteDelivery {
        size_lamports: size,
        input_mint: SOL_MINT.to_string(),
        output_mint: USDC_MINT.to_string(),
        hop: 1,
        out_amount: Some(42_000_000),
        provider: Some("Titan".to_string()),
        quote_ms,
        age_ms: 0,
        source: "titan_ws".to_string(),
    };
    fanout.publish_hop1_quote(&hop1, &board).await;

    let hop1_key = hop1_redis_key(size);
    let hop1_json: String = conn.get(&hop1_key).await.unwrap();
    let board_json: String = conn.get(mux_keys::TITAN_SIZE_BOARD).await.unwrap();

    let parsed_hop1: Hop1QuoteDelivery = serde_json::from_str(&hop1_json).unwrap();
    assert_eq!(parsed_hop1.size_lamports, size);
    assert_eq!(parsed_hop1.hop, 1);
    assert_eq!(parsed_hop1.out_amount, Some(42_000_000));

    let parsed_board: SizeBoard = serde_json::from_str(&board_json).unwrap();
    assert_eq!(parsed_board.sizes.len(), 1);
    assert_eq!(parsed_board.sizes[0].size_lamports, size);

    let _: () = conn.del(hop1_key).await.unwrap();
}
