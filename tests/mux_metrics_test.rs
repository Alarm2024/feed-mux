use feed_mux::redis_fanout::{mux_keys, now_ms, RedisFanout, TitanMessageOutcome};
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
    let _: () = conn.set(mux_keys::TITAN_PAIRS_LIVE, "1").await.unwrap();

    fanout.reset_titan_state_at_boot().await;

    let frames: String = conn.get(mux_keys::TITAN_FRAMES).await.unwrap();
    let last_frame_ms: String = conn.get(mux_keys::TITAN_LAST_FRAME_MS).await.unwrap();
    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert_eq!(frames, "0");
    assert_eq!(last_frame_ms, "0");
    assert_eq!(titan_up, "0");
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

    fanout.reset_titan_state_at_boot().await;
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

    fanout.reset_titan_state_at_boot().await;
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

    fanout.reset_titan_state_at_boot().await;
    fanout
        .record_titan_message(TitanMessageOutcome::QuotePublished)
        .await;
    fanout.set_titan_upstream_up(false).await;

    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert_eq!(titan_up, "0");
    assert_eq!(pairs_live, "0");
}
