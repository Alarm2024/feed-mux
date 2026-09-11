use feed_mux::redis_fanout::{mux_keys, now_ms, RedisFanout, TitanMessageOutcome};
use redis::AsyncCommands;

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

#[tokio::test]
async fn live_mux_metrics_update_redis_keys() {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://:changeme-local-only@127.0.0.1:6379/0".to_string());
    if !redis_available(&url).await {
        eprintln!("skipping live mux metrics test — redis unavailable at REDIS_URL");
        return;
    }

    let suffix = now_ms();
    let channel = format!("feed:350:test:{suffix}");
    let fanout = RedisFanout::connect(Some(url.clone()), channel, false).await;

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    for key in [
        mux_keys::TITAN_FRAMES,
        mux_keys::TITAN_DECODED,
        mux_keys::TITAN_ERRORS,
        mux_keys::TITAN_SERVED,
        mux_keys::TITAN_FELL_THROUGH,
    ] {
        let _: () = conn.del(key).await.unwrap();
    }

    fanout.heartbeat().await;
    fanout.set_titan_upstream_up(true).await;

    let heartbeat: String = conn.get(mux_keys::META_HEARTBEAT_MS).await.unwrap();
    assert!(heartbeat.parse::<u64>().unwrap() > 0);

    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    assert_eq!(titan_up, "1");

    let pairs_live: String = conn.get(mux_keys::TITAN_PAIRS_LIVE).await.unwrap();
    assert_eq!(pairs_live, "1");

    fanout.record_titan_decode_error().await;
    let errors: i64 = conn.get(mux_keys::TITAN_ERRORS).await.unwrap();
    assert_eq!(errors, 1);

    fanout
        .record_titan_message(TitanMessageOutcome::QuotePublished)
        .await;
    let frames: i64 = conn.get(mux_keys::TITAN_FRAMES).await.unwrap();
    let decoded: i64 = conn.get(mux_keys::TITAN_DECODED).await.unwrap();
    let served: i64 = conn.get(mux_keys::TITAN_SERVED).await.unwrap();
    assert_eq!(frames, 1);
    assert_eq!(decoded, 1);
    assert_eq!(served, 1);

    let last_frame_ms: String = conn.get(mux_keys::TITAN_LAST_FRAME_MS).await.unwrap();
    let freshest_ms: String = conn.get(mux_keys::TITAN_FRESHEST_MS).await.unwrap();
    assert_eq!(last_frame_ms, freshest_ms);

    fanout.set_titan_upstream_up(false).await;
    let titan_up: String = conn.get(mux_keys::META_TITAN_UP).await.unwrap();
    assert_eq!(titan_up, "0");
}
