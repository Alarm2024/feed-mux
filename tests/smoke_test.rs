use feed_mux::config::Config;
use feed_mux::http::{router, AppState};
use feed_mux::redis_fanout::RedisFanout;
use feed_mux::upstream::UpstreamHub;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn config_defaults_to_dry_run() {
    std::env::set_var("DRY_RUN", "");
    std::env::remove_var("DRY_RUN");
    let config = Config::from_env();
    assert!(config.dry_run, "DRY_RUN should default to true");
}

#[tokio::test]
async fn redacted_summary_never_contains_redis_password() {
    let config = Config {
        bind_addr: "127.0.0.1:8787".to_string(),
        dry_run: true,
        redis_url: Some("redis://:super-secret-password@127.0.0.1:6379/0".to_string()),
        redis_channel: "feed:350".to_string(),
        enable_chainstack: false,
        chainstack_rpc_url: None,
        chainstack_ws_url: None,
        enable_helius: false,
        helius_rpc_url: None,
        enable_triton_grpc: false,
        triton_grpc_url: None,
        triton_rate_limit_rps: 25,
        enable_titan_ws: false,
        titan_ws_url: None,
        titan_wallet_pubkey: None,
        titan_rate_limit_rps: 15,
        titan_local_bind: "127.0.0.1:19001".to_string(),
        mock_publish_interval_secs: 0,
    };
    let summary = config.redacted_summary();
    assert!(!summary.contains("super-secret-password"));
    assert!(summary.contains("<set>"));
}

#[tokio::test]
async fn health_and_mock_publish_smoke() {
    std::env::set_var("BIND_ADDR", "127.0.0.1:0");
    std::env::set_var("DRY_RUN", "true");
    std::env::set_var("MOCK_PUBLISH_INTERVAL_SECS", "0");
    std::env::remove_var("REDIS_URL");

    let config = Config::from_env();
    let fanout = RedisFanout::connect(
        config.redis_url.clone(),
        config.redis_channel.clone(),
        config.dry_run,
    )
    .await;
    let upstreams = Arc::new(UpstreamHub::from_config(&config));
    let state = AppState {
        config: config.clone(),
        fanout,
        upstreams,
    };
    let app = router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    let health: serde_json::Value = client
        .get(format!("{}/health", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["dry_run"], true);

    let publish: serde_json::Value = client
        .post(format!("{}/publish", base))
        .json(&serde_json::json!({
            "event": "test.smoke",
            "source": "integration-test"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(publish["ok"], true);
    assert_eq!(publish["result"]["dry_run"], true);
}

#[test]
fn titan_live_requires_wallet_pubkey() {
    let config = Config {
        bind_addr: "127.0.0.1:8787".to_string(),
        dry_run: false,
        redis_url: None,
        redis_channel: "feed:350".to_string(),
        enable_chainstack: false,
        chainstack_rpc_url: None,
        chainstack_ws_url: None,
        enable_helius: false,
        helius_rpc_url: None,
        enable_triton_grpc: false,
        triton_grpc_url: None,
        triton_rate_limit_rps: 25,
        enable_titan_ws: true,
        titan_ws_url: Some("wss://example.test/api/v1/ws".to_string()),
        titan_wallet_pubkey: None,
        titan_rate_limit_rps: 15,
        titan_local_bind: "127.0.0.1:19001".to_string(),
        mock_publish_interval_secs: 0,
    };

    let upstream = feed_mux::upstream::titan::TitanWsUpstream::new(&config);
    let status = upstream.status();
    assert_eq!(status.mode, "error/wallet-pubkey-required");
    assert!(!status.connected);
}

#[test]
fn titan_dry_run_stays_stub_without_wallet_pubkey() {
    let config = Config {
        bind_addr: "127.0.0.1:8787".to_string(),
        dry_run: true,
        redis_url: None,
        redis_channel: "feed:350".to_string(),
        enable_chainstack: false,
        chainstack_rpc_url: None,
        chainstack_ws_url: None,
        enable_helius: false,
        helius_rpc_url: None,
        enable_triton_grpc: false,
        triton_grpc_url: None,
        triton_rate_limit_rps: 25,
        enable_titan_ws: true,
        titan_ws_url: Some("wss://example.test/api/v1/ws".to_string()),
        titan_wallet_pubkey: None,
        titan_rate_limit_rps: 15,
        titan_local_bind: "127.0.0.1:19001".to_string(),
        mock_publish_interval_secs: 0,
    };

    let upstream = feed_mux::upstream::titan::TitanWsUpstream::new(&config);
    let status = upstream.status();
    assert_eq!(status.mode, "stub/dry-run");
    assert!(!status.connected);
}
