pub mod config;
pub mod http;
pub mod mock;
pub mod rate_limit;
pub mod redis_fanout;
pub mod titan_local;
pub mod upstream;

use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use config::Config;
use http::{router, AppState};
use mock::spawn_mock_publisher;
use redis_fanout::RedisFanout;
use titan_local::TitanLocalRelay;
use upstream::UpstreamHub;

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(config = %config.redacted_summary(), "feed-mux starting");

    let fanout = RedisFanout::connect(
        config.redis_url.clone(),
        config.redis_channel.clone(),
        config.dry_run,
    )
    .await;

    let upstreams = Arc::new(UpstreamHub::from_config(&config));

    let titan_local_relay = TitanLocalRelay::new();
    titan_local_relay.clone().spawn_server(
        config.titan_local_bind.clone(),
        config.enable_titan_ws,
    );

    if config.dry_run {
        tracing::info!("DRY_RUN=true — upstream stubs and redis publish are mocked by default");
        spawn_mock_publisher(config.clone(), fanout.clone()).await;
    } else {
        fanout.reset_titan_state_at_boot().await;
        upstreams.spawn_live(fanout.clone(), Some(titan_local_relay));

        let heartbeat_fanout = fanout.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                heartbeat_fanout.heartbeat().await;
            }
        });
    }

    let upstreams_poll = upstreams.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            upstreams_poll.poll_stubs().await;
        }
    });

    let state = AppState {
        config: config.clone(),
        fanout,
        upstreams,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;

    tracing::info!(addr = %config.bind_addr, "listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
