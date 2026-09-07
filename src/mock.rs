use crate::config::Config;
use crate::redis_fanout::{FeedPayload, RedisFanout};
use chrono::Utc;

pub async fn spawn_mock_publisher(config: Config, fanout: RedisFanout) {
    if config.mock_publish_interval_secs == 0 {
        tracing::info!("mock publish loop disabled (MOCK_PUBLISH_INTERVAL_SECS=0)");
        return;
    }

    let interval = config.mock_publish_interval_secs;
    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
            tick += 1;

            let payload = FeedPayload {
                event: "mock.tick".to_string(),
                source: "feed-mux".to_string(),
                ts: Utc::now().to_rfc3339(),
                data: Some(serde_json::json!({
                    "tick": tick,
                    "dry_run": fanout.is_dry_run(),
                    "note": "MVP mock event for Bot 350 / python 35-bot consumers"
                })),
            };

            match fanout.publish(&payload).await {
                Ok(result) => {
                    tracing::info!(
                        tick,
                        dry_run = result.dry_run,
                        bytes = result.payload_bytes,
                        "mock publish tick"
                    );
                }
                Err(e) => {
                    tracing::warn!(tick, error = %e, "mock publish failed");
                }
            }
        }
    });
}
