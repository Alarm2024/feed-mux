use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum FanoutError {
    #[error("redis not configured")]
    NotConfigured,
    #[error("redis publish failed: {0}")]
    Publish(String),
}

#[derive(Clone)]
pub struct RedisFanout {
    channel: String,
    dry_run: bool,
    conn: Arc<Mutex<Option<ConnectionManager>>>,
}

impl RedisFanout {
    pub async fn connect(redis_url: Option<String>, channel: String, dry_run: bool) -> Self {
        let conn = if dry_run {
            None
        } else if let Some(url) = redis_url {
            match redis::Client::open(url.as_str()) {
                Ok(client) => match ConnectionManager::new(client).await {
                    Ok(manager) => {
                        tracing::info!(channel = %channel, "redis fan-out connected");
                        Some(manager)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "redis fan-out connection failed; continuing without publish");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "invalid redis URL; continuing without publish");
                    None
                }
            }
        } else {
            None
        };

        Self {
            channel,
            dry_run,
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    pub async fn publish(&self, payload: &FeedPayload) -> Result<PublishResult, FanoutError> {
        let json = serde_json::to_string(payload).map_err(|e| FanoutError::Publish(e.to_string()))?;

        if self.dry_run {
            tracing::info!(
                channel = %self.channel,
                event = %payload.event,
                source = %payload.source,
                "dry-run mock publish (redis skipped)"
            );
            return Ok(PublishResult {
                channel: self.channel.clone(),
                dry_run: true,
                delivered: false,
                payload_bytes: json.len(),
            });
        }

        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().ok_or(FanoutError::NotConfigured)?;

        conn.publish::<_, _, i64>(&self.channel, &json)
            .await
            .map_err(|e| FanoutError::Publish(e.to_string()))?;

        tracing::debug!(
            channel = %self.channel,
            event = %payload.event,
            "redis fan-out published"
        );

        Ok(PublishResult {
            channel: self.channel.clone(),
            dry_run: false,
            delivered: true,
            payload_bytes: json.len(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FeedPayload {
    pub event: String,
    pub source: String,
    pub ts: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishResult {
    pub channel: String,
    pub dry_run: bool,
    pub delivered: bool,
    pub payload_bytes: usize,
}
