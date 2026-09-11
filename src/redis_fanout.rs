use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// Redis keys read by Bot 350 eyes (legacy Python mux schema).
pub mod mux_keys {
    pub const TITAN_FRAMES: &str = "mux:titan:frames";
    pub const TITAN_DECODED: &str = "mux:titan:decoded";
    pub const TITAN_ERRORS: &str = "mux:titan:errors";
    pub const TITAN_SERVED: &str = "mux:titan:served";
    pub const TITAN_FELL_THROUGH: &str = "mux:titan:fell_through";
    pub const TITAN_PAIRS_LIVE: &str = "mux:titan:pairs_live";
    pub const TITAN_LAST_FRAME_MS: &str = "mux:titan:last_frame_ms";
    pub const TITAN_FRESHEST_MS: &str = "mux:titan:freshest_ms";
    pub const META_HEARTBEAT_MS: &str = "mux:meta:heartbeat_ms";
    pub const META_TITAN_UP: &str = "mux:meta:titan_up";
}

#[derive(Debug, thiserror::Error)]
pub enum FanoutError {
    #[error("redis not configured")]
    NotConfigured,
    #[error("redis publish failed: {0}")]
    Publish(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TitanMessageOutcome {
    QuotePublished,
    RpcError,
    Unhandled,
}

#[derive(Clone)]
pub struct RedisFanout {
    channel: String,
    dry_run: bool,
    conn: Arc<Mutex<Option<ConnectionManager>>>,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

    /// Periodic liveness tick for Bot 350 eyes (`mux:meta:heartbeat_ms`).
    pub async fn heartbeat(&self) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let ts = now_ms().to_string();
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::META_HEARTBEAT_MS, &ts)
            .await
        {
            tracing::warn!(error = %e, "failed to write mux heartbeat");
        }
    }

    /// Reflect Titan WS upstream connectivity for eyes (`mux:meta:titan_up`, `mux:titan:pairs_live`).
    pub async fn set_titan_upstream_up(&self, up: bool) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let flag = if up { "1" } else { "0" };
        let pairs = if up { "1" } else { "0" };
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::META_TITAN_UP, flag)
            .await
        {
            tracing::warn!(error = %e, up, "failed to write mux titan_up");
        }
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::TITAN_PAIRS_LIVE, pairs)
            .await
        {
            tracing::warn!(error = %e, up, "failed to write mux pairs_live");
        }
    }

    /// Record decode failure on a Titan WS binary frame.
    pub async fn record_titan_decode_error(&self) {
        self.incr(mux_keys::TITAN_ERRORS).await;
    }

    /// Record outcome after msgpack decode of a Titan WS message.
    pub async fn record_titan_message(&self, outcome: TitanMessageOutcome) {
        if self.dry_run {
            return;
        }
        match outcome {
            TitanMessageOutcome::QuotePublished => {
                let ts = now_ms();
                self.record_titan_quote_success(ts).await;
            }
            TitanMessageOutcome::RpcError => {
                self.incr(mux_keys::TITAN_ERRORS).await;
            }
            TitanMessageOutcome::Unhandled => {
                self.incr(mux_keys::TITAN_FELL_THROUGH).await;
            }
        }
    }

    async fn record_titan_quote_success(&self, ts: u64) {
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let ts_str = ts.to_string();
        let results: Result<((), (), (), (), (), ()), redis::RedisError> = redis::pipe()
            .incr(mux_keys::TITAN_FRAMES, 1_i64)
            .incr(mux_keys::TITAN_DECODED, 1_i64)
            .incr(mux_keys::TITAN_SERVED, 1_i64)
            .set(mux_keys::TITAN_LAST_FRAME_MS, &ts_str)
            .set(mux_keys::TITAN_FRESHEST_MS, &ts_str)
            .set(mux_keys::META_TITAN_UP, "1")
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, "failed to update mux titan quote counters");
        }
    }

    async fn incr(&self, key: &str) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        if let Err(e) = conn.incr::<_, _, i64>(key, 1_i64).await {
            tracing::warn!(error = %e, key, "failed to incr mux counter");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mux_key_schema_matches_bot350_eyes() {
        let keys = [
            mux_keys::TITAN_FRAMES,
            mux_keys::TITAN_DECODED,
            mux_keys::TITAN_ERRORS,
            mux_keys::TITAN_SERVED,
            mux_keys::TITAN_FELL_THROUGH,
            mux_keys::TITAN_PAIRS_LIVE,
            mux_keys::TITAN_LAST_FRAME_MS,
            mux_keys::TITAN_FRESHEST_MS,
            mux_keys::META_HEARTBEAT_MS,
            mux_keys::META_TITAN_UP,
        ];
        assert_eq!(keys.len(), 10);
        for key in keys {
            assert!(key.starts_with("mux:"));
        }
    }

    #[test]
    fn now_ms_is_non_zero() {
        assert!(now_ms() > 0);
    }

    #[tokio::test]
    async fn dry_run_metrics_are_no_ops() {
        let fanout = RedisFanout::connect(None, "feed:350".to_string(), true).await;
        fanout.heartbeat().await;
        fanout.set_titan_upstream_up(true).await;
        fanout.set_titan_upstream_up(false).await;
        fanout.record_titan_decode_error().await;
        fanout
            .record_titan_message(TitanMessageOutcome::QuotePublished)
            .await;
        fanout
            .record_titan_message(TitanMessageOutcome::RpcError)
            .await;
        fanout
            .record_titan_message(TitanMessageOutcome::Unhandled)
            .await;
    }
}
