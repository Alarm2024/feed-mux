use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

// Bot 350 mux Redis metrics (`mux:titan:*`, `mux:meta:heartbeat_ms`, `mux:meta:titan_up`).
// Rust feed-mux owns these keys on FR; the legacy Python mux is dead — no dual-write.

/// Redis keys read by Bot 350 eyes (legacy Python mux schema + hunt delivery).
pub mod mux_keys {
    pub const TITAN_FRAMES: &str = "mux:titan:frames";
    pub const TITAN_DECODED: &str = "mux:titan:decoded";
    pub const TITAN_ERRORS: &str = "mux:titan:errors";
    /// Downstream mux proxy sessions only — never incremented on hop-1 decode/publish.
    pub const TITAN_SERVED: &str = "mux:titan:served";
    pub const TITAN_HOP1_SERVED: &str = "mux:titan:hop1_served";
    pub const TITAN_SIZE_BOARD: &str = "mux:titan:size_board";
    pub const TITAN_FELL_THROUGH: &str = "mux:titan:fell_through";
    pub const TITAN_PAIRS_LIVE: &str = "mux:titan:pairs_live";
    pub const TITAN_LAST_FRAME_MS: &str = "mux:titan:last_frame_ms";
    pub const TITAN_FRESHEST_MS: &str = "mux:titan:freshest_ms";
    pub const META_HEARTBEAT_MS: &str = "mux:meta:heartbeat_ms";
    pub const META_TITAN_UP: &str = "mux:meta:titan_up";
    pub const META_TRITON_UP: &str = "mux:meta:triton_up";
    pub const TRITON_FRAMES: &str = "mux:triton:frames";
    pub const TRITON_LAST_FRAME_MS: &str = "mux:triton:last_frame_ms";
    pub const META_SHRED_UP: &str = "mux:meta:shred_up";
    pub const SHRED_SHREDS: &str = "mux:shred:shreds";
    pub const SHRED_TXS_DESHREDDED: &str = "mux:shred:txs_deshredded";
    pub const SHRED_VAULT_HITS: &str = "mux:shred:vault_hits";
    pub const SHRED_LAST_MS: &str = "mux:shred:last_ms";
    pub const SHRED_LAST_HIT_MS: &str = "mux:shred:last_hit_ms";
    pub const SHRED_HIT: &str = "mux:shred:hit";

    /// Per-size hop-1 quote row: `mux:titan:hop1:<BASE>-<MID>:<size_lamports>`
    pub fn hop1_row_key(pair: &str, size_lamports: u64) -> String {
        format!("mux:titan:hop1:{pair}:{size_lamports}")
    }
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

    /// Clear stale Titan mux counters left by the dead Python mux or a prior process.
    /// Called once at boot before the heartbeat task starts so eyes never inherit ghost frames.
    pub async fn reset_titan_state_at_boot(&self) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let zero = "0";
        let empty_board = r#"{"schema":"mux.titan.size_board.v1","pins":[]}"#;
        let titan_results: Result<
            ((), (), (), (), (), (), (), (), (), (), ()),
            redis::RedisError,
        > = redis::pipe()
            .set(mux_keys::TITAN_FRAMES, zero)
            .set(mux_keys::TITAN_DECODED, zero)
            .set(mux_keys::TITAN_ERRORS, zero)
            .set(mux_keys::TITAN_SERVED, zero)
            .set(mux_keys::TITAN_HOP1_SERVED, zero)
            .set(mux_keys::TITAN_SIZE_BOARD, empty_board)
            .set(mux_keys::TITAN_FELL_THROUGH, zero)
            .set(mux_keys::TITAN_PAIRS_LIVE, zero)
            .set(mux_keys::TITAN_LAST_FRAME_MS, zero)
            .set(mux_keys::TITAN_FRESHEST_MS, zero)
            .set(mux_keys::META_TITAN_UP, zero)
            .query_async(conn)
            .await;
        let triton_results: Result<((), (), ()), redis::RedisError> = redis::pipe()
            .set(mux_keys::META_TRITON_UP, zero)
            .set(mux_keys::TRITON_FRAMES, zero)
            .set(mux_keys::TRITON_LAST_FRAME_MS, zero)
            .query_async(conn)
            .await;
        if let Err(e) = titan_results {
            tracing::warn!(error = %e, "failed to reset mux titan state at boot");
        } else if let Err(e) = triton_results {
            tracing::warn!(error = %e, "failed to reset mux triton state at boot");
        } else {
            tracing::info!("mux counters reset at boot (rust feed-mux owns mux:* keys)");
        }
    }

    /// Clear stale shred mux counters at boot (Bot 350 user=350 ACL keys only).
    pub async fn reset_shred_state_at_boot(&self) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let zero = "0";
        let results: Result<((), (), (), (), (), (), ()), redis::RedisError> = redis::pipe()
            .set(mux_keys::META_SHRED_UP, zero)
            .set(mux_keys::SHRED_SHREDS, zero)
            .set(mux_keys::SHRED_TXS_DESHREDDED, zero)
            .set(mux_keys::SHRED_VAULT_HITS, zero)
            .set(mux_keys::SHRED_LAST_MS, zero)
            .set(mux_keys::SHRED_LAST_HIT_MS, zero)
            .del(mux_keys::SHRED_HIT)
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, "failed to reset mux shred state at boot");
        }
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

    /// Reflect Titan WS upstream connectivity (`mux:meta:titan_up` only).
    /// `pairs_live` is set only after a real quote decode/publish — never on handshake.
    pub async fn set_titan_upstream_up(&self, up: bool) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let flag = if up { "1" } else { "0" };
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::META_TITAN_UP, flag)
            .await
        {
            tracing::warn!(error = %e, up, "failed to write mux titan_up");
        }
        if !up {
            if let Err(e) = conn
                .set::<_, _, ()>(mux_keys::TITAN_PAIRS_LIVE, "0")
                .await
            {
                tracing::warn!(error = %e, "failed to clear mux pairs_live on disconnect");
            }
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
        // `served` is downstream-only in the legacy schema — never mirror decode count here.
        let results: Result<((), (), (), (), (), ()), redis::RedisError> = redis::pipe()
            .incr(mux_keys::TITAN_FRAMES, 1_i64)
            .incr(mux_keys::TITAN_DECODED, 1_i64)
            .set(mux_keys::TITAN_LAST_FRAME_MS, &ts_str)
            .set(mux_keys::TITAN_FRESHEST_MS, &ts_str)
            .set(mux_keys::TITAN_PAIRS_LIVE, "1")
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

    /// Write a hop-1 quote row for Bot 350 hunt consumption.
    pub async fn publish_titan_hop1(
        &self,
        row: &impl Serialize,
        pair: &str,
        size_lamports: u64,
        ttl_secs: u64,
    ) {
        if self.dry_run {
            tracing::info!(
                pair = %pair,
                size_lamports,
                "dry-run hop-1 quote (redis skipped)"
            );
            return;
        }

        let json = match serde_json::to_string(row) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize hop-1 row");
                return;
            }
        };

        let key = mux_keys::hop1_row_key(pair, size_lamports);
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };

        let ttl = ttl_secs.max(1);
        let results: Result<((), ()), redis::RedisError> = redis::pipe()
            .set_ex(&key, &json, ttl)
            .incr(mux_keys::TITAN_HOP1_SERVED, 1_i64)
            .query_async(conn)
            .await;

        if let Err(e) = results {
            tracing::warn!(error = %e, key = %key, "failed to publish hop-1 row");
        } else {
            tracing::debug!(key = %key, ttl_secs = ttl, "published Titan hop-1 row");
        }
    }

    /// Reflect Triton gRPC upstream connectivity (`mux:meta:triton_up` only).
    pub async fn set_triton_upstream_up(&self, up: bool) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let flag = if up { "1" } else { "0" };
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::META_TRITON_UP, flag)
            .await
        {
            tracing::warn!(error = %e, up, "failed to write mux triton_up");
        }
    }

    /// Reflect Triton UDP shred listener connectivity (`mux:meta:shred_up` only).
    pub async fn set_shred_upstream_up(&self, up: bool) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let flag = if up { "1" } else { "0" };
        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::META_SHRED_UP, flag)
            .await
        {
            tracing::warn!(error = %e, up, "failed to write mux shred_up");
        }
    }

    /// Record one UDP shred datagram (`mux:shred:shreds`, `mux:shred:last_ms`).
    pub async fn record_shred_frame(&self, ts: u64) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let ts_str = ts.to_string();
        let results: Result<((), (), ()), redis::RedisError> = redis::pipe()
            .incr(mux_keys::SHRED_SHREDS, 1_i64)
            .set(mux_keys::SHRED_LAST_MS, &ts_str)
            .set(mux_keys::META_SHRED_UP, "1")
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, "failed to update mux shred counters");
        }
    }

    /// Record deshredded transactions estimate (`mux:shred:txs_deshredded`).
    pub async fn record_shred_deshred(&self, tx_count: u64, ts: u64) {
        if self.dry_run {
            return;
        }
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let ts_str = ts.to_string();
        let results: Result<((), ()), redis::RedisError> = redis::pipe()
            .incr(mux_keys::SHRED_TXS_DESHREDDED, tx_count as i64)
            .set(mux_keys::SHRED_LAST_MS, &ts_str)
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, "failed to update mux shred deshred counters");
        }
    }

    /// Publish a vault wake row with TTL honesty (`mux:shred:hit`, counters).
    pub async fn publish_shred_hit(
        &self,
        hit: &crate::shred::VaultHit,
        slot: u64,
        fec_set_index: u32,
        src: &str,
        ttl_secs: u64,
        ts: u64,
    ) {
        if self.dry_run {
            tracing::info!(
                vault = %hit.vault_b58,
                slot,
                "dry-run shred vault hit (redis skipped)"
            );
            return;
        }

        let payload = serde_json::json!({
            "schema": "mux.shred.hit.v1",
            "vault": hit.vault_b58,
            "slot": slot,
            "fec_set_index": fec_set_index,
            "src": src,
            "ts_ms": ts,
        });
        let json = match serde_json::to_string(&payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize shred hit");
                return;
            }
        };

        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };

        let ttl = ttl_secs.max(1);
        let ts_str = ts.to_string();
        let results: Result<((), (), ()), redis::RedisError> = redis::pipe()
            .set_ex(mux_keys::SHRED_HIT, &json, ttl)
            .incr(mux_keys::SHRED_VAULT_HITS, 1_i64)
            .set(mux_keys::SHRED_LAST_HIT_MS, &ts_str)
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, vault = %hit.vault_b58, "failed to publish mux shred hit");
        }
    }

    /// Record a Triton gRPC slot/update frame.
    pub async fn record_triton_frame(&self) {
        if self.dry_run {
            return;
        }
        let ts = now_ms();
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };
        let ts_str = ts.to_string();
        let results: Result<((), (), ()), redis::RedisError> = redis::pipe()
            .incr(mux_keys::TRITON_FRAMES, 1_i64)
            .set(mux_keys::TRITON_LAST_FRAME_MS, &ts_str)
            .set(mux_keys::META_TRITON_UP, "1")
            .query_async(conn)
            .await;
        if let Err(e) = results {
            tracing::warn!(error = %e, "failed to update mux triton counters");
        }
    }

    /// Write aggregate per-size hop ages for Bot 350 /titan pin board.
    pub async fn publish_titan_size_board(&self, board: &impl Serialize) {
        if self.dry_run {
            tracing::info!("dry-run size_board (redis skipped)");
            return;
        }

        let json = match serde_json::to_string(board) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize size_board");
                return;
            }
        };

        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return;
        };

        if let Err(e) = conn
            .set::<_, _, ()>(mux_keys::TITAN_SIZE_BOARD, &json)
            .await
        {
            tracing::warn!(error = %e, "failed to publish size_board");
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
            mux_keys::TITAN_HOP1_SERVED,
            mux_keys::TITAN_SIZE_BOARD,
            mux_keys::TITAN_FELL_THROUGH,
            mux_keys::TITAN_PAIRS_LIVE,
            mux_keys::TITAN_LAST_FRAME_MS,
            mux_keys::TITAN_FRESHEST_MS,
            mux_keys::META_HEARTBEAT_MS,
            mux_keys::META_TITAN_UP,
            mux_keys::META_TRITON_UP,
            mux_keys::TRITON_FRAMES,
            mux_keys::TRITON_LAST_FRAME_MS,
            mux_keys::META_SHRED_UP,
            mux_keys::SHRED_SHREDS,
            mux_keys::SHRED_TXS_DESHREDDED,
            mux_keys::SHRED_VAULT_HITS,
            mux_keys::SHRED_LAST_MS,
            mux_keys::SHRED_LAST_HIT_MS,
            mux_keys::SHRED_HIT,
        ];
        assert_eq!(keys.len(), 22);
        assert_eq!(
            mux_keys::hop1_row_key("SOL-USDC", 1_000_000_000),
            "mux:titan:hop1:SOL-USDC:1000000000"
        );
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
        fanout.reset_titan_state_at_boot().await;
        fanout.heartbeat().await;
        fanout.set_titan_upstream_up(true).await;
        fanout.set_titan_upstream_up(false).await;
        fanout.set_triton_upstream_up(true).await;
        fanout.set_shred_upstream_up(true).await;
        fanout.record_triton_frame().await;
        fanout.record_shred_frame(now_ms()).await;
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
