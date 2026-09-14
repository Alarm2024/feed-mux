use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::Config;
use crate::redis_fanout::{now_ms, FeedPayload, PublishResult, RedisFanout};
use crate::upstream::triton_shred::ShredStatsSnapshot;
use crate::upstream::{UpstreamHub, UpstreamStatus};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub fanout: RedisFanout,
    pub upstreams: Arc<UpstreamHub>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub dry_run: bool,
    pub redis_configured: bool,
    pub redis_channel: String,
    pub upstreams: Vec<UpstreamStatus>,
    pub shred: ShredHealth,
    pub ts: String,
}

#[derive(Serialize)]
pub struct ShredHealth {
    pub enabled: bool,
    pub bind: String,
    pub vaults: usize,
    pub shreds: u64,
    pub txs_deshredded: u64,
    pub vault_hits: u64,
    pub last_ms: u64,
    pub last_hit_ms: u64,
    pub last_age_ms: u64,
    pub bound: bool,
}

impl From<ShredStatsSnapshot> for ShredHealth {
    fn from(snapshot: ShredStatsSnapshot) -> Self {
        Self {
            enabled: true,
            bind: String::new(),
            vaults: 0,
            shreds: snapshot.shreds,
            txs_deshredded: snapshot.txs_deshredded,
            vault_hits: snapshot.vault_hits,
            last_ms: snapshot.last_ms,
            last_hit_ms: snapshot.last_hit_ms,
            last_age_ms: snapshot.last_age_ms,
            bound: snapshot.bound,
        }
    }
}

#[derive(Deserialize)]
pub struct PublishRequest {
    pub event: Option<String>,
    pub source: Option<String>,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct PublishResponse {
    pub ok: bool,
    pub result: PublishResult,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/publish", post(publish))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let shred_snapshot = state.upstreams.shred_stats().snapshot(now_ms());
    let mut shred = ShredHealth::from(shred_snapshot);
    shred.enabled = state.config.enable_triton_shred;
    shred.bind = state.config.shred_bind.clone();
    shred.vaults = state.config.shred_watch_vaults.len();

    let body = HealthResponse {
        status: "ok",
        dry_run: state.config.dry_run,
        redis_configured: state.config.redis_url.is_some(),
        redis_channel: state.config.redis_channel.clone(),
        upstreams: state.upstreams.statuses(),
        shred,
        ts: Utc::now().to_rfc3339(),
    };
    Json(body)
}

async fn publish(
    State(state): State<AppState>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<PublishResponse>, StatusCode> {
    let payload = FeedPayload {
        event: req.event.unwrap_or_else(|| "manual.publish".to_string()),
        source: req
            .source
            .unwrap_or_else(|| "feed-mux".to_string()),
        ts: Utc::now().to_rfc3339(),
        data: req.data,
    };

    match state.fanout.publish(&payload).await {
        Ok(result) => Ok(Json(PublishResponse { ok: true, result })),
        Err(e) => {
            tracing::warn!(error = %e, "publish failed");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}
