use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::Config;
use crate::redis_fanout::{FeedPayload, PublishResult, RedisFanout};
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
    pub ts: String,
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
    let body = HealthResponse {
        status: "ok",
        dry_run: state.config.dry_run,
        redis_configured: state.config.redis_url.is_some(),
        redis_channel: state.config.redis_channel.clone(),
        upstreams: state.upstreams.statuses(),
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
