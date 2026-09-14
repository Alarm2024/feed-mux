use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{routing::get, Json, Router};
use serde::Serialize;

/// Local HTTP probe on TRITON_LOCAL_BIND (default :19000) for Bot 350 eyes.
#[derive(Clone)]
pub struct TritonLocalProbe {
    upstream_connected: Arc<AtomicBool>,
}

impl TritonLocalProbe {
    pub fn new() -> Self {
        Self {
            upstream_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_upstream_connected(&self, connected: bool) {
        self.upstream_connected
            .store(connected, Ordering::Relaxed);
    }

    pub fn spawn_server(self, bind_addr: String, enabled: bool) {
        if !enabled {
            return;
        }

        tokio::spawn(async move {
            let app = Router::new().route(
                "/",
                get({
                    let probe = self.clone();
                    move || async move {
                        Json(TritonLocalStatus {
                            event: "triton.local.status",
                            upstream: "triton_grpc",
                            connected: probe.upstream_connected.load(Ordering::Relaxed),
                        })
                    }
                }),
            );

            let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        bind = %bind_addr,
                        error = %e,
                        "failed to bind Triton local probe"
                    );
                    return;
                }
            };

            tracing::info!(
                bind = %bind_addr,
                "Triton local probe listening (Bot 350 MUX_TRITON_BIND eyes)"
            );

            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(bind = %bind_addr, error = %e, "Triton local probe exited");
            }
        });
    }
}

#[derive(Serialize)]
struct TritonLocalStatus {
    event: &'static str,
    upstream: &'static str,
    connected: bool,
}
