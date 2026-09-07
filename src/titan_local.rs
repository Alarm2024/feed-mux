use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

use crate::upstream::titan::TITAN_WS_PROTOCOL;

const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub enum TitanLocalEvent {
    UpstreamFrame(Vec<u8>),
    UpstreamConnected(bool),
}

/// Fan-out hub for upstream Titan frames and connection state to local WS clients.
#[derive(Clone)]
pub struct TitanLocalRelay {
    tx: broadcast::Sender<TitanLocalEvent>,
    upstream_connected: Arc<AtomicBool>,
}

impl TitanLocalRelay {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            upstream_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn upstream_connected(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.upstream_connected)
    }

    pub fn publish_frame(&self, data: Vec<u8>) {
        let _ = self.tx.send(TitanLocalEvent::UpstreamFrame(data));
    }

    pub fn set_upstream_connected(&self, connected: bool) {
        self.upstream_connected
            .store(connected, Ordering::Relaxed);
        let _ = self
            .tx
            .send(TitanLocalEvent::UpstreamConnected(connected));
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TitanLocalEvent> {
        self.tx.subscribe()
    }

    pub fn spawn_server(self, bind_addr: String, enabled: bool) {
        if !enabled {
            return;
        }

        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        bind = %bind_addr,
                        error = %e,
                        "failed to bind Titan local WebSocket relay"
                    );
                    return;
                }
            };

            tracing::info!(
                bind = %bind_addr,
                "Titan local WebSocket relay listening (Bot 350 MUX_TITAN_BIND probe)"
            );

            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "Titan local relay accept failed");
                        continue;
                    }
                };

                let relay = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_client(stream, relay).await {
                        tracing::debug!(peer = %peer, error = %e, "Titan local client session ended");
                    }
                });
            }
        });
    }
}

async fn serve_client(
    stream: tokio::net::TcpStream,
    relay: TitanLocalRelay,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws = accept_hdr_async(stream, |req: &Request, mut resp: Response| {
        if !req
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(',').any(|p| p.trim() == TITAN_WS_PROTOCOL))
            .unwrap_or(false)
        {
            tracing::debug!("Titan local client missing subprotocol; accepting anyway");
        }
        resp.headers_mut().append(
            "Sec-WebSocket-Protocol",
            TITAN_WS_PROTOCOL.parse().expect("valid protocol header"),
        );
        Ok(resp)
    })
    .await?;

    let (mut write, mut read) = ws.split();
    let mut events = relay.subscribe();

    let connected = relay.upstream_connected.load(Ordering::Relaxed);
    let status = serde_json::json!({
        "event": "titan.local.status",
        "upstream": "titan_ws",
        "connected": connected,
    });
    write
        .send(Message::Text(status.to_string().into()))
        .await?;

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Ok(TitanLocalEvent::UpstreamFrame(data)) => {
                        write.send(Message::Binary(data.into())).await?;
                    }
                    Ok(TitanLocalEvent::UpstreamConnected(connected)) => {
                        let status = serde_json::json!({
                            "event": "titan.local.status",
                            "upstream": "titan_ws",
                            "connected": connected,
                        });
                        write.send(Message::Text(status.to_string().into())).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(
                            skipped,
                            "Titan local client lagged; continuing with latest frames"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_publishes_connection_state() {
        let relay = TitanLocalRelay::new();
        let mut rx = relay.subscribe();
        relay.set_upstream_connected(true);
        match rx.try_recv() {
            Ok(TitanLocalEvent::UpstreamConnected(true)) => {}
            other => panic!("expected connected event, got {other:?}"),
        }
    }
}
