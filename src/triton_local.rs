use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tokio_stream::{empty, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use yellowstone_grpc_proto::geyser::geyser_server::{Geyser, GeyserServer};
use yellowstone_grpc_proto::geyser::{
    GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
    GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
    PongResponse, SubscribeDeshredRequest, SubscribeGossipRequest, SubscribeReplayInfoRequest,
    SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
    SubscribeUpdateGossip,
};

const BROADCAST_CAPACITY: usize = 512;

#[derive(Clone, Debug)]
pub enum TritonLocalEvent {
    Update(SubscribeUpdate),
    UpstreamConnected(bool),
}

/// Fan-out hub for upstream Triton gRPC updates to local Bot 350 consumers.
#[derive(Clone)]
pub struct TritonLocalRelay {
    tx: broadcast::Sender<TritonLocalEvent>,
    upstream_connected: Arc<AtomicBool>,
}

impl TritonLocalRelay {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            upstream_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn publish_update(&self, update: SubscribeUpdate) {
        let _ = self.tx.send(TritonLocalEvent::Update(update));
    }

    pub fn set_upstream_connected(&self, connected: bool) {
        self.upstream_connected
            .store(connected, Ordering::Relaxed);
        let _ = self
            .tx
            .send(TritonLocalEvent::UpstreamConnected(connected));
    }

    pub fn upstream_connected(&self) -> bool {
        self.upstream_connected.load(Ordering::Relaxed)
    }

    pub fn spawn_server(self, bind_addr: String, enabled: bool) {
        if !enabled {
            return;
        }

        tokio::spawn(async move {
            let addr: SocketAddr = match bind_addr.parse() {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(
                        bind = %bind_addr,
                        error = %e,
                        "invalid TRITON_LOCAL_BIND address"
                    );
                    return;
                }
            };

            let service = TritonLocalGeyser { relay: self.clone() };
            let (health_reporter, health_service) = tonic_health::server::health_reporter();
            health_reporter
                .set_serving::<GeyserServer<TritonLocalGeyser>>()
                .await;

            tracing::info!(
                bind = %bind_addr,
                "Triton local gRPC relay listening (Bot 350 uses mux — no second Yellowstone subscribe)"
            );

            if let Err(e) = Server::builder()
                .add_service(health_service)
                .add_service(GeyserServer::new(service))
                .serve(addr)
                .await
            {
                tracing::error!(
                    bind = %bind_addr,
                    error = %e,
                    "Triton local gRPC relay server exited"
                );
            }
        });
    }
}

struct TritonLocalGeyser {
    relay: TritonLocalRelay,
}

#[async_trait]
impl Geyser for TritonLocalGeyser {
    type SubscribeStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;
    type SubscribeDeshredStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send>,
    >;
    type SubscribeGossipStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<SubscribeUpdateGossip, Status>> + Send>,
    >;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if !self.relay.upstream_connected() {
            return Err(Status::unavailable(
                "Triton upstream not connected — check ENABLE_TRITON_GRPC and TRITON_GRPC_URL",
            ));
        }

        // Consume client request stream in the background (pings / filter updates).
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            while let Ok(Some(_req)) = inbound.message().await {
                // Upstream mux owns the Yellowstone subscription; local clients share the relay.
            }
        });

        let rx = self.relay.tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|event| match event {
            Ok(TritonLocalEvent::Update(update)) => Some(Ok(update)),
            Ok(TritonLocalEvent::UpstreamConnected(false)) => {
                Some(Err(Status::unavailable("Triton upstream disconnected")))
            }
            Ok(TritonLocalEvent::UpstreamConnected(true)) => None,
            Err(BroadcastStreamRecvError::Lagged(_)) => None,
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Ok(Response::new(Box::pin(empty())))
    }

    async fn subscribe_gossip(
        &self,
        _request: Request<SubscribeGossipRequest>,
    ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
        Ok(Response::new(Box::pin(empty())))
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented("SubscribeReplayInfo not relayed"))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse {
            count: request.into_inner().count,
        }))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("get_latest_blockhash not relayed"))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("get_block_height not relayed"))
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("get_slot not relayed"))
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("is_blockhash_valid not relayed"))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_publishes_connection_state() {
        let relay = TritonLocalRelay::new();
        relay.set_upstream_connected(true);
        assert!(relay.upstream_connected());
    }
}
