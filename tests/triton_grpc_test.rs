use feed_mux::config::Config;
use feed_mux::upstream::triton::TritonGrpcUpstream;
use feed_mux::ws_reconnect::{classify_grpc_stream_error, grpc_error_summary, GrpcStreamFailure};
/// Live FR symptom (2026-09-14): subscribe opens then stream fails with HTTP 403
/// mis-parsed as "invalid compression flag: 32".
const FR_403_COMPRESSION_SYMPTOM: &str = r#"Triton stream error: code: 'Internal error', message: "protocol error: received message with invalid compression flag: 32 (valid flags are 0 and 1) while receiving response with status: 403 Forbidden""#;

fn live_triton_config() -> Config {
    Config {
        bind_addr: "127.0.0.1:8787".to_string(),
        dry_run: false,
        redis_url: None,
        redis_channel: "feed:350".to_string(),
        enable_chainstack: false,
        chainstack_rpc_url: None,
        chainstack_ws_url: None,
        enable_helius: false,
        helius_rpc_url: None,
        enable_triton_grpc: true,
        triton_grpc_url: Some("https://grpc.example.test".to_string()),
        triton_grpc_token: Some("test-token".to_string()),
        triton_rate_limit_rps: 25,
        triton_local_bind: "127.0.0.1:19000".to_string(),
        enable_titan_ws: false,
        titan_ws_url: None,
        titan_wallet_pubkey: None,
        titan_rate_limit_rps: 15,
        titan_local_bind: "127.0.0.1:19001".to_string(),
        titan_hunt_size_lamports: None,
        titan_hop1_ttl_secs: 2,
        enable_triton_shred: false,
        shred_bind: "0.0.0.0:8003".to_string(),
        shred_watch_vaults: Vec::new(),
        shred_hit_ttl_secs: 2,
        shred_udp_prefix_skip: 0,
        mock_publish_interval_secs: 0,
    }
}

#[test]
fn fr_403_compression_flag_classified_as_auth_denied() {
    assert_eq!(
        classify_grpc_stream_error(FR_403_COMPRESSION_SYMPTOM),
        GrpcStreamFailure::AuthDenied
    );
}

#[test]
fn grpc_error_summary_for_403_is_actionable_without_secrets() {
    let summary = grpc_error_summary(FR_403_COMPRESSION_SYMPTOM);
    assert!(summary.contains("403"));
    assert!(summary.contains("TRITON_GRPC_TOKEN"));
    assert!(!summary.contains("test-token"));
}

#[test]
fn transient_connect_errors_remain_retryable() {
    assert_eq!(
        classify_grpc_stream_error("Triton gRPC connect failed: connection reset"),
        GrpcStreamFailure::Retryable
    );
    assert_eq!(
        classify_grpc_stream_error("Triton subscribe failed: deadline exceeded"),
        GrpcStreamFailure::Retryable
    );
}

#[test]
fn triton_ready_before_stream_stays_connecting() {
    let upstream = TritonGrpcUpstream::new(&live_triton_config());
    let status = upstream.status();
    assert_eq!(status.mode, "live/connecting");
    assert!(!status.connected);
}
