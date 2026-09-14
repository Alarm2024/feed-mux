use feed_mux::config::Config;
use feed_mux::redis_fanout::{mux_keys, now_ms, RedisFanout};
use feed_mux::shred::{parse_data_shred, ShredReassembler, VaultWatchSet};
use feed_mux::upstream::triton_shred::TritonShredUpstream;
use redis::AsyncCommands;
use serial_test::serial;

#[test]
fn shred_upstream_rejects_keep_port_8002() {
    let config = Config {
        bind_addr: "127.0.0.1:8787".to_string(),
        dry_run: false,
        redis_url: None,
        redis_channel: "feed:350".to_string(),
        enable_chainstack: false,
        chainstack_rpc_url: None,
        chainstack_ws_url: None,
        enable_helius: false,
        helius_rpc_url: None,
        enable_triton_grpc: false,
        triton_grpc_url: None,
        triton_grpc_token: None,
        triton_rate_limit_rps: 25,
        triton_local_bind: "127.0.0.1:19000".to_string(),
        enable_titan_ws: false,
        titan_ws_url: None,
        titan_wallet_pubkey: None,
        titan_rate_limit_rps: 15,
        titan_local_bind: "127.0.0.1:19001".to_string(),
        titan_hunt_size_lamports: None,
        titan_hop1_ttl_secs: 2,
        enable_triton_shred: true,
        shred_bind: "0.0.0.0:8002".to_string(),
        shred_watch_vaults: vec![[1u8; 32]],
        shred_hit_ttl_secs: 2,
        shred_udp_prefix_skip: 0,
        mock_publish_interval_secs: 0,
    };

    let upstream = TritonShredUpstream::new(&config);
    assert_eq!(upstream.status().mode, "error/keep-port-8002-conflict");
}

#[test]
fn vault_watch_detects_pubkey_in_deshredded_blob() {
    let vault = feed_mux::config::parse_wallet_pubkey("11111111111111111111111111111111")
        .expect("vault");
    let watch = VaultWatchSet::from_pubkeys(&[vault]);

    let mut reassembler = ShredReassembler::new(8);
    let payload = {
        let mut blob = vec![0u8; 96];
        blob[40..72].copy_from_slice(&vault);
        blob
    };

    let first = parse_data_shred(&sample_data_shred(0, b"pre", false)).expect("first");
    let _ = reassembler.push(first);
    let last = parse_data_shred(&sample_data_shred(1, &payload, true)).expect("last");
    let batch = reassembler.push(last).expect("batch");

    let hits = watch.scan_hits(&batch.bytes);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].vault, vault);
}

fn sample_data_shred(index: u32, payload: &[u8], data_complete: bool) -> Vec<u8> {
    let mut packet = vec![0u8; 88 + payload.len()];
    packet[64] = 0x96;
    packet[65..73].copy_from_slice(&99_u64.to_le_bytes());
    packet[73..77].copy_from_slice(&index.to_le_bytes());
    packet[79..83].copy_from_slice(&5_u32.to_le_bytes());
    packet[85] = if data_complete { 0b0000_0001 } else { 0 };
    let size = (88 + payload.len()) as u16;
    packet[86..88].copy_from_slice(&size.to_le_bytes());
    packet[88..88 + payload.len()].copy_from_slice(payload);
    packet
}

async fn redis_available(url: &str) -> bool {
    let Ok(client) = redis::Client::open(url) else {
        return false;
    };
    let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
        return false;
    };
    redis::cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .map(|p| p == "PONG")
        .unwrap_or(false)
}

#[tokio::test]
#[serial]
async fn shred_hit_writes_mux_keys_with_ttl() {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://:changeme-local-only@127.0.0.1:6379/0".to_string());
    if !redis_available(&url).await {
        eprintln!("note: shred redis integration test skipped (REDIS_URL unreachable)");
        return;
    }

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let fanout = RedisFanout::connect(Some(url), format!("feed:350:test:shred:{}", now_ms()), false).await;

    fanout.reset_shred_state_at_boot().await;

    let hit = feed_mux::shred::VaultHit {
        vault: [7u8; 32],
        vault_b58: "7".repeat(44),
    };
    let ts = now_ms();
    fanout.publish_shred_hit(&hit, 123, 4, "127.0.0.1:8003", 2, ts).await;
    fanout.record_shred_frame(ts).await;

    let hits: i64 = conn.get(mux_keys::SHRED_VAULT_HITS).await.unwrap();
    let shreds: i64 = conn.get(mux_keys::SHRED_SHREDS).await.unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg(mux_keys::SHRED_HIT)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(hits, 1);
    assert_eq!(shreds, 1);
    assert!(ttl > 0 && ttl <= 2, "mux:shred:hit TTL must be honest, got {ttl}");
}
