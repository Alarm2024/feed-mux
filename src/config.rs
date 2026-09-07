use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub dry_run: bool,
    pub redis_url: Option<String>,
    pub redis_channel: String,
    /// Chainstack RPC/WS upstream (stub)
    pub enable_chainstack: bool,
    pub chainstack_rpc_url: Option<String>,
    pub chainstack_ws_url: Option<String>,
    /// Helius backup upstream (stub)
    pub enable_helius: bool,
    pub helius_rpc_url: Option<String>,
    /// Triton gRPC upstream (stub) — rate-limited separately
    pub enable_triton_grpc: bool,
    pub triton_grpc_url: Option<String>,
    pub triton_rate_limit_rps: u32,
    /// Titan WS upstream (stub) — rate-limited separately
    pub enable_titan_ws: bool,
    pub titan_ws_url: Option<String>,
    pub titan_rate_limit_rps: u32,
    /// Mock publish interval in dry-run mode (seconds, 0 = disabled)
    pub mock_publish_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env_or("BIND_ADDR", "0.0.0.0:8787"),
            dry_run: env_bool("DRY_RUN", true),
            redis_url: env::var("REDIS_URL").ok().filter(|s| !s.is_empty()),
            redis_channel: env_or("REDIS_CHANNEL", "feed:350"),
            enable_chainstack: env_bool("ENABLE_CHAINSTACK", false),
            chainstack_rpc_url: env::var("CHAINSTACK_RPC_URL").ok(),
            chainstack_ws_url: env::var("CHAINSTACK_WS_URL").ok(),
            enable_helius: env_bool("ENABLE_HELIUS", false),
            helius_rpc_url: env::var("HELIUS_RPC_URL").ok(),
            enable_triton_grpc: env_bool("ENABLE_TRITON_GRPC", false),
            triton_grpc_url: env::var("TRITON_GRPC_URL").ok(),
            triton_rate_limit_rps: env_u32("TRITON_RATE_LIMIT_RPS", 50),
            enable_titan_ws: env_bool("ENABLE_TITAN_WS", false),
            titan_ws_url: env::var("TITAN_WS_URL").ok(),
            titan_rate_limit_rps: env_u32("TITAN_RATE_LIMIT_RPS", 30),
            mock_publish_interval_secs: env_u64("MOCK_PUBLISH_INTERVAL_SECS", 30),
        }
    }

    /// Safe summary for logs — never includes secrets or full Redis URL.
    pub fn redacted_summary(&self) -> String {
        format!(
            "bind={} dry_run={} redis={} channel={} chainstack={} helius={} triton_grpc={} titan_ws={} triton_rps={} titan_rps={}",
            self.bind_addr,
            self.dry_run,
            self.redis_url.as_ref().map(|_| "<set>").unwrap_or("<none>"),
            self.redis_channel,
            self.enable_chainstack,
            self.enable_helius,
            self.enable_triton_grpc,
            self.enable_titan_ws,
            self.triton_rate_limit_rps,
            self.titan_rate_limit_rps,
        )
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
