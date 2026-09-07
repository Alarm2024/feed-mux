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
    /// Helius backup upstream
    pub enable_helius: bool,
    pub helius_rpc_url: Option<String>,
    /// Triton gRPC upstream — rate-limited separately
    pub enable_triton_grpc: bool,
    pub triton_grpc_url: Option<String>,
    pub triton_rate_limit_rps: u32,
    /// Titan WS upstream — rate-limited separately; requires wallet pubkey when live
    pub enable_titan_ws: bool,
    pub titan_ws_url: Option<String>,
    pub titan_wallet_pubkey: Option<String>,
    pub titan_rate_limit_rps: u32,
    /// Local Titan WS relay for Bot 350 MUX_TITAN_BIND eyes probe
    pub titan_local_bind: String,
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
            chainstack_rpc_url: env_optional("CHAINSTACK_RPC_URL"),
            chainstack_ws_url: env_optional("CHAINSTACK_WS_URL"),
            enable_helius: env_bool("ENABLE_HELIUS", false),
            helius_rpc_url: env_optional("HELIUS_RPC_URL"),
            enable_triton_grpc: env_bool("ENABLE_TRITON_GRPC", false),
            triton_grpc_url: env_optional("TRITON_GRPC_URL"),
            triton_rate_limit_rps: env_u32("TRITON_RATE_LIMIT_RPS", 25),
            enable_titan_ws: env_bool("ENABLE_TITAN_WS", false),
            titan_ws_url: env_optional("TITAN_WS_URL"),
            titan_wallet_pubkey: env_optional("TITAN_WALLET_PUBKEY"),
            titan_rate_limit_rps: env_u32("TITAN_RATE_LIMIT_RPS", 15),
            titan_local_bind: env_or("TITAN_LOCAL_BIND", "127.0.0.1:19001"),
            mock_publish_interval_secs: env_u64("MOCK_PUBLISH_INTERVAL_SECS", 30),
        }
    }

    /// Safe summary for logs — never includes secrets or full Redis URL.
    pub fn redacted_summary(&self) -> String {
        format!(
            "bind={} dry_run={} redis={} channel={} chainstack={} helius={} triton_grpc={} titan_ws={} titan_wallet={} titan_local={} triton_rps={} titan_rps={}",
            self.bind_addr,
            self.dry_run,
            self.redis_url.as_ref().map(|_| "<set>").unwrap_or("<none>"),
            self.redis_channel,
            self.enable_chainstack,
            self.enable_helius,
            self.enable_triton_grpc,
            self.enable_titan_ws,
            self.titan_wallet_pubkey
                .as_ref()
                .map(|_| "<set>")
                .unwrap_or("<none>"),
            self.titan_local_bind,
            self.triton_rate_limit_rps,
            self.titan_rate_limit_rps,
        )
    }
}

/// Parse and validate a Solana wallet public key (base58, 32 bytes).
pub fn parse_wallet_pubkey(value: &str) -> Result<[u8; 32], String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("wallet pubkey is empty".to_string());
    }
    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|e| format!("invalid base58 wallet pubkey: {e}"))?;
    if decoded.len() != 32 {
        return Err(format!(
            "wallet pubkey must decode to 32 bytes, got {}",
            decoded.len()
        ));
    }
    decoded
        .try_into()
        .map_err(|_| "wallet pubkey conversion failed".to_string())
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_optional(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.trim().is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wallet_pubkey_rejects_empty() {
        assert!(parse_wallet_pubkey("").is_err());
        assert!(parse_wallet_pubkey("   ").is_err());
    }

    #[test]
    fn parse_wallet_pubkey_accepts_valid_base58() {
        // System program id — valid 32-byte pubkey encoding.
        let pk = "11111111111111111111111111111111";
        let bytes = parse_wallet_pubkey(pk).expect("valid pubkey");
        assert_eq!(bytes.len(), 32);
    }
}
