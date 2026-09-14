use std::collections::HashSet;

use bs58;

#[derive(Debug, Clone)]
pub struct VaultWatchSet {
    vaults: HashSet<[u8; 32]>,
    vault_labels: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultHit {
    pub vault: [u8; 32],
    pub vault_b58: String,
}

impl VaultWatchSet {
    pub fn from_pubkeys(pubkeys: &[[u8; 32]]) -> Self {
        let vault_labels = pubkeys
            .iter()
            .map(|pk| bs58::encode(pk).into_string())
            .collect();
        Self {
            vaults: pubkeys.iter().copied().collect(),
            vault_labels,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.vaults.is_empty()
    }

    pub fn len(&self) -> usize {
        self.vaults.len()
    }

    pub fn labels(&self) -> &HashSet<String> {
        &self.vault_labels
    }

    /// Scan deshredded entry bytes for any watched vault pubkey (wake pre-filter).
    pub fn scan_hits(&self, haystack: &[u8]) -> Vec<VaultHit> {
        if self.vaults.is_empty() || haystack.len() < 32 {
            return Vec::new();
        }

        let mut hits = Vec::new();
        for vault in &self.vaults {
            if contains_subslice(haystack, vault) {
                hits.push(VaultHit {
                    vault: *vault,
                    vault_b58: bs58::encode(vault).into_string(),
                });
            }
        }
        hits
    }

    /// Estimate transaction count from deshredded bytes.
    pub fn count_transactions_estimate(&self, bytes: &[u8]) -> u64 {
        if bytes.is_empty() {
            return 0;
        }

        // Heuristic: count likely ed25519 signatures (64-byte non-zero blocks).
        signature_like_count(bytes).max(1)
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8; 32]) -> bool {
    haystack
        .windows(32)
        .any(|window| window == needle.as_slice())
}

fn signature_like_count(bytes: &[u8]) -> u64 {
    bytes
        .windows(64)
        .filter(|window| window.iter().any(|b| *b != 0))
        .take(256)
        .count() as u64
}

pub fn parse_vault_list(raw: &str) -> Result<Vec<[u8; 32]>, String> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let bytes = crate::config::parse_wallet_pubkey(trimmed)?;
        out.push(bytes);
    }
    if out.is_empty() {
        return Err("SHRED_WATCH_VAULTS is empty".to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_finds_embedded_vault() {
        let vault = crate::config::parse_wallet_pubkey("11111111111111111111111111111111")
            .expect("vault");
        let watch = VaultWatchSet::from_pubkeys(&[vault]);
        let mut blob = vec![0u8; 100];
        blob[40..72].copy_from_slice(&vault);
        let hits = watch.scan_hits(&blob);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].vault, vault);
    }

    #[test]
    fn parse_vault_list_accepts_csv() {
        let vaults = parse_vault_list(
            "11111111111111111111111111111111,11111111111111111111111111111112",
        )
        .expect("parse");
        assert_eq!(vaults.len(), 2);
    }
}
