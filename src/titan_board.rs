use serde::{Deserialize, Serialize};

pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

/// KEEP-like default board: 0.1, 0.5, 1, 2.5 (pin), 5 SOL.
pub const DEFAULT_BOARD_SIZES_LAMPORTS: &[u64] =
    &[100_000_000, 500_000_000, 1_000_000_000, 2_500_000_035, 5_000_000_000];

/// Parse comma-separated lamport sizes from env; falls back to [`DEFAULT_BOARD_SIZES_LAMPORTS`].
pub fn parse_board_sizes(raw: Option<&str>) -> Vec<u64> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_BOARD_SIZES_LAMPORTS.to_vec();
    };
    let mut sizes: Vec<u64> = raw
        .split(',')
        .filter_map(|part| part.trim().parse().ok())
        .filter(|v| *v > 0)
        .collect();
    sizes.sort_unstable();
    sizes.dedup();
    if sizes.is_empty() {
        DEFAULT_BOARD_SIZES_LAMPORTS.to_vec()
    } else {
        sizes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SizeBoardEntry {
    pub size_lamports: u64,
    pub hop1_age_ms: u64,
    pub hop1_last_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SizeBoard {
    pub updated_ms: u64,
    pub pair: String,
    pub input_mint: String,
    pub output_mint: String,
    pub sizes: Vec<SizeBoardEntry>,
}

impl SizeBoard {
    pub fn new(now_ms: u64) -> Self {
        Self {
            updated_ms: now_ms,
            pair: "SOL-USDC".to_string(),
            input_mint: SOL_MINT.to_string(),
            output_mint: USDC_MINT.to_string(),
            sizes: Vec::new(),
        }
    }

    pub fn upsert(&mut self, entry: SizeBoardEntry, now_ms: u64) {
        self.updated_ms = now_ms;
        if let Some(existing) = self
            .sizes
            .iter_mut()
            .find(|e| e.size_lamports == entry.size_lamports)
        {
            *existing = entry;
        } else {
            self.sizes.push(entry);
        }
        self.sizes.sort_by_key(|e| e.size_lamports);
    }
}

/// Hop-1 quote payload written to `mux:titan:hop1:<size_lamports>` for Bot 350 hunt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hop1QuoteDelivery {
    pub size_lamports: u64,
    pub input_mint: String,
    pub output_mint: String,
    pub hop: u8,
    pub out_amount: Option<u64>,
    pub provider: Option<String>,
    pub quote_ms: u64,
    pub age_ms: u64,
    pub source: String,
}

pub fn hop1_redis_key(size_lamports: u64) -> String {
    format!("mux:titan:hop1:{size_lamports}")
}

#[derive(Debug, Clone, Default)]
pub struct ParsedSwapQuotes {
    pub amount: Option<u64>,
    pub input_mint: Option<String>,
    pub output_mint: Option<String>,
    pub out_amount: Option<u64>,
    pub provider: Option<String>,
}

/// Extract SwapQuotes fields from a Titan `StreamData` msgpack frame.
pub fn parse_stream_data_quotes(data: &[u8]) -> Option<ParsedSwapQuotes> {
    let value = rmpv::decode::read_value(&mut std::io::Cursor::new(data)).ok()?;
    let swap_quotes = extract_swap_quotes(&value)?;
    Some(swap_quotes)
}

fn extract_swap_quotes(root: &rmpv::Value) -> Option<ParsedSwapQuotes> {
    let stream_data = map_get_pascal(root, "StreamData")?;
    let payload = map_get_pascal(stream_data, "payload")?;
    let swap_quotes = map_get_pascal(payload, "SwapQuotes")?;

    let amount = map_get_u64(swap_quotes, "amount");
    let input_mint = map_get_pubkey_b58(swap_quotes, "inputMint");
    let output_mint = map_get_pubkey_b58(swap_quotes, "outputMint");

    let winner = map_get_string(
        map_get_pascal(swap_quotes, "metadata").unwrap_or(&rmpv::Value::Nil),
        "ExpectedWinner",
    );
    let quotes = map_get_pascal(swap_quotes, "quotes").or_else(|| map_get(swap_quotes, "quotes"))?;
    let (provider, out_amount) = best_quote(quotes, winner.as_deref());

    Some(ParsedSwapQuotes {
        amount,
        input_mint,
        output_mint,
        out_amount,
        provider,
    })
}

fn best_quote(
    quotes: &rmpv::Value,
    preferred: Option<&str>,
) -> (Option<String>, Option<u64>) {
    let rmpv::Value::Map(map) = quotes else {
        return (None, None);
    };

    if let Some(name) = preferred {
        for (k, v) in map {
            if key_as_str(k).as_deref() == Some(name) {
                if let Some(out) = route_out_amount(v) {
                    return (Some(name.to_string()), Some(out));
                }
            }
        }
    }

    for (k, v) in map {
        if let Some(out) = route_out_amount(v) {
            return (key_as_str(k), Some(out));
        }
    }
    (None, None)
}

fn route_out_amount(route: &rmpv::Value) -> Option<u64> {
    map_get_u64(route, "outAmount")
        .or_else(|| map_get_u64(route, "out_amount"))
}

fn map_get<'a>(map: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    let rmpv::Value::Map(m) = map else {
        return None;
    };
    m.iter()
        .find(|(k, _)| key_as_str(k).as_deref() == Some(key))
        .map(|(_, v)| v)
}

fn map_get_pascal<'a>(map: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    map_get(map, key)
}

fn map_get_u64(map: &rmpv::Value, key: &str) -> Option<u64> {
    match map_get(map, key)? {
        rmpv::Value::Integer(i) => i.as_u64(),
        _ => None,
    }
}

fn map_get_string(map: &rmpv::Value, key: &str) -> Option<String> {
    match map_get(map, key)? {
        rmpv::Value::String(s) => s.as_str().map(|v| v.to_string()),
        _ => None,
    }
}

fn map_get_pubkey_b58(map: &rmpv::Value, key: &str) -> Option<String> {
    match map_get(map, key)? {
        rmpv::Value::Binary(bytes) if bytes.len() == 32 => Some(bs58::encode(bytes).into_string()),
        rmpv::Value::String(s) => s.as_str().map(|v| v.to_string()),
        _ => None,
    }
}

fn key_as_str(key: &rmpv::Value) -> Option<String> {
    match key {
        rmpv::Value::String(s) => s.as_str().map(|v| v.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::collections::HashMap;

    #[derive(Serialize)]
    struct TestStreamData {
        StreamData: TestStreamInner,
    }

    #[derive(Serialize)]
    struct TestStreamInner {
        id: u32,
        seq: u32,
        payload: TestPayload,
    }

    #[derive(Serialize)]
    struct TestPayload {
        SwapQuotes: TestSwapQuotes,
    }

    #[derive(Serialize)]
    struct TestSwapQuotes {
        #[serde(rename = "inputMint")]
        input_mint: [u8; 32],
        #[serde(rename = "outputMint")]
        output_mint: [u8; 32],
        amount: u64,
        quotes: HashMap<String, TestRoute>,
        metadata: TestMetadata,
    }

    #[derive(Serialize)]
    struct TestMetadata {
        ExpectedWinner: String,
    }

    #[derive(Serialize)]
    struct TestRoute {
        #[serde(rename = "outAmount")]
        out_amount: u64,
    }

    #[test]
    fn parse_board_sizes_defaults_when_empty() {
        assert_eq!(
            parse_board_sizes(None),
            DEFAULT_BOARD_SIZES_LAMPORTS.to_vec()
        );
    }

    #[test]
    fn parse_board_sizes_dedupes_and_sorts() {
        assert_eq!(
            parse_board_sizes(Some("2500000035,100000000,2500000035")),
            vec![100_000_000, 2_500_000_035]
        );
    }

    #[test]
    fn parse_stream_data_quotes_extracts_winner_out_amount() {
        let mut quotes = HashMap::new();
        quotes.insert(
            "Titan".to_string(),
            TestRoute {
                out_amount: 42_000_000,
            },
        );
        let frame = TestStreamData {
            StreamData: TestStreamInner {
                id: 1,
                seq: 1,
                payload: TestPayload {
                    SwapQuotes: TestSwapQuotes {
                        input_mint: [1u8; 32],
                        output_mint: [2u8; 32],
                        amount: 2_500_000_035,
                        quotes,
                        metadata: TestMetadata {
                            ExpectedWinner: "Titan".to_string(),
                        },
                    },
                },
            },
        };
        let buf = rmp_serde::to_vec_named(&frame).unwrap();
        let parsed = parse_stream_data_quotes(&buf).expect("parse");
        assert_eq!(parsed.amount, Some(2_500_000_035));
        assert_eq!(parsed.out_amount, Some(42_000_000));
        assert_eq!(parsed.provider.as_deref(), Some("Titan"));
    }

    #[test]
    fn hop1_redis_key_format() {
        assert_eq!(hop1_redis_key(2_500_000_035), "mux:titan:hop1:2500000035");
    }
}
