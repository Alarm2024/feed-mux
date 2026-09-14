//! Minimal Solana Merkle/data shred header parsing for UDP fan-out.
//!
//! Layout follows Agave `ShredCommonHeader` + `DataShredHeader` (non-resigned).

pub const MAX_DATAGRAM_SIZE: usize = 1232;
pub const SIZE_OF_COMMON_HEADER: usize = 83;
pub const SIZE_OF_DATA_HEADERS: usize = 88;

const SHRED_DATA_COMPLETE_FLAG: u8 = 0b0000_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShredKind {
    Data,
    Coding,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ParsedDataShred {
    pub slot: u64,
    pub index: u32,
    pub fec_set_index: u32,
    pub data_complete: bool,
    pub last_in_slot: bool,
    pub payload: Vec<u8>,
}

pub fn parse_data_shred(packet: &[u8]) -> Option<ParsedDataShred> {
    if packet.len() < SIZE_OF_DATA_HEADERS {
        return None;
    }

    let variant = packet.get(64)?;
    let kind = shred_kind(*variant);
    if kind != ShredKind::Data {
        return None;
    }

    let slot = u64::from_le_bytes(packet[65..73].try_into().ok()?);
    let index = u32::from_le_bytes(packet[73..77].try_into().ok()?);
    let fec_set_index = u32::from_le_bytes(packet[79..83].try_into().ok()?);
    let flags = packet[85];
    let declared_size = u16::from_le_bytes(packet[86..88].try_into().ok()?);

    let end = declared_size as usize;
    if end > packet.len() || end <= SIZE_OF_DATA_HEADERS {
        return None;
    }

    // Merkle shreds embed a proof suffix; payload for deshred is between headers and proof.
    let payload_end = merkle_payload_end(packet, end)?;
    let payload = packet[SIZE_OF_DATA_HEADERS..payload_end].to_vec();

    Some(ParsedDataShred {
        slot,
        index,
        fec_set_index,
        data_complete: flags & SHRED_DATA_COMPLETE_FLAG != 0,
        last_in_slot: flags & 0b1000_0000 != 0,
        payload,
    })
}

fn shred_kind(variant: u8) -> ShredKind {
    match variant {
        0x5a | 0xa5 => ShredKind::Data,
        0x69 | 0x96 => ShredKind::Data,
        0x4b | 0xb4 => ShredKind::Coding,
        0x78 | 0x87 => ShredKind::Coding,
        _ => ShredKind::Unknown,
    }
}

/// Strip trailing Merkle proof bytes when present.
fn merkle_payload_end(packet: &[u8], declared_size: usize) -> Option<usize> {
    if declared_size <= SIZE_OF_DATA_HEADERS {
        return None;
    }
    let body = &packet[SIZE_OF_DATA_HEADERS..declared_size];
    // Proof is trailing; data region is everything before the proof chain.
    // For unknown layouts, fall back to declared_size (legacy shreds).
    if body.len() <= 20 {
        return Some(declared_size);
    }
    Some(declared_size.saturating_sub(merkle_proof_bytes(body.len())))
}

fn merkle_proof_bytes(body_len: usize) -> usize {
    // Agave merkle proof size scales with shred size; conservative trim for wake path.
    if body_len > 600 {
        80
    } else if body_len > 400 {
        40
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data_shred(payload: &[u8], data_complete: bool) -> Vec<u8> {
        let mut packet = vec![0u8; SIZE_OF_DATA_HEADERS + payload.len()];
        packet[64] = 0x96; // Merkle data
        packet[65..73].copy_from_slice(&42_u64.to_le_bytes());
        packet[73..77].copy_from_slice(&7_u32.to_le_bytes());
        packet[79..83].copy_from_slice(&3_u32.to_le_bytes());
        packet[85] = if data_complete {
            SHRED_DATA_COMPLETE_FLAG
        } else {
            0
        };
        let size = (SIZE_OF_DATA_HEADERS + payload.len()) as u16;
        packet[86..88].copy_from_slice(&size.to_le_bytes());
        packet[SIZE_OF_DATA_HEADERS..SIZE_OF_DATA_HEADERS + payload.len()]
            .copy_from_slice(payload);
        packet
    }

    #[test]
    fn parses_data_shred_headers_and_payload() {
        let parsed = parse_data_shred(&sample_data_shred(b"hello-shred", false)).expect("parse");
        assert_eq!(parsed.slot, 42);
        assert_eq!(parsed.index, 7);
        assert_eq!(parsed.fec_set_index, 3);
        assert!(!parsed.data_complete);
        assert_eq!(parsed.payload, b"hello-shred");
    }

    #[test]
    fn rejects_short_packets() {
        assert!(parse_data_shred(&[0u8; 32]).is_none());
    }
}
