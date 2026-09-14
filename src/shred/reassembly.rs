use std::collections::BTreeMap;

use super::parse::ParsedDataShred;

#[derive(Debug, Default)]
pub struct FecSetBuffer {
    shreds: BTreeMap<u32, Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct ShredReassembler {
    sets: BTreeMap<(u64, u32), FecSetBuffer>,
    max_sets: usize,
}

impl ShredReassembler {
    pub fn new(max_sets: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            max_sets: max_sets.max(16),
        }
    }

    /// Insert a data shred; returns concatenated entry bytes when the FEC set completes.
    pub fn push(&mut self, shred: ParsedDataShred) -> Option<DeshredBatch> {
        let key = (shred.slot, shred.fec_set_index);
        let buf = self.sets.entry(key).or_default();
        buf.shreds.insert(shred.index, shred.payload);

        if !shred.data_complete {
            self.trim_old_sets();
            return None;
        }

        let Some(batch) = buf.deshred(shred.slot, shred.fec_set_index) else {
            self.trim_old_sets();
            return None;
        };

        self.sets.remove(&key);
        self.trim_old_sets();
        Some(batch)
    }

    fn trim_old_sets(&mut self) {
        while self.sets.len() > self.max_sets {
            let oldest = self.sets.keys().next().copied();
            if let Some(k) = oldest {
                self.sets.remove(&k);
            } else {
                break;
            }
        }
    }
}

impl FecSetBuffer {
    fn deshred(&self, slot: u64, fec_set_index: u32) -> Option<DeshredBatch> {
        if self.shreds.is_empty() {
            return None;
        }
        let mut bytes = Vec::new();
        for payload in self.shreds.values() {
            bytes.extend_from_slice(payload);
        }
        if bytes.is_empty() {
            return None;
        }
        Some(DeshredBatch {
            slot,
            fec_set_index,
            bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeshredBatch {
    pub slot: u64,
    pub fec_set_index: u32,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shred::parse::ParsedDataShred;

    #[test]
    fn deshreds_on_data_complete() {
        let mut reassembler = ShredReassembler::new(8);
        let first = ParsedDataShred {
            slot: 1,
            index: 0,
            fec_set_index: 9,
            data_complete: false,
            last_in_slot: false,
            payload: b"abc".to_vec(),
        };
        assert!(reassembler.push(first).is_none());

        let last = ParsedDataShred {
            slot: 1,
            index: 1,
            fec_set_index: 9,
            data_complete: true,
            last_in_slot: false,
            payload: b"def".to_vec(),
        };
        let batch = reassembler.push(last).expect("batch");
        assert_eq!(batch.bytes, b"abcdef");
    }
}
