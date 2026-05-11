use crate::codec::{Encode, Decode};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Heartbeat {
    pub source_id:    u32,
    pub sequence_num: u64,
}

// u32(4) + u64(8) = 12 bytes

impl Encode for Heartbeat {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&self.source_id.to_le_bytes());
        buf.extend_from_slice(&self.sequence_num.to_le_bytes());
        buf
    }
}

impl Decode for Heartbeat {
    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < 12 {
            return None;
        }
        Some(Self {
            source_id:    u32::from_le_bytes(buf[0..4].try_into().ok()?),
            sequence_num: u64::from_le_bytes(buf[4..12].try_into().ok()?),
        })
    }
}