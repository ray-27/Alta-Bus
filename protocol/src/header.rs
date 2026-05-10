/// This is a fixed-size binary header prepended to every message on the wire.
#[derive(Debug, Clone, Copy)]
pub struct MsgHeader {
    pub msg_type: u8,
    pub channel_id: u32,
    pub timestamp_ns: u64,
    pub payload_len: u32,
}

pub const HEADER_SIZE: usize = std::mem::size_of::<MsgHeader>();

impl MsgHeader {
    pub fn new(msg_type: u8, channel_id: u32, payload_len: u32) -> Self {
        Self {
            msg_type,
            channel_id,
            timestamp_ns: Self::now_ns(),
            payload_len,
        }
    }

    /// Serilize the header to a fixed 17-byte array. 
    pub fn to_bytes(self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0] = self.msg_type;
        buf[1..5].copy_from_slice(&self.channel_id.to_le_bytes());
        buf[5..13].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        buf[13..17].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    /// Deserialize the header from 17-bytes slice
    /// Returns None if the slice is not exaclty HEADER_SIZE bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        Some(Self {
            msg_type: buf[0],
            channel_id: u32::from_le_bytes(buf[1..5].try_into().ok()?),
            timestamp_ns: u64::from_le_bytes(buf[5..13].try_into().ok()?),
            payload_len: u32::from_le_bytes(buf[13..17].try_into().ok()?),
        })
    }

    fn now_ns() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
