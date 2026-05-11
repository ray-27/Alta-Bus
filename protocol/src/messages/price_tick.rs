use crate::codec::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceTick {
    pub instrument_id: u32,
    pub last_price: f64,
    pub volume: u32,
    pub total_traded_volume: u64,
    pub timestamp_ns: u64,
}

//Total encode size: 32 bytes

impl Encode for PriceTick {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        // using little-endian order for encoding and decoding
        buf.extend_from_slice(&self.instrument_id.to_le_bytes());
        buf.extend_from_slice(&self.last_price.to_le_bytes());
        buf.extend_from_slice(&self.volume.to_le_bytes());
        buf.extend_from_slice(&self.total_traded_volume.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        buf
    }
}

impl Decode for PriceTick {
    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < 32 {
            return None;
        }
        Some(Self {
            instrument_id: u32::from_le_bytes(buf[0..4].try_into().ok()?),
            last_price: f64::from_le_bytes(buf[4..12].try_into().ok()?),
            volume: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            total_traded_volume: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            timestamp_ns: u64::from_le_bytes(buf[24..32].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_tick_round_trip() {
        let original = PriceTick {
            instrument_id: 1001,
            last_price: 24150.50,
            volume: 500,
            total_traded_volume: 1_200_000,
            timestamp_ns: 1_700_000_000_000_000_000,
        };

        let bytes = original.encode();
        assert_eq!(bytes.len(), 32);

        let recovered = PriceTick::decode(&bytes).unwrap();
        assert_eq!(recovered.instrument_id, original.instrument_id);
        assert_eq!(recovered.volume, original.volume);
        assert_eq!(recovered.total_traded_volume, original.total_traded_volume);
        assert_eq!(recovered.timestamp_ns, original.timestamp_ns);
        // f64 comparison needs explicit epsilon check
        assert!((recovered.last_price - original.last_price).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_returns_none_on_short_buffer() {
        let short = [0u8; 10];
        assert!(PriceTick::decode(&short).is_none());
    }
}
