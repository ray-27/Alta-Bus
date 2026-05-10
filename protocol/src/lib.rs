pub mod header;
pub mod types;
// pub mod messages;
// pub mod codec;
// pub mod config;

pub use header::{HEADER_SIZE, MsgHeader};
pub use types::MsgType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_serialize_deserialize() {
        let original = MsgHeader {
            msg_type: 1,
            channel_id: 1001,
            timestamp_ns: 1_700_000_000_000_000_000,
            payload_len: 24,
        };

        let bytes = original.to_bytes();

        let recovered = MsgHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.msg_type, original.msg_type);
        assert_eq!(recovered.channel_id, original.channel_id);
        assert_eq!(recovered.timestamp_ns, original.timestamp_ns);
        assert_eq!(recovered.payload_len, original.payload_len);
    }

    #[test]
    fn byte_layout_is_little_endian() {
        let header = MsgHeader {
            msg_type: 1,
            channel_id: 0x00_00_03_E9, // 1001 in hex
            timestamp_ns: 0,
            payload_len: 0,
        };
        let bytes = header.to_bytes();

        // msg_type at offset 0
        assert_eq!(bytes[0], 1);

        // channel_id at offsets 1-4, little-endian
        // 1001 = 0xE9 0x03 0x00 0x00
        assert_eq!(bytes[1], 0xE9);
        assert_eq!(bytes[2], 0x03);
        assert_eq!(bytes[3], 0x00);
        assert_eq!(bytes[4], 0x00);
    }

    #[test]
    fn from_bytes_returns_none_on_short_slice() {
        let short = [0u8; 10];
        assert!(MsgHeader::from_bytes(&short).is_none());
    }
}
