//! Rust SDK for publishers.
//!
//! A publisher opens one persistent TCP connection to the bus and sends
//! messages as fast as it can. The bus never replies — it is a pure write path.
//!
//! # Example
//!
//! ```no_run
//! use publisher_client::Publisher;
//! use protocol::messages::PriceTick;
//!
//! let mut pub_ = Publisher::connect("127.0.0.1:8000").unwrap();
//!
//! let tick = PriceTick {
//!     instrument_id: 1001,
//!     last_price: 22_450.50,
//!     volume: 100,
//!     total_traded_volume: 5_000_000,
//!     timestamp_ns: 0,
//! };
//!
//! pub_.publish(1001, protocol::MsgType::PriceTick as u8, &tick).unwrap();
//! ```

use protocol::{Encode, MsgHeader, HEADER_SIZE};
use std::io::{self, Write};
use std::net::TcpStream;

// Maximum payload the bus accepts (matches ring::PAYLOAD_CAP).
const MAX_PAYLOAD: usize = 1024;
// Inline buffer big enough for header + any valid payload in one stack alloc.
const INLINE_BUF: usize = HEADER_SIZE + MAX_PAYLOAD; // 17 + 1024 = 1041

pub struct Publisher {
    stream: TcpStream,
}

impl Publisher {
    /// Connect to the bus at `addr` (e.g. `"127.0.0.1:8000"`).
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        // Nagle off: we combine header + payload into one write ourselves,
        // so there is never a reason for the kernel to wait for more data.
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Publish a typed message.
    pub fn publish<T: Encode>(
        &mut self,
        channel_id: u32,
        msg_type: u8,
        msg: &T,
    ) -> io::Result<()> {
        let payload = msg.encode();
        self.publish_raw(channel_id, msg_type, &payload)
    }

    /// Publish a pre-encoded byte slice.
    ///
    /// Header and payload are combined into a **single `write_all` call**,
    /// which means a single syscall and a single TCP segment (with
    /// `TCP_NODELAY`). The previous two-write approach paid two syscall
    /// round-trips and sent two separate packets on every message.
    ///
    /// For payloads up to `MAX_PAYLOAD` (1 KB) the combined buffer lives
    /// entirely on the stack — no heap allocation on the hot path.
    pub fn publish_raw(
        &mut self,
        channel_id: u32,
        msg_type: u8,
        payload: &[u8],
    ) -> io::Result<()> {
        let header = MsgHeader::new(msg_type, channel_id, payload.len() as u32);
        let hdr_bytes = header.to_bytes(); // [u8; 17]
        let total = HEADER_SIZE + payload.len();

        if total <= INLINE_BUF {
            // Stack path: one write, one syscall, no heap allocation.
            let mut buf = [0u8; INLINE_BUF];
            buf[..HEADER_SIZE].copy_from_slice(&hdr_bytes);
            buf[HEADER_SIZE..total].copy_from_slice(payload);
            self.stream.write_all(&buf[..total])
        } else {
            // Oversized payload (> MAX_PAYLOAD). The bus will reject this, but
            // we still send gracefully rather than panicking.
            self.stream.write_all(&hdr_bytes)?;
            self.stream.write_all(payload)
        }
    }
}
