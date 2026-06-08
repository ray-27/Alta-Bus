//! Rust SDK for subscribers.
//!
//! # Subscribe to all channels
//!
//! ```no_run
//! use subscriber_client::Subscriber;
//! use protocol::messages::PriceTick;
//! use protocol::Decode;
//!
//! let mut sub = Subscriber::connect("127.0.0.1:8000").unwrap();
//!
//! loop {
//!     let (header, payload) = sub.next_raw().unwrap();
//!     if header.msg_type == protocol::MsgType::PriceTick as u8 {
//!         if let Some(tick) = PriceTick::decode(&payload) {
//!             println!("channel={} price={}", header.channel_id, tick.last_price);
//!         }
//!     }
//! }
//! ```
//!
//! # Subscribe to specific channels
//!
//! ```no_run
//! use subscriber_client::Subscriber;
//!
//! let mut sub = Subscriber::connect_filtered("127.0.0.1:8000", &[1001, 1002]).unwrap();
//! sub.run(|header, payload| {
//!     println!("channel={} msg_type={}", header.channel_id, header.msg_type);
//!     true
//! })
//! .unwrap();
//! ```

use protocol::{MsgHeader, HEADER_SIZE};
use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;

// 64 KB read buffer — amortises kernel recv() calls across many messages.
// At 33 bytes per message (17 header + 16 payload for benchmark traffic),
// one syscall fills the buffer with ~2000 messages.
const BUF_CAPACITY: usize = 1 << 16;

pub struct Subscriber {
    // After the subscribe handshake we only ever read from this connection,
    // so wrapping in BufReader is safe and eliminates the per-message syscall.
    reader: BufReader<TcpStream>,
}

impl Subscriber {
    /// Connect to the bus and subscribe to **all** channels.
    pub fn connect(addr: &str) -> io::Result<Self> {
        Self::connect_filtered(addr, &[])
    }

    /// Connect and subscribe to a specific set of `channel_id`s.
    /// An empty slice means "all channels" (same as `connect`).
    pub fn connect_filtered(addr: &str, channels: &[u32]) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;

        let payload: Vec<u8> = channels
            .iter()
            .flat_map(|id| id.to_le_bytes())
            .collect();

        // Subscribe control message: msg_type=0 tells the bus this is a subscriber.
        let control = MsgHeader {
            msg_type: 0,
            channel_id: 0,
            timestamp_ns: 0,
            payload_len: payload.len() as u32,
        };
        stream.write_all(&control.to_bytes())?;
        if !payload.is_empty() {
            stream.write_all(&payload)?;
        }

        // Wrap in BufReader after the handshake write. The write half of the
        // TcpStream is moved into the BufReader, but we never write again
        // after the handshake, so no write capability is needed.
        Ok(Self {
            reader: BufReader::with_capacity(BUF_CAPACITY, stream),
        })
    }

    /// Block until the next message arrives. Returns `(header, payload)`.
    ///
    /// Reads are served from a 64 KB user-space buffer, so the per-call
    /// syscall cost is amortised across thousands of messages.
    pub fn next_raw(&mut self) -> io::Result<(MsgHeader, Vec<u8>)> {
        let mut header_buf = [0u8; HEADER_SIZE];
        self.reader.read_exact(&mut header_buf)?;

        let header = MsgHeader::from_bytes(&header_buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "malformed header from bus")
        })?;

        let mut payload = vec![0u8; header.payload_len as usize];
        if header.payload_len > 0 {
            self.reader.read_exact(&mut payload)?;
        }

        Ok((header, payload))
    }

    /// Run a blocking dispatch loop.
    /// `handler` receives `(&header, &payload)` and returns `true` to continue.
    pub fn run<F>(&mut self, mut handler: F) -> io::Result<()>
    where
        F: FnMut(&MsgHeader, &[u8]) -> bool,
    {
        loop {
            let (header, payload) = self.next_raw()?;
            if !handler(&header, &payload) {
                return Ok(());
            }
        }
    }
}
