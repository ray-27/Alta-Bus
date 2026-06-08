# alta_bus

`alta_bus` is a low-latency TCP message bus for market data. Publishers send binary frames into the bus, and subscribers receive the frames for the channels they subscribed to.

The bus listens on port `8000` by default.

```bash
cargo run --bin bus
```

To bind to a different address:

```bash
BUS_ADDR=127.0.0.1:9000 cargo run --bin bus
```

## Connection Model

Every client opens a TCP connection to the bus. The first frame sent by the client decides what kind of connection it is.

- `msg_type = 0`: subscriber connection
- `msg_type != 0`: publisher connection

There is no HTTP, no JSON, no websocket, and no request/response handshake. This is a raw TCP binary protocol.

## Wire Frame Format

Every frame is:

```text
[17 byte header][payload bytes]
```

All multi-byte fields are little-endian.

Header layout:

| Offset | Size | Field | Type | Meaning |
|---|---:|---|---|---|
| 0 | 1 | `msg_type` | `u8` | Message type. `0` is subscriber control. |
| 1 | 4 | `channel_id` | `u32` | Routing key used by the bus. |
| 5 | 8 | `timestamp_ns` | `u64` | Nanosecond timestamp. |
| 13 | 4 | `payload_len` | `u32` | Number of payload bytes after the header. |

The header is always exactly 17 bytes. Do not serialize an in-memory struct directly, because struct padding can change the size. Pack each field explicitly.

Rust equivalent:

```rust
pub struct MsgHeader {
    pub msg_type: u8,
    pub channel_id: u32,
    pub timestamp_ns: u64,
    pub payload_len: u32,
}
```

The on-wire order is:

```text
msg_type:     1 byte
channel_id:   4 bytes little-endian
timestamp_ns: 8 bytes little-endian
payload_len:  4 bytes little-endian
payload:      payload_len bytes
```

## Publishing Data

A publisher connects to `127.0.0.1:8000` and immediately sends a data frame with `msg_type != 0`.

The bus reads the header, reads `payload_len` bytes, and writes the message into its ring buffer. The bus does not decode the payload. Payload meaning is owned by publishers and subscribers.

Payload limit: `1024` bytes.

### Rust Publisher

```rust
use protocol::messages::PriceTick;
use protocol::MsgType;
use publisher_client::Publisher;

let mut publisher = Publisher::connect("127.0.0.1:8000").unwrap();

let tick = PriceTick {
    instrument_id: 1001,
    last_price: 22450.50,
    volume: 100,
    total_traded_volume: 5_000_000,
    timestamp_ns: 1_700_000_000_000_000_000,
};

publisher
    .publish(1001, MsgType::PriceTick as u8, &tick)
    .unwrap();
```

This sends:

```text
header.msg_type = 1
header.channel_id = 1001
header.payload_len = 32
payload = PriceTick encoded as little-endian bytes
```

### Raw Publisher Example

Any language can publish if it writes the same bytes.

Python example:

```python
import socket
import struct
import time

HOST = "127.0.0.1"
PORT = 8000

def header(msg_type: int, channel_id: int, payload: bytes) -> bytes:
    timestamp_ns = time.time_ns()
    return struct.pack("<BIQI", msg_type, channel_id, timestamp_ns, len(payload))

def price_tick() -> bytes:
    return struct.pack(
        "<IdIQQ",
        1001,                    # instrument_id: u32
        22450.50,                # last_price: f64
        100,                     # volume: u32
        5_000_000,               # total_traded_volume: u64
        time.time_ns(),          # timestamp_ns: u64
    )

payload = price_tick()

with socket.create_connection((HOST, PORT)) as sock:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.sendall(header(1, 1001, payload) + payload)
```

## Subscribing To Data

A subscriber connects to `127.0.0.1:8000` and immediately sends a subscribe control frame.

Subscribe control frame:

```text
msg_type = 0
channel_id = 0
timestamp_ns = 0
payload_len = number_of_channels * 4
payload = channel ids as little-endian u32 values
```

If the subscribe payload is empty, the subscriber receives all channels.

If the subscribe payload contains channel IDs, the subscriber receives only matching `channel_id`s.

The bus supports up to 256 channel IDs in the subscribe payload.

### Rust Subscriber - All Channels

```rust
use protocol::messages::PriceTick;
use protocol::{Decode, MsgType};
use subscriber_client::Subscriber;

let mut subscriber = Subscriber::connect("127.0.0.1:8000").unwrap();

loop {
    let (header, payload) = subscriber.next_raw().unwrap();

    if header.msg_type == MsgType::PriceTick as u8 {
        if let Some(tick) = PriceTick::decode(&payload) {
            println!(
                "channel={} instrument={} price={} volume={}",
                header.channel_id,
                tick.instrument_id,
                tick.last_price,
                tick.volume
            );
        }
    }
}
```

### Rust Subscriber - Selected Channels

```rust
use subscriber_client::Subscriber;

let mut subscriber =
    Subscriber::connect_filtered("127.0.0.1:8000", &[1001, 1002]).unwrap();

subscriber
    .run(|header, payload| {
        println!(
            "channel={} msg_type={} payload_len={}",
            header.channel_id,
            header.msg_type,
            payload.len()
        );

        true
    })
    .unwrap();
```

### Raw Subscriber Example

Python example for subscribing to channels `1001` and `1002`:

```python
import socket
import struct

HOST = "127.0.0.1"
PORT = 8000
HEADER_SIZE = 17

def read_exact(sock, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise ConnectionError("bus disconnected")
        out.extend(chunk)
    return bytes(out)

def subscribe(sock, channels):
    payload = b"".join(struct.pack("<I", ch) for ch in channels)
    control_header = struct.pack(
        "<BIQI",
        0,              # msg_type = subscribe control
        0,              # channel_id unused
        0,              # timestamp_ns unused
        len(payload),   # payload_len
    )
    sock.sendall(control_header + payload)

with socket.create_connection((HOST, PORT)) as sock:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    subscribe(sock, [1001, 1002])

    while True:
        raw_header = read_exact(sock, HEADER_SIZE)
        msg_type, channel_id, timestamp_ns, payload_len = struct.unpack(
            "<BIQI",
            raw_header,
        )
        payload = read_exact(sock, payload_len)

        if msg_type == 1:
            instrument_id, last_price, volume, total_volume, tick_ts = struct.unpack(
                "<IdIQQ",
                payload,
            )
            print(channel_id, instrument_id, last_price, volume, total_volume, tick_ts)
```

To subscribe to all channels, send the same control header with `payload_len = 0` and no payload.

## What The Subscriber Receives

After the subscribe control frame, the subscriber only reads from the socket. For every matching published message, the bus writes:

```text
[17 byte header][payload_len bytes]
```

The subscriber should:

1. Read exactly 17 bytes.
2. Decode the header fields as little-endian.
3. Read exactly `header.payload_len` bytes.
4. Use `header.msg_type` to decide how to decode the payload.
5. Use `header.channel_id` to know which stream the message belongs to.

The bus forwards the same payload bytes the publisher sent. It does not transform payloads.

## Payload Formats

Currently implemented payload structs in `protocol/src/messages` are `PriceTick` and `Heartbeat`. Other message type IDs are reserved in `MsgType` for future protocol structs.

### PriceTick

`msg_type = 1`

Encoded size: 32 bytes

| Offset | Size | Field | Type |
|---|---:|---|---|
| 0 | 4 | `instrument_id` | `u32` |
| 4 | 8 | `last_price` | `f64` |
| 12 | 4 | `volume` | `u32` |
| 16 | 8 | `total_traded_volume` | `u64` |
| 24 | 8 | `timestamp_ns` | `u64` |

Rust decode:

```rust
let tick = PriceTick::decode(&payload);
```

Python decode:

```python
instrument_id, last_price, volume, total_volume, timestamp_ns = struct.unpack(
    "<IdIQQ",
    payload,
)
```

### Heartbeat

`msg_type = 6`

Encoded size: 12 bytes

| Offset | Size | Field | Type |
|---|---:|---|---|
| 0 | 4 | `source_id` | `u32` |
| 4 | 8 | `sequence_num` | `u64` |

Python decode:

```python
source_id, sequence_num = struct.unpack("<IQ", payload)
```

## Message Type IDs

```text
0 = subscribe control
1 = PriceTick
2 = OrderBookDelta
3 = Signal
4 = ExecutionReport
5 = RiskBreach
6 = Heartbeat
```

The bus does not validate these types beyond checking whether the first frame is `0` or non-zero.

## Channel IDs

`channel_id` is an opaque `u32` routing key. The bus does not know that channel `1001` means NIFTY or that channel `9000` means heartbeat. It only compares IDs against each subscriber's filter set.

Suggested channel ranges:

```text
1001-1200 = price tick streams
2001-2200 = order book delta streams
3001-3100 = options tick streams
5001-5030 = strategy signal streams
9000      = heartbeat
9001      = execution reports
9002      = risk breach
9003      = system status
```

## Slow Subscriber Behavior

Each subscriber has an in-memory queue with `8192` frames. The dispatch thread uses non-blocking `try_send`.

If a subscriber is too slow and its queue fills, the bus drops new messages for that subscriber and logs:

```text
[bus] subscriber <id> (<addr>) slow - dropped msg on channel <channel_id>
```

The bus does not block publishers for a slow subscriber.

## Important Limits

- Default bus address: `0.0.0.0:8000`
- Payload max: `1024` bytes
- Subscribe filter max: `256` channel IDs
- Per-subscriber queue depth: `8192` frames
- Header size: `17` bytes
- Encoding: little-endian
- Transport: raw TCP
