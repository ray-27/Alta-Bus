mod ring;

use protocol::{MsgHeader, HEADER_SIZE};
use ring::{ConsumerError, PublishError, Ring, PAYLOAD_CAP};
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

// Per-subscriber send queue depth.
// The dispatch thread does try_send (non-blocking). A full queue means the
// subscriber is consistently slow — messages are dropped.
const DEFAULT_QUEUE_DEPTH: usize = 8_192;

// A wire frame shared across all subscribers via Arc — one allocation per
// published message regardless of fan-out count.
type Frame = Arc<Vec<u8>>;

struct SubEntry {
    tx: SyncSender<Frame>,
    channels: HashSet<u32>,
    filter_all: bool,
    addr: SocketAddr,
}

type SubRegistry = Arc<Mutex<HashMap<usize, SubEntry>>>;

static SUB_ID: AtomicUsize = AtomicUsize::new(0);

fn main() {
    let ring: &'static Ring = Box::leak(Ring::new());
    let publish_lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let sub_registry: SubRegistry = Arc::new(Mutex::new(HashMap::new()));

    // ONE dispatch thread holds a single ring consumer cursor and fans each
    // message into per-subscriber mpsc queues.  Subscriber I/O threads block
    // on rx.recv() — zero spinning outside the dispatch thread itself.
    //
    // Previously: N subscriber threads each spinning on consume_one → N threads
    //   competing for CPU → 80ms+ scheduling gaps at N=30.
    // Now: 1 dispatch thread spinning + N I/O threads sleeping in recv() →
    //   only 1 core consumed by spinning, latency ≤ 1µs for typical workloads.
    let dispatch_consumer_id = ring
        .register_consumer()
        .expect("[bus] failed to register dispatch consumer");
    {
        let registry = Arc::clone(&sub_registry);
        thread::Builder::new()
            .name("bus-dispatch".into())
            .spawn(move || dispatch_loop(ring, dispatch_consumer_id, registry))
            .expect("[bus] failed to spawn dispatch thread");
    }

    let addr = std::env::var("BUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("[bus] failed to bind {}: {}", addr, e);
        std::process::exit(1);
    });
    println!("[bus] listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "?".into());
                println!("[bus] connection from {}", peer);
                let lock = Arc::clone(&publish_lock);
                let registry = Arc::clone(&sub_registry);
                thread::spawn(move || handle_connection(stream, ring, lock, registry));
            }
            Err(e) => eprintln!("[bus] accept error: {}", e),
        }
    }
}

// ---- Dispatch loop ----------------------------------------------------------
//
// Reads one ring slot, builds a single Arc<Vec<u8>> frame, and try_sends it to
// every matching subscriber's queue.  try_send never blocks — if the queue is
// full the message is dropped and the subscriber is flagged as slow.

fn dispatch_loop(ring: &'static Ring, consumer_id: usize, registry: SubRegistry) {
    loop {
        let result = ring.consume_one(consumer_id, |d| {
            let hdr = MsgHeader {
                msg_type: d.msg_type,
                channel_id: d.channel_id,
                timestamp_ns: d.timestamp_ns,
                payload_len: d.payload_len as u32,
            };
            let payload_len = d.payload_len as usize;

            // Build the wire frame once and share via Arc (one heap alloc per
            // published message, not one per subscriber).
            let frame: Frame = Arc::new({
                let mut v = Vec::with_capacity(HEADER_SIZE + payload_len);
                v.extend_from_slice(&hdr.to_bytes());
                v.extend_from_slice(&d.payload[..payload_len]);
                v
            });

            let subs = registry.lock().unwrap();
            for (&id, entry) in subs.iter() {
                if !entry.filter_all && !entry.channels.contains(&d.channel_id) {
                    continue;
                }
                // Arc::clone is ~2ns — cheap regardless of subscriber count.
                if entry.tx.try_send(Arc::clone(&frame)).is_err() {
                    eprintln!(
                        "[bus] subscriber {} ({}) slow — dropped msg on channel {}",
                        id, entry.addr, d.channel_id
                    );
                }
            }
        });

        if let Err(ConsumerError::FellBehind) = result {
            eprintln!("[bus] dispatch fell behind ring — increase ring size or reduce publish rate");
        }
    }
}

// ---- Handshake --------------------------------------------------------------

fn handle_connection(
    stream: TcpStream,
    ring: &'static Ring,
    publish_lock: Arc<Mutex<()>>,
    sub_registry: SubRegistry,
) {
    let addr = match stream.peer_addr() {
        Ok(a) => a,
        Err(_) => return,
    };

    let mut reader = BufReader::with_capacity(1 << 16, stream);
    let mut hdr_buf = [0u8; HEADER_SIZE];

    if reader.read_exact(&mut hdr_buf).is_err() {
        return;
    }
    let header = match MsgHeader::from_bytes(&hdr_buf) {
        Some(h) => h,
        None => {
            eprintln!("[bus] malformed header from {}", addr);
            return;
        }
    };

    if header.msg_type == 0 {
        // ---- Subscriber handshake ----
        let plen = header.payload_len as usize;
        let mut ch_payload = [0u8; 256 * 4]; // up to 256 channels inline
        let ch_slice = if plen <= ch_payload.len() {
            &mut ch_payload[..plen]
        } else {
            eprintln!("[bus] subscribe payload too large from {}", addr);
            return;
        };
        if plen > 0 && reader.read_exact(ch_slice).is_err() {
            return;
        }

        let channels: HashSet<u32> = ch_slice
            .chunks_exact(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect();

        let stream = reader.into_inner();
        let (tx, rx): (SyncSender<Frame>, Receiver<Frame>) =
            mpsc::sync_channel(DEFAULT_QUEUE_DEPTH);
        let id = SUB_ID.fetch_add(1, Ordering::Relaxed);
        let filter_all = channels.is_empty();

        {
            let desc = if filter_all {
                "all channels".into()
            } else {
                format!("{} channels", channels.len())
            };
            println!("[bus] {} → subscriber (id={}, {})", addr, id, desc);
            sub_registry.lock().unwrap().insert(
                id,
                SubEntry {
                    tx,
                    channels,
                    filter_all,
                    addr,
                },
            );
        }

        run_subscriber_io(stream, rx, id, addr, sub_registry);
    } else {
        // ---- Publisher — first message already in reader buffer ----
        let plen = header.payload_len as usize;
        let mut first_payload = [0u8; PAYLOAD_CAP];
        if plen > PAYLOAD_CAP {
            eprintln!("[bus] payload too large from {}", addr);
            return;
        }
        if plen > 0 && reader.read_exact(&mut first_payload[..plen]).is_err() {
            return;
        }
        println!("[bus] {} → publisher", addr);
        publish_one(ring, &publish_lock, &header, &first_payload[..plen], addr);
        run_publisher(reader, ring, publish_lock, addr);
    }
}

// ---- Publisher loop ---------------------------------------------------------

fn run_publisher(
    mut reader: BufReader<TcpStream>,
    ring: &'static Ring,
    publish_lock: Arc<Mutex<()>>,
    addr: SocketAddr,
) {
    let mut hdr_buf = [0u8; HEADER_SIZE];
    let mut payload_buf = [0u8; PAYLOAD_CAP];

    loop {
        if reader.read_exact(&mut hdr_buf).is_err() {
            println!("[bus] publisher {} disconnected", addr);
            return;
        }
        let header = match MsgHeader::from_bytes(&hdr_buf) {
            Some(h) => h,
            None => {
                eprintln!("[bus] malformed header from publisher {}", addr);
                return;
            }
        };
        let plen = header.payload_len as usize;
        if plen > PAYLOAD_CAP {
            eprintln!("[bus] payload too large ({} B) from {} — dropped", plen, addr);
            return;
        }
        if plen > 0 && reader.read_exact(&mut payload_buf[..plen]).is_err() {
            eprintln!("[bus] incomplete payload from publisher {}", addr);
            return;
        }
        publish_one(ring, &publish_lock, &header, &payload_buf[..plen], addr);
    }
}

fn publish_one(
    ring: &Ring,
    lock: &Mutex<()>,
    header: &MsgHeader,
    payload: &[u8],
    addr: SocketAddr,
) {
    loop {
        let result = {
            let _guard = lock.lock().unwrap();
            ring.publish(header.channel_id, header.msg_type, payload)
        };
        match result {
            Ok(()) => return,
            Err(PublishError::SlowConsumer) => std::thread::yield_now(),
            Err(PublishError::PayloadTooLarge) => {
                eprintln!(
                    "[bus] publisher {} sent payload too large ({} B) — dropped",
                    addr,
                    payload.len()
                );
                return;
            }
        }
    }
}

// ---- Subscriber I/O thread --------------------------------------------------
//
// Blocks on rx.recv() (no CPU usage while idle), then writes the frame to the
// subscriber's TCP socket.  Removes itself from the registry on disconnect.

fn run_subscriber_io(
    mut stream: TcpStream,
    rx: Receiver<Frame>,
    id: usize,
    addr: SocketAddr,
    registry: SubRegistry,
) {
    loop {
        let frame = match rx.recv() {
            Ok(f) => f,
            Err(_) => break, // dispatch thread dropped — bus shutting down
        };
        if stream.write_all(&frame).is_err() {
            println!("[bus] subscriber {} ({}) disconnected", id, addr);
            break;
        }
    }
    registry.lock().unwrap().remove(&id);
}
