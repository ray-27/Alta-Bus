mod ring;

use protocol::{MsgHeader, HEADER_SIZE};
use ring::{ConsumerError, PublishError, Ring, PAYLOAD_CAP};
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;

// Per-subscriber send queue depth.
// The dispatch thread does try_send (non-blocking). A full queue means the
// subscriber is consistently slow — messages are dropped.
const DEFAULT_QUEUE_DEPTH: usize = 8_192;

// How many consecutive slow-consumer drops before we forcibly disconnect.
// Set to 0 to disable forced disconnection (just keep dropping).
const DROP_DISCONNECT_THRESHOLD: usize = 50_000;

// A wire frame shared across all subscribers via Arc — one allocation per
// published message regardless of fan-out count.
type Frame = Arc<Vec<u8>>;

struct SubEntry {
    tx: SyncSender<Frame>,
    channels: HashSet<u32>,
    filter_all: bool,
    addr: SocketAddr,
    /// Running count of frames dropped because this subscriber's queue was full.
    drop_count: usize,
}

type SubRegistry = Arc<Mutex<HashMap<usize, SubEntry>>>;

static SUB_ID: AtomicUsize = AtomicUsize::new(0);

fn main() {
    let ring: &'static Ring = Box::leak(Ring::new());
    let sub_registry: SubRegistry = Arc::new(Mutex::new(HashMap::new()));

    // ONE dispatch thread holds a single ring consumer cursor and fans each
    // message into per-subscriber mpsc queues. Subscriber I/O threads block
    // on rx.recv() — zero spinning outside the dispatch thread itself.
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
                let registry = Arc::clone(&sub_registry);
                thread::spawn(move || handle_connection(stream, ring, registry));
            }
            Err(e) => eprintln!("[bus] accept error: {}", e),
        }
    }
}

// ---- Dispatch loop ----------------------------------------------------------

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

            let mut subs = registry.lock().unwrap();
            let mut to_disconnect: Vec<usize> = Vec::new();

            for (&id, entry) in subs.iter_mut() {
                if !entry.filter_all && !entry.channels.contains(&d.channel_id) {
                    continue;
                }
                if entry.tx.try_send(Arc::clone(&frame)).is_err() {
                    entry.drop_count += 1;
                    // Log every power-of-two drop so the console isn't flooded
                    // but you still see the trend building.
                    if entry.drop_count.is_power_of_two() {
                        eprintln!(
                            "[bus] subscriber {} ({}) slow — {} frames dropped so far (queue depth {})",
                            id, entry.addr, entry.drop_count, DEFAULT_QUEUE_DEPTH
                        );
                    }
                    if DROP_DISCONNECT_THRESHOLD > 0
                        && entry.drop_count >= DROP_DISCONNECT_THRESHOLD
                    {
                        eprintln!(
                            "[bus] subscriber {} ({}) exceeded drop threshold ({}) — forcing disconnect",
                            id, entry.addr, DROP_DISCONNECT_THRESHOLD
                        );
                        to_disconnect.push(id);
                    }
                }
            }
            // Drop the tx handle for each subscriber that crossed the threshold.
            // Their I/O thread will see rx.recv() -> Err and exit cleanly.
            for id in to_disconnect {
                subs.remove(&id);
            }
        });

        if let Err(ConsumerError::FellBehind) = result {
            eprintln!("[bus] dispatch fell behind ring — increase ring size or reduce publish rate");
        }
    }
}

// ---- Handshake --------------------------------------------------------------

fn handle_connection(stream: TcpStream, ring: &'static Ring, sub_registry: SubRegistry) {
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
                    drop_count: 0,
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
        publish_one(ring, &header, &first_payload[..plen], addr);
        run_publisher(reader, ring, addr);
    }
}

// ---- Publisher loop ---------------------------------------------------------
//
// NOTE: no mutex anymore. Ring::publish is lock-free multi-producer — each
// publisher thread CAS-claims its own sequence slot directly. Uncontended,
// this costs the same as a plain atomic store; contended, it never parks a
// thread in the kernel the way Mutex does.

fn run_publisher(mut reader: BufReader<TcpStream>, ring: &'static Ring, addr: SocketAddr) {
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
        publish_one(ring, &header, &payload_buf[..plen], addr);
    }
}

fn publish_one(ring: &Ring, header: &MsgHeader, payload: &[u8], addr: SocketAddr) {
    loop {
        // Pass the origin timestamp from the wire header through to the ring
        // instead of re-reading the clock inside publish().
        match ring.publish(
            header.channel_id,
            header.msg_type,
            header.timestamp_ns,
            payload,
        ) {
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
// Blocks on rx.recv() (no CPU while idle), then DRAINS everything queued and
// coalesces it into a single write_all. Under load this collapses N syscalls
// into 1 — the same trick Redis uses (output buffering) — and is usually the
// difference between losing to and beating Redis pub/sub on throughput,
// without adding any latency in the idle case (the first frame is still
// written immediately after recv() wakes).

const COALESCE_BUF_CAP: usize = 64 * 1024;

fn run_subscriber_io(
    mut stream: TcpStream,
    rx: Receiver<Frame>,
    id: usize,
    addr: SocketAddr,
    registry: SubRegistry,
) {
    let mut wbuf: Vec<u8> = Vec::with_capacity(COALESCE_BUF_CAP);
    let mut frames_sent: u64 = 0;
    let mut bytes_sent: u64 = 0;

    let disconnect_reason: &str = 'outer: {
        loop {
            // Block for the first frame (no CPU while idle).
            let frame = match rx.recv() {
                Ok(f) => f,
                // tx dropped — either bus is shutting down OR the dispatch
                // loop forcibly removed this subscriber (drop threshold hit).
                Err(_) => break 'outer "dispatch sender dropped (slow-consumer disconnect or bus shutdown)",
            };

            wbuf.clear();
            wbuf.extend_from_slice(&frame);

            // Opportunistically drain whatever else is already queued.
            while wbuf.len() < COALESCE_BUF_CAP {
                match rx.try_recv() {
                    Ok(f) => wbuf.extend_from_slice(&f),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        let _ = stream.write_all(&wbuf);
                        break 'outer "dispatch sender dropped mid-drain";
                    }
                }
            }

            match stream.write_all(&wbuf) {
                Ok(()) => {
                    frames_sent += 1;
                    bytes_sent += wbuf.len() as u64;
                }
                Err(e) => {
                    // e.kind() tells us exactly what happened:
                    //   BrokenPipe      — client closed the connection cleanly
                    //   ConnectionReset — client crashed / network drop
                    //   TimedOut        — write_timeout expired (not set here)
                    //   WouldBlock      — non-blocking socket, not used here
                    eprintln!(
                        "[bus] subscriber {} ({}) write error: {} ({:?}) — frames_sent={} bytes_sent={}",
                        id, addr, e, e.kind(), frames_sent, bytes_sent
                    );
                    break 'outer "TCP write error";
                }
            }
        }
    };

    println!(
        "[bus] subscriber {} ({}) disconnected — reason: {} | frames_sent={} bytes_sent={}",
        id, addr, disconnect_reason, frames_sent, bytes_sent
    );
    registry.lock().unwrap().remove(&id);
}