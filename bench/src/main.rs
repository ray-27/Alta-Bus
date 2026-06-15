//! mbus vs Redis pub/sub — end-to-end latency benchmark
//!
//! Connects N subscribers to each transport BEFORE the publisher starts, then
//! sends M messages at a controlled rate and measures the round-trip latency
//! seen by every individual subscriber.
//!
//! # Usage (always build in --release for accurate numbers)
//!
//! ```bash
//! cargo run --bin bench --release -- [OPTIONS]
//! ```
//!
//! # Options
//!
//! | Flag          | Default                  | Description                            |
//! |---------------|--------------------------|----------------------------------------|
//! | --fans N      | 1                        | subscribers per transport              |
//! | --messages M  | 100_000                  | measured messages (after warmup)       |
//! | --warmup W    | 1_000                    | messages to discard at start           |
//! | --rate-hz R   | 10_000                   | publish rate Hz; 0 = full speed        |
//! | --bus-addr A  | 127.0.0.1:8000           | mbus server address                    |
//! | --redis-url U | redis://127.0.0.1:6379   | Redis connection URL                   |
//!
//! # What latency measures
//!
//! ```
//! origin_ns  ← stamped by publisher BEFORE encoding, BEFORE any network send
//!     │
//!     │   kernel TCP send → bus routing / Redis broker → kernel TCP recv
//!     ▼
//! recv_ns   ← stamped by subscriber immediately after read() returns
//!
//! latency = recv_ns − origin_ns
//! ```
//!
//! Encoding time is NOT included (it happens before origin_ns is stamped).
//! Clock skew is NOT a factor — publisher and subscribers share the same process
//! on the same machine, so CLOCK_REALTIME is authoritative for all timestamps.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use protocol::{MsgHeader, HEADER_SIZE};
use publisher_client::Publisher;

// ─── Benchmark payload ────────────────────────────────────────────────────────
//
//  A fixed 41-byte little-endian struct sent to BOTH transports unchanged.
//  origin_ns lives at offset 0 so subscribers can decode it with a single
//  8-byte read — no struct import needed on the consumer side.
//
//  offset  0 : origin_ns     u64   publisher wall-clock ns, set BEFORE any send
//  offset  8 : price         f64
//  offset 16 : bid           f64
//  offset 24 : ask           f64
//  offset 32 : instrument_id u32
//  offset 36 : seq_num       u32
//  offset 40 : flags         u8    (reserved, always 0)

const PAYLOAD_LEN: usize = 41;
const BENCH_CHANNEL: u32 = 1001;
const BENCH_MSG_TYPE: u8 = 1;
const REDIS_CHANNEL: &str = "mbus_bench";

fn encode_tick(origin_ns: u64, price: f64, bid: f64, ask: f64, seq: u32) -> [u8; PAYLOAD_LEN] {
    let mut b = [0u8; PAYLOAD_LEN];
    b[0..8].copy_from_slice(&origin_ns.to_le_bytes());
    b[8..16].copy_from_slice(&price.to_le_bytes());
    b[16..24].copy_from_slice(&bid.to_le_bytes());
    b[24..32].copy_from_slice(&ask.to_le_bytes());
    b[32..36].copy_from_slice(&1001u32.to_le_bytes());
    b[36..40].copy_from_slice(&seq.to_le_bytes());
    b[40] = 0;
    b
}

/// Read the origin timestamp from a received payload.
/// Inline so the hot path in each subscriber has zero call overhead.
#[inline(always)]
fn extract_origin_ns(payload: &[u8]) -> Option<u64> {
    if payload.len() < 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&payload[..8]);
    Some(u64::from_le_bytes(arr))
}

// ─── Clock ───────────────────────────────────────────────────────────────────

#[inline(always)]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ─── CLI args ─────────────────────────────────────────────────────────────────

struct Args {
    fans:      usize,
    messages:  usize,
    warmup:    usize,
    rate_hz:   u64,
    bus_addr:  String,
    redis_url: String,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let get = |flag: &str, default: &str| -> String {
        argv.windows(2)
            .find(|w| w[0] == flag)
            .map(|w| w[1].clone())
            .unwrap_or_else(|| default.to_string())
    };
    Args {
        fans:      get("--fans",      "1").parse().unwrap_or(1),
        messages:  get("--messages",  "100000").parse().unwrap_or(100_000),
        warmup:    get("--warmup",    "1000").parse().unwrap_or(1_000),
        rate_hz:   get("--rate-hz",   "10000").parse().unwrap_or(10_000),
        bus_addr:  get("--bus-addr",  "127.0.0.1:8000"),
        redis_url: get("--redis-url", "redis://127.0.0.1:6379"),
    }
}

// ─── mbus subscriber thread ───────────────────────────────────────────────────

fn run_mbus_sub(
    bus_addr: String,
    total:    usize,   // warmup + messages
    warmup:   usize,
    ready:    Arc<AtomicUsize>,
) -> Vec<u64> {
    let stream = TcpStream::connect(&bus_addr).unwrap_or_else(|e| {
        eprintln!("[mbus-sub] connect failed: {e}");
        std::process::exit(1);
    });
    stream.set_nodelay(true).unwrap();
    // Unblock after publisher finishes + generous margin
    stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();

    // Subscribe handshake: msg_type=0, empty payload → all channels
    let ctrl = MsgHeader { msg_type: 0, channel_id: 0, timestamp_ns: 0, payload_len: 0 };
    (&stream).write_all(&ctrl.to_bytes()).expect("[mbus-sub] handshake failed");

    let mut reader = BufReader::with_capacity(1 << 16, stream);
    let mut hdr_buf = [0u8; HEADER_SIZE];
    let mut pay_buf = [0u8; 1024];

    // Signal ready — publisher waits until all subscribers increment this.
    ready.fetch_add(1, Ordering::Release);

    let mut samples = Vec::with_capacity(total.saturating_sub(warmup));
    let mut count = 0usize;

    loop {
        if reader.read_exact(&mut hdr_buf).is_err() {
            break;
        }
        let hdr = match MsgHeader::from_bytes(&hdr_buf) {
            Some(h) => h,
            None    => break,
        };
        let plen = hdr.payload_len as usize;
        if plen > 0 && reader.read_exact(&mut pay_buf[..plen]).is_err() {
            break;
        }

        // Stamp receive time IMMEDIATELY after the read returns.
        let recv_ns = now_ns();
        count += 1;

        if count > warmup {
            if let Some(origin_ns) = extract_origin_ns(&pay_buf[..plen]) {
                samples.push(recv_ns.saturating_sub(origin_ns));
            }
        }

        if count >= total {
            break;
        }
    }

    samples
}

// ─── Redis subscriber thread ──────────────────────────────────────────────────

fn run_redis_sub(
    redis_url: String,
    total:     usize,
    warmup:    usize,
    ready:     Arc<AtomicUsize>,
) -> Vec<u64> {
    let client = redis::Client::open(redis_url.as_str()).unwrap_or_else(|e| {
        eprintln!("[redis-sub] client open failed: {e}");
        std::process::exit(1);
    });
    let mut conn = client.get_connection().unwrap_or_else(|e| {
        eprintln!("[redis-sub] connect failed: {e}");
        std::process::exit(1);
    });
    let mut pubsub = conn.as_pubsub();
    // Unblock after publisher finishes + generous margin
    pubsub.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    pubsub.subscribe(REDIS_CHANNEL).expect("[redis-sub] subscribe failed");

    // Signal ready
    ready.fetch_add(1, Ordering::Release);

    let mut samples = Vec::with_capacity(total.saturating_sub(warmup));
    let mut count = 0usize;

    loop {
        let msg = match pubsub.get_message() {
            Ok(m)  => m,
            Err(_) => break,
        };
        let recv_ns = now_ns();
        count += 1;

        if count > warmup {
            let payload: Vec<u8> = match msg.get_payload() {
                Ok(p)  => p,
                Err(_) => continue,
            };
            if let Some(origin_ns) = extract_origin_ns(&payload) {
                samples.push(recv_ns.saturating_sub(origin_ns));
            }
        }

        if count >= total {
            break;
        }
    }

    samples
}

// ─── Publisher (runs on main thread after subscribers are ready) ───────────────

fn run_publisher(args: &Args) {
    let total = args.messages + args.warmup;

    let mut mbus_pub = Publisher::connect(&args.bus_addr).unwrap_or_else(|e| {
        eprintln!("[pub] mbus connect failed: {e}");
        std::process::exit(1);
    });

    let redis_client = redis::Client::open(args.redis_url.as_str()).unwrap_or_else(|e| {
        eprintln!("[pub] redis client failed: {e}");
        std::process::exit(1);
    });
    let mut redis_conn = redis_client.get_connection().unwrap_or_else(|e| {
        eprintln!("[pub] redis connect failed: {e}");
        std::process::exit(1);
    });

    // interval_ns = 0 means full-speed busy loop
    let interval_ns: u64 = if args.rate_hz == 0 {
        0
    } else {
        1_000_000_000 / args.rate_hz
    };

    let mut price = 24_150.50f64;
    let mut rng   = Rng::new(now_ns());
    let start_ns  = now_ns();

    for i in 0u32..(total as u32) {
        // ── Rate gate ──────────────────────────────────────────────────────
        // Busy-wait against the wall clock for sub-millisecond precision.
        // A thread::sleep here would have 1–2 ms jitter on Linux, which
        // would dominate latency measurements at 10 kHz.
        if interval_ns > 0 {
            let target = start_ns + (i as u64) * interval_ns;
            while now_ns() < target {
                std::hint::spin_loop();
            }
        }

        // ── Build payload ──────────────────────────────────────────────────
        let move_ = (rng.f64() + rng.f64() - 1.0) * 5.0;
        price = (price + move_).max(24_000.0);
        let spread = 0.05 + rng.f64() * 0.10;

        // origin_ns is stamped AFTER the rate gate, immediately before encode.
        // This is the moment "the message exists" — both transports receive
        // the same origin_ns and latency is measured against it independently.
        let origin_ns = now_ns();
        let payload = encode_tick(origin_ns, price, price - spread, price + spread, i);

        // ── Send to mbus ───────────────────────────────────────────────────
        // publish_raw: header + payload in a single write_all (one syscall).
        if let Err(e) = mbus_pub.publish_raw(BENCH_CHANNEL, BENCH_MSG_TYPE, &payload) {
            eprintln!("[pub] mbus write error at seq {i}: {e}");
        }

        // ── Send to Redis ──────────────────────────────────────────────────
        let _: redis::RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(REDIS_CHANNEL)
            .arg(&payload[..])
            .query(&mut redis_conn);
    }

    println!("[pub] done — sent {} messages", total);
}

// ─── Stats & reporting ────────────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn avg(samples: &[u64]) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.iter().sum::<u64>() / samples.len() as u64
}

fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    }
}

fn print_report(mut mbus: Vec<u64>, mut redis: Vec<u64>, fans: usize, rate_hz: u64) {
    mbus.sort_unstable();
    redis.sort_unstable();

    // ── Header ────────────────────────────────────────────────────────────────
    let sep = "─".repeat(99);
    println!();
    println!("┌{sep}┐");
    println!(
        "│{:^99}│",
        format!(
            "mbus vs Redis pub/sub — end-to-end latency   fans={}  rate={}Hz  mbus_n={}  redis_n={}",
            fans,
            if rate_hz == 0 { "max".to_string() } else { rate_hz.to_string() },
            mbus.len(),
            redis.len()
        )
    );
    println!("├───────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┤");
    println!(
        "│{:<11}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│",
        " Transport", " Samples", "    Min", "    Avg", "    P50", "    P95", "    P99", "  P99.9", "    Max"
    );
    println!("├───────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤");

    // ── Data rows ─────────────────────────────────────────────────────────────
    let row = |label: &str, s: &[u64]| {
        println!(
            "│{:<11}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│",
            format!(" {label}"),
            s.len(),
            fmt_ns(s.first().copied().unwrap_or(0)),
            fmt_ns(avg(s)),
            fmt_ns(percentile(s, 50.0)),
            fmt_ns(percentile(s, 95.0)),
            fmt_ns(percentile(s, 99.0)),
            fmt_ns(percentile(s, 99.9)),
            fmt_ns(s.last().copied().unwrap_or(0)),
        );
    };

    row("mbus", &mbus);
    row("Redis", &redis);

    println!("├───────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤");

    // ── Redis / mbus ratio row ────────────────────────────────────────────────
    let ratio = |m: u64, r: u64| -> String {
        if m == 0 || r == 0 {
            return "     —".to_string();
        }
        format!("{:>8.2}x", r as f64 / m as f64)
    };

    println!(
        "│{:<11}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│{:>10}│",
        " ratio",
        "(R/M)",
        ratio(mbus.first().copied().unwrap_or(0), redis.first().copied().unwrap_or(0)),
        ratio(avg(&mbus), avg(&redis)),
        ratio(percentile(&mbus, 50.0), percentile(&redis, 50.0)),
        ratio(percentile(&mbus, 95.0), percentile(&redis, 95.0)),
        ratio(percentile(&mbus, 99.0), percentile(&redis, 99.0)),
        ratio(percentile(&mbus, 99.9), percentile(&redis, 99.9)),
        ratio(mbus.last().copied().unwrap_or(0), redis.last().copied().unwrap_or(0)),
    );

    println!("└───────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘");

    // ── Histogram of mbus latency (quick visual) ──────────────────────────────
    if !mbus.is_empty() {
        println!();
        println!("  mbus latency histogram (µs):");
        print_histogram(&mbus);
    }
    if !redis.is_empty() {
        println!();
        println!("  Redis latency histogram (µs):");
        print_histogram(&redis);
    }
}

/// Logarithmic bucket histogram printed to stdout.
fn print_histogram(sorted: &[u64]) {
    // Buckets in µs: <1, 1-2, 2-5, 5-10, 10-20, 20-50, 50-100, 100-200, 200-500, 500-1000, 1ms+
    let buckets: &[(u64, u64, &str)] = &[
        (0,         1_000,   "< 1µs   "),
        (1_000,     2_000,   "1–2µs   "),
        (2_000,     5_000,   "2–5µs   "),
        (5_000,     10_000,  "5–10µs  "),
        (10_000,    20_000,  "10–20µs "),
        (20_000,    50_000,  "20–50µs "),
        (50_000,    100_000, "50–100µs"),
        (100_000,   200_000, "100–200µs"),
        (200_000,   500_000, "200–500µs"),
        (500_000,   1_000_000, "500µs–1ms"),
        (1_000_000, u64::MAX, "> 1ms   "),
    ];

    let total = sorted.len() as f64;
    const BAR_WIDTH: usize = 40;

    for (lo, hi, label) in buckets {
        let count = sorted.partition_point(|&x| x < *hi)
            - sorted.partition_point(|&x| x < *lo);
        if count == 0 {
            continue;
        }
        let pct  = count as f64 / total * 100.0;
        let bars = ((pct / 100.0) * BAR_WIDTH as f64).round() as usize;
        println!(
            "  {label}  {:>7} ({:>5.1}%)  {}",
            count,
            pct,
            "█".repeat(bars)
        );
    }
}

// ─── Minimal xorshift64 PRNG ──────────────────────────────────────────────────

struct Rng {
    s: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { s: if seed == 0 { 0xdeadbeef_cafef00d } else { seed } }
    }

    #[inline(always)]
    fn next(&mut self) -> u64 {
        self.s ^= self.s << 13;
        self.s ^= self.s >> 7;
        self.s ^= self.s << 17;
        self.s
    }

    #[inline(always)]
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args = Arc::new(parse_args());
    let total = args.messages + args.warmup;

    println!(
        "[bench] fans={fans}  messages={messages}  warmup={warmup}  rate={rate}Hz  bus={bus}  redis={redis}",
        fans     = args.fans,
        messages = args.messages,
        warmup   = args.warmup,
        rate     = if args.rate_hz == 0 { "MAX".to_string() } else { args.rate_hz.to_string() },
        bus      = args.bus_addr,
        redis    = args.redis_url,
    );

    let ready           = Arc::new(AtomicUsize::new(0));
    let expected_ready  = args.fans * 2;

    // ── Spawn mbus subscriber threads ─────────────────────────────────────────
    let mbus_handles: Vec<_> = (0..args.fans)
        .map(|i| {
            let addr    = args.bus_addr.clone();
            let r       = Arc::clone(&ready);
            let warmup  = args.warmup;
            thread::Builder::new()
                .name(format!("mbus-sub-{i}"))
                .spawn(move || run_mbus_sub(addr, total, warmup, r))
                .expect("spawn mbus sub failed")
        })
        .collect();

    // ── Spawn Redis subscriber threads ────────────────────────────────────────
    let redis_handles: Vec<_> = (0..args.fans)
        .map(|i| {
            let url    = args.redis_url.clone();
            let r      = Arc::clone(&ready);
            let warmup = args.warmup;
            thread::Builder::new()
                .name(format!("redis-sub-{i}"))
                .spawn(move || run_redis_sub(url, total, warmup, r))
                .expect("spawn redis sub failed")
        })
        .collect();

    // ── Wait until every subscriber has connected and is listening ────────────
    print!("[bench] waiting for {expected_ready} subscribers to connect");
    while ready.load(Ordering::Acquire) < expected_ready {
        thread::sleep(Duration::from_millis(20));
        print!(".");
        let _ = std::io::stdout().flush();
    }
    println!(" done");

    // Small grace period so Redis SUBSCRIBE fully propagates before the first
    // PUBLISH arrives. 200 ms is overkill for localhost but costs nothing.
    thread::sleep(Duration::from_millis(200));
    println!("[bench] all subscribers ready — starting publisher");

    // ── Publish (main thread) ─────────────────────────────────────────────────
    run_publisher(&args);

    // ── Collect results ───────────────────────────────────────────────────────
    let mbus_samples: Vec<u64> = mbus_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap_or_default())
        .collect();

    let redis_samples: Vec<u64> = redis_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap_or_default())
        .collect();

    println!(
        "[bench] collected {} mbus samples, {} redis samples",
        mbus_samples.len(),
        redis_samples.len()
    );

    print_report(mbus_samples, redis_samples, args.fans, args.rate_hz);
}
