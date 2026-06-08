//! Rust-native one-way latency benchmark: alta_bus vs Valkey (Redis).
//!
//! Methodology
//! -----------
//! Publisher and subscriber run in the same OS process — clocks are identical,
//! zero synchronisation error. Each message carries an 8-byte send-timestamp
//! in its payload. The subscriber reads `recv_ns - send_ns` to get one-way
//! transit time without any Python overhead.
//!
//! Rate control uses a busy-wait deadline loop for sub-microsecond accuracy.
//! `thread::sleep` on macOS has ~50 µs granularity and would dominate the
//! measurement; spinning avoids that entirely.
//!
//! Usage
//! -----
//!     cargo run --bin latency --release
//!     cargo run --bin latency --release -- --count 200000 --rate 200000
//!     cargo run --bin latency --release -- --no-redis   # bus only
//!
//! Requirements
//! ------------
//!     cargo run --bin bus          # alta_bus must be running
//!     Valkey / Redis at 127.0.0.1:6379

use publisher_client::Publisher;
use subscriber_client::Subscriber;

use std::sync::{Arc, Condvar, Mutex};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

// ---- Clock ------------------------------------------------------------------

// One shared epoch so publisher and subscriber threads produce comparable ns values.
static T0: OnceLock<Instant> = OnceLock::new();

#[inline(always)]
fn now_ns() -> u64 {
    T0.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

// ---- Constants --------------------------------------------------------------

const BENCH_CHANNEL: u32 = 9999;
const BENCH_MSG_TYPE: u8  = 99;
const REDIS_CHANNEL:  &str = "alta_bus:bench";

// ---- Statistics -------------------------------------------------------------

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let k  = (sorted.len() as f64 - 1.0) * p / 100.0;
    let lo = k as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    sorted[lo] as f64 + (sorted[hi] as f64 - sorted[lo] as f64) * (k - lo as f64)
}

fn mean(d: &[u64]) -> f64 { d.iter().sum::<u64>() as f64 / d.len() as f64 }

fn stdev(d: &[u64]) -> f64 {
    let m = mean(d);
    (d.iter().map(|&x| (x as f64 - m).powi(2)).sum::<f64>() / d.len() as f64).sqrt()
}

/// Log-scale ASCII histogram.  Each bucket doubles in width.
fn histogram(sorted: &[u64], label: &str) {
    // Buckets: ..250ns, 250-500, 500-1µs, 1-2, 2-4, 4-8, 8-16, 16-32,
    //          32-64, 64-128, 128-256, 256-512, 512µs-1ms, 1ms-2ms, >2ms
    let boundaries: &[u64] = &[
        250, 500, 1_000, 2_000, 4_000, 8_000, 16_000,
        32_000, 64_000, 128_000, 256_000, 512_000, 1_000_000, 2_000_000,
    ];
    let labels = [
        "<250ns", "250-500ns", "500ns-1µs", "1-2µs", "2-4µs", "4-8µs",
        "8-16µs", "16-32µs", "32-64µs", "64-128µs", "128-256µs",
        "256-512µs", "512µs-1ms", "1-2ms", ">2ms",
    ];
    let mut counts = vec![0usize; labels.len()];
    for &v in sorted {
        let idx = boundaries.partition_point(|&b| v >= b);
        counts[idx] += 1;
    }
    let peak = *counts.iter().max().unwrap_or(&1).max(&1);
    const BAR_W: usize = 40;

    println!("\n  {} latency distribution", label);
    println!("  {:<12}  {:BAR_W$}  count   pct", "bucket", "");
    for (i, &c) in counts.iter().enumerate() {
        if c == 0 { continue; }
        let bar = "█".repeat(c * BAR_W / peak);
        let pct = c as f64 * 100.0 / sorted.len() as f64;
        println!("  {:<12}  {:<BAR_W$}  {:>6}  {:>5.1}%", labels[i], bar, c, pct);
    }
}

fn print_stats(label: &str, lats: &[u64], show_hist: bool) {
    let mut s = lats.to_vec();
    s.sort_unstable();
    let us = |ns: f64| ns / 1_000.0;

    println!("  ┌─ {}", label);
    println!("  │  samples : {:>10}", s.len());
    println!("  │  min     : {:>10.3} µs", us(s[0] as f64));
    println!("  │  p50     : {:>10.3} µs", us(percentile(&s, 50.0)));
    println!("  │  p95     : {:>10.3} µs", us(percentile(&s, 95.0)));
    println!("  │  p99     : {:>10.3} µs", us(percentile(&s, 99.0)));
    println!("  │  p99.9   : {:>10.3} µs", us(percentile(&s, 99.9)));
    println!("  │  max     : {:>10.3} µs", us(*s.last().unwrap() as f64));
    println!("  │  mean    : {:>10.3} µs", us(mean(&s)));
    println!("  └  stdev   : {:>10.3} µs", us(stdev(&s)));

    if show_hist {
        histogram(&s, label);
    }
}

fn print_comparison(bus: &[u64], redis: &[u64]) {
    let mut b = bus.to_vec();   b.sort_unstable();
    let mut r = redis.to_vec(); r.sort_unstable();
    let us = |ns: f64| ns / 1_000.0;

    println!("\n  {}", "═".repeat(66));
    println!("  {:<8}  {:>12}  {:>12}  {:>7}  bar (Valkey relative)", "", "alta_bus", "Valkey", "ratio");
    println!("  {}", "─".repeat(66));

    for (label, p) in [("p50", 50.0), ("p95", 95.0), ("p99", 99.0), ("p99.9", 99.9), ("max", 100.0)] {
        let bv = percentile(&b, p);
        let rv = percentile(&r, p);
        let ratio = if bv > 0.0 { rv / bv } else { f64::INFINITY };
        let bar   = "▓".repeat((ratio * 6.0).min(48.0) as usize);
        println!("  {:<8}  {:>10.3} µs  {:>10.3} µs  {:>6.1}x  {}",
                 label, us(bv), us(rv), ratio, bar);
    }
    println!("  {}", "═".repeat(66));

    let ratio_p50 = percentile(&r, 50.0) / percentile(&b, 50.0).max(1.0);
    let ratio_p99 = percentile(&r, 99.0) / percentile(&b, 99.0).max(1.0);
    println!("\n  alta_bus is {:.1}x faster at p50  |  {:.1}x faster at p99", ratio_p50, ratio_p99);
}

// ---- Rate control -----------------------------------------------------------

/// Busy-wait until `elapsed >= target_ns` from `start`.
/// More accurate than `thread::sleep` for intervals < 1 ms.
#[inline(always)]
fn wait_until(start: Instant, target_ns: u64) {
    loop {
        if start.elapsed().as_nanos() as u64 >= target_ns {
            return;
        }
        std::hint::spin_loop();
    }
}

// ---- alta_bus benchmark -----------------------------------------------------

fn bench_bus(addr: &str, n_warmup: usize, n_msgs: usize, rate_hz: u64)
    -> Result<Vec<u64>, Box<dyn std::error::Error>>
{
    let total       = n_warmup + n_msgs;
    let interval_ns = if rate_hz > 0 { 1_000_000_000u64 / rate_hz } else { 0 };

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = Arc::clone(&pair);
    let addr2 = addr.to_string();

    let sub_handle = thread::spawn(move || -> Vec<u64> {
        let mut sub = Subscriber::connect_filtered(&addr2, &[BENCH_CHANNEL])
            .expect("subscriber connect failed — is the bus running?");

        // Signal that we are connected and the subscribe handshake is sent.
        let (lock, cvar) = &*pair2;
        *lock.lock().unwrap() = true;
        cvar.notify_one();

        let mut lats = Vec::with_capacity(n_msgs);
        for i in 0..total {
            let (_hdr, payload) = sub.next_raw().expect("subscriber recv");
            let recv_ns = now_ns();
            if payload.len() >= 8 {
                let send_ns = u64::from_le_bytes(payload[..8].try_into().unwrap());
                if i >= n_warmup {
                    lats.push(recv_ns.saturating_sub(send_ns));
                }
            }
        }
        lats
    });

    // Wait until subscriber is registered, then give the bus a beat to settle.
    {
        let (lock, cvar) = &*pair;
        let mut ready = lock.lock().unwrap();
        while !*ready { ready = cvar.wait(ready).unwrap(); }
    }
    thread::sleep(Duration::from_millis(50));

    // Publish with busy-wait rate control.
    let mut pub_ = Publisher::connect(addr)?;
    let run_start = Instant::now();

    for i in 0..total {
        if interval_ns > 0 {
            wait_until(run_start, i as u64 * interval_ns);
        }
        let send_ns = now_ns();
        pub_.publish_raw(BENCH_CHANNEL, BENCH_MSG_TYPE, &send_ns.to_le_bytes())?;
    }

    Ok(sub_handle.join().expect("subscriber thread panicked"))
}

// ---- Valkey / Redis benchmark -----------------------------------------------

fn bench_redis(addr: &str, n_warmup: usize, n_msgs: usize, rate_hz: u64)
    -> Result<Vec<u64>, Box<dyn std::error::Error>>
{
    let total       = n_warmup + n_msgs;
    let interval_ns = if rate_hz > 0 { 1_000_000_000u64 / rate_hz } else { 0 };
    let redis_url   = format!("redis://{}", addr);

    let pair  = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = Arc::clone(&pair);
    let url2  = redis_url.clone();

    let sub_handle = thread::spawn(move || -> Vec<u64> {
        let client = redis::Client::open(url2).expect("redis client");
        let mut con = client.get_connection().expect("redis connection");
        let mut ps  = con.as_pubsub();
        ps.subscribe(REDIS_CHANNEL).expect("redis subscribe");

        // Signal ready.
        let (lock, cvar) = &*pair2;
        *lock.lock().unwrap() = true;
        cvar.notify_one();

        let mut lats = Vec::with_capacity(n_msgs);
        for i in 0..total {
            let msg     = ps.get_message().expect("redis get_message");
            let recv_ns = now_ns();
            let payload: Vec<u8> = msg.get_payload().expect("redis payload");
            if payload.len() >= 8 {
                let send_ns = u64::from_le_bytes(payload[..8].try_into().unwrap());
                if i >= n_warmup {
                    lats.push(recv_ns.saturating_sub(send_ns));
                }
            }
        }
        lats
    });

    {
        let (lock, cvar) = &*pair;
        let mut ready = lock.lock().unwrap();
        while !*ready { ready = cvar.wait(ready).unwrap(); }
    }
    thread::sleep(Duration::from_millis(50));

    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_connection()?;
    let run_start = Instant::now();

    for i in 0..total {
        if interval_ns > 0 {
            wait_until(run_start, i as u64 * interval_ns);
        }
        let send_ns = now_ns();
        let _: i64 = redis::cmd("PUBLISH")
            .arg(REDIS_CHANNEL)
            .arg(send_ns.to_le_bytes().as_slice())
            .query(&mut con)?;
    }

    Ok(sub_handle.join().expect("subscriber thread panicked"))
}

// ---- Fan-out benchmark (N concurrent subscribers) ---------------------------

/// Spawn `n_subs` subscribers, wait for all to register, then publish.
/// Returns latency samples per subscriber as a flat Vec (all combined).
fn bench_bus_fanout(
    addr: &str,
    n_warmup: usize,
    n_msgs: usize,
    rate_hz: u64,
    n_subs: usize,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let total       = n_warmup + n_msgs;
    let interval_ns = if rate_hz > 0 { 1_000_000_000u64 / rate_hz } else { 0 };

    // Barrier: publisher waits until ALL subscribers have registered.
    let ready_count = Arc::new((Mutex::new(0usize), Condvar::new()));

    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let rc  = Arc::clone(&ready_count);
        let a   = addr.to_string();

        handles.push(thread::spawn(move || -> Vec<u64> {
            let mut sub = Subscriber::connect_filtered(&a, &[BENCH_CHANNEL])
                .expect("subscriber connect");

            let (lock, cvar) = &*rc;
            { let mut c = lock.lock().unwrap(); *c += 1; }
            cvar.notify_one();

            let mut lats = Vec::with_capacity(n_msgs);
            for i in 0..total {
                let (_hdr, payload) = sub.next_raw().expect("recv");
                let recv_ns = now_ns();
                if payload.len() >= 8 {
                    let send_ns = u64::from_le_bytes(payload[..8].try_into().unwrap());
                    if i >= n_warmup { lats.push(recv_ns.saturating_sub(send_ns)); }
                }
            }
            lats
        }));
    }

    // Wait for every subscriber to check in.
    {
        let (lock, cvar) = &*ready_count;
        let mut c = lock.lock().unwrap();
        while *c < n_subs { c = cvar.wait(c).unwrap(); }
    }
    thread::sleep(Duration::from_millis(50));

    let mut pub_ = Publisher::connect(addr)?;
    let run_start = Instant::now();
    for i in 0..total {
        if interval_ns > 0 { wait_until(run_start, i as u64 * interval_ns); }
        let send_ns = now_ns();
        pub_.publish_raw(BENCH_CHANNEL, BENCH_MSG_TYPE, &send_ns.to_le_bytes())?;
    }

    let mut all = Vec::new();
    for h in handles { all.extend(h.join().expect("subscriber panic")); }
    Ok(all)
}

fn bench_redis_fanout(
    addr: &str,
    n_warmup: usize,
    n_msgs: usize,
    rate_hz: u64,
    n_subs: usize,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let total       = n_warmup + n_msgs;
    let interval_ns = if rate_hz > 0 { 1_000_000_000u64 / rate_hz } else { 0 };
    let redis_url   = format!("redis://{}", addr);

    let ready_count = Arc::new((Mutex::new(0usize), Condvar::new()));

    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let rc  = Arc::clone(&ready_count);
        let url = redis_url.clone();

        handles.push(thread::spawn(move || -> Vec<u64> {
            let client  = redis::Client::open(url).expect("redis client");
            let mut con = client.get_connection().expect("redis conn");
            let mut ps  = con.as_pubsub();
            ps.subscribe(REDIS_CHANNEL).expect("redis sub");

            let (lock, cvar) = &*rc;
            { let mut c = lock.lock().unwrap(); *c += 1; }
            cvar.notify_one();

            let mut lats = Vec::with_capacity(n_msgs);
            for i in 0..total {
                let msg     = ps.get_message().expect("redis recv");
                let recv_ns = now_ns();
                let payload: Vec<u8> = msg.get_payload().expect("payload");
                if payload.len() >= 8 {
                    let send_ns = u64::from_le_bytes(payload[..8].try_into().unwrap());
                    if i >= n_warmup { lats.push(recv_ns.saturating_sub(send_ns)); }
                }
            }
            lats
        }));
    }

    {
        let (lock, cvar) = &*ready_count;
        let mut c = lock.lock().unwrap();
        while *c < n_subs { c = cvar.wait(c).unwrap(); }
    }
    thread::sleep(Duration::from_millis(50));

    let client  = redis::Client::open(redis_url)?;
    let mut con = client.get_connection()?;
    let run_start = Instant::now();
    for i in 0..total {
        if interval_ns > 0 { wait_until(run_start, i as u64 * interval_ns); }
        let send_ns = now_ns();
        let _: i64 = redis::cmd("PUBLISH")
            .arg(REDIS_CHANNEL)
            .arg(send_ns.to_le_bytes().as_slice())
            .query(&mut con)?;
    }

    let mut all = Vec::new();
    for h in handles { all.extend(h.join().expect("subscriber panic")); }
    Ok(all)
}

// ---- CLI args ---------------------------------------------------------------

struct Args {
    bus_addr:   String,
    redis_addr: String,
    n_msgs:     usize,
    n_warmup:   usize,
    rate_hz:    u64,
    no_hist:    bool,
    no_redis:   bool,
    no_bus:     bool,
    subscribers: usize,  // 0 = single-subscriber mode, N = fan-out mode
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            bus_addr:   "127.0.0.1:8000".into(),
            redis_addr: "127.0.0.1:6379".into(),
            n_msgs:     100_000,
            n_warmup:   10_000,
            rate_hz:    100_000,
            no_hist:    false,
            no_redis:   false,
            no_bus:     false,
            subscribers: 0,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            match argv[i].as_str() {
                "--bus-addr"    => { a.bus_addr    = argv[i+1].clone(); i += 2; }
                "--redis-addr"  => { a.redis_addr  = argv[i+1].clone(); i += 2; }
                "--count"       => { a.n_msgs      = argv[i+1].parse().unwrap(); i += 2; }
                "--warmup"      => { a.n_warmup    = argv[i+1].parse().unwrap(); i += 2; }
                "--rate"        => { a.rate_hz     = argv[i+1].parse().unwrap(); i += 2; }
                "--subscribers" => { a.subscribers = argv[i+1].parse().unwrap(); i += 2; }
                "--no-hist"     => { a.no_hist     = true; i += 1; }
                "--no-redis"    => { a.no_redis    = true; i += 1; }
                "--no-bus"      => { a.no_bus      = true; i += 1; }
                other           => { eprintln!("unknown arg: {}", other); i += 1; }
            }
        }
        a
    }
}

// ---- Entry point ------------------------------------------------------------

fn main() {
    let args = Args::parse();

    // Initialise the shared epoch BEFORE spawning any threads.
    let _ = now_ns();

    let total    = args.n_msgs + args.n_warmup;
    let est_secs = total as f64 / args.rate_hz as f64;

    let n_subs = args.subscribers.max(1);
    let mode   = if n_subs > 1 { format!("fan-out × {}", n_subs) } else { "single subscriber".into() };

    // Architecture note for fan-out mode:
    // The bus now uses a single dispatch thread (spins on the ring) + N
    // blocking I/O threads (parked in rx.recv until a frame arrives).
    // Only 1 thread spins — subscriber count does not affect CPU spinning.
    // The practical limit is TCP socket bandwidth: at 100K msg/s × N subscribers
    // × 25 bytes/msg, make sure the loopback NIC isn't the bottleneck.
    let n_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    if n_subs > 200 {
        println!("  ⚠  NOTE: {} subscribers — at this scale the dispatch thread's", n_subs);
        println!("     O(N) registry scan becomes the bottleneck.  Phase 4 adds a");
        println!("     lock-free subscriber list for O(1) per-subscriber dispatch.");
        println!();
    }
    let _ = n_cpus; // suppress unused warning

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  alta_bus vs Valkey — Rust-native one-way latency benchmark");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  bus addr  : {}", args.bus_addr);
    println!("  redis addr: {}", args.redis_addr);
    println!("  mode      : {}", mode);
    println!("  messages  : {}  (+{} warmup discarded)", args.n_msgs, args.n_warmup);
    println!("  rate      : {} msg/s  (busy-wait, no OS sleep jitter)", args.rate_hz);
    println!("  est. time : ~{:.1}s per system", est_secs);
    println!();

    let mut bus_lats:   Vec<u64> = Vec::new();
    let mut redis_lats: Vec<u64> = Vec::new();

    // ---- alta_bus -----------------------------------------------------------
    if !args.no_bus {
        print!("[1/2] alta_bus … "); std::io::Write::flush(&mut std::io::stdout()).ok();
        let result = if n_subs > 1 {
            bench_bus_fanout(&args.bus_addr, args.n_warmup, args.n_msgs, args.rate_hz, n_subs)
        } else {
            bench_bus(&args.bus_addr, args.n_warmup, args.n_msgs, args.rate_hz)
        };
        match result {
            Ok(lats) => {
                println!("done  ({} samples)", lats.len());
                let label = format!("alta_bus  one-way latency  [{}]", mode);
                print_stats(&label, &lats, !args.no_hist);
                bus_lats = lats;
            }
            Err(e) => {
                println!("FAILED\n  {}", e);
                println!("  → start the bus:  cargo run --bin bus");
            }
        }
    }

    // ---- Valkey / Redis -----------------------------------------------------
    if !args.no_redis {
        println!();
        print!("[2/2] Valkey … "); std::io::Write::flush(&mut std::io::stdout()).ok();
        let result = if n_subs > 1 {
            bench_redis_fanout(&args.redis_addr, args.n_warmup, args.n_msgs, args.rate_hz, n_subs)
        } else {
            bench_redis(&args.redis_addr, args.n_warmup, args.n_msgs, args.rate_hz)
        };
        match result {
            Ok(lats) => {
                println!("done  ({} samples)", lats.len());
                let label = format!("Valkey  one-way latency  [{}]", mode);
                print_stats(&label, &lats, !args.no_hist);
                redis_lats = lats;
            }
            Err(e) => {
                println!("FAILED\n  {}", e);
                println!("  → is Valkey running at {}?", args.redis_addr);
            }
        }
    }

    // ---- Side-by-side -------------------------------------------------------
    if !bus_lats.is_empty() && !redis_lats.is_empty() {
        println!("\n  Side-by-side comparison  [{}]", mode);
        print_comparison(&bus_lats, &redis_lats);
    }
    println!();
}
