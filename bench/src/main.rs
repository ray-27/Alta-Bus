//! Dummy market data producer for manual testing and latency benchmarking.
//!
//! Connects to the bus as a publisher and continuously emits PriceTick messages
//! for four NSE instruments using a random walk price model, plus periodic
//! heartbeats on channel 9000.
//!
//! # Usage
//!
//! ```bash
//! # Default: 100 ticks/sec total, connects to 127.0.0.1:8000
//! cargo run --bin bench
//!
//! # Override rate and address
//! RATE_HZ=500 BUS_ADDR=192.168.1.10:8000 cargo run --bin bench
//! ```

use protocol::messages::price_tick::PriceTick;
use protocol::MsgType;
use publisher_client::Publisher;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---- Instruments ------------------------------------------------------------

struct Instrument {
    instrument_id: u32,
    channel_id: u32,
    name: &'static str,
    price: f64,
    /// Max price move per tick (± this value).
    tick_size: f64,
    total_vol: u64,
}

impl Instrument {
    fn tick(&mut self, rng: &mut Rng) -> PriceTick {
        // Gaussian-ish price walk: sum of two uniform random numbers.
        let move_ = (rng.f64() + rng.f64() - 1.0) * self.tick_size;
        self.price = (self.price + move_).max(self.price * 0.95); // price floor at -5%
        let vol = (rng.f64() * 500.0) as u32 + 50;
        self.total_vol += vol as u64;

        PriceTick {
            instrument_id: self.instrument_id,
            last_price: (self.price * 100.0).round() / 100.0, // 2 decimal places
            volume: vol,
            total_traded_volume: self.total_vol,
            timestamp_ns: now_ns(),
        }
    }
}

// ---- Minimal xorshift64 PRNG ------------------------------------------------
// No external dependency; good enough for price simulation.

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

    /// Float in [0, 1).
    #[inline(always)]
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---- Entry point ------------------------------------------------------------

fn main() {
    let addr = std::env::var("BUS_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".into());
    let rate_hz: u64 = std::env::var("RATE_HZ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let interval = Duration::from_micros(1_000_000 / rate_hz.max(1));

    let mut pub_ = Publisher::connect(&addr).unwrap_or_else(|e| {
        eprintln!("[bench] cannot connect to {}: {}", addr, e);
        std::process::exit(1);
    });

    println!("[bench] connected to {}", addr);
    println!("[bench] publishing at {} Hz ({} µs/tick)", rate_hz, interval.as_micros());

    let mut instruments = vec![
        Instrument { instrument_id: 1001, channel_id: 1001, name: "NIFTY_SPOT",     price: 24_150.50, tick_size: 5.0,   total_vol: 0 },
        Instrument { instrument_id: 1002, channel_id: 1002, name: "BANKNIFTY_SPOT", price: 51_800.00, tick_size: 12.0,  total_vol: 0 },
        Instrument { instrument_id: 1003, channel_id: 1003, name: "RELIANCE",       price:  2_945.75, tick_size: 0.8,   total_vol: 0 },
        Instrument { instrument_id: 1004, channel_id: 1004, name: "TCS",            price:  4_215.30, tick_size: 1.2,   total_vol: 0 },
    ];

    let mut rng = Rng::new(now_ns());
    let mut total_published: u64 = 0;
    let mut heartbeat_seq: u64 = 0;
    let mut last_stat = Instant::now();
    let mut last_heartbeat = Instant::now();
    let mut inst_idx: usize = 0;

    loop {
        let tick_start = Instant::now();

        // Round-robin across instruments so each gets an equal share.
        let idx = inst_idx % instruments.len();
        inst_idx += 1;

        let tick = instruments[idx].tick(&mut rng);
        let ch = instruments[idx].channel_id;

        match pub_.publish(ch, MsgType::PriceTick as u8, &tick) {
            Ok(()) => total_published += 1,
            Err(e) => {
                eprintln!("[bench] publish error: {}", e);
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        }

        // Heartbeat every second.
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let hb = protocol::messages::heartbeat::Heartbeat {
                source_id: 1,
                sequence_num: heartbeat_seq,
            };
            let _ = pub_.publish(9000, MsgType::Heartbeat as u8, &hb);
            heartbeat_seq += 1;
            last_heartbeat = Instant::now();
        }

        // Stats line every second.
        if last_stat.elapsed() >= Duration::from_secs(1) {
            println!(
                "[bench] {:>8} msgs sent | {} {:.2}  {} {:.2}  {} {:.2}  {} {:.2}",
                total_published,
                instruments[0].name, instruments[0].price,
                instruments[1].name, instruments[1].price,
                instruments[2].name, instruments[2].price,
                instruments[3].name, instruments[3].price,
            );
            last_stat = Instant::now();
        }

        // Sleep for the remainder of the interval. This is not high-precision
        // timing but fine for a dev producer. For benchmarking use a busy-wait.
        let elapsed = tick_start.elapsed();
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
    }
}

#[inline(always)]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
