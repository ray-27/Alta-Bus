use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const RING_SIZE: usize = 1 << 16; //64K bytes
pub const RING_MASK: u64 = (RING_SIZE - 1) as u64;

pub const PAYLOAD_CAP: usize = 1024;
pub const MAX_CONSUMERS: usize = 64;
const CONSUMER_FREE: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishError {
    SlowConsumer,
    PayloadTooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerError {
    FellBehind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    Full,
}

// Slot layout
#[repr(C)]
pub struct SlotData {
    pub channel_id: u32,
    pub msg_type: u8,
    _pad1: [u8; 3],
    pub payload_len: u16,
    _pad2: [u8; 6],
    pub timestamp_ns: u64,
    pub payload: [u8; PAYLOAD_CAP],
}

impl SlotData {
    /// All zero construction so we can build a slice of `Slot(s)` without touching `unsafe`.
    /// Plain integer field and byte arrays are well-defined when zeroed.
    const fn zeroed() -> Self {
        Self {
            channel_id: 0,
            msg_type: 0,
            _pad1: [0; 3],
            payload_len: 0,
            _pad2: [0; 6],
            timestamp_ns: 0,
            payload: [0u8; PAYLOAD_CAP],
        }
    }
}

#[repr(align(64))] //Cache-line aligned
pub struct Slot {
    /// 0 -> never written, Otherwise stores `published_sequence + 1`
    pub seq: AtomicU64,
    pub data: UnsafeCell<SlotData>,
}

unsafe impl Sync for Slot {}

// Cursor
/// A 64-bit atomic counter that occupies its own 64-byte cache line.
#[repr(align(64))]
pub struct Cursor {
    pub value: AtomicU64,
    _pad: [u8; 64 - std::mem::size_of::<AtomicU64>()],
}

impl Cursor {
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
            _pad: [0u8; 64 - std::mem::size_of::<AtomicU64>()],
        }
    }
}

#[repr(align(64))]
pub struct ConsumerEntry {
    pub cursor: AtomicU64,
    _pad: [u8; 64 - std::mem::size_of::<AtomicU64>()],
}

impl ConsumerEntry {
    const fn new() -> Self {
        Self {
            cursor: AtomicU64::new(CONSUMER_FREE),
            _pad: [0u8; 64 - std::mem::size_of::<AtomicU64>()],
        }
    }
}

// RING

pub struct Ring {
    pub slots: Box<[Slot]>,
    pub producer_seq: Cursor,
    pub consumers: Box<[ConsumerEntry]>,
}

unsafe impl Sync for Ring {}
unsafe impl Send for Ring {}

impl Ring {
    pub fn new() -> Box<Self> {
        let slots: Box<[Slot]> = (0..RING_SIZE)
            .map(|_| Slot {
                seq: AtomicU64::new(0),
                data: UnsafeCell::new(SlotData::zeroed()),
            })
            .collect();

        let consumers: Box<[ConsumerEntry]> =
            (0..MAX_CONSUMERS).map(|_| ConsumerEntry::new()).collect();

        Box::new(Self {
            slots,
            producer_seq: Cursor::new(),
            consumers,
        })
    }

    // consumer-registeration
    pub fn register_consumer(&self) -> Result<usize, RegisterError> {
        // Acquire ensures we see the producer's most recent head, we will start consuming at this point and not before that
        let start = self.producer_seq.value.load(Ordering::Acquire);

        for (i, entry) in self.consumers.iter().enumerate() {
            if entry
                .cursor
                .compare_exchange(CONSUMER_FREE, start, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(i);
            }
        }
        Err(RegisterError::Full)
    }

    pub fn unregister_consumer(&self, id: usize) {
        if id < self.consumers.len() {
            self.consumers[id].cursor.store(CONSUMER_FREE, Ordering::Release);
        }
    }

    #[inline(always)]
    fn min_consumer_cursor(&self) -> u64 {
        let mut min = u64::MAX;
        for entry in self.consumers.iter() {
            let v = entry.cursor.load(Ordering::Acquire);
            if v < min {
                min = v;
            }
        }
        min
    }

    ///Publish one message. Single-producer; do not call from more then one thread at a time.
    #[inline(always)]
    pub fn publish(
        &self,
        channel_id: u32,
        msg_type: u8,
        payload: &[u8],
    ) -> Result<(), PublishError> {
        let payload_len = payload.len();
        if payload_len > PAYLOAD_CAP {
            return Err(PublishError::PayloadTooLarge);
        }

        let seq = self.producer_seq.value.load(Ordering::Acquire);
        let slowest = self.min_consumer_cursor();
        if slowest != u64::MAX && seq.wrapping_sub(slowest) >= RING_SIZE as u64 {
            return Err(PublishError::SlowConsumer);
        }

        let slot = unsafe {self.slots.get_unchecked((seq & RING_MASK) as usize)};

        unsafe {
            let d = &mut *slot.data.get();
            d.channel_id = channel_id;
            d.msg_type = msg_type;
            d.payload_len = payload_len as u16;
            d.timestamp_ns = now_ns();

            std::ptr::copy_nonoverlapping(payload.as_ptr(), d.payload.as_mut_ptr(), payload_len);
        }

        slot.seq.store(seq + 1, Ordering::Release);
        self.producer_seq.value.store(seq + 1, Ordering::Release);

        Ok(())
    }

    ///Non-blocking single message consume.
    ///   Returns:
    ///   * `Ok(true)`  - one message was delivered to `f`
    ///   * `Ok(false)` - no new message available right now
    ///   * `Err(FellBehind)` - the producer has lapped this consumer
    #[inline(always)]
    pub fn try_consume<F: FnOnce(&SlotData)>(
        &self,
        consumer_id: usize,
        f: F
    ) -> Result<bool, ConsumerError> {
        let entry = &self.consumers[consumer_id];
        let next = entry.cursor.load(Ordering::Relaxed);
        let slot = unsafe {self.slots.get_unchecked((next & RING_MASK) as usize)};
        let expected = next.wrapping_add(1);

        let s = slot.seq.load(Ordering::Acquire);
        if s < expected {
            return Ok(false); // not yet published
        }
        if s > expected {
            return Err(ConsumerError::FellBehind); //lagged
        }

        unsafe {
            let d = &*slot.data.get();
            f(d);
        }

        entry.cursor.store(next.wrapping_add(1), Ordering::Release);
        Ok(true)
    }

    #[inline(always)]
    pub fn consume_one<F: FnOnce(&SlotData)>(
        &self,
        consumer_id: usize,
        f: F,
    ) -> Result<(), ConsumerError> {
        let entry = &self.consumers[consumer_id];
        let next = entry.cursor.load(Ordering::Relaxed);
        let slot = unsafe { self.slots.get_unchecked((next & RING_MASK) as usize)};
        let expected = next.wrapping_add(1);

        let mut backoff: u32 = 0;
        loop {
            let s = slot.seq.load(Ordering::Acquire);
            if s == expected {
                break;
            }
            if s > expected {
                return Err(ConsumerError::FellBehind);
            }
            if backoff < 64 {
                std::hint::spin_loop();
            }else if backoff < 4096 {
                std::hint::spin_loop();
            }else {
                std::thread::yield_now();
            }
            backoff = backoff.saturating_add(1);
        }

        unsafe {
            let d = &*slot.data.get();
            f(d);
        }

        entry.cursor.store(next.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    #[inline(always)]
    pub fn producer_head(&self) -> u64 {
        self.producer_seq.value.load(Ordering::Acquire)
    }
}

#[inline(always)]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}


// ----- Tests ------
//

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_publish_consume_roundtrip() {
        let ring = Ring::new();
        let id = ring.register_consumer().unwrap();

        ring.publish(42, 1, b"hello").unwrap();

        let mut got = Vec::new();
        ring.try_consume(id, |d| {
            assert_eq!(d.channel_id, 42);
            assert_eq!(d.msg_type, 1);
            assert_eq!(d.payload_len, 5);
            got.extend_from_slice(&d.payload[..d.payload_len as usize]);
        })
        .unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn try_consume_returns_false_when_empty() {
        let ring = Ring::new();
        let id = ring.register_consumer().unwrap();
        let r = ring.try_consume(id, |_| panic!("should not deliver")).unwrap();
        assert!(!r);
    }

    #[test]
    fn slow_consumer_is_detected() {
        let ring = Ring::new();
        let _id = ring.register_consumer().unwrap();
        // Fill the ring without consuming. The last publish must trip the
        // backpressure detector.
        let mut last = Ok(());
        for _ in 0..RING_SIZE + 1 {
            last = ring.publish(0, 0, b"x");
            if last.is_err() {
                break;
            }
        }
        assert_eq!(last, Err(PublishError::SlowConsumer));
    }

    #[test]
    fn payload_too_large_is_rejected() {
        let ring = Ring::new();
        let big = vec![0u8; PAYLOAD_CAP + 1];
        assert_eq!(ring.publish(0, 0, &big), Err(PublishError::PayloadTooLarge));
    }

    #[test]
    fn concurrent_producer_consumer_streams_in_order() {
        // Leak the Ring to get a 'static reference for cheap thread sharing.
        let ring: &'static Ring = Box::leak(Ring::new());
        let id = ring.register_consumer().unwrap();
        const N: u64 = 100_000;

        let prod = thread::spawn(move || {
            for i in 0..N {
                let bytes = i.to_le_bytes();
                loop {
                    match ring.publish(0, 0, &bytes) {
                        Ok(()) => break,
                        Err(PublishError::SlowConsumer) => std::hint::spin_loop(),
                        Err(e) => panic!("publish failed: {:?}", e),
                    }
                }
            }
        });

        let cons = thread::spawn(move || {
            let mut received = 0u64;
            while received < N {
                ring.consume_one(id, |d| {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&d.payload[..8]);
                    assert_eq!(u64::from_le_bytes(buf), received);
                })
                .expect("should not fall behind");
                received += 1;
            }
            received
        });

        prod.join().unwrap();
        let got = cons.join().unwrap();
        assert_eq!(got, N);
    }

    #[test]
    fn fan_out_to_many_consumers() {
        let ring: &'static Ring = Box::leak(Ring::new());
        const C: usize = 4;
        const N: u64 = 10_000;

        let ids: Vec<usize> = (0..C).map(|_| ring.register_consumer().unwrap()).collect();

        let mut handles = Vec::new();
        for id in ids {
            handles.push(thread::spawn(move || {
                let mut received = 0u64;
                while received < N {
                    ring.consume_one(id, |d| {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&d.payload[..8]);
                        assert_eq!(u64::from_le_bytes(buf), received);
                    })
                    .expect("no fall-behind in test");
                    received += 1;
                }
            }));
        }

        for i in 0..N {
            let bytes = i.to_le_bytes();
            loop {
                match ring.publish(0, 0, &bytes) {
                    Ok(()) => break,
                    Err(PublishError::SlowConsumer) => std::hint::spin_loop(),
                    Err(e) => panic!("publish failed: {:?}", e),
                }
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn registration_is_capped() {
        let ring = Ring::new();
        let mut ids = Vec::new();
        for _ in 0..MAX_CONSUMERS {
            ids.push(ring.register_consumer().unwrap());
        }
        assert_eq!(ring.register_consumer(), Err(RegisterError::Full));
        // Freeing a slot should let us register again.
        ring.unregister_consumer(ids.pop().unwrap());
        ring.register_consumer().unwrap();
    }

    #[test]
    fn arc_share_is_sound() {
        // Smoke test: `Ring` must be `Send + Sync` so it can be wrapped in
        // an `Arc` and shared across threads.
        let ring = Arc::new(*Ring::new());
        let r1 = ring.clone();
        thread::spawn(move || {
            let _ = r1.producer_head();
        })
        .join()
        .unwrap();
    }
}
