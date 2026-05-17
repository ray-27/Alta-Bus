# The LMAX-Style SPMC Ring Buffer — A Ground-Up Walkthrough

> File this walks through: `bus/src/ring.rs`
> Audience: you, learning low-level concurrent systems design from scratch.
> Goal: by the end you should be able to (re)derive every line of `ring.rs`
> from first principles, and explain *why* each design choice exists.

---

## Table of Contents

1. [The problem we are actually solving](#1-the-problem-we-are-actually-solving)
2. [Why a ring buffer? (vs queue, vs channel, vs Kafka)](#2-why-a-ring-buffer-vs-queue-vs-channel-vs-kafka)
3. [SPMC: one producer, many consumers, no coordination between consumers](#3-spmc-one-producer-many-consumers-no-coordination-between-consumers)
4. [Sequence numbers: the entire algorithm in one idea](#4-sequence-numbers-the-entire-algorithm-in-one-idea)
5. [Memory layout: slot, cursor, padding](#5-memory-layout-slot-cursor-padding)
6. [Cache lines and false sharing (where the 64 comes from)](#6-cache-lines-and-false-sharing-where-the-64-comes-from)
7. [Power-of-two ring size and the `& MASK` trick](#7-power-of-two-ring-size-and-the--mask-trick)
8. [The memory model: Acquire / Release / Relaxed](#8-the-memory-model-acquire--release--relaxed)
9. [The `seq + 1` slot-state encoding](#9-the-seq--1-slot-state-encoding)
10. [Publish, step by step](#10-publish-step-by-step)
11. [Consume, step by step](#11-consume-step-by-step)
12. [Backpressure and the slow-consumer problem](#12-backpressure-and-the-slow-consumer-problem)
13. [Registering and unregistering a consumer (single-CAS sentinel)](#13-registering-and-unregistering-a-consumer-single-cas-sentinel)
14. [Why this scales: throughput in concrete terms](#14-why-this-scales-throughput-in-concrete-terms)
15. [`UnsafeCell` and why the borrow checker steps aside](#15-unsafecell-and-why-the-borrow-checker-steps-aside)
16. [End-to-end flow diagram](#16-end-to-end-flow-diagram)
17. [Tradeoffs and constraints to remember](#17-tradeoffs-and-constraints-to-remember)

---

## 1. The problem we are actually solving

You have **one** thread that pulls market data off a TCP socket and parses it into messages.
You have **many** threads — one per subscriber socket — that need to forward those messages out.

A naive design would do this:

```
for each message:
    for each subscriber:
        subscriber.tcp_write(message)        // blocking I/O
```

That is catastrophic, because:

* If one subscriber's TCP socket blocks (slow consumer / network jitter), the **producer freezes** and every other subscriber stalls too.
* The producer pays N socket writes per message, where N grows with subscriber count.
* Every iteration touches kernel space (system calls).
* You allocate per-message buffers, hammering the allocator and the cache.

What we want instead:

> The producer drops the message into a **shared in-memory data structure** as fast as memory can move,
> and N subscriber threads independently pick it up at their own pace, writing to their own TCP socket without ever blocking the producer or each other.

That data structure is the ring buffer in `ring.rs`.

---

## 2. Why a ring buffer? (vs queue, vs channel, vs Kafka)

Let's eliminate alternatives:

| Choice | Why it loses for this job |
|---|---|
| `std::sync::mpsc` / `crossbeam` channel | One consumer per channel. Multi-consumer would require N channels, N copies per message. |
| `Mutex<VecDeque<Msg>>` | Lock contention. Every consumer fights the producer for the same lock. |
| `Arc<RwLock<Vec<Msg>>>` | Same problem. Plus growth = allocation = jitter. |
| Kafka / Redis Streams | Network hop. Persistence. Hundreds of microseconds. We need nanoseconds. |
| Lock-free MPMC queue (e.g. `crossbeam::ArrayQueue`) | Each consumer pops once → message is gone. We need every consumer to see every message. |

A **ring buffer** fits because:

* **Bounded** → no allocation on the hot path.
* **Each slot exists once** → consumers read it in place, no copies.
* **All consumers see all messages** → fan-out is free; we just have N read cursors over the same array.
* **Cache-friendly** → linear access patterns, hardware prefetcher loves it.

The LMAX Disruptor paper (2011) is where this pattern was made famous. We are reimplementing its core idea, simplified for SPMC.

---

## 3. SPMC: one producer, many consumers, no coordination between consumers

**SPMC** = Single Producer, Multiple Consumers.

* The producer is the market-data parser thread. There is exactly one. The single-producer rule is what lets us skip CAS on the publish path.
* The consumers are the per-subscriber TCP-writer threads. There can be up to `MAX_CONSUMERS = 256`. They never talk to each other.

The crucial property:

> Every consumer reads every message. Different consumers are at different positions in the ring at the same time.

```
                  ring (RING_SIZE slots)
            ┌────┬────┬────┬────┬────┬────┬────┬────┐
            │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │ 6  │ 7  │ ...
            └────┴────┴────┴────┴────┴────┴────┴────┘
                              ▲    ▲         ▲
                              │    │         │
                            Cons_B │       producer_seq
                                 Cons_A    (next write here)
```

Consumer A is two slots behind. Consumer B is five slots behind. The producer is about to write to slot 6 next. None of them need to know about each other on the hot path.

---

## 4. Sequence numbers: the entire algorithm in one idea

Forget arrays for a second. Imagine an **infinite stream** of messages, numbered `0, 1, 2, 3, …`. Both producer and consumers think in terms of this infinite sequence.

```rust
pub const RING_SIZE: usize = 1 << 16;          // 65536
pub const RING_MASK: u64 = (RING_SIZE - 1) as u64;
```

To map sequence number `S` onto a physical slot we just do `S & RING_MASK`. So sequence `0` and sequence `65536` and sequence `131072` all live in slot `0` — physically reused, **but their identity is different**.

Why does that matter? Because every slot carries a stamp (`slot.seq`) telling you which sequence number is currently sitting there. A consumer never asks *"is slot 4 ready?"*. It asks *"is sequence 196612 ready in slot 196612 & MASK?"*.

That single shift — from "slot ID" to "sequence number" — is the entire reason this design works. Cursor `next` is a monotonic 64-bit counter; physical slot reuse is invisible to the algorithm.

---

## 5. Memory layout: slot, cursor, padding

Three structs:

### `SlotData` — the payload container

```126:135:bus/src/ring.rs
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
```

Layout, with byte offsets:

```
offset  field
 0..4   channel_id     (u32)
 4..5   msg_type       (u8)
 5..8   _pad1          (3 bytes)
 8..10  payload_len    (u16)
10..16  _pad2          (6 bytes)
16..24  timestamp_ns   (u64, naturally aligned at 8)
24..1048 payload[1024]  (the actual message bytes)
```

The `_pad1` / `_pad2` bytes give us:

* `payload_len` aligned to 2 (it already would be, but explicit padding makes layout obvious).
* `timestamp_ns` aligned to 8 — important because misaligned 8-byte loads are slower or non-atomic on some architectures.
* A `#[repr(C)]` layout that does not depend on compiler whims, so we can reason about it and (eventually) `mmap` it across processes if we want.

`PAYLOAD_CAP = 1024` bounds the largest message you can broadcast. The whole `SlotData` is therefore about **1048 bytes**.

### `Slot` — `SlotData` plus the synchronization word

```160:165:bus/src/ring.rs
#[repr(C, align(64))]
pub struct Slot {
    pub seq: AtomicU64,
    pub data: UnsafeCell<SlotData>,
}
```

* `seq` is the synchronization atomic. **This is the only thing the producer and consumers ping-pong on per slot.**
* `data` is the message bytes — interior-mutable, so we can write through `&Slot` without the borrow checker complaining.
* `#[repr(C, align(64))]` is **alignment**, not size. The next section explains why.

### `Cursor` and `ConsumerEntry` — padded atomic counters

```181:185:bus/src/ring.rs
#[repr(C, align(64))]
pub struct Cursor {
    pub value: AtomicU64,
    _pad: [u8; 64 - std::mem::size_of::<AtomicU64>()],
}
```

```200:204:bus/src/ring.rs
#[repr(C, align(64))]
pub struct ConsumerEntry {
    pub cursor: AtomicU64,
    _pad: [u8; 64 - std::mem::size_of::<AtomicU64>()],
}
```

These are **identical** in shape. Same idea, different role: `Cursor` is the producer head, `ConsumerEntry` is one consumer's read position. Each is exactly 64 bytes wide and 64-byte aligned. Why? See section 6.

### `Ring` — the whole thing

```217:231:bus/src/ring.rs
pub struct Ring {
    pub slots: Box<[Slot]>,
    pub producer_seq: Cursor,
    pub consumers: Box<[ConsumerEntry]>,
}
```

* `slots`: 65536 contiguous slots → ~64 MB heap region. Contiguous = prefetcher-friendly.
* `producer_seq`: the producer's monotonic counter.
* `consumers`: fixed-size table of 256 consumer cursors. Pre-allocated so subscribing or unsubscribing never resizes anything.

---

## 6. Cache lines and false sharing (where the 64 comes from)

This is the single most important hardware detail in the whole file.

### What's a cache line?

When a CPU reads from RAM, it does not read one byte. It reads a **cache line** — on every CPU you care about (x86-64, Apple Silicon, modern ARM), that's **64 bytes**.

That line is brought into the core's L1/L2 cache. If another core also wants to read it, both cores can hold a copy at the same time (the cache coherency protocol — MESI / MOESI — calls this "Shared" state).

But the moment **any** core *writes* to that line, the protocol must:

1. Invalidate every other core's copy.
2. Transfer exclusive ownership to the writer.
3. When another core reads or writes that line, the cycle repeats.

That transition costs ~30–100 nanoseconds on modern CPUs. **Per write**. **Per line.** That number dominates everything else in this code.

### False sharing — the silent killer

Suppose two unrelated atomic counters happen to live in the **same 64-byte cache line**:

```
┌──────────────────────────────────────────────────────────┐
│ producer_seq (8 bytes) │ consumer_A_cursor (8 bytes) │ … │
└──────────────────────────────────────────────────────────┘
```

Logically they are independent. Physically they share a line. When core 0 writes `producer_seq` and core 1 writes `consumer_A_cursor`, the coherency protocol thinks they are touching the same data and ping-pongs the entire line between cores at every write.

This is called **false sharing**. Two unrelated variables become as slow as a single contested mutex.

### The fix: pad each atomic to its own line

```rust
#[repr(C, align(64))]
pub struct Cursor {
    pub value: AtomicU64,                  // 8 bytes of useful data
    _pad: [u8; 64 - std::mem::size_of::<AtomicU64>()],  // 56 bytes of nothing
}
```

* `align(64)` says **start me at a 64-byte address boundary**.
* The padding makes the struct **exactly 64 bytes**, so the *next* `Cursor` in an array also starts on a new line.

Now `producer_seq` lives alone on its line. Each `ConsumerEntry` lives alone on its line. Two cores writing two different cursors at full speed will not invalidate each other.

### What about `Slot`?

```rust
#[repr(C, align(64))]
pub struct Slot {
    pub seq: AtomicU64,
    pub data: UnsafeCell<SlotData>,
}
```

`Slot` is **not** 64 bytes wide — it's ~1056 bytes wide because of `SlotData`. So why `align(64)`?

Two reasons:

1. **`seq` lands at offset 0 of the slot.** Aligning the slot to 64 means `seq` is always at the start of a fresh cache line, not split across two lines. An atomic load that straddles two lines is much slower (and on some architectures not even atomic).
2. **Hot/cold separation.** The producer writes `data` (cold, large, mostly memcpy bandwidth) and then writes `seq` (hot, contended with consumers). Putting `seq` at the start of an aligned region keeps the hot byte predictable for the coherency protocol.

> **TL;DR on the number 64:** it's the CPU cache-line size. The 64-byte `Cursor` is *size + alignment* to defeat false sharing between counters. The 64-byte *alignment* on `Slot` puts the hot `seq` on its own line. Neither number has anything to do with `PAYLOAD_CAP` or the 17-byte wire header.

---

## 7. Power-of-two ring size and the `& MASK` trick

```rust
pub const RING_SIZE: usize = 1 << 16;          // 65536
pub const RING_MASK: u64 = (RING_SIZE - 1) as u64; // 0x0000_0000_0000_FFFF
```

Mapping sequence to slot is one of the hottest operations in the whole bus. Naively:

```rust
let slot_idx = seq % RING_SIZE;        // integer division → ~20–40 cycles
```

With a power-of-two size:

```rust
let slot_idx = seq & RING_MASK;        // single AND → 1 cycle
```

A 20× speedup on a per-message operation. This is also why `RING_SIZE` is `const` — the compiler bakes the mask into the instruction stream as an immediate, so there is not even a memory load to fetch it.

Why 65536 specifically?

* Large enough to absorb microsecond-scale jitter on consumers (~65k messages of headroom).
* Small enough that the working set (~64 MB of slot storage) still fits in modern L2/L3 caches when traffic is hot.
* Easy to grow later; everything else in the file is parameterised on this constant.

---

## 8. The memory model: Acquire / Release / Relaxed

This is where most people lose the plot. Let's go slowly.

CPUs and compilers reorder memory operations aggressively. Without explicit ordering, a consumer might see `slot.seq` updated *before* it sees the payload writes that should logically come first. That's a torn read, and it's the bug this entire algorithm is designed to prevent.

Rust's atomic orderings give you exactly the tools you need:

| Ordering | What it promises |
|---|---|
| **Relaxed** | The operation is atomic. **No ordering** relative to other memory. |
| **Acquire** (on load) | All memory operations *after* this load cannot be reordered before it. |
| **Release** (on store) | All memory operations *before* this store cannot be reordered after it. |
| **AcqRel** | Both Acquire and Release for read-modify-write ops like CAS. |

The pattern that matters here is the **Acquire/Release pair**:

```
producer thread                       consumer thread
───────────────                       ───────────────
write slot.data.channel_id            
write slot.data.payload_len           
write slot.data.payload[…]            
                                      // these later happen-AFTER ↓
slot.seq.store(seq+1, Release) ─────► slot.seq.load(Acquire)  ── observe seq+1
                                      read slot.data.channel_id   ← guaranteed visible
                                      read slot.data.payload[…]   ← guaranteed visible
```

If the consumer's `Acquire` load observes a value that the producer wrote with `Release`, then **every memory write the producer did before the Release is visible to the consumer after the Acquire**. The compiler and the CPU both respect this. That is the only inter-thread synchronization in the file. No mutex, no fence intrinsic, no kernel call.

### Why `Relaxed` shows up

Look at this in `publish`:

```360:360:bus/src/ring.rs
let seq = self.producer_seq.value.load(Ordering::Relaxed);
```

The producer is the **only** writer of `producer_seq`. There is no other thread writing to it that we need ordering with. Reading our own writes never needs synchronization with anyone else — the CPU's program-order guarantee covers it. So `Relaxed` is correct and saves the cost of a memory fence.

Same logic on the consumer:

```433:433:bus/src/ring.rs
let next = entry.cursor.load(Ordering::Relaxed);
```

A consumer is the sole writer of its own cursor. `Relaxed` is fine.

### The three edges (memorize these)

```12:19:bus/src/ring.rs
//!   1. Producer commit:  `slot.seq.store(seq + 1, Release)`
//!      - Publishes every preceding plain-memory write of `slot.data`.
//!   2. Consumer observe: `slot.seq.load(Acquire)`
//!      - Pairs with the producer's Release; once the consumer sees the new
//!        value of `slot.seq`, all the data writes above it are also visible.
//!   3. Consumer advance: `cursor.store(next + 1, Release)`
//!      - Visible to the producer's "slowest consumer" backpressure scan,
//!        which uses `Acquire` loads on each cursor.
```

The whole correctness proof of this code reduces to those three edges. Tattoo it.

---

## 9. The `seq + 1` slot-state encoding

There is one subtle gotcha. Slots are zeroed at construction:

```247:252:bus/src/ring.rs
let slots: Box<[Slot]> = (0..RING_SIZE)
    .map(|_| Slot {
        seq: AtomicU64::new(0),
        data: UnsafeCell::new(SlotData::zeroed()),
    })
    .collect();
```

If the producer stored `seq` literally (i.e. wrote `0` for the first publish), a fresh consumer would have no way to distinguish "I haven't published anything yet" from "I published sequence 0".

Fix: **store `seq + 1`** after publishing sequence `seq`:

```404:404:bus/src/ring.rs
slot.seq.store(seq + 1, Ordering::Release);
```

Now:

* `slot.seq == 0` → never written.
* `slot.seq == N+1` → sequence `N` has been published in this slot.

A consumer wanting sequence `N` waits for `slot[N & MASK].seq == N + 1`:

```435:445:bus/src/ring.rs
let expected = next.wrapping_add(1);

// Acquire pairs with the producer's Release-store; once we observe
// `seq >= expected`, all of `slot.data` is visible to us.
let s = slot.seq.load(Ordering::Acquire);
if s < expected {
    return Ok(false); // not yet published
}
if s > expected {
    return Err(ConsumeError::FellBehind); // lapped
}
```

Notice the three states:

* `s == expected` → exactly the message we want, hand it to `f`.
* `s < expected` → producer hasn't caught up; nothing to do.
* `s > expected` → producer has wrapped around and overwritten our spot — we **fell behind**.

This is also how the bus survives a slow consumer that managed to slip past backpressure (e.g. just registered with a stale cursor). It can be detected and evicted without corrupting anything.

---

## 10. Publish, step by step

Full function:

```346:412:bus/src/ring.rs
#[inline(always)]
pub fn publish(
    &self,
    channel_id: u32,
    msg_type: u8,
    payload: &[u8],
) -> Result<(), PublishError> {
    if payload.len() > PAYLOAD_CAP {
        return Err(PublishError::PayloadTooLarge);
    }

    let seq = self.producer_seq.value.load(Ordering::Relaxed);

    let slowest = self.min_consumer_cursor();
    if slowest != u64::MAX && seq.wrapping_sub(slowest) >= RING_SIZE as u64 {
        return Err(PublishError::SlowConsumer);
    }

    let slot = unsafe { self.slots.get_unchecked((seq & RING_MASK) as usize) };

    unsafe {
        let d = &mut *slot.data.get();
        d.channel_id = channel_id;
        d.msg_type = msg_type;
        d.payload_len = payload.len() as u16;
        d.timestamp_ns = now_ns();
        std::ptr::copy_nonoverlapping(
            payload.as_ptr(),
            d.payload.as_mut_ptr(),
            payload.len(),
        );
    }

    slot.seq.store(seq + 1, Ordering::Release);
    self.producer_seq.value.store(seq + 1, Ordering::Release);

    Ok(())
}
```

Step by step:

1. **Bounds check the payload.** Cheap CPU branch. Anything bigger than `PAYLOAD_CAP` is a programmer error; we surface it.

2. **Load `producer_seq` relaxed.** We're the only writer; no fence needed.

3. **Backpressure scan.**
   ```rust
   let slowest = self.min_consumer_cursor();
   if slowest != u64::MAX && seq.wrapping_sub(slowest) >= RING_SIZE as u64 {
       return Err(PublishError::SlowConsumer);
   }
   ```
   We compute `seq - slowest`. If the gap is ≥ `RING_SIZE`, the slot we are about to overwrite still contains a message some consumer hasn't read. Refuse to publish. The caller (in `bus/src/main.rs`) will evict the slow subscriber and retry. This is *the* mechanism that keeps the producer from ever waiting.

   The `wrapping_sub` is correct modular arithmetic across u64 wraparound — which would take centuries to actually trigger but is morally important.

4. **Map sequence to slot.**
   ```rust
   let slot = unsafe { self.slots.get_unchecked((seq & RING_MASK) as usize) };
   ```
   `get_unchecked` removes the bounds check. Sound because `(seq & RING_MASK) < RING_SIZE` is a compile-time guarantee.

5. **Write the payload.** Plain stores. **No atomic semantics here — they don't need any.** From any consumer's perspective, `slot.seq` is still the *old* value, so consumers don't even know this slot is being touched.
   The `copy_nonoverlapping` lowers to a vectorised memcpy. This is the bandwidth-dominant part of `publish`.

6. **Release the slot.**
   ```rust
   slot.seq.store(seq + 1, Ordering::Release);
   ```
   This is the single instruction that publishes everything in step 5. Any consumer that subsequently observes `slot.seq == seq + 1` is guaranteed to see all the writes above. This is the producer's half of the publish edge.

7. **Advance the producer head.** Also Release. The backpressure scan reads this; new consumers register by reading this; observability tools read this.

That's 7 steps, and steps 4–7 are all branch-free and L1-resident.

---

## 11. Consume, step by step

Non-blocking variant:

```424:459:bus/src/ring.rs
#[inline(always)]
pub fn try_consume<F: FnOnce(&SlotData)>(
    &self,
    consumer_id: usize,
    f: F,
) -> Result<bool, ConsumeError> {
    let entry = &self.consumers[consumer_id];

    let next = entry.cursor.load(Ordering::Relaxed);
    let slot = unsafe { self.slots.get_unchecked((next & RING_MASK) as usize) };
    let expected = next.wrapping_add(1);

    let s = slot.seq.load(Ordering::Acquire);
    if s < expected {
        return Ok(false);
    }
    if s > expected {
        return Err(ConsumeError::FellBehind);
    }

    unsafe {
        let d = &*slot.data.get();
        f(d);
    }

    entry
        .cursor
        .store(next.wrapping_add(1), Ordering::Release);
    Ok(true)
}
```

1. **Load my own cursor.** I'm the sole writer; `Relaxed`.
2. **Pick the slot.** `next & RING_MASK`.
3. **Compute `expected = next + 1`** (the slot-state encoding we discussed).
4. **Acquire-load `slot.seq`.** Three outcomes:
   * `< expected` → nothing new yet, return `Ok(false)`.
   * `> expected` → the producer wrapped past me, return `FellBehind`. Caller drops the subscriber.
   * `== expected` → exactly mine. Proceed.
5. **Call the callback with `&SlotData`.** The Acquire above ordered the data writes correctly; the borrow is valid for this call only.
6. **Advance my cursor with Release.** That's how the producer's backpressure scan learns I made progress.

The blocking variant (`consume_one`) only adds an adaptive backoff spin loop around step 4:

```485:502:bus/src/ring.rs
let mut backoff: u32 = 0;
loop {
    let s = slot.seq.load(Ordering::Acquire);
    if s == expected {
        break;
    }
    if s > expected {
        return Err(ConsumeError::FellBehind);
    }
    if backoff < 64 {
        std::hint::spin_loop();
    } else if backoff < 4096 {
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
    backoff = backoff.saturating_add(1);
}
```

`std::hint::spin_loop()` compiles to `pause` on x86 (tells the CPU to back off the speculation pipeline; saves power; reduces hyperthread contention) and `yield` on ARM. After a few thousand spins we hand control to the OS scheduler. This is the LMAX adaptive-backoff ladder, slightly simplified.

---

## 12. Backpressure and the slow-consumer problem

The hard question with fan-out: **what do you do when one consumer is slow?**

Three possible answers:

1. **Block the producer until everyone catches up.** Kills throughput. One slow subscriber stalls every other subscriber. Unacceptable.
2. **Drop the message for the slow consumer only.** Then the slow consumer's stream is broken anyway, so it's useless to them.
3. **Evict the slow consumer.** Keep producing at full speed. Their socket is closed; they reconnect and resync.

This bus picks **#3**.

The mechanism:

```327:336:bus/src/ring.rs
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
```

```367:370:bus/src/ring.rs
let slowest = self.min_consumer_cursor();
if slowest != u64::MAX && seq.wrapping_sub(slowest) >= RING_SIZE as u64 {
    return Err(PublishError::SlowConsumer);
}
```

If the gap between the producer head and the slowest cursor reaches one full ring, **the producer refuses to overwrite that slot**. It returns `Err(SlowConsumer)` to the caller, which:

1. Identifies which consumer is too far behind.
2. Closes that subscriber's TCP socket.
3. Calls `unregister_consumer`, which flips that entry back to `CONSUMER_FREE = u64::MAX`.
4. Retries `publish` — now the slowest cursor is some other consumer that *is* keeping up, and `seq - slowest` drops below `RING_SIZE`.

That `CONSUMER_FREE = u64::MAX` sentinel is clever:

* It's the largest possible `u64`, so it never wins the `min()` comparison unless **every** consumer is free, in which case `min` stays `u64::MAX` and the `slowest != u64::MAX` guard skips the check entirely.
* It lives in the same atomic as the cursor itself, so claim/release is a single CAS — no extra "active" flag, no torn state.

The scan is `O(MAX_CONSUMERS) = O(256)` and runs **on every publish**. That's 256 Acquire loads — a few hundred nanoseconds. We can optimize this further (caching, or only re-scanning when the producer is *close* to the previous slowest cursor) but it's correct as written.

---

## 13. Registering and unregistering a consumer (single-CAS sentinel)

```280:299:bus/src/ring.rs
pub fn register_consumer(&self) -> Result<usize, RegisterError> {
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
```

Algorithm:

1. Snapshot the current producer head. The new consumer starts **at the head**, meaning it sees only messages published *after* registration. This avoids two pathologies:
   * Starting at 0 would immediately yield `FellBehind` on any ring whose producer has already wrapped at least once.
   * Starting at 0 would also deliver "ghost" messages on a fresh ring (already-stale slot contents).
2. Walk the consumer table, CAS the first free slot from `CONSUMER_FREE` to `start`.
3. If every slot is taken, return `Full`.

The CAS is `AcqRel`:
* **Acquire on success** — sees the previous owner's Release-store from `unregister_consumer`.
* **Release on success** — any subsequent reads of state we set up are ordered after the claim.
* **Relaxed on failure** — we lost the race, no data to publish.

Unregistration:

```310:316:bus/src/ring.rs
pub fn unregister_consumer(&self, id: usize) {
    if id < self.consumers.len() {
        self.consumers[id]
            .cursor
            .store(CONSUMER_FREE, Ordering::Release);
    }
}
```

One Release store. The Release ordering ensures that any thread that subsequently CASes this slot from `CONSUMER_FREE` to a new start point sees our final cursor value (and conceptually, the cleaned-up subscriber state).

**Critical invariant** the caller must uphold: the consumer thread for `id` must have **already stopped touching the ring** before `unregister_consumer` is called. The producer immediately starts ignoring that slot for backpressure, so it may overwrite slots the (defunct) consumer was reading.

---

## 14. Why this scales: throughput in concrete terms

Let's count cycles for a publish, in cache-warm steady state on a 3 GHz core:

| Step | Approx cycles | Why |
|---|---|---|
| Load `producer_seq` (Relaxed) | 1 | L1 hit, no fence |
| Backpressure scan, 256 cursors | ~500–1500 | 256 L2/L3-resident atomic loads |
| Pick the slot, get_unchecked | 1 | branchless `& MASK` |
| Plain stores to header fields | 5–10 | L1 hit |
| `copy_nonoverlapping` for payload | ~payload_len / 32 | AVX2/NEON memcpy |
| `slot.seq.store(_, Release)` | ~5 | L1, requires line ownership |
| `producer_seq.store(_, Release)` | ~5 | same |

For a 64-byte payload, that's roughly **2000 cycles ≈ 700 ns** per publish, dominated by the backpressure scan. That's ~1.4 M messages/sec from one thread, before we even start sharding hot channels.

Consumers are similarly cheap:

| Step | Approx cycles |
|---|---|
| Load own cursor (Relaxed) | 1 |
| Pick slot | 1 |
| `slot.seq.load(Acquire)` | ~5–20 (cold the first time; <5 cache-warm) |
| Callback / TCP write | dominated by the user code |
| `cursor.store(_, Release)` | ~5 |

On the consumer side the actual bottleneck is whatever I/O the callback does (TCP `send`), not the ring.

The architectural reason this is fast:

* **Zero allocations.** Slots are reused. No allocator contention, no fragmentation.
* **Zero locks.** Three atomic edges replace everything you'd otherwise build with mutexes.
* **Zero copies between consumers.** All N consumers read the same slot.
* **Predictable cache behaviour.** The producer streams linearly through the ring; consumers chase it. Both patterns are exactly what the hardware prefetcher is designed for.
* **No false sharing.** Padding ensures unrelated atomics don't collide on cache lines.
* **No system calls** on the hot path. No `futex`, no `read`/`write` to a pipe, no mutex.

---

## 15. `UnsafeCell` and why the borrow checker steps aside

Rust's safety model is "exclusive write OR shared read, never both". This algorithm violates the *letter* of that rule:

* The producer writes `slot.data` through a `&Slot` (shared reference).
* Many threads might hold `&Slot` at the same time.

But it does NOT violate the *spirit* of the rule. The producer only writes `slot.data` while the slot is in the "not yet published" state. No consumer reads it until the `Release` store on `slot.seq` makes it visible. The backpressure scan prevents the producer from reusing a slot while a consumer is still reading. So in real time, the producer and any consumer **never** touch the same `SlotData` simultaneously.

The compiler can't prove that dynamically-enforced invariant, so we tell it to stand down with:

```rust
pub data: UnsafeCell<SlotData>,
```

```167:173:bus/src/ring.rs
unsafe impl Sync for Slot {}
```

`UnsafeCell` is the only legal way in Rust to mutate through a shared reference. `unsafe impl Sync` promises the type is safe to share across threads — a promise we discharge with the seq Release/Acquire edge plus the backpressure scan.

The cost is exactly two `unsafe` blocks — one in `publish`, one in `try_consume` / `consume_one` — and they are carefully justified in module docs.

---

## 16. End-to-end flow diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              SHARED RING                                 │
│   ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
│   │  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │ ... │N-1  │          │
│   └──┬──┴──┬──┴──┬──┴──┬──┴──┬──┴──┬──┴──┬──┴──┬──┴─────┴──┬──┘          │
│      │     │     │     │     │     │     │     │            │             │
│   seq=1 seq=1 seq=1 seq=1 seq=1 seq=0 seq=0 seq=0          seq=0          │
│   data  data  data  data  data  ----- ----- -----          -----          │
│                                ▲     ▲                                    │
│                                │     │                                    │
│                            Cons_B    producer_seq = 5                    │
│                            (cursor=4) (next write here)                   │
│                                                                           │
│                       ▲                                                   │
│                       │                                                   │
│                   Cons_A (cursor=2)                                       │
│                                                                           │
│   Slot states:                                                            │
│     seq=1, data=valid : sequence 0 lives here, slot[0 & MASK]             │
│     seq=2, data=valid : sequence 1 lives here, slot[1 & MASK]             │
│     seq=0             : never written (or freshly allocated)              │
└──────────────────────────────────────────────────────────────────────────┘

   Producer thread                                      Consumer thread (one of many)
   ────────────────                                     ─────────────────────────────
1) seq = producer_seq.load(Relaxed)               1) next = my_cursor.load(Relaxed)
2) scan all cursors, find slowest                 2) slot = slots[next & MASK]
3) if seq - slowest >= RING_SIZE: bail            3) expected = next + 1
4) slot = slots[seq & MASK]                       4) s = slot.seq.load(Acquire)
5) write slot.data fields (plain stores)          5) if s < expected: not ready
6) slot.seq.store(seq + 1, Release)  ─────────┐   6) if s > expected: FellBehind
7) producer_seq.store(seq + 1, Release)       │   7) if s == expected:
                                              │      8) read slot.data (visible)
   Release ────────────────────────────────► Acquire
                                                  9) my_cursor.store(next + 1, Release) ┐
                                                                                        │
   Producer's next backpressure scan sees this advance  ◄─────────────────────────────┘
```

---

## 17. Tradeoffs and constraints to remember

This is *not* a general-purpose primitive. Every design decision pays for itself somewhere and costs you somewhere else.

| Decision | Win | Cost |
|---|---|---|
| Single producer | No CAS on publish; just stores | Need an external mux upstream if multiple feed sources |
| Bounded ring | Zero alloc; cache-friendly | Slow consumers must be evicted, not buffered indefinitely |
| Power-of-two size | `& MASK` instead of `%` | Wastes memory if your ideal size isn't a power of two |
| Per-slot synchronization (no global lock) | Each consumer scales independently | Backpressure scan is O(N_consumers) per publish |
| Fixed `MAX_CONSUMERS` | No allocation when subscribers come/go | Hard cap on subscriber count per ring |
| `PAYLOAD_CAP` per slot | Predictable memory; no chained buffers | Wastes space when most messages are small |
| Cache-line padding | No false sharing | 56 bytes wasted per cursor; ~16 KB total |
| `UnsafeCell` + manual safety proof | Borrow checker doesn't get in the way | Reviewer must understand the proof — hence this doc |
| Drop-or-evict slow consumer policy | Producer never blocks | Consumers must be prepared to be disconnected and resync |

### When this design breaks down

* **More than one producer.** You'd need a CAS-based sequence claim like LMAX's original Disruptor. Not implemented here.
* **Variable-size messages bigger than `PAYLOAD_CAP`.** Either bump the cap or move to a chained-slot design.
* **Cross-process sharing.** You'd need `mmap`-backed slots and POSIX shared memory. The `#[repr(C)]` layout already prepares us for that.
* **Persistence.** This bus is intentionally ephemeral. Add WAL writes if you need it; expect 10–100× slowdown on the hot path.

---

## Where to look in the source

* Module-level rationale: `bus/src/ring.rs` lines 1–61.
* The publish hot path: `bus/src/ring.rs` `publish()`.
* The consume hot paths: `bus/src/ring.rs` `try_consume()` and `consume_one()`.
* Cache-line padding: `Cursor` and `ConsumerEntry` definitions.
* Tests showing every property in action: `mod tests` at the bottom of the file. Especially:
  * `concurrent_producer_consumer_streams_in_order` — proves ordering under real concurrency.
  * `fan_out_to_many_consumers` — proves multiple consumers each see every message.
  * `slow_consumer_is_detected` — proves backpressure works.
  * `registration_is_capped` — proves the consumer-slot lifecycle.

---

## Suggested next reading

1. Martin Thompson et al., *Disruptor: High Performance Alternative to Bounded Queues* (2011). The original LMAX paper.
2. Mara Bos, *Rust Atomics and Locks* (2023). Chapter on memory orderings is the cleanest treatment in Rust-land.
3. Ulrich Drepper, *What Every Programmer Should Know About Memory* (2007). For the cache-line stuff if you want to go deeper.
4. The Linux `perf c2c` tool. Once you build a real workload on this ring, `c2c` will show you false-sharing events in your binary — or, ideally, the absence of them.
