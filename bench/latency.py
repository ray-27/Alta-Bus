#!/usr/bin/env python3
"""
Latency benchmark: alta_bus vs Valkey (Redis-compatible) pub/sub.

Methodology
-----------
One-way latency is measured by embedding a nanosecond send-timestamp in the
payload of each message. The subscriber reads the timestamp, calls time.time_ns()
immediately after recv(), and computes:

    latency = recv_ns - send_ns

Both publisher and subscriber run in the same process on the same machine,
so the clock is shared and there is no clock-synchronisation error.

The publisher sends at a controlled rate (--interval µs, default 500µs) to
prevent TCP buffer queueing. True single-message transit time is measured, not
the average-under-load.

Requirements
------------
    cargo run --bin bus            # alta_bus must be running before this script
    Valkey / Redis at 127.0.0.1:6379 (default)

Usage
-----
    python bench/latency.py
    python bench/latency.py --count 20000 --interval 200
    python bench/latency.py --bus-addr 127.0.0.1:8000 --redis-addr 127.0.0.1:6379
"""

import argparse
import socket
import struct
import sys
import threading
import time

try:
    import redis as redis_lib
except ImportError:
    print("ERROR: redis-py not installed.  Run:  pip install redis", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# alta_bus wire format
# ---------------------------------------------------------------------------

# Wire header: exactly 17 bytes (1 + 4 + 8 + 4).
# HEADER_SIZE in the Rust protocol crate was fixed from size_of::<MsgHeader>()=24
# (which included 7 bytes of struct-alignment padding) to the explicit wire size.
HEADER_SIZE = 17
HEADER_FMT  = "<BIQI"   # u8 msg_type, u32 channel_id, u64 ts_ns, u32 payload_len
assert struct.calcsize(HEADER_FMT) == HEADER_SIZE

BENCH_CHANNEL  = 9999   # dedicated channel, stays out of real data channels
BENCH_MSG_TYPE = 99     # opaque to the bus; never matched against known types

# 8-byte send timestamp + 8 bytes of zero padding (total 16 bytes payload)
BENCH_PAYLOAD_SIZE = 16


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)


def _bus_connect(addr: str) -> socket.socket:
    host, _, port_s = addr.rpartition(":")
    sock = socket.create_connection((host or "127.0.0.1", int(port_s)))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


def _bus_subscribe(sock: socket.socket, channels: list[int]) -> None:
    """Send subscribe control message (msg_type=0)."""
    ch_payload = struct.pack(f"<{len(channels)}I", *channels) if channels else b""
    hdr = struct.pack(HEADER_FMT, 0, 0, 0, len(ch_payload))
    sock.sendall(hdr + ch_payload)


def _bus_publish(sock: socket.socket, channel: int, msg_type: int, payload: bytes) -> None:
    hdr = struct.pack(HEADER_FMT, msg_type, channel, 0, len(payload))
    sock.sendall(hdr + payload)


def _bus_recv(sock: socket.socket) -> tuple[int, int, bytes]:
    """Read one message from the bus. Returns (msg_type, channel_id, payload)."""
    raw = _recv_exact(sock, HEADER_SIZE)
    msg_type, channel_id, _ts, payload_len = struct.unpack_from(HEADER_FMT, raw)
    payload = _recv_exact(sock, payload_len) if payload_len else b""
    return msg_type, channel_id, payload


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _pct(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    k  = (len(data) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (k - lo)


def _mean(data: list[float]) -> float:
    return sum(data) / len(data) if data else 0.0


def _stdev(data: list[float]) -> float:
    if len(data) < 2:
        return 0.0
    m  = _mean(data)
    return (sum((x - m) ** 2 for x in data) / (len(data) - 1)) ** 0.5


def _histogram(data: list[float], label: str, width: int = 50) -> None:
    """Print a simple ASCII histogram in µs buckets."""
    lo, hi = data[0], data[-1]
    n_buckets = 20
    bucket_w  = (hi - lo) / n_buckets if hi > lo else 1.0
    counts    = [0] * n_buckets
    for v in data:
        idx = min(int((v - lo) / bucket_w), n_buckets - 1)
        counts[idx] += 1
    peak = max(counts)
    print(f"\n  {label} distribution (µs)")
    print(f"  {'':>8}  {'':50}  count")
    for i, c in enumerate(counts):
        lo_b = (lo + i * bucket_w) / 1000
        bar  = "█" * int(c / peak * width) if peak else ""
        print(f"  {lo_b:>7.1f}µ  {bar:<{width}}  {c}")


def print_stats(label: str, raw_ns: list[int], show_hist: bool = True) -> None:
    data = sorted(float(x) for x in raw_ns)
    us   = lambda ns: ns / 1_000
    print(f"\n  ┌─ {label}")
    print(f"  │  samples : {len(data):>10,}")
    print(f"  │  min     : {us(data[0]):>10.1f} µs")
    print(f"  │  p50     : {us(_pct(data,50)):>10.1f} µs")
    print(f"  │  p95     : {us(_pct(data,95)):>10.1f} µs")
    print(f"  │  p99     : {us(_pct(data,99)):>10.1f} µs")
    print(f"  │  p99.9   : {us(_pct(data,99.9)):>10.1f} µs")
    print(f"  │  max     : {us(data[-1]):>10.1f} µs")
    print(f"  │  mean    : {us(_mean(data)):>10.1f} µs")
    print(f"  └  stdev   : {us(_stdev(data)):>10.1f} µs")
    if show_hist:
        _histogram(data, label)


# ---------------------------------------------------------------------------
# alta_bus benchmark
# ---------------------------------------------------------------------------

def bench_bus(addr: str, n_warmup: int, n_msgs: int, interval_us: int) -> list[int]:
    """
    Measure one-way latency through alta_bus.
    Returns a list of latency values in nanoseconds.
    """
    total     = n_warmup + n_msgs
    latencies: list[int] = []
    sub_ready = threading.Event()
    sub_error: list[Exception] = []

    def _subscriber() -> None:
        try:
            sock = _bus_connect(addr)
            _bus_subscribe(sock, [BENCH_CHANNEL])
            sub_ready.set()
            for i in range(total):
                _, _, payload = _bus_recv(sock)
                recv_ns = time.time_ns()
                if len(payload) >= 8:
                    send_ns = struct.unpack_from("<Q", payload)[0]
                    if i >= n_warmup:
                        latencies.append(recv_ns - send_ns)
            sock.close()
        except Exception as e:
            sub_error.append(e)
            sub_ready.set()

    t = threading.Thread(target=_subscriber, daemon=True)
    t.start()
    sub_ready.wait(timeout=5)

    if sub_error:
        raise sub_error[0]

    # Give the bus a moment to process the subscribe handshake before publishing.
    time.sleep(0.05)

    pub = _bus_connect(addr)
    interval_s = interval_us / 1_000_000

    for _ in range(total):
        send_ns = time.time_ns()
        payload = struct.pack("<Q", send_ns) + b"\x00" * 8
        _bus_publish(pub, BENCH_CHANNEL, BENCH_MSG_TYPE, payload)
        time.sleep(interval_s)

    t.join(timeout=30)
    pub.close()

    if sub_error:
        raise sub_error[0]

    return latencies


# ---------------------------------------------------------------------------
# Valkey / Redis benchmark
# ---------------------------------------------------------------------------

def bench_redis(addr: str, n_warmup: int, n_msgs: int, interval_us: int) -> list[int]:
    """
    Measure one-way pub/sub latency through Valkey / Redis.
    Returns a list of latency values in nanoseconds.
    """
    CHANNEL   = "alta_bus:bench"
    total     = n_warmup + n_msgs
    latencies: list[int] = []
    sub_ready = threading.Event()
    sub_error: list[Exception] = []

    def _subscriber() -> None:
        try:
            r  = redis_lib.Redis.from_url(f"redis://{addr}", decode_responses=False)
            ps = r.pubsub()
            ps.subscribe(CHANNEL)
            # Consume the subscribe-confirmation message before signalling ready.
            for msg in ps.listen():
                if msg["type"] == "subscribe":
                    break
            sub_ready.set()

            received = 0
            for msg in ps.listen():
                if msg["type"] != "message":
                    continue
                recv_ns = time.time_ns()
                data = msg["data"]
                if len(data) >= 8:
                    send_ns = struct.unpack_from("<Q", data)[0]
                    if received >= n_warmup:
                        latencies.append(recv_ns - send_ns)
                received += 1
                if received >= total:
                    break

            ps.unsubscribe()
            ps.close()
        except Exception as e:
            sub_error.append(e)
            sub_ready.set()

    t = threading.Thread(target=_subscriber, daemon=True)
    t.start()
    sub_ready.wait(timeout=5)

    if sub_error:
        raise sub_error[0]

    time.sleep(0.05)

    r = redis_lib.Redis.from_url(f"redis://{addr}", decode_responses=False)
    interval_s = interval_us / 1_000_000

    for _ in range(total):
        send_ns = time.time_ns()
        payload = struct.pack("<Q", send_ns) + b"\x00" * 8
        r.publish(CHANNEL, payload)
        time.sleep(interval_s)

    t.join(timeout=30)

    if sub_error:
        raise sub_error[0]

    return latencies


# ---------------------------------------------------------------------------
# Comparison table
# ---------------------------------------------------------------------------

def print_comparison(bus_ns: list[int], redis_ns: list[int]) -> None:
    bd = sorted(float(x) for x in bus_ns)
    rd = sorted(float(x) for x in redis_ns)

    def row(label: str, pct: float) -> None:
        b = _pct(bd, pct) / 1000
        r = _pct(rd, pct) / 1000
        ratio = r / b if b > 0 else float("inf")
        bar   = "▓" * min(int(ratio * 4), 40)
        print(f"  {label:<8}  {b:>9.1f} µs  {r:>9.1f} µs  {ratio:>5.1f}x  {bar}")

    print()
    print("  " + "═" * 62)
    print(f"  {'':8}  {'alta_bus':>11}  {'Valkey':>9}  {'ratio':>6}")
    print("  " + "─" * 62)
    row("p50",   50)
    row("p95",   95)
    row("p99",   99)
    row("p99.9", 99.9)
    row("max",  100)
    print("  " + "═" * 62)
    ratio_p50 = _pct(rd, 50) / _pct(bd, 50) if _pct(bd, 50) > 0 else 0
    ratio_p99 = _pct(rd, 99) / _pct(bd, 99) if _pct(bd, 99) > 0 else 0
    print(f"\n  alta_bus is {ratio_p50:.1f}x faster at p50  |  {ratio_p99:.1f}x faster at p99")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Latency benchmark: alta_bus vs Valkey pub/sub"
    )
    ap.add_argument("--bus-addr",   default="127.0.0.1:8000", metavar="HOST:PORT")
    ap.add_argument("--redis-addr", default="127.0.0.1:6379", metavar="HOST:PORT")
    ap.add_argument("--count",    type=int, default=5_000,
                    help="messages to measure (default 5000)")
    ap.add_argument("--warmup",   type=int, default=500,
                    help="warmup messages discarded (default 500)")
    ap.add_argument("--interval", type=int, default=500,
                    help="µs between publishes (default 500). "
                         "Must be > expected one-way latency to avoid queueing.")
    ap.add_argument("--no-hist",  action="store_true",
                    help="skip ASCII histograms")
    args = ap.parse_args()

    total_msgs = args.count + args.warmup
    est_sec    = total_msgs * args.interval / 1_000_000

    print("━" * 60)
    print("  alta_bus vs Valkey  —  one-way latency benchmark")
    print("━" * 60)
    print(f"  bus addr  : {args.bus_addr}")
    print(f"  redis addr: {args.redis_addr}")
    print(f"  messages  : {args.count:,}  (+ {args.warmup:,} warmup discarded)")
    print(f"  interval  : {args.interval} µs between sends")
    print(f"  est. time : ~{est_sec:.0f}s per system  (~{est_sec*2:.0f}s total)")
    print()

    # ---- alta_bus ----
    print(f"[1/2] alta_bus benchmark  ({total_msgs:,} msgs × {args.interval}µs) …")
    bus_latencies: list[int] = []
    try:
        bus_latencies = bench_bus(
            args.bus_addr, args.warmup, args.count, args.interval
        )
        print_stats("alta_bus", bus_latencies, show_hist=not args.no_hist)
    except ConnectionRefusedError:
        print("\n  ✗ Could not connect to alta_bus.")
        print("    Start it first:  cargo run --bin bus\n")
    except Exception as e:
        print(f"\n  ✗ alta_bus error: {e}\n")

    # ---- Valkey / Redis ----
    print(f"\n[2/2] Valkey benchmark  ({total_msgs:,} msgs × {args.interval}µs) …")
    redis_latencies: list[int] = []
    try:
        redis_latencies = bench_redis(
            args.redis_addr, args.warmup, args.count, args.interval
        )
        print_stats("Valkey", redis_latencies, show_hist=not args.no_hist)
    except ConnectionRefusedError:
        print(f"\n  ✗ Could not connect to Valkey at {args.redis_addr}.\n")
    except Exception as e:
        print(f"\n  ✗ Valkey error: {e}\n")

    # ---- Side-by-side comparison ----
    if bus_latencies and redis_latencies:
        print("\n\n  Side-by-side comparison")
        print_comparison(bus_latencies, redis_latencies)
    print()


if __name__ == "__main__":
    main()
