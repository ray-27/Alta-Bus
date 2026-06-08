#!/usr/bin/env python3
"""
alta_bus subscriber — print every message received from the bus.

Usage:
    python sub.py                           # subscribe to ALL channels
    python sub.py 1001 1002                 # subscribe to NIFTY + BANKNIFTY only
    python sub.py --addr 192.168.1.10:8000  # custom bus address
    BUS_ADDR=0.0.0.0:8000 python sub.py

Wire protocol
-------------
HEADER_SIZE = 17 bytes  (1 + 4 + 8 + 4, explicit wire format)

  Offset  Size  Type      Field
  ------  ----  --------  -----------
       0     1  u8        msg_type
       1     4  u32 LE    channel_id
       5     8  u64 LE    timestamp_ns
      13     4  u32 LE    payload_len

All multi-byte integers are little-endian.
"""

import os
import socket
import struct
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HEADER_SIZE = 17          # wire format: 1 + 4 + 8 + 4 = 17 bytes
HEADER_FMT  = "<BIQI"     # u8 msg_type + u32 channel_id + u64 timestamp_ns + u32 payload_len

MSG_TYPE_NAMES = {
    0: "SUBSCRIBE",
    1: "PriceTick",
    2: "OrderBookDelta",
    3: "Signal",
    4: "ExecutionReport",
    5: "RiskBreach",
    6: "Heartbeat",
}

CHANNEL_NAMES = {
    1001: "NIFTY_SPOT",
    1002: "BANKNIFTY_SPOT",
    1003: "RELIANCE",
    1004: "TCS",
    9000: "HEARTBEAT",
    9001: "EXECUTION_REPORTS",
    9002: "RISK_BREACH",
    9003: "SYSTEM_STATUS",
}

# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes, blocking until all arrive or connection drops."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("bus closed the connection")
        buf.extend(chunk)
    return bytes(buf)


def send_subscribe(sock: socket.socket, channels: list[int]) -> None:
    """
    Send the subscribe control message.

    msg_type = 0 tells the bus this connection is a subscriber.
    If channels is empty the bus will fan-out ALL channels to us.
    Otherwise the payload carries the channel list as packed u32 LE values.
    """
    channel_payload = b""
    if channels:
        channel_payload = struct.pack(f"<{len(channels)}I", *channels)

    # Pack the 17 meaningful header bytes, then pad to HEADER_SIZE (24).
    header_fields = struct.pack(HEADER_FMT, 0, 0, 0, len(channel_payload))
    header = header_fields  # exactly HEADER_SIZE (17) bytes — no padding needed

    sock.sendall(header)
    if channel_payload:
        sock.sendall(channel_payload)


def read_message(sock: socket.socket) -> tuple[int, int, int, bytes]:
    """
    Block until a full message (header + payload) arrives.

    Returns (msg_type, channel_id, timestamp_ns, payload_bytes).
    """
    raw = recv_exact(sock, HEADER_SIZE)
    msg_type, channel_id, timestamp_ns, payload_len = struct.unpack_from(HEADER_FMT, raw)
    payload = recv_exact(sock, payload_len) if payload_len else b""
    return msg_type, channel_id, timestamp_ns, payload


# ---------------------------------------------------------------------------
# Payload decoders
# ---------------------------------------------------------------------------

def decode_price_tick(payload: bytes) -> dict | None:
    """PriceTick — 32 bytes: u32 instrument_id, f64 last_price, u32 volume,
                             u64 total_traded_volume, u64 timestamp_ns"""
    if len(payload) < 32:
        return None
    instrument_id, last_price, volume, total_vol, ts_ns = struct.unpack_from("<IdIQQ", payload)
    return {
        "instrument_id": instrument_id,
        "last_price":    last_price,
        "volume":        volume,
        "total_vol":     total_vol,
        "timestamp_ns":  ts_ns,
    }


def decode_heartbeat(payload: bytes) -> dict | None:
    """Heartbeat — 12 bytes: u32 source_id, u64 sequence_num"""
    if len(payload) < 12:
        return None
    source_id, seq_num = struct.unpack_from("<IQ", payload)
    return {"source_id": source_id, "sequence_num": seq_num}


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------

def ns_to_time(ns: int) -> str:
    """Format a nanosecond Unix timestamp as HH:MM:SS.mmm"""
    if ns == 0:
        return "----"
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).astimezone()
    return dt.strftime("%H:%M:%S.") + f"{(ns % 1_000_000_000) // 1_000_000:03d}"


def print_message(msg_type: int, channel_id: int, timestamp_ns: int, payload: bytes) -> None:
    type_name    = MSG_TYPE_NAMES.get(msg_type, f"UNKNOWN({msg_type})")
    channel_name = CHANNEL_NAMES.get(channel_id, f"ch={channel_id}")
    wall         = ns_to_time(timestamp_ns)

    if msg_type == 1:  # PriceTick
        d = decode_price_tick(payload)
        if d:
            print(
                f"[{wall}] PriceTick  {channel_name:<18}"
                f"  price={d['last_price']:>10.2f}"
                f"  vol={d['volume']:>6}"
                f"  total_vol={d['total_vol']:>12,}"
            )
            return

    if msg_type == 6:  # Heartbeat
        d = decode_heartbeat(payload)
        if d:
            print(
                f"[{wall}] Heartbeat  src={d['source_id']}"
                f"  seq={d['sequence_num']}"
            )
            return

    # Fallback: raw hex dump for unknown / unimplemented types.
    hex_preview = payload[:16].hex()
    ellipsis = "…" if len(payload) > 16 else ""
    print(
        f"[{wall}] {type_name:<18}  {channel_name:<18}"
        f"  {len(payload)} bytes  {hex_preview}{ellipsis}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> tuple[str, list[int]]:
    args = sys.argv[1:]
    addr = os.environ.get("BUS_ADDR", "127.0.0.1:8000")
    channels = []

    i = 0
    while i < len(args):
        if args[i] == "--addr" and i + 1 < len(args):
            addr = args[i + 1]
            i += 2
        else:
            try:
                channels.append(int(args[i]))
            except ValueError:
                print(f"[sub] unknown argument: {args[i]}", file=sys.stderr)
                sys.exit(1)
            i += 1

    return addr, channels


def main() -> None:
    addr, channels = parse_args()

    host, _, port_str = addr.rpartition(":")
    host = host or "127.0.0.1"
    port = int(port_str) if port_str else 8000

    print(f"[sub] connecting to {host}:{port}")
    sock = socket.create_connection((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    if channels:
        print(f"[sub] subscribing to channels: {channels}")
    else:
        print("[sub] subscribing to ALL channels")

    send_subscribe(sock, channels)
    print("[sub] handshake sent — waiting for messages\n")

    msg_count = 0
    try:
        while True:
            msg_type, channel_id, timestamp_ns, payload = read_message(sock)
            msg_count += 1
            print_message(msg_type, channel_id, timestamp_ns, payload)
    except KeyboardInterrupt:
        print(f"\n[sub] stopped after {msg_count} messages")
    except ConnectionError as e:
        print(f"\n[sub] disconnected: {e}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
