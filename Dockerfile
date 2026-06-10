# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:slim AS builder

WORKDIR /build

# Copy the full workspace so Cargo can resolve all path dependencies.
COPY . .

RUN cargo build --bin bus --release

# ── Stage 2: minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim

# Copy only the compiled binary.
COPY --from=builder /build/target/release/bus /usr/local/bin/bus

# Default port the bus listens on.
EXPOSE 8000

# BUS_ADDR controls the bind address inside the container.
ENV BUS_ADDR=0.0.0.0:8000

ENTRYPOINT ["/usr/local/bin/bus"]
