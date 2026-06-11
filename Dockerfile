# ---- builder ----
FROM rust:1-trixie AS builder

# rocksdb builds its C++ sources via cc-rs; cmake/clang/pkg-config are required.
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler cmake clang pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src

# Pass e.g. RUSTFLAGS="-C target-cpu=x86-64-v3" for microarch-tuned builds.
ARG RUSTFLAGS=""
ENV RUSTFLAGS=${RUSTFLAGS}
RUN cargo build --release --locked

# ---- dirs (creates writable data dir owned by nonroot) ----
FROM busybox AS dirs
RUN mkdir -p /data/pathlockd && chown 65532:65532 /data/pathlockd

# ---- runtime ----
FROM gcr.io/distroless/cc-debian13 AS runtime

COPY --from=dirs --chown=65532:65532 /data /data
COPY --from=builder /build/target/release/pathlockd /usr/local/bin/pathlockd

# 50051 client gRPC; 50052 internal raft gRPC; 7946/udp SWIM gossip.
EXPOSE 50051 50052 7946/udp
ENV PATHLOCKD_LISTEN=0.0.0.0:50051
ENV PATHLOCKD_DATA_DIR=/data/pathlockd

# distroless/base ships a built-in nonroot user (uid 65532).
USER nonroot

# Liveness/readiness via the daemon's own Health RPC (verifies internal readiness).
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/pathlockd", "--health-check"]

ENTRYPOINT ["/usr/local/bin/pathlockd"]
