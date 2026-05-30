# ---- builder ----
FROM rust:1-trixie AS builder

# grpcio (pulled in by tikv-client) builds the gRPC C-core via cmake; bindgen is
# not required (checked-in bindings) but cmake/clang/pkg-config/openssl are.
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

# ---- runtime ----
FROM gcr.io/distroless/cc-debian13 AS runtime

COPY --from=builder /build/target/release/pathlockd /usr/local/bin/pathlockd

EXPOSE 50051
ENV PATHLOCKD_LISTEN=0.0.0.0:50051

# distroless/base ships a built-in nonroot user (uid 65532).
USER nonroot

# Liveness/readiness via the daemon's own Health RPC (also verifies TiKV
# reachability). Uses the binary itself, so no extra tooling in the image.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/pathlockd", "--health-check"]

ENTRYPOINT ["/usr/local/bin/pathlockd"]
