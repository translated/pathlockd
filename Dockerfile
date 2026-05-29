# ---- builder ----
FROM rust:1-bookworm AS builder

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

RUN cargo build --release --locked

# ---- runtime ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/pathlockd /usr/local/bin/pathlockd

EXPOSE 50051
ENV PATHLOCKD_LISTEN=0.0.0.0:50051
ENTRYPOINT ["/usr/local/bin/pathlockd"]
