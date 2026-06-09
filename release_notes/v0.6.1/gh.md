Removed `-C target-cpu=x86-64-v3` from Docker builds to fix RocksDB C++
compilation errors. The compiler now uses the generic `x86-64` baseline.

## Changes

### Fixed: RocksDB build failure in Docker images

The container image build was failing during `cargo build --release` because
`-C target-cpu=x86-64-v3` caused `cc-rs` to emit CPU-specific instructions
(`-mavx2`, `-mbmi`, `-mlzcnt`) when compiling RocksDB's C++ sources. These
flags caused compilation failures in the Docker build environment.

The fix removes the `x86-64-v3` RUSTFLAGS from the Docker build, using the
compiler's default `x86-64` target instead. The resulting binaries are
compatible with any x86-64 CPU.

## Upgrade note

No API or configuration changes. No data migration required. The Docker image
now targets the generic `x86-64` baseline — compatible with older hardware that
previously could not run the `x86-64-v3`-optimized image.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.6.1-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.6.1-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.6.1   # amd64 + arm64
```
