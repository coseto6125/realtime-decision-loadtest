#!/usr/bin/env bash
# Build lt-collector-rs:1 on the load host with the compile pinned to core 2 (container lt-rust-build),
# then wrap the binary in the distroless runtime image. Cargo registry cache lives in volume lt-cargo.
set -euo pipefail
: "${LT_SSH:?set LT_SSH}"
R=${LT_REMOTE_DIR:-lt-loadtest}
ssh "$LT_SSH" "cd $R/rust-collector && mkdir -p target && \
  docker run --rm --name lt-rust-build --cpuset-cpus 2 --memory 4g -e HUID=\$(id -u) -e HGID=\$(id -g) \
    -v \$PWD:/src -v lt-cargo:/usr/local/cargo/registry -w /src rust:1.97.1-slim-bookworm \
    sh -c 'cargo build --release --locked 2>&1 | tail -3; chown -R \$HUID:\$HGID target' && \
  docker build -q -f Dockerfile.runtime -t lt-collector-rs:1 target/release && \
  docker image ls lt-collector-rs:1 --format '{{.Repository}}:{{.Tag}} {{.Size}}'"
