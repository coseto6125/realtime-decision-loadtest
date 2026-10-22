# Build natively on the aarch64 host:  docker build -t lt-collector-rs:1 .
# Builder tag verified on Docker Hub: rust:1.97.1-slim-bookworm (amd64 + arm64). Matches rust-toolchain.toml.
FROM rust:1.97.1-slim-bookworm AS builder
WORKDIR /src
# Dependency layer: compile the dependency graph against a stub main, so source edits do not rebuild it.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/deps/lt_collector_rs* target/release/lt-collector-rs*
COPY src ./src
RUN cargo build --release --locked

# glibc build, so the runtime needs libc/libgcc: distroless/cc-debian12 (same glibc 2.36 as bookworm).
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/lt-collector-rs /usr/local/bin/lt-collector-rs
ENV PORT=8000
EXPOSE 8000
USER nonroot
ENTRYPOINT ["/usr/local/bin/lt-collector-rs"]
