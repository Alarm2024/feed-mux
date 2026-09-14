FROM rust:1.89-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/feed-mux /usr/local/bin/feed-mux
ENV BIND_ADDR=0.0.0.0:8787
EXPOSE 8787
CMD ["feed-mux"]
