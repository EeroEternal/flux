FROM rust:1.83-slim-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/xlb /usr/local/bin/xlb
COPY config.toml /etc/xlb/config.toml
EXPOSE 8006 9106
ENTRYPOINT ["xlb"]
CMD ["--config", "/etc/xlb/config.toml"]
