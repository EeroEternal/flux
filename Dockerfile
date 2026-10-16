ARG RUST_IMAGE=rust:1.93.1-bookworm

FROM ${RUST_IMAGE} AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ARG CRATES_MIRROR=sparse+https://rsproxy.cn/index/
RUN if [ -n "$CRATES_MIRROR" ]; then \
      mkdir -p /src/.cargo \
        && printf '[source.crates-io]\nreplace-with = "crates-mirror"\n\n[source.crates-mirror]\nregistry = "%s"\n' \
           "$CRATES_MIRROR" > /src/.cargo/config.toml; \
    fi \
    && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/xlb /usr/local/bin/xlb
COPY config.toml /etc/xlb/config.toml
EXPOSE 8006 9106
ENTRYPOINT ["xlb"]
CMD ["--config", "/etc/xlb/config.toml"]
