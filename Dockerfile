# Сборка с помощью пакета Rust
# https://hub.docker.com/_/rust
FROM rust:latest AS builder

RUN apt-get update -y
RUN apt-get install -y \
    libzmq5 libczmq-dev

WORKDIR /usr/src/spiderq

COPY ./src/ ./src/
COPY ./spiderq-proto/ ./spiderq-proto/
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock

RUN cargo build \
    --release

######################################################################################################## 

# Сборка рабочего пакета
FROM debian:11
RUN apt-get update -y
RUN apt-get install -y \
    libzmq5 libczmq-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /spiderq
COPY --from=builder \
    /usr/src/spiderq/target/release/spiderq \
    spiderq
ENTRYPOINT ["./spiderq"]
CMD []