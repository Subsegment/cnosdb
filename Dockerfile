FROM rust:1.64-slim-bullseye as builder

RUN apt update && apt install -y pkg-config openssl libssl-dev curl g++

RUN curl -o flatbuffers.zip -sL https://github.com/google/flatbuffers/releases/download/v22.9.29/Linux.flatc.binary.clang++-12.zip \
RUN unzip  flatbuffers.zip
RUN mv flatc /usr/local/bin

COPY . /cnosdb
WORKDIR /cnosdb
RUN cargo build --release --bin main \
    && cargo build --release --package client

FROM ubuntu:focal

ENV RUST_BACKTRACE 1

COPY --from=builder /cnosdb/target/release/main /usr/bin/cnosdb
COPY --from=builder /cnosdb/target/release/client /usr/bin/cnosdb-cli

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT ["/bin/bash", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait"]