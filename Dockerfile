FROM rust:1.65-slim-bullseye as builder

RUN apt update && apt install -y pkg-config openssl libssl-dev curl g++ unzip make protobuf-compiler

RUN curl -o flatbuffers.zip -sL https://github.com/google/flatbuffers/releases/download/v22.9.29/Linux.flatc.binary.clang++-12.zip
RUN unzip  flatbuffers.zip
RUN mv flatc /usr/local/bin
RUN protoc --version

COPY . /cnosdb
WORKDIR /cnosdb
RUN make build-release

FROM ubuntu:focal

ENV RUST_BACKTRACE 1

COPY --from=builder /cnosdb/target/release/cnosdb /usr/bin/cnosdb
COPY --from=builder /cnosdb/target/release/cnosdb-cli /usr/bin/cnosdb-cli
COPY --from=builder /cnosdb/target/release/cnosdb-meta /usr/bin/cnosdb-meta

COPY ./config/sample.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT /usr/bin/cnosdb run--config /etc/cnosdb/cnosdb.conf