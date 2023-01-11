FROM rust:1.65-slim-bullseye as builder

RUN apt update && apt install -y pkg-config openssl libssl-dev curl g++ unzip make protobuf-compiler

RUN curl -o flatbuffers.zip -sL https://github.com/google/flatbuffers/releases/download/v22.9.29/Linux.flatc.binary.clang++-12.zip
RUN unzip  flatbuffers.zip
RUN mv flatc /usr/local/bin
RUN protoc --version

## RUN curl -o protoc-21.12-linux-x86_64.zip -sL https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-x86_64.zip && unzip protoc-21.12-linux-x86_64.zip && mv bin/protoc /usr/local/bin

COPY . /cnosdb
WORKDIR /cnosdb
RUN make build

FROM ubuntu:focal

ENV RUST_BACKTRACE 1

COPY --from=builder /cnosdb/target/release/cnosdb /usr/bin/cnosdb
COPY --from=builder /cnosdb/target/release/cnosdb-cli /usr/bin/cnosdb-cli
COPY --from=builder /cnosdb/target/release/cnosdb-meta /usr/bin/cnosdb-meta

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT ["/bin/bash", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait"]