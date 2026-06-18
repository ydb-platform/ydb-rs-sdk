FROM rust:1.85.0-slim-bookworm

ARG PROTOC_VERSION=33.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/protoc.zip \
        "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" \
    && unzip /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip \
    && protoc --version

WORKDIR /workspace
