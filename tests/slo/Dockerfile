# SLO workloads build on the pinned modern Rust version used by CI.
FROM rust:1.96.0-bookworm AS build
ARG PKG_NAME=slo-native-query
COPY . /src
WORKDIR /src
RUN cargo build --release -p "${PKG_NAME}"

FROM debian:bookworm-slim
ARG PKG_NAME=slo-native-query
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/${PKG_NAME} /slo-rust-workload
ENTRYPOINT ["/slo-rust-workload"]
