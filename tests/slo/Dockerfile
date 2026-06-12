FROM rust:1.91-bookworm AS build
ARG SRC_PATH=native/table
COPY . /src
WORKDIR /src
RUN cargo build --release -p slo-native-table

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/slo-native-table /slo-rust-workload
ENTRYPOINT ["/slo-rust-workload"]
