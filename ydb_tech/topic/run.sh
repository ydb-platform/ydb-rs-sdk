#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
cargo run -p ydb --example ydb-tech-topic
