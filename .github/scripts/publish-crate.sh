#!/usr/bin/env bash

set -eu

CRATE_DIR="$1"
VERSION="$2"

cd "${CRATE_DIR}"

sed -i "s/^publish *= *false/publish=true/; s/^version *=.*/version=\"${VERSION}\"/" Cargo.toml
cat Cargo.toml

echo
echo
echo

# Allow dirty because publish with changes Cargo.toml
cargo publish --allow-dirty
