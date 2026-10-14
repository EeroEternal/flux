#!/usr/bin/env bash
# Local quality gate mirroring CI. Run before every push that touches src/,
# tests/, Cargo.toml or CI config.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo clippy --all-targets -- -D warnings"
cargo clippy --all-targets -- -D warnings

echo "==> cargo test --workspace"
cargo test --workspace

echo
echo "All local gates passed."
