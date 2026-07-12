#!/bin/bash
# Everything CI runs, locally. The fast-loop gate: a branch that passes this
# merges without a GitHub round-trip.
#
#   scripts/check.sh          # workspace checks (skips wasm)
#   scripts/check.sh --wasm   # also the wasm target — run when coven-core changes
set -e

cd "$(dirname "$0")/.."

WITH_WASM=0
[ "$1" = "--wasm" ] && WITH_WASM=1

step() { echo ""; echo "── $1"; }

step "cargo fmt --all --check"
cargo fmt --all --check

step "cargo clippy --all-targets --all-features"
cargo clippy --all-targets --all-features -- -D warnings

step "scripts/check-lib-only.sh (the feature sets a host actually ships)"
scripts/check-lib-only.sh

step "cargo test --all-features"
cargo test --all-features

step "cargo test (default features)"
cargo test

if [ "$WITH_WASM" -eq 1 ]; then
    step "scripts/check-wasm.sh"
    scripts/check-wasm.sh
fi

echo ""
echo "✅ all checks passed"
