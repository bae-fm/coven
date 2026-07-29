#!/bin/bash
# Everything CI runs, locally. The fast-loop gate: a branch that passes this
# merges without a GitHub round-trip.
#
#   scripts/check.sh          # workspace checks
set -e

if [ "$#" -ne 0 ]; then
    echo "usage: scripts/check.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

step "restricted-path Rust visibility"
scripts/check-rust-visibility.sh

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

echo ""
echo "✅ all checks passed"
