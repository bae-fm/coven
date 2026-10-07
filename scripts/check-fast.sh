#!/bin/bash
# The fast checks of §20.4, which the pre-commit hook runs: formatting,
# clippy, and the checker's rules of §20.1 to §20.3.
#
#   scripts/check-fast.sh
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: scripts/check-fast.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

step "cargo fmt --check"
cargo fmt --all --check

step "cargo clippy --all-targets --all-features"
cargo clippy --workspace --all-targets --all-features -- -D warnings

step "owner-construction-check: §20.1 dependencies, §20.2 capabilities and owners, §20.3 conventions"
cargo run --quiet -p owner-construction-check -- .
