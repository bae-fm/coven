#!/bin/bash
# The fast checks of §21.4, which the pre-commit hook runs: formatting,
# clippy, and the checker's rules of §21.1 to §21.3.
#
#   next/scripts/check-fast.sh
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: next/scripts/check-fast.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

step "cargo fmt --check"
cargo fmt --all --check

step "cargo clippy --all-targets --all-features"
cargo clippy --workspace --all-targets --all-features -- -D warnings

step "owner-construction-check: §21.1 dependencies, §21.2 capabilities and owners, §21.3 conventions"
cargo run --quiet -p owner-construction-check -- .
