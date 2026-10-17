#!/bin/bash
# The fast checks of §20.4, which the pre-commit hook runs: formatting,
# clippy, and the checker's rules of §20.1 to §20.3.
#
#   scripts/check-fast.sh
# COVEN_CHECK_PACKAGES may select Cargo packages for clippy and checker
# diagnostics. Formatting and the checker's analysis still read the workspace.
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: scripts/check-fast.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

step "cargo fmt --check"
cargo fmt --all --check

packages=(--workspace)
checker_args=(.)
if [ -n "${COVEN_CHECK_PACKAGES:-}" ]; then
    read -r -a selected <<< "$COVEN_CHECK_PACKAGES"
    packages=()
    for package in "${selected[@]}"; do
        packages+=(-p "$package")
        checker_args+=("$package")
    done
fi
step "owner-construction-check: §20.1 dependencies, §20.2 capabilities and owners, §20.3 conventions"
cargo run --quiet -p owner-construction-check -- "${checker_args[@]}"

step "cargo clippy ${packages[*]} --all-targets --all-features"
cargo clippy "${packages[@]}" --all-targets --all-features -- -D warnings
