#!/bin/bash
# The fast checks of Appendix F.6, which the pre-commit hook runs: formatting,
# clippy, the checker's rules of Appendix F.1 to F.5, and the spec's readability
# checks (scripts/check-spec.py).
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

step "spec/coven.md and spec/format.md against spec/WRITING.md"
"$(command -v python3 || command -v python)" scripts/check-spec.py

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
step "owner-construction-check: Appendix F.1 dependencies, F.2 and F.3 capabilities and owners, F.5 conventions"
cargo run --quiet -p owner-construction-check -- "${checker_args[@]}"

step "cargo deny check bans: the crates Appendix F.1 refuses"
cargo deny check bans

step "cargo clippy ${packages[*]} --all-targets --all-features"
cargo clippy "${packages[@]}" --all-targets --all-features -- -D warnings
