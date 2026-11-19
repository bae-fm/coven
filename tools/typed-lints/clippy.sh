#!/bin/bash
# Runs clippy, with the typed lints beside clippy's own, over coven's
# workspace: `tools/typed-lints/clippy.sh <cargo args> [-- <clippy args>]`,
# as `cargo clippy` takes them. It builds the driver (release, so linting
# stays fast), then does what cargo-clippy does with clippy-driver: runs
# `cargo check` with the driver as the workspace's rustc wrapper and the
# clippy arguments in CLIPPY_ARGS, which the driver reads.
#
# The driver is built on this machine, so cargo runs here
# (BUILDS_CARGO_HOST=local) and not on another build host.
set -euo pipefail
cd "$(dirname "$0")/../.."
tools/typed-lints/cargo.sh build --quiet --release -p typed-lints-driver
driver="$PWD/target/typed-lints/release/typed-lints-driver"

cargo_args=()
clippy_args=""
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    cargo_args+=("$1")
    shift
done
[ "$#" -gt 0 ] && shift
for arg in "$@"; do
    clippy_args+="${arg}__CLIPPY_HACKERY__"
done

BUILDS_CARGO_HOST=local RUSTC_WORKSPACE_WRAPPER="$driver" CLIPPY_ARGS="$clippy_args" \
    exec scripts/cargo.sh check "${cargo_args[@]}"
