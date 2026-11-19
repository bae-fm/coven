#!/bin/bash
# Runs clippy, with the typed lints beside clippy's own, over coven's
# workspace: `tools/typed-lints/clippy.sh <cargo args> [-- <clippy args>]`,
# as `cargo clippy` takes them. It does what cargo-clippy does with
# clippy-driver: runs `cargo check` with the typed lints' driver as the
# workspace's rustc wrapper and the clippy arguments in CLIPPY_ARGS, which the
# driver reads. The wrapper is typed-lints-rustc, which runs the driver with
# rustc writing every function's MIR into each workspace crate's metadata
# (driver/src/rustc.rs). cargo may run on another build machine: driver.sh
# runs first on whichever machine runs it and points tools/typed-lints/built
# at the driver there.
set -euo pipefail
cd "$(dirname "$0")/../.."
case "$(rustc -vV | sed -n 's/^host: //p')" in
*windows*) exe=.exe ;;
*) exe= ;;
esac

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

BUILDS_CARGO_BEFORE=tools/typed-lints/driver.sh \
    RUSTC_WORKSPACE_WRAPPER="$PWD/tools/typed-lints/built/typed-lints-rustc$exe" \
    CLIPPY_ARGS="$clippy_args" \
    exec scripts/cargo.sh check "${cargo_args[@]}"
