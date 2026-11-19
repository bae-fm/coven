#!/bin/sh
# Runs cargo on this workspace, as `./cargo.sh <subcommand> [<args>...]`, after
# prepare-build.sh. The compiler's crates are unstable, so RUSTC_BOOTSTRAP=1
# lets this stable toolchain build against them; the lints' users run the
# built driver without it. The build goes in coven's own target folder. Paths
# are relative to coven's top folder, so cargo reads them alike on Windows.
set -eu
cd "$(dirname "$0")/../.."
tools/typed-lints/prepare-build.sh
subcommand=$1
shift
RUSTC_BOOTSTRAP=1 exec scripts/cargo.sh "$subcommand" \
    --manifest-path tools/typed-lints/Cargo.toml \
    --config 'build.target-dir="target/typed-lints"' "$@"
