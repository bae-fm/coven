#!/bin/sh
# Runs cargo on this workspace, as `./cargo.sh <subcommand> [<args>...]`.
# The compiler's crates are unstable, so RUSTC_BOOTSTRAP=1 lets this stable
# toolchain build against them; the lints' users run the built driver without
# it. The build goes in coven's own target folder. Paths are relative to
# coven's top folder, so cargo reads them alike on Windows.
#
# The build reads clippy's source through clippy/ and the compiler crates of
# rustup's rustc-dev component from the toolchain's library folder, which
# prepare-build.sh puts in place. A build that fails without one of them fails
# within seconds, in the first crate that names them, and then this names
# the missing path and `builds setup`, which runs prepare-build.sh.
set -eu
cd "$(dirname "$0")/../.."
subcommand=$1
shift
status=0
RUSTC_BOOTSTRAP=1 scripts/cargo.sh "$subcommand" \
    --manifest-path tools/typed-lints/Cargo.toml \
    --config 'build.target-dir="target/typed-lints"' "$@" || status=$?
[ "$status" = 0 ] && exit 0

if [ ! -f tools/typed-lints/clippy/Cargo.toml ]; then
    echo "typed lints: clippy's source is missing at tools/typed-lints/clippy; run \`builds setup\` (tools/typed-lints/prepare-build.sh fetches it)" >&2
fi
version=$(rustc -vV)
host=$(printf '%s\n' "$version" | sed -n 's/^host: //p')
library="$(rustc --print sysroot)/lib/rustlib/$host/lib"
if ! ls "$library"/librustc_middle-* >/dev/null 2>&1; then
    echo "typed lints: the compiler crates are missing from $library; run \`builds setup\` (tools/typed-lints/prepare-build.sh installs rustup's rustc-dev component)" >&2
fi
exit "$status"
