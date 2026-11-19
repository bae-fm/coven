#!/bin/sh
# Puts in place what building the typed lints needs beside their own source:
# the toolchain's compiler crates (rustup's rustc-dev component), and clippy's
# source at the commit the toolchain's rustc was built from, in clippy/. Only
# that source compiles against the toolchain's compiler crates, and only it
# holds the lints the toolchain's own clippy runs. rust-lang/rust keeps it in
# src/tools/clippy; this fetches that folder alone.
#
# The toolchain is the one rustup picks here: coven's pin, or RUSTUP_TOOLCHAIN.
# clippy/.rust-commit records the commit clippy/ came from, and a folder from
# another commit is fetched again.
set -eu
cd "$(dirname "$0")"

version=$(rustc -vV)
commit=$(printf '%s\n' "$version" | sed -n 's/^commit-hash: //p')
host=$(printf '%s\n' "$version" | sed -n 's/^host: //p')
if [ -z "$commit" ] || [ -z "$host" ]; then
    echo "prepare-build.sh: rustc -vV names no commit or host" >&2
    exit 1
fi

toolchain=$(rustup show active-toolchain | cut -d' ' -f1)
if ! rustup component list --installed --toolchain "$toolchain" | grep -qx "rustc-dev-$host"; then
    rustup component add --toolchain "$toolchain" rustc-dev
fi

[ "$(cat clippy/.rust-commit 2>/dev/null)" = "$commit" ] && exit 0

work=$(mktemp -d "${TMPDIR:-/tmp}/typed-lints-clippy.XXXXXX")
trap 'rm -rf "$work"' EXIT
git -C "$work" init --quiet rust
git -C "$work/rust" sparse-checkout set --no-cone /src/tools/clippy/
git -C "$work/rust" fetch --quiet --depth 1 --filter=blob:none \
    https://github.com/rust-lang/rust "$commit"
git -C "$work/rust" checkout --quiet FETCH_HEAD
printf '%s\n' "$commit" > "$work/rust/src/tools/clippy/.rust-commit"
rm -rf clippy
mv "$work/rust/src/tools/clippy" clippy
