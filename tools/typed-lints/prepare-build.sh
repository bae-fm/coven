#!/bin/sh
# Puts in place what building the typed lints needs beside their own source:
# the toolchain's compiler crates (rustup's rustc-dev component), and clippy's
# source at the commit the toolchain's rustc was built from, in clippy/. Only
# that source compiles against the toolchain's compiler crates, and only it
# holds the lints the toolchain's own clippy runs. rust-lang/rust keeps it in
# src/tools/clippy; this fetches that folder alone. clippy/.rust-commit records
# the commit clippy/ came from, and a folder from another commit is fetched
# again.
#
# It installs and downloads, so a machine's setup runs it, and CI before its
# checks; the commit hook runs check-prepared.sh instead.
set -eu
cd "$(dirname "$0")"
. ./toolchain.sh

has_rustc_dev || rustup component add --toolchain "$toolchain" rustc-dev
[ "$clippy_commit" = "$commit" ] && exit 0

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
