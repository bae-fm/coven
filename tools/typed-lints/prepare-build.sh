#!/bin/sh
# Puts in place what building the typed lints needs beside their own source:
# the toolchain's compiler crates (rustup's rustc-dev component), and clippy's
# source at the commit the toolchain's rustc was built from, which clippy/
# leads to. Only that source compiles against the toolchain's compiler crates,
# and only it holds the lints the toolchain's own clippy runs. rust-lang/rust
# keeps it in src/tools/clippy; this fetches that folder alone, once per
# commit on this machine, into a folder every checkout shares
# (toolchain.sh's clippy_home), and points clippy/ at it. It does nothing
# when both are in place.
#
# It installs and downloads, so `builds setup` runs it (the `prepare` line of
# .build-dirs), and CI before its checks. The commit hook's typed lints checks
# never run it: without what it puts in place their build fails and names
# `builds setup` (cargo.sh).
set -eu
cd "$(dirname "$0")"
. ./toolchain.sh

has_rustc_dev || rustup component add --toolchain "$toolchain" rustc-dev
[ "$clippy_commit" = "$commit" ] && exit 0

shared="$clippy_home/$commit"
if [ ! -e "$shared" ]; then
    mkdir -p "$clippy_home"
    fetched=$(mktemp -d "$clippy_home/fetch.XXXXXX")
    trap 'rm -rf "$fetched"' EXIT
    git -C "$fetched" init --quiet rust
    git -C "$fetched/rust" sparse-checkout set --no-cone /src/tools/clippy/
    git -C "$fetched/rust" fetch --quiet --depth 1 --filter=blob:none \
        https://github.com/rust-lang/rust "$commit"
    git -C "$fetched/rust" checkout --quiet FETCH_HEAD
    printf '%s\n' "$commit" > "$fetched/rust/src/tools/clippy/.rust-commit"
    # A link appears whole or not at all, and fails when another checkout's
    # fetch made it first; that fetch's source is the same.
    ln -s "$fetched/rust/src/tools/clippy" "$shared" 2>/dev/null && fetched=
fi
rm -rf clippy
ln -s "$shared" clippy
