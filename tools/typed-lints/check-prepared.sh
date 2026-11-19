#!/bin/sh
# Fails, naming each missing piece, unless what prepare-build.sh puts in place
# is there for the toolchain rustup picks here. It installs and downloads
# nothing, so the commit hook can run it.
set -eu
cd "$(dirname "$0")"
. ./toolchain.sh

missing=0
if ! has_rustc_dev; then
    echo "typed lints: toolchain $toolchain lacks the $rustc_dev component, which holds the compiler crates the lints build against" >&2
    missing=1
fi
if [ "$clippy_commit" != "$commit" ]; then
    echo "typed lints: tools/typed-lints/clippy holds clippy's source at ${clippy_commit:-no commit}, not at $commit, the commit $toolchain's rustc was built from" >&2
    missing=1
fi
if [ "$missing" = 1 ]; then
    echo "typed lints: tools/typed-lints/prepare-build.sh puts both in place" >&2
    exit 1
fi
