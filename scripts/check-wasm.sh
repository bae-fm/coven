#!/usr/bin/env sh
# Compile the browser crate for wasm32.
#
# sqlite-wasm-rs's build script compiles a C SQLite to wasm, which needs a clang
# whose backend can emit wasm objects. Apple's system clang can't, so on macOS we
# point cargo's per-target build-script CC/AR at Homebrew LLVM. On Linux the system
# clang already targets wasm32 (and ships llvm-ar), so we leave CC/AR unset and let
# cargo's defaults find them — this matches what CI does on its Linux runner.
#
# Pass-through args go to `cargo check`, e.g. `scripts/check-wasm.sh --tests`.
set -eu

. "$(dirname -- "$0")/wasm-toolchain-env.sh"

exec cargo check -p coven-wasm --target wasm32-unknown-unknown "$@"
