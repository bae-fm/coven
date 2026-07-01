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

if [ "$(uname -s)" = "Darwin" ]; then
    llvm_prefix="$(brew --prefix llvm)"
    CC_wasm32_unknown_unknown="$llvm_prefix/bin/clang"
    AR_wasm32_unknown_unknown="$llvm_prefix/bin/llvm-ar"
    export CC_wasm32_unknown_unknown AR_wasm32_unknown_unknown
fi

exec cargo check -p coven-wasm --target wasm32-unknown-unknown "$@"
