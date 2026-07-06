#!/usr/bin/env sh
# Configure C toolchain variables needed by sqlite-wasm-rs on macOS.
set -eu

if [ "$(uname -s)" = "Darwin" ]; then
    llvm_prefix="$(brew --prefix llvm)"
    CC_wasm32_unknown_unknown="$llvm_prefix/bin/clang"
    AR_wasm32_unknown_unknown="$llvm_prefix/bin/llvm-ar"
    export CC_wasm32_unknown_unknown AR_wasm32_unknown_unknown
fi
