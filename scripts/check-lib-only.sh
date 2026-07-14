#!/usr/bin/env sh
# Check coven-core and coven's lib target alone, with the feature sets a
# production host actually enables.
#
# `cargo clippy --all-targets --all-features` (the main CI gate) always turns
# on every optional feature, including `test-utils` — a host never enables
# that outside its own tests. It also always compiles in `--tests`, so a
# crate-private item reached only from a `#[cfg(test)] mod tests` looks used.
# Both together mean an item whose only real caller is test code, or code
# gated on `test-utils`, never shows up as `dead_code`: it's `pub`, so
# `unreachable_pub` doesn't apply, and the all-features/all-targets build
# always exercises its would-be caller.
#
# A bare `cargo check -p <crate> --lib`, run with only the features a host
# would actually turn on, has neither of those: no `--tests`, no
# `test-utils`. If a demoted-to-`pub(crate)` item's only caller lived in test
# code, this is where it goes dead.
#
set -eu

cargo check -p coven-core --lib

cargo check -p coven --lib
# oauth-providers is off by default but a real (if optional) host
# configuration — bae-bridge's "full" build turns it on — so check it too.
cargo check -p coven --lib --features oauth-providers
