#!/bin/bash
# Every check of §21.4, in order. CI runs this same script on every platform;
# a branch that passes it here passes there.
#
#   next/scripts/check.sh
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: next/scripts/check.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

step "cargo fmt --check"
cargo fmt --all --check

step "cargo clippy --all-targets --all-features"
cargo clippy --workspace --all-targets --all-features -- -D warnings

step "owner-construction-check: §21.1 dependencies, §21.2 capabilities and owners, §21.3 conventions"
cargo run --quiet -p owner-construction-check -- .

step "cargo doc (broken links denied)"
RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D warnings" \
    cargo doc --workspace --no-deps --all-features

# Default targets — every library and binary, no tests — with production
# features only, so an item only test code or a test-only feature uses shows
# up as dead. (`--lib` alone would skip the binaries, and fails outright while
# the workspace holds no library.)
step "every crate built without test code"
cargo check --workspace

step "cargo test --all-features"
cargo test --workspace --all-features

step "cargo test --no-default-features"
cargo test --workspace --no-default-features

# The Lean model of the merge (plans/coven-merge-proof.md), built from
# scratch, with no `sorry` and no axiom beyond Lean's own.
step "Lean proof of the merge"
proof=../plans/proofs/merge
(cd "$proof" && lake clean && lake build)
if grep -rnwE 'sorry|admit' "$proof/CovenMerge" "$proof/CovenMerge.lean"; then
    echo "the merge proof has an unfinished step" >&2
    exit 1
fi
if grep -rnE '^[[:space:]]*axiom[[:space:]]' "$proof/CovenMerge" "$proof/CovenMerge.lean"; then
    echo "the merge proof declares an axiom of its own" >&2
    exit 1
fi
# `#print axioms` for each main result, as printed by CovenMerge/Axioms.lean:
# every axiom named must be one of Lean's own.
axioms=$(cd "$proof" && lake env lean CovenMerge/Axioms.lean | tr -d '\r' | tr '\n' ' ')
foreign=$(printf '%s' "$axioms" |
    grep -oE '\[[^]]*\]' |
    tr -d '[]' |
    tr ',' '\n' |
    sed 's/^[[:space:]]*//; s/[[:space:]]*$//' |
    grep -v '^$' |
    grep -vxE 'propext|Classical\.choice|Quot\.sound' || true)
if [ -n "$foreign" ]; then
    printf '%s\n' "$axioms" >&2
    echo "the merge proof depends on axioms beyond Lean's own: $foreign" >&2
    exit 1
fi

echo ""
echo "✅ all checks passed"
