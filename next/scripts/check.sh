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

# A Lean proof, built from scratch: every module is imported by its root, so
# none can drop out of the build; no `sorry`, and no axiom beyond Lean's own.
lean_proof() {
    local what=$1 proof=$2 lib=$3
    step "Lean proof of $what"
    (cd "$proof" && lake clean && lake build)
    local module
    for module in "$proof/$lib"/*.lean; do
        module=$(basename "$module" .lean)
        if [ "$module" != Axioms ] && ! tr -d '\r' < "$proof/$lib.lean" | grep -qxF "import $lib.$module"; then
            echo "$proof/$lib.lean does not import $lib.$module" >&2
            exit 1
        fi
    done
    if grep -rnwE 'sorry|admit' "$proof/$lib" "$proof/$lib.lean"; then
        echo "the $what proof has an unfinished step" >&2
        exit 1
    fi
    if grep -rnE '^[[:space:]]*axiom[[:space:]]' "$proof/$lib" "$proof/$lib.lean"; then
        echo "the $what proof declares an axiom of its own" >&2
        exit 1
    fi
    # `#print axioms` for each main result, as printed by $lib/Axioms.lean:
    # every axiom named must be one of Lean's own.
    local axioms foreign
    if ! axioms=$(cd "$proof" && lake env lean "$lib/Axioms.lean" 2>&1); then
        printf '%s\n' "$axioms" >&2
        echo "$proof/$lib/Axioms.lean failed" >&2
        exit 1
    fi
    axioms=$(printf '%s' "$axioms" | tr -d '\r' | tr '\n' ' ')
    foreign=$(printf '%s' "$axioms" |
        grep -oE '\[[^]]*\]' |
        tr -d '[]' |
        tr ',' '\n' |
        sed 's/^[[:space:]]*//; s/[[:space:]]*$//' |
        grep -v '^$' |
        grep -vxE 'propext|Classical\.choice|Quot\.sound' || true)
    if [ -n "$foreign" ]; then
        printf '%s\n' "$axioms" >&2
        echo "the $what proof depends on axioms beyond Lean's own: $foreign" >&2
        exit 1
    fi
}

# The merge (plans/coven-merge-proof.md) and the store log
# (plans/coven-storelog-proof.md).
lean_proof "the merge" ../plans/proofs/merge CovenMerge
lean_proof "the store log" ../plans/proofs/storelog CovenStorelog

step "Rust / Lean differential merge test"
runner="$(cd ../plans/proofs/merge && pwd)/.lake/build/bin/mergeRunner"
if [ -f "$runner.exe" ]; then
    runner="$runner.exe"
fi
COVEN_MERGE_LEAN="$runner" cargo test -p coven-merge --all-features lean_differential -- --ignored

step "Rust / Lean differential store-log test"
runner="$(cd ../plans/proofs/storelog && pwd)/.lake/build/bin/storelogRunner"
if [ -f "$runner.exe" ]; then
    runner="$runner.exe"
fi
COVEN_STORELOG_LEAN="$runner" cargo test -p coven-sync --all-features lean_differential -- --ignored

echo ""
echo "✅ all checks passed"
