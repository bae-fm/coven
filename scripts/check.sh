#!/bin/bash
# Every check of §20.4, in order. CI runs the code checks on every platform
# and the proofs once, on Linux: a proof checks the same everywhere.
#
#   scripts/check.sh           everything
#   scripts/check.sh code      the code checks only
#   scripts/check.sh proofs    the Lean proofs and their differential tests
set -euo pipefail

case "${1-all}:$#" in
    all:0 | code:1 | proofs:1) part=${1-all} ;;
    *)
        echo "usage: scripts/check.sh [code | proofs]" >&2
        exit 2
        ;;
esac

cd "$(dirname "$0")/.."

step() { echo ""; echo "── $1"; }

code_checks() {
    # Build the structural checker before the crates whose graph it validates (§20.4).
    step "owner-construction-check: §20.1 dependencies, §20.2 capabilities and owners, §20.3 conventions"
    cargo run --quiet -p owner-construction-check -- .

    step "cargo fmt --check"
    cargo fmt --all --check

    step "cargo clippy --all-targets --all-features"
    cargo clippy --workspace --all-targets --all-features -- -D warnings

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

    step "release store-log replay cost (2,000 entries)"
    cargo test -p coven-sync --release replay_cost -- --ignored --nocapture
}

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

# The merge (spec/proofs/merge.md) and the store log
# (spec/proofs/storelog.md), each checked against Rust by a differential test.
proofs() {
    lean_proof "the merge" spec/proofs/merge CovenMerge
    lean_proof "the store log" spec/proofs/storelog CovenStorelog

    step "Rust / Lean differential merge test"
    runner="$(cd spec/proofs/merge && pwd)/.lake/build/bin/mergeRunner"
    if [ -f "$runner.exe" ]; then
        runner="$runner.exe"
    fi
    COVEN_MERGE_LEAN="$runner" cargo test -p coven-merge --all-features lean_differential -- --ignored

    step "Rust / Lean differential store-log test"
    runner="$(cd spec/proofs/storelog && pwd)/.lake/build/bin/storelogRunner"
    if [ -f "$runner.exe" ]; then
        runner="$runner.exe"
    fi
    COVEN_STORELOG_LEAN="$runner" cargo test -p coven-sync --all-features lean_differential -- --ignored
}

if [ "$part" != proofs ]; then
    code_checks
fi
if [ "$part" != code ]; then
    proofs
fi

echo ""
echo "✅ all checks passed"
