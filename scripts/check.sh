#!/bin/bash
# Every check of Appendix F.6, in order. CI runs the code checks on every platform
# and the proofs once, on Linux: a proof checks the same everywhere.
#
#   scripts/check.sh                  everything
#   scripts/check.sh code             the code checks only
#   scripts/check.sh proofs           the Lean proofs and their differential tests
#   scripts/check.sh changed [base]   the same checks, limited to what differs from
#                                     base (default origin/main): changed crates and
#                                     the crates that depend on them, changed proof
#                                     packages and the packages that require them,
#                                     and every differential whose Lean package or
#                                     Rust crate is in either set. Local work uses
#                                     it; CI runs everything on every push.
set -euo pipefail

base=
case "${1-all}:$#" in
    all:0 | code:1 | proofs:1) part=${1-all} ;;
    changed:1 | changed:2)
        part=changed
        base=${2-origin/main}
        ;;
    *)
        echo "usage: scripts/check.sh [code | proofs | changed [base]]" >&2
        exit 2
        ;;
esac

cd "$(dirname "$0")/.."
./scripts/build-layout.sh

# nextest gives each test a fresh process, whose glibc malloc starts with its
# smallest trim threshold: a test allocating and freeing large buffers then
# returns pages to the kernel on nearly every free and faults them back in.
# coven-format's mutation tests spent most of their time there (45 of 107
# seconds in a Linux container, over 400 on a CI runner). Never trimming keeps
# that memory in the process; no test measures the process's resident size.
# Other platforms' allocators ignore this.
export GLIBC_TUNABLES=glibc.malloc.trim_threshold=1073741824:glibc.malloc.mmap_threshold=33554432

step() { echo ""; echo "── $1"; }

# The workspace crates and proof packages this run checks. A full run checks
# all of them; `changed` selects from the files that differ from its base.
crates=()
proof_packages=()
select_all() {
    crates=(all)
    proof_packages=(storage merge queue io sequence)
}
select_changed() {
    local files
    files=$({ git diff --name-only "$base"; git ls-files --others --exclude-standard; } | sort -u)
    step "changed since $base: $(printf '%s\n' "$files" | grep -c . || true) files"
    # Crates: those owning a changed file, then every member that depends on one.
    # A change to the workspace manifest, lockfile or toolchain selects them all.
    local selected
    selected=$(cargo metadata --format-version 1 --no-deps | FILES="$files" python3 -c '
import json, os, sys
meta = json.load(sys.stdin)
root = meta["workspace_root"] + "/"
files = [f for f in os.environ["FILES"].splitlines() if f]
members = {}
for package in meta["packages"]:
    directory = os.path.dirname(package["manifest_path"])[len(root):] + "/"
    members[package["name"]] = (directory, {d["name"] for d in package["dependencies"] if d.get("path")})
if any(f in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml") or f.startswith(".cargo/") for f in files):
    print("\n".join(sorted(members)))
    sys.exit()
chosen = {name for name, (directory, _) in members.items() if any(f.startswith(directory) for f in files)}
while True:
    more = {name for name, (_, deps) in members.items() if deps & chosen} - chosen
    if not more:
        break
    chosen |= more
print("\n".join(sorted(chosen)))
')
    crates=()
    local crate
    for crate in $selected; do
        crates+=("$crate")
    done
    # Proof packages: those holding a changed file, then every package that
    # requires one through a lakefile path dependency.
    local package required changed=1
    proof_packages=()
    for package in storage merge queue io sequence; do
        if printf '%s\n' "$files" | grep -q "^spec/proofs/$package/"; then
            proof_packages+=("$package")
        fi
    done
    while [ "$changed" = 1 ]; do
        changed=0
        for package in storage merge queue io sequence; do
            proof_selected "$package" && continue
            for required in $(grep -A2 '\[\[require\]\]' "spec/proofs/$package/lakefile.toml" | sed -n 's/.*path = "\.\.\/\(.*\)"/\1/p'); do
                if proof_selected "$required"; then
                    proof_packages+=("$package")
                    changed=1
                    break
                fi
            done
        done
    done
    echo "crates: ${crates[*]:-none}"
    echo "proof packages: ${proof_packages[*]:-none}"
}
crate_selected() {
    local crate
    for crate in ${crates[@]+"${crates[@]}"}; do
        [ "$crate" = all ] || [ "$crate" = "$1" ] && return 0
    done
    return 1
}
proof_selected() {
    local package
    for package in ${proof_packages[@]+"${proof_packages[@]}"}; do
        [ "$package" = "$1" ] && return 0
    done
    return 1
}
# Cargo's package arguments for the selected crates.
cargo_packages() {
    if [ "${crates[0]-}" = all ]; then
        echo --workspace
    else
        printf -- '-p %s ' "${crates[@]}"
    fi
}

code_checks() {
    step "spec/coven.md and spec/format.md against spec/WRITING.md"
    "$(command -v python3 || command -v python)" scripts/check-spec.py

    # Build the structural checker before the crates whose graph it validates (Appendix F.6).
    # It reads the whole workspace and runs in seconds, so every run includes it.
    step "owner-construction-check: Appendix F.1 dependencies, F.2 and F.3 capabilities and owners, F.5 conventions"
    scripts/cargo.sh run --quiet -p owner-construction-check -- .

    step "cargo deny check bans: the crates Appendix F.1 refuses"
    cargo deny check bans

    step "cargo fmt --check"
    cargo fmt --all --check

    if [ ${#crates[@]} = 0 ]; then
        return
    fi
    local packages
    packages=$(cargo_packages)

    step "cargo clippy --all-targets --all-features ($packages)"
    # shellcheck disable=SC2086
    scripts/cargo.sh clippy $packages --all-targets --all-features -- -D warnings

    step "cargo doc (broken links denied)"
    # shellcheck disable=SC2086
    RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D warnings" \
        scripts/cargo.sh doc $packages --no-deps --all-features

    # Default targets — every library and binary, no tests — with production
    # features only, so an item only test code or a test-only feature uses shows
    # up as dead. (`--lib` alone would skip the binaries, and fails outright while
    # the workspace holds no library.)
    step "every crate built without test code"
    # shellcheck disable=SC2086
    scripts/cargo.sh check $packages

    # nextest runs every test binary's tests side by side, each in its own
    # process; its set is cargo test's, less the doctests run after it.
    step "cargo nextest run --all-features"
    # shellcheck disable=SC2086
    scripts/cargo.sh nextest run $packages --all-features --no-fail-fast

    step "cargo test --doc --all-features"
    # shellcheck disable=SC2086
    scripts/cargo.sh test $packages --all-features --doc

    # A crate's features only ever add code, so the pass above already runs
    # everything its build without them would, except what that build alone
    # compiles. Those crates are tested again without their features: beside
    # the others when no member turns those features on, so only they rebuild;
    # otherwise on their own, since a dependent's feature list would turn them
    # back on.
    local passes crate tests built together=() filters=()
    passes=$(feature_off_tests)
    while read -r crate tests built; do
        [ -n "$crate" ] && crate_selected "$crate" || continue
        if [ "$built" = together ]; then
            together+=("$crate")
            if [ "$tests" = all ]; then
                filters+=("package($crate)")
            fi
            continue
        fi
        if [ "$tests" = all ]; then
            step "cargo nextest run -p $crate --no-default-features"
            scripts/cargo.sh nextest run -p "$crate" --no-default-features --no-fail-fast
        fi
        step "cargo test -p $crate --no-default-features --doc"
        scripts/cargo.sh test -p "$crate" --no-default-features --doc
    done <<< "$passes"
    if [ ${#together[@]} != 0 ]; then
        if [ ${#filters[@]} != 0 ]; then
            local filter
            filter=$(printf ' | %s' "${filters[@]}")
            step "cargo nextest run --no-default-features (${filter:3})"
            # shellcheck disable=SC2086
            scripts/cargo.sh nextest run $packages --no-default-features --no-fail-fast -E "${filter:3}"
        fi
        step "cargo test --doc --no-default-features (for ${together[*]})"
        # shellcheck disable=SC2086
        scripts/cargo.sh test $packages --no-default-features --doc
    fi
}

# The crates whose build without their features compiles something the build
# with them all does not, each followed by the tests that build changes, `all`,
# or `doc` when the only such code is a `cfg_attr(not(feature …), doc = …)`
# example; and then by `together` when no workspace member turns on any of its
# features, so a build of the whole workspace leaves them off, or `alone`. That
# code is a `not(…)` cfg naming a feature, a `cfg!` naming one, or a feature
# that turns on a feature of a dependency outside the workspace.
feature_off_tests() {
    cargo metadata --format-version 1 --no-deps | "$(command -v python3 || command -v python)" -c '
import json, os, re, sys
meta = json.load(sys.stdin)
members = {package["name"] for package in meta["packages"]}
for package in sorted(meta["packages"], key=lambda package: package["name"]):
    features = package["features"]
    if not features:
        continue
    code = examples = False
    for enabled in (entry for entries in features.values() for entry in entries):
        dependency = enabled.removeprefix("dep:").split("/")[0].rstrip("?")
        if dependency not in members and dependency not in features:
            code = True
    for directory, _, files in os.walk(os.path.dirname(package["manifest_path"])):
        for name in (name for name in files if name.endswith(".rs")):
            with open(os.path.join(directory, name), encoding="utf-8") as file:
                text = file.read()
            for negation in re.finditer(r"\bnot\(", text):
                depth, end = 1, negation.end()
                while depth and end < len(text):
                    depth += {"(": 1, ")": -1}.get(text[end], 0)
                    end += 1
                if "feature" not in text[negation.start():end]:
                    continue
                if re.search(r"cfg_attr\(\s*\Z", text[:negation.start()]) and re.match(r"\s*,\s*doc\s*=", text[end:]):
                    examples = True
                else:
                    code = True
            if re.search(r"cfg!\([^;]*feature", text):
                code = True
    if not (code or examples):
        continue
    forced = any(
        dependency["name"] == package["name"]
        and (dependency["features"] or (dependency["uses_default_features"] and features.get("default")))
        for dependent in meta["packages"]
        for dependency in dependent["dependencies"]
    )
    print(package["name"], "all" if code else "doc", "alone" if forced else "together")
' | tr -d '\r'
}

# A Lean proof, built from scratch in a full run and incrementally in a changed
# one: every module is imported by its root, so none can drop out of the build;
# no `sorry`, and no axiom beyond Lean's own.
lean_proof() {
    local what=$1 proof=$2 lib=$3 package=$4
    shift 4
    proof_selected "${proof#spec/proofs/}" || return 0
    step "Lean proof of $what"
    # Path dependencies are checked separately; preserve their runner binaries.
    if [ "$part" = changed ]; then
        (cd "$proof" && lake build "$@")
    else
        (cd "$proof" && lake clean "$package" && lake build "$@")
    fi
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

# The path of a proof package's runner. A package this run did not select is
# brought up to date first, so a differential never runs a stale runner.
runner() {
    local package=$1 exe=$2
    if ! proof_selected "$package"; then
        (cd "spec/proofs/$package" && lake build "$exe" > /dev/null)
    fi
    local path
    path="$(cd "spec/proofs/$package" && pwd)/.lake/build/bin/$exe"
    if [ -f "$path.exe" ]; then
        path="$path.exe"
    fi
    echo "$path"
}

# Run exactly the named ignored tests of one package: cargo flags, `--`, then
# full test paths, and `--nocapture` to show each one's output. They are built
# with the whole workspace, as the code checks build it, so a run that checks
# both, or restores CI's saved build, compiles them once. A test the package
# lacks would run nothing, so each one must be listed.
exact_ignored() {
    local package=$1 flags=() tests=() output=() filter listed test
    shift
    while [ "$1" != -- ]; do
        flags+=("$1")
        shift
    done
    shift
    for test in "$@"; do
        case $test in
            --nocapture) output=(--success-output immediate --failure-output immediate) ;;
            --*)
                echo "exact_ignored: unknown flag $test" >&2
                exit 2
                ;;
            *) tests+=("$test") ;;
        esac
    done
    filter="package($package) & ($(printf 'test(=%s) | ' "${tests[@]}"))"
    filter="${filter% | )})"
    listed=$(scripts/cargo.sh nextest list --workspace ${flags[@]+"${flags[@]}"} --run-ignored only \
        -E "$filter" --message-format oneline | tr -d '\r')
    for test in "${tests[@]}"; do
        if ! printf '%s\n' "$listed" | grep -qE " ${test//./\\.}\$"; then
            echo "the $package tests are missing $test" >&2
            exit 1
        fi
    done
    scripts/cargo.sh nextest run --workspace ${flags[@]+"${flags[@]}"} --run-ignored only -E "$filter" \
        --no-fail-fast ${output[@]+"${output[@]}"}
}

# Each differential runs when its model or the Rust crate it compares is selected.
proofs() {
    lean_proof "storage publication, files and clocks" spec/proofs/storage CovenStorage covenStorage
    lean_proof "the merge" spec/proofs/merge CovenMerge covenMerge
    lean_proof "queued originals and conversion" spec/proofs/queue CovenQueue covenQueue
    lean_proof "file IO, retries and wakes" spec/proofs/io CovenIO covenIO
    lean_proof "the store sequence and its JSON runner" spec/proofs/sequence CovenSequence covenSequence \
        CovenSequence sequenceRunner SequenceTests

    local runner
    if proof_selected sequence || crate_selected coven-merge; then
        runner=$(runner sequence sequenceRunner)
        step "Rust / Lean sequence causality"
        COVEN_SEQUENCE_LEAN="$runner" exact_ignored coven-merge --all-features -- --nocapture \
            sequence::tests::differential::sequence_past_matches_lean
    fi
    if proof_selected sequence || crate_selected coven-database; then
        runner=$(runner sequence sequenceRunner)
        step "Rust / Lean sequence settlement, first-slot claims and cut-off"
        COVEN_SEQUENCE_LEAN="$runner" exact_ignored coven-database --all-features -- --nocapture \
            claim::tests::sequence_settlement_matches_lean \
            sequence_apply::tests::sequence_rows_match_lean
    fi
    if proof_selected sequence || crate_selected coven-sync; then
        runner=$(runner sequence sequenceRunner)
        step "Rust / Lean sequence floor checks, authenticated settlement, administration and keys"
        COVEN_SEQUENCE_LEAN="$runner" exact_ignored coven-sync --all-features -- --nocapture \
            sequence_sync::tests::differential::sequence_floor_matches_lean \
            sequence_sync::sequence_reload::tests::sequence_blocked_floor_matches_lean \
            sequence_sync::sequence_retention::tests::sequence_audience_coverage_matches_lean \
            sequence_sync::sequence_retention::tests::sequence_daily_retention_matches_lean \
            sequence_sync::sequence_reload::operation::tests::sequence_reload_selection_matches_lean \
            sequence_sync::sequence_reload::operation::tests::sequence_queue_reset_matches_lean \
            sequence_sync::sequence_reload::operation::tests::sequence_queue_breaking_raise_matches_lean \
            sequence_sync::sequence_reload::operation::tests::sequence_queue_newly_readable_matches_lean \
            sequence_sync::sequence_reload::operation::tests::sequence_queue_self_replacement_matches_lean \
            sequence_sync::sequence_claim::tests::differential::sequence_kept_settlement_matches_lean \
            effects::tests::sequence_differential::sequence_admin_matches_lean \
            history::tests::sequence_key_selection_matches_lean \
            sequence_sync::sequence_operations::tests::sequence_authored_circle_permission_matches_lean \
            sequence_sync::sequence_keys::tests::key_custody_preparation_matches_lean \
            sequence_sync::sequence_access_keys::retry::tests::access_replacement_retries_and_undo_match_lean \
            sequence_sync::sequence_member_keys::tests::member_replacement_binding_matches_lean \
            sequence_sync::sequence_keys::tests::sequence_rotation_matches_lean
    fi

    if proof_selected merge || crate_selected coven-merge; then
        runner=$(runner merge mergeRunner)
        step "Rust / Lean merge and circle-entry differentials"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-merge --all-features -- \
            tests::differential::lean_differential \
            tests::differential::entry_differential_histories \
            tests::differential::entry_differential_agreement
        runner=$(runner merge receiveRunner)
        step "Rust / Lean data dependency"
        COVEN_RECEIVE_LEAN="$runner" exact_ignored coven-merge --all-features -- \
            dependency::tests::mg_differential_data_dependency
    fi

    if proof_selected merge || crate_selected coven-database; then
        runner=$(runner merge mergeRunner)
        step "Rust / Lean composite references, frozen losses and owner permissions"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            download::tests::streams::composite_reference_differential \
            migration_hidden::tests::freeze_differential_histories \
            migration_hidden::tests::freeze_differential_agreement \
            execution_capture::tests::lean::captured_actions_are_the_models_consequences \
            author_judgement::tests::local_admission_matches_the_model \
            judge_view::tests::receipt_matches_the_model \
            judge_view::tests::cascade_delete_old_matches_the_model \
            judge_view::tests::classification_matches_the_model
    fi

    if proof_selected storage || crate_selected coven-database; then
        runner=$(runner storage authorshipRunner)
        step "Rust / Lean authorship multiproofs"
        COVEN_AUTHORSHIP_LEAN="$runner" exact_ignored coven-database --all-features -- \
            authorship::tests::multiproofs_match_the_storage_model \
            authorship::tests::retained_openings_match_the_cell_proof_model
        runner=$(runner storage clockRunner)
        step "Rust / Lean hybrid timestamps"
        COVEN_CLOCK_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_encoding::tests::lean_differential_clocks
    fi

    if proof_selected queue || crate_selected coven-database; then
        runner=$(runner queue authoredRunner)
        step "Rust / Lean authored source evidence and data validity"
        COVEN_AUTHORED_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_sources::tests::authored_sources_differential
        runner=$(runner queue conversionRunner)
        step "Rust / Lean conversion across breaking changes and additions"
        COVEN_CONVERSION_LEAN="$runner" exact_ignored coven-database --all-features -- \
            migration_receive::tests::conversion_chain_matches_lean
    fi

    if proof_selected io || crate_selected coven-sync; then
        runner=$(runner io ioRunner)
        step "Rust / Lean downloads, kept pins, retained ranges, retry evidence and removed-device file listings"
        COVEN_IO_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            files::tests::cache::retries::io_differential_download \
            files::tests::keep::io_differential_keep \
            object_input::tests::io_differential_retained_ranges_and_refusal_eviction \
            retry_waits::tests::io_differential_waiting \
            sequence_sync::sequence_retry::tests::io_differential_waiting_evidence \
            sequence_sync::sequence_files::tests::io_differential_file_scan
    fi

    if proof_selected io || crate_selected coven-database; then
        runner=$(runner io ioRunner)
        step "Rust / Lean write-queue wakes"
        COVEN_IO_LEAN="$runner" exact_ignored coven-database --all-features -- \
            database_changes::tests::io_differential_wakes
    fi

    if proof_selected storage || crate_selected coven-sync; then
        runner=$(runner storage filesRunner)
        step "Rust / Lean file attachment, upload completion and status"
        COVEN_FILES_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            files::tests::uploads::differential::st_differential_files \
            files::tests::status::st_differential_file_status
    fi

    if proof_selected storage || crate_selected coven-storage; then
        runner=$(runner storage floorUploadRunner)
        step "Rust / Lean conditional floor uploads"
        COVEN_FLOOR_UPLOAD_LEAN="$runner" exact_ignored coven-storage --all-features -- \
            test_utils::memory_upload::tests::conditional_floor_upload_matches_lean
        runner=$(runner storage createRunner)
        step "Rust / Lean provider create-once tests"
        COVEN_CREATE_LEAN="$runner" exact_ignored coven-storage --all-features -- \
            providers::s3::tests::bucket_claim_tests::bucket_claims_match_lean \
            providers::s3::tests::gcs::lean_differential_s3_create \
            providers::onedrive::tests::upload_tests::lean_differential_onedrive_create \
            providers::onedrive::tests::upload_tests::lean_differential_onedrive_records_before_initiation
    fi
}

if [ "$part" = changed ]; then
    select_changed
else
    select_all
fi
if [ "$part" != proofs ]; then
    code_checks
fi
if [ "$part" != code ]; then
    proofs
fi

echo ""
echo "✅ all checks passed"
