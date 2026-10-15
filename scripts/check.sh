#!/bin/bash
# Every check of §20.4, in order. CI runs the code checks on every platform
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

step() { echo ""; echo "── $1"; }

# The workspace crates and proof packages this run checks. A full run checks
# all of them; `changed` selects from the files that differ from its base.
crates=()
proof_packages=()
select_all() {
    crates=(all)
    proof_packages=(merge storelog storelog-data storage queue io)
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
    for package in merge storelog storelog-data storage queue io; do
        if printf '%s\n' "$files" | grep -q "^spec/proofs/$package/"; then
            proof_packages+=("$package")
        fi
    done
    while [ "$changed" = 1 ]; do
        changed=0
        for package in merge storelog storelog-data storage queue io; do
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
    # Build the structural checker before the crates whose graph it validates (§20.4).
    # It reads the whole workspace and runs in seconds, so every run includes it.
    step "owner-construction-check: §20.1 dependencies, §20.2 capabilities and owners, §20.3 conventions"
    cargo run --quiet -p owner-construction-check -- .

    step "cargo fmt --check"
    cargo fmt --all --check

    if [ ${#crates[@]} = 0 ]; then
        return
    fi
    local packages
    packages=$(cargo_packages)

    step "cargo clippy --all-targets --all-features ($packages)"
    # shellcheck disable=SC2086
    cargo clippy $packages --all-targets --all-features -- -D warnings

    step "cargo doc (broken links denied)"
    # shellcheck disable=SC2086
    RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D warnings" \
        cargo doc $packages --no-deps --all-features

    # Default targets — every library and binary, no tests — with production
    # features only, so an item only test code or a test-only feature uses shows
    # up as dead. (`--lib` alone would skip the binaries, and fails outright while
    # the workspace holds no library.)
    step "every crate built without test code"
    # shellcheck disable=SC2086
    cargo check $packages

    step "cargo test --all-features"
    # shellcheck disable=SC2086
    cargo test $packages --all-features

    step "cargo test --no-default-features"
    # shellcheck disable=SC2086
    cargo test $packages --no-default-features

    if crate_selected coven-sync; then
        step "release store-log replay cost (2,000 entries)"
        exact_ignored coven-sync --release -- --nocapture replay::tests::replay_cost \
            replay_cache::tests::read_view_replay_cost
    fi
}

# A Lean proof, built from scratch in a full run and incrementally in a changed
# one: every module is imported by its root, so none can drop out of the build;
# no `sorry`, and no axiom beyond Lean's own.
lean_proof() {
    local what=$1 proof=$2 lib=$3 package=$4
    proof_selected "${proof#spec/proofs/}" || return 0
    step "Lean proof of $what"
    # Path dependencies are checked separately; preserve their runner binaries.
    if [ "$part" = changed ]; then
        (cd "$proof" && lake build)
    else
        (cd "$proof" && lake clean "$package" && lake build)
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
# full test paths and test-harness flags. A bare filter would also run every
# longer name containing it, which can need another runner; an --exact name
# the binary lacks would run nothing, so each one must be listed.
exact_ignored() {
    local package=$1 flags=() listed test
    shift
    while [ "$1" != -- ]; do
        flags+=("$1")
        shift
    done
    shift
    listed=$(cargo test -p "$package" ${flags[@]+"${flags[@]}"} -- --list --ignored | tr -d '\r')
    for test in "$@"; do
        case $test in --*) continue ;; esac
        if ! printf '%s\n' "$listed" | grep -qxF "$test: test"; then
            echo "the $package test binary is missing $test" >&2
            exit 1
        fi
    done
    cargo test -p "$package" ${flags[@]+"${flags[@]}"} -- --ignored --exact "$@"
}

# Merge, replay, access visibility, clocks and create-once uploads have Rust differential tests.
# Data coupling also has checked counterexamples (spec/proofs/storelog-data.md).
# Each runs when its Lean package or the Rust crate it compares is selected.
proofs() {
    lean_proof "the merge" spec/proofs/merge CovenMerge covenMerge
    lean_proof "the store log" spec/proofs/storelog CovenStorelog covenStorelog
    lean_proof "store-log data effects" spec/proofs/storelog-data CovenStorelogData covenStorelogData
    lean_proof "storage identity, files and clocks" spec/proofs/storage CovenStorage covenStorage
    lean_proof "device upload queues" spec/proofs/queue CovenQueue covenQueue
    lean_proof "sync IO and discovery" spec/proofs/io CovenIO covenIO

    local runner
    if proof_selected merge || crate_selected coven-merge; then
        runner=$(runner merge mergeRunner)
        step "Rust / Lean Boolean merge differential"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-merge --all-features -- \
            tests::differential::lean_differential

        step "Rust / Lean circle-entry differential"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-merge --all-features -- \
            tests::differential::entry_differential_histories \
            tests::differential::entry_differential_agreement
    fi

    if proof_selected merge || crate_selected coven-database; then
        runner=$(runner merge mergeRunner)
        step "Rust / Lean composite reference differential"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            download::tests::streams::composite_reference_differential

        step "Rust / Lean reset differential"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_boundary::tests::differential::reset_differential_histories \
            write_boundary::tests::differential::reset_differential_agreement

        step "Rust / Lean schema-boundary differential"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            snapshot_writes::tests::differential::schema_differential_histories

        step "Rust / Lean migration selection"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            migration_state::tests::migration_differential_histories \
            migration_state::tests::migration_differential_agreement

        step "Rust / Lean frozen losses under dropped schema"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            migration_hidden::tests::freeze_differential_histories \
            migration_hidden::tests::freeze_differential_agreement

        step "Rust / Lean owner creation, foreign-key effects, local admission and receipt"
        COVEN_MERGE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            ownership::tests::creation_differential_histories \
            execution_capture::tests::lean::captured_actions_are_the_models_consequences \
            owner_permission::tests::local_admission_matches_the_model \
            judge_view::tests::receipt_matches_the_model
    fi

    if proof_selected storage || crate_selected coven-database; then
        runner=$(runner storage authorshipRunner)
        step "Rust / Lean authorship multiproofs"
        COVEN_AUTHORSHIP_LEAN="$runner" exact_ignored coven-database --all-features -- \
            authorship::tests::multiproofs_match_the_storage_model \
            authorship::tests::retained_openings_match_the_cell_proof_model
    fi

    if proof_selected merge || proof_selected queue || crate_selected coven-database; then
        runner=$(runner queue authoredRunner)
        step "Rust / Lean authored source evidence and data validity"
        COVEN_AUTHORED_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_sources::tests::authored_sources_differential

        runner=$(runner queue queueRunner)
        step "Rust / Lean queue change verdicts and row merge"
        COVEN_QUEUE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            interpretation::tests::queue_reset_differential_histories \
            interpretation::tests::queue_conversion_differential_histories \
            interpretation::tests::retire::queue_retirement_differential_histories
    fi

    if proof_selected queue || crate_selected coven-sync; then
        runner=$(runner queue queueRunner)
        step "Rust / Lean upload queue and raise publication"
        COVEN_QUEUE_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            device_log_sync::tests::schema::raise_publication::differential::queue_raise_differential_histories
    fi

    if proof_selected storelog || crate_selected coven-sync; then
        runner=$(runner storelog storelogRunner)
        step "Rust / Lean CurrentReplay agreement"
        COVEN_STORELOG_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            tests::lean_differential_generated \
            tests::lean_differential_checked_examples \
            tests::lean_differential_circle_replay \
            tests::lean_differential_online_administration \
            replay::landing::tests::lean_differential_storage_admission \
            tests::lean_differential_rejects_invalid_inputs \
            store_log_sync::store_log_batch::tests::lean_differential_batch_partitions \
            key_introductions::tests::lean_differential_introductions \
            key_introductions::tests::lean_differential_accepted_copies \
            key_introductions::tests::lean_differential_first_attempts \
            finality::tests::lean_differential_finality_horizon
    fi

    if proof_selected storelog-data || crate_selected coven-sync; then
        runner=$(runner storelog-data accessRunner)
        step "Rust / Lean serialized provider access and pending visibility"
        COVEN_ACCESS_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::tests::operations::access::lean_differential_access_phases \
            store_log_sync::tests::operations::pending_records::lean_differential_access_pending \
            store_log_sync::tests::operations::pending_records::lean_differential_access_rejects_invalid_inputs
    fi

    if proof_selected storelog-data || crate_selected coven-sync; then
        runner=$(runner storelog-data rotationRunner)
        step "Rust / Lean durable rotation and first-send phases"
        COVEN_ROTATION_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::key_rotation::tests::lean_differential_rotation_phases
    fi

    if proof_selected storelog-data || crate_selected coven-sync; then
        runner=$(runner storelog-data memberKeyRunner)
        step "Rust / Lean stable member ids, entry signers and copy credentials"
        COVEN_MEMBER_KEY_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            entry_signature::tests::lean_differential_member_keys
        step "Rust / Lean member-key replacements, old-key write windows, copy credentials, joins and calls"
        COVEN_MEMBER_KEY_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            object_author::tests::lean_differential_member_key_replacements \
            store_log_sync::tests::member_credentials::lean_differential_member_key_credentials \
            store_log_sync::tests::operations::join_admission::lean_differential_join_admission \
            store_log_sync::tests::operations::member_keys_replacement::lean_differential_member_key_calls
    fi

    if proof_selected storelog-data || crate_selected coven-sync; then
        runner=$(runner storelog-data accessKeyRunner)
        step "Rust / Lean S3 key deletion journal"
        COVEN_ACCESS_KEY_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            access_keys::tests::lean_differential_access_keys
    fi

    if proof_selected storelog-data || crate_selected coven-sync || crate_selected coven-database; then
        runner=$(runner storelog-data storelogDataRunner)
        step "Rust / Lean replay-selected boundaries"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::tests::snapshots::boundaries::data_differential_boundaries
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-database --all-features -- \
            entry_inputs::tests::data_differential_entry_effects \
            entry_inputs::tests::data_differential_entry_effects_circles \
            file_write::sources::tests::data_differential_local_sources
        step "Rust / Lean circles readable again"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::snapshots::reload::tests::data_differential_circle_returns
        step "Rust / Lean first unmet condition per subject"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            pending::tests::data_differential_pending_records
        step "Rust / Lean reload choice and wait records"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::snapshots::reload::tests::data_differential_reload_choice
        step "Rust / Lean uploaded-file deletion"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            files::tests::retention::data_differential_file_deletion
        step "Rust / Lean snapshot pruning from the catalog"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::tests::snapshots::selection::data_differential_snapshot_pruning
        step "Rust / Lean retention records and deletion"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::tests::snapshots::retention_waits::data_differential_retention_record
        step "Rust / Lean which entries a deletion depends on"
        COVEN_STORELOG_DATA_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            store_log_sync::tests::snapshots::retention_dependencies::data_differential_retention_dependencies
    fi

    if proof_selected io || crate_selected coven-sync; then
        runner=$(runner io finalityRunner)
        step "Rust / Lean finality observation endpoint"
        COVEN_IO_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            finality::tests::io_differential_finality_observation \
            finality::tests::io_differential_finality_requires_the_old_prefix_even_when_the_window_is_quiet \
            store_log_sync::store_log_finality::tests::listing::io_differential_finality_discovery_receipts
        runner=$(runner io ioRunner)
        step "Rust / Lean numbered discovery, snapshot selection and recent returns"
        COVEN_IO_RUNNER="$runner" exact_ignored coven-sync --all-features -- \
            discovery::tests::io_differential_discovery \
            store_log_sync::tests::snapshots::selection::io_differential_snapshot_counts \
            store_log_sync::tests::snapshots::selection::io_differential_catalog_current \
            store_log_sync::tests::snapshots::retention_waits::io_differential_deletable \
            store_log_sync::tests::snapshots::retention_waits::io_differential_refresh_due \
            store_log_sync::tests::snapshots::witnesses::io_differential_retain_writes \
            store_log_sync::tests::snapshots::witnesses::io_differential_storage_deadline \
            recent_return::tests::io_differential_needs_snapshots \
            recent_return::tests::io_differential_recent_return \
            device_log_sync::tests::validation::io_differential_streaming_download_dispositions \
            device_log_sync::positions::tests::io_differential_positions \
            object_input::tests::io_differential_retained_ranges_and_refusal_eviction \
            send_gate::tests::io_differential_send_gate \
            device_log_sync::drain::tests::io_differential_retired_drain \
            operations::pass::tests::io_differential_pass_phases \
            device_log_sync::tests::schema::raise_publication::differential::io_differential_snapshot_writes \
            store_log_sync::tests::snapshots::publication::io_differential_publication \
            store_log_sync::tests::snapshots::publication::io_differential_ordinary_snapshot_writes \
            store_log_sync::tests::snapshots::held::io_differential_held \
            store_log_sync::tests::snapshots::held::io_differential_boundary_select \
            store_log_sync::tests::snapshots::common_point::io_differential_common_point \
            retry_waits::tests::io_differential_waiting \
            store_log_sync::tests::operations::evidence::io_differential_waiting_evidence \
            store_log_sync::tests::operations::evidence::io_differential_wait_rows \
            files::tests::upload_retry::io_differential_upload_worker \
            store_log_sync::snapshots::file_scan::tests::io_differential_file_scan \
            store_log_sync::snapshots::file_scan::tests::io_differential_folder_restore \
            write_object::tests::io_differential_dropped_registration \
            write_object::tests::io_differential_dropped_registration_retires \
            store_log_landing::tests::io_differential_late_registration_retires
    fi

    if proof_selected io || crate_selected coven-database; then
        runner=$(runner io ioRunner)
        step "Rust / Lean write-queue wakes and prerequisite rows"
        COVEN_IO_RUNNER="$runner" exact_ignored coven-database --all-features -- \
            database_changes::tests::io_differential_wakes \
            prerequisite_rows::tests::io_differential_prerequisite_rows
    fi

    if proof_selected merge || crate_selected coven-sync; then
        runner=$(runner merge receiveRunner)
        step "Rust / Lean send-gate publication bound"
        COVEN_RECEIVE_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            send_gate::tests::mg_differential_send_gate_publication_bound
    fi

    if proof_selected storage || crate_selected coven-database; then
        runner=$(runner storage clockRunner)
        step "Rust / Lean differential clock test"
        COVEN_CLOCK_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_encoding::tests::lean_differential_clocks
    fi

    if proof_selected storage || crate_selected coven-sync; then
        runner=$(runner storage identityRunner)
        step "Rust / Lean identity check and replacement read"
        COVEN_IDENTITY_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            identity::tests::st_differential_identity \
            identity::tests::st_differential_replacement_read \
            identity::tests::st_differential_replacement_admission \
            store_log_sync::store_log_identity::tests::st_differential_snapshot_sequence
        runner=$(runner storage filesRunner)
        step "Rust / Lean file attachment, upload completion and status"
        COVEN_FILES_LEAN="$runner" exact_ignored coven-sync --all-features -- \
            files::tests::uploads::differential::st_differential_files \
            files::tests::status::st_differential_file_status
    fi

    if proof_selected storage || crate_selected coven; then
        runner=$(runner storage identityRunner)
        step "Rust / Lean device reset boundary"
        COVEN_IDENTITY_LEAN="$runner" exact_ignored coven --all-features -- \
            builder::bootstrap::reset::tests::st_differential_reset_boundary
    fi

    if proof_selected storage || crate_selected coven-storage; then
        runner=$(runner storage createRunner)
        step "Rust / Lean differential provider create-once tests"
        COVEN_CREATE_LEAN="$runner" exact_ignored coven-storage --all-features -- \
            providers::s3::tests::gcs::lean_differential_s3_create \
            providers::onedrive::tests::upload_tests::lean_differential_onedrive_create
    fi

    if proof_selected merge || crate_selected coven-database; then
        runner=$(runner merge receiveRunner)
        step "Rust / Lean differential causal receive test"
        COVEN_RECEIVE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            download::tests::lean_differential_receive
        step "Rust / Lean retirement window"
        COVEN_RECEIVE_LEAN="$runner" exact_ignored coven-database --all-features -- \
            write_landing::tests::mg_differential_retirement_window
        step "Rust / Lean data dependency"
        COVEN_RECEIVE_LEAN="$runner" exact_ignored coven-merge -- \
            dependency::tests::mg_differential_data_dependency
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
