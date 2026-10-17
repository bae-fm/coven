# Storage publication, files, clocks and authorship

The Lean package in [storage](storage/) covers create-once publication,
immutable bytes, file lifetimes, timestamp ordering, signed openings and
provider key custody. The [sequence model](sequence.md) covers device-copy
detection, claim settlement, floor replacement, cut-off and snapshot selection.
Neither package proves the binary encodings in `format.md`.

## Create-once publication

[Objects.lean](storage/CovenStorage/Objects.lean) models slots, snapshots and
files as paths with immutable contents. `create_preserves` and
`stored_history_preserved` retain occupied objects. `settled_iff_equal` requires
complete byte equality to settle a retry. `conversion_preserves_upload` keeps
tried and untried records unchanged; `first_attempt_keeps_original` preserves
the untried record through sealing. `retry_fixed` and
`retries_identical` keep the attempt unchanged. The nonce results assume the
modeled encoder and path assignment; they do not establish cipher security.

[CopyPublication.lean](storage/CovenStorage/CopyPublication.lean) models a
private staged upload copied atomically to a shared destination. Private
staging and cleanup do not publish shared objects. `race_one_winner` and
`race_occupied` establish create-once behavior under the model's conditional
copy primitive. `lost_reply_settles` and `marker_settles_bytes` use the fixed
attempt and destination evidence to settle a lost reply.

`check_then_write_two_winners` shows why a separate existence check cannot
supply that atomicity. `put_racing_copy_two_winners` shows why racing a direct
write against the copy is insufficient. `every_copy_at_most_one` and
`copied_at_most` cover one destination per copy.

The `Recorded` model journals private temporary objects. `staged_recorded`,
`retry_clears` and `failed_deletion_retry_accepted` cover retry after staging
or cleanup failures. `attempt_naming_strands` demonstrates why naming a
private object by a later attempt can strand an earlier upload.
`shared_never_fails`, `shared_occupied_identical`, `attemptCopy_retry_empty`,
`retry_publishes`, `shared_temporary_missing` and `shared_temporary_both_stored`
state the shared/private publication outcomes cited in Appendix E.10.

Atomic conditional copy, truthful reads and provider replies are assumptions.
`createRunner` compares these model operations with the S3 and OneDrive
request-level tests through `COVEN_CREATE_LEAN`. Provider certification and
live races are separate from these proofs.

## Files

[Files.lean](storage/CovenStorage/Files.lean) distinguishes absence, damage,
read failure, availability and active upload. `available_iff` and
`wrong_size_is_damage` characterize status; `read_absence_matches_status`,
`read_agrees_with_status`, `wrong_size_read_is_damage`, `past_end_is_damage`,
`present_never_absent` and `complete_object_reads` connect status with range
reads. An error cannot become an absent or valid object.

The upload owns its captured reference and source independently of the row.
`steps_keep_upload` and `inflight_upload_settles` show that deleting or
retargeting the row leaves that upload intact until confirmation. Completion
changes no row. `completion_retires_both` and `completion_idempotent` cover
removing upload work and its pending record together. Unequal bytes leave the
queue intact and refuse that file attempt; they do not request a device reset.
`encrypted_chunk_is_captured` and `file_retries_fixed` require
verified original chunks before encryption and preserve retry plaintext.

`filesRunner` supplies the file attachment, completion and status comparisons
in coven-sync through `COVEN_FILES_LEAN`. The model assumes truthful source
checks, authenticated chunks and atomic local transactions. The removed-device
folder scan is proved in [IO](io.md#removed-device-file-scan); reference-based
file deletion also needs the evidence required by §17.8.

## Hybrid timestamps and retry time

[Clocks.lean](storage/CovenStorage/Clocks.lean) orders a local timestamp after
all observed timestamps without waiting for the wall clock. `stamp_after_latest`,
`stamp_after_every_read`, `different_devices_distinct` and
`stamp_after_snapshot` provide the causal and uniqueness properties used by
the merge. `representable_stamp_exists` states the bounded-counter premise;
the model does not allow arithmetic overflow to become a timestamp.

Storage ages and retry delays are distinct from authored timestamps.
`age_never_early`, `wall_jump_changes_no_age_or_retry` and
`retry_delay_bounded` describe the supplied storage/monotonic clock model.
Persisted wait reopening is covered in [IO](io.md#waiting-and-reopening).
`clockRunner` compares timestamp generation with
`write_encoding::tests::lean_differential_clocks` via `COVEN_CLOCK_LEAN`.

## Signed roots and row authorship

[RowAuthorship.lean](storage/CovenStorage/RowAuthorship.lean) models signed
roots and openings of original row changes. `revealed_authentic`,
`snapshot_authorship`, `owned_row_true_author` and `cell_true_author` connect
accepted openings to their original author and values. `multiproof_complete`,
`prune_covers`, `multiproof_minimal` and `hashes_le_absent` cover retained
openings and hashes needed for absent subtrees.

[CellProof.lean](storage/CovenStorage/CellProof.lean) separates creation
identity from per-cell values. `creation_complete`, `cell_complete`,
`every_leaf_openable` and `overwritten_not_opened` allow an original creation
and retained cells to be verified without opening overwritten scalar values.
The hash and signature properties are assumptions, not cryptographic proofs.
Snapshot freshness and owner selection belong to the sequence model;
[merge B18](merge.md#b18-owner-tables-and-row-authenticity) uses authenticated
creation identity for permissions.

`authorshipRunner` compares Rust's multiproofs and retained cell openings with
these functions through `COVEN_AUTHORSHIP_LEAN`. The database tests are
`multiproofs_match_the_storage_model` and
`retained_openings_match_the_cell_proof_model`.

## Provider key custody

[KeyDelivery.lean](storage/CovenStorage/KeyDelivery.lean) models minting,
recording and copying or losing a database. `run_keeps_provider` proves these
steps retain every previously minted key. This result covers mint isolation;
delivery, claim settlement, operation undo and provider deletion are outside
this model.

[KeyListing.lean](storage/CovenStorage/KeyListing.lean) retains the cited
`clone_mint_spares_recorded_key` example: another copy mints a fresh principal
without replacing the original copy's recorded key. This package does not
prove the current sequence's provider-key reclaim scheduling.

[AdminAccess.lean](storage/CovenStorage/AdminAccess.lean) models supplied
administrative access. `released_makes_nothing` and `check_keeps_admin` state
that released credentials mint nothing and a successful check retains the
supplied administrator. Provider credential validity is an input.

## Checking

Run `lake build` in `spec/proofs/storage`. The root imports every retained
module. [Axioms.lean](storage/CovenStorage/Axioms.lean) audits their results;
`scripts/check.sh` rejects unfinished steps and any axioms beyond `propext`,
`Classical.choice` and `Quot.sound`, then runs the exact Rust comparisons above.
