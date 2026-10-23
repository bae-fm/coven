# Storage publication, files, clocks and authorship

The Lean package in [storage](storage/) covers create-once publication,
immutable bytes, conditional floor uploads, file lifetimes, timestamp ordering,
signed openings and provider key custody. The [sequence model](sequence.md) covers device-copy
detection, claim settlement, floor coverage, cut-off and snapshot selection.
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

## Conditional floor uploads

[FloorUpload.lean](storage/CovenStorage/FloorUpload.lean) models the floor as one
object with a revision. Each upload retains either an absent-path condition or
the revision it originally observed. Receiving parts cannot change that condition.
Incomplete or stale publication leaves the floor unchanged; successful publication
stores the whole object. Two uploads sharing one condition cannot both publish.

The model assumes invisible staging, atomic comparison at publication and a fresh
revision for new floor bytes. It does not prove a provider's implementation or
local journal persistence. `floorUploadRunner` compares interleaved production
storage calls, including serialized session restoration, with the model through
`COVEN_FLOOR_UPLOAD_LEAN`. Native request tests check the provider conditions;
sequence tests check that failed parts, uncertain replies and failed cleanup
retain the journal and prevent early slot deletion.

## Files

[Files.lean](storage/CovenStorage/Files.lean) distinguishes absence, damage,
read failure, availability and this device's own pending attachment.
`available_iff` and `wrong_size_is_damage` characterize status;
`absence_is_lost` shows that a confirmed absence at a path a row names is
missing, never uploading, and `uploading_only_here` and
`pending_needs_no_storage` that only the attaching device's own pending
attachment is uploading, judged with no storage request.
`read_absence_matches_status`, `read_agrees_with_status`,
`wrong_size_read_is_damage`, `past_end_is_damage`, `present_never_absent` and
`complete_object_reads` connect status with range reads. An error cannot become
an absent or valid object.

The attachment model follows one device: merged rows whose file cells carry
the step that last set them, the upload queue, pending attachments, abandoned
ones, what storage holds and the deletion candidates. `attach_changes_no_row`
shows the attaching write names no file. `completion_writes_current` shows a
completion writes exactly the queued file, which storage holds, when the row's
pending attachment is current; `stale_completion_writes_nothing` that it
writes nothing otherwise. `newer_change_wins` and `reattach_wins` show that
once this device sees a newer change to the row's file (its own write, a
write applied from another device, a delete, or another attachment), no
later step but attaching the same upload again lets that upload's completion
change a row. `completion_idempotent`, `completion_retires_queue` and
`unconfirmed_changes_nothing` show a completion happens once, in the
transaction that retires its upload, and that an unconfirmed attempt or a
crash before it changes nothing. Unequal bytes leave the queue intact and
refuse that file attempt; they do not request a device reset.

Two invariants hold over every step. `steps_named_stored`: every file a row
names is in storage, given that other devices' writes name only stored files.
`steps_reachable`: every stored path is named by a row, held by the upload
queue, or a deletion candidate, so an abandoned or retired upload's bytes stay
within §17.8's reach. `encrypted_chunk_is_captured` and `file_retries_fixed`
require verified original chunks before encryption and preserve retry
plaintext.

`filesRunner` supplies the file attachment, completion and status comparisons
in coven-sync through `COVEN_FILES_LEAN`. The model assumes truthful source
checks, authenticated chunks and atomic local transactions, and keeps lost
values and snapshots out: they only add protection, and deleting a candidate
still needs the evidence §17.8 requires. The removed-device folder scan is
proved in [IO](io.md#removed-device-file-scan).

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

## Checking

Run `lake build` in `spec/proofs/storage`. The root imports every retained
module. [Axioms.lean](storage/CovenStorage/Axioms.lean) audits their results;
`scripts/check.sh` rejects unfinished steps and any axioms beyond `propext`,
`Classical.choice` and `Quot.sound`, then runs the exact Rust comparisons above.
