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
read failure, availability, a file on this device only while it uploads, and a
file on another device only. `available_iff` and `wrong_size_is_damage`
characterize status; `absence_is_lost` shows that a confirmed absence at a
stored file's path is missing, never uploading, and `uploading_only_here`,
`device_needs_no_storage` and `device_only_elsewhere` that only a file on this
device only is uploading, and that a file on one device only is judged with no
storage request. `read_absence_matches_status`, `read_agrees_with_status`,
`wrong_size_read_is_damage`, `past_end_is_damage`, `present_never_absent` and
`complete_object_reads` connect status with range reads. An error cannot become
an absent or valid object.

The file model follows one device: the rows' references to files, its file
records with their coordinates, the upload queue with the files each upload
stores, what storage holds, the stored paths named by records its unpublished
writes deleted, and the deletion candidates. `stage_writes_no_cloud` shows the
staging write records no file as stored, and `stage_local_writes_no_cloud`
that staging a file on this device only, with no upload, doesn't either. This
model has one upload per switch, as a move of one file has.
`completion_stores_waiting` shows a completion switches each file still
waiting for the upload to the stored location, which storage holds, in one
write; `stale_completion_writes_nothing`
that an upload no file waits for switches nothing. `shared_file_stays` shows a
record stays while another row references it, `last_reference_deletes` that
the write removing the last reference deletes it, and
`replaced_upload_writes_nothing` that the upload of a file a write left with no
reference then writes nothing. `late_reference_taken_out` shows another
device's late reference to a deleted record references nothing.
`completion_idempotent`, `completion_retires_queue` and
`unconfirmed_changes_nothing` show a completion happens once, in the
transaction that retires its upload, and that an unconfirmed attempt or a crash
before it changes nothing. Unequal bytes leave the queue intact and refuse that
file attempt; they do not request a device reset.

[Moves.lean](storage/CovenStorage/Moves.lean) models moves of several
files: each file waits for at most one move through its upload, storing an
upload marks it stored, and a move switches every file still waiting for it,
in one step, only once every one is stored (`switch_needs_every_upload`). A
move that fails or is cancelled switches none, and a file whose last
reference goes drops out. `move_whole` shows that from a state with no moves,
in every reachable state, a move's files are all in the cloud or none is.

[Keep.lean](storage/CovenStorage/Keep.lean) models files kept here (§17.7):
the write switching a move's files records a pin request for each one kept
here, a made pin ends its request, and a file's record going ends it too.
`requested_by_switch` shows a request appears only in the write switching its
file to the cloud, for a file kept here, and `unkept_unrequested` that a file
not kept here gets none. `request_ends_made_or_gone` shows a request ends only
once its pin is made or its file's record is gone: a failed attempt and the
app's own pins and unpins leave it waiting.

`change_keeps_candidates` and `settle_keeps_candidates` show that no write of
this device makes stored bytes a deletion candidate: a deleted record's
coordinates stay with its unpublished write, and `publish_releases` shows
publishing it releases exactly them.

Two invariants hold over every step, other devices' writes included.
`steps_cloud_stored`: every file whose coordinates say cloud is in storage,
given that other devices' writes name only stored bytes as stored.
`steps_reachable`: every stored path is named by a record, held by the upload
queue, kept by an unpublished write of this device, or a deletion candidate, so
a retired upload's bytes and a deleted record's stay within §17.9's reach.
`encrypted_chunk_is_captured` and `file_retries_fixed` require verified
original chunks before encryption and preserve retry plaintext.

`filesRunner` supplies the file staging, completion and status comparisons in
coven-sync through `COVEN_FILES_LEAN`. The model assumes truthful source
checks, authenticated chunks and atomic local transactions, and keeps lost
values and snapshots out: they only add protection, and deleting a candidate
still needs the evidence §17.9 requires. The removed-device folder scan is
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
