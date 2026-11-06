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

[LocationClaim.lean](storage/CovenStorage/LocationClaim.lean) models the
location marker of a store that needs its location to itself: a Dropbox shared
folder, a OneDrive folder, or an S3 bucket (coven.md §20.3). Setup creates the marker with the same create as a slot and
passes only when the marker names its store; no listing appears in it.
`at_most_one_store` shows that two setups that both pass, with any creates
between them, name the same store. `retry_passes` lets a retry of one store's
setup pass, and `reconnect_refuses_other` refuses another store at a claimed
location. `createRunner` compares setup and reconnect with the S3, Dropbox and
OneDrive adapters' request-level tests.

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

Each move also lists its files, which `coven_moves` shows the app. The state
gives each file at most the one move listing it, so a file is in one move at a
time: recording a move or joining one takes only a file no move lists, a
failed one included. The step recording a move lists its files, and a file
that comes to wait for a moving move, as a new file over a waiting file's
bytes does, joins it. A failure keeps the listing, a cancel or a switch ends
it, and a file whose last reference goes leaves it. `list_while_live` shows
that from a state with no moves, in every reachable state, only a moving or
failed move lists files, and every file waiting for a moving move is listed
under it. `fail_keeps_list` shows the step failing a move keeps every listing
and leaves no file waiting for it, so a failed move lists the files the app
can cancel it for.

[Shared.lean](storage/CovenStorage/Shared.lean) adds to those steps a
write making a new file over an existing file's bytes, as `existing_file`
stages one for a row in another audience and as a moved row gets one: a
record of its own with the same size, content hash and coordinates, waiting
for the same upload if one still stores them. It also adds deleting stored
bytes, which happens only to a deletion candidate that no record names, no
upload holds and no unpublished write keeps. `steps_cloud_stored` and
`steps_reachable` show both invariants above hold with these steps too, and
`purge_unnamed` that stored bytes are deleted only when no record names them.
`bytes_stay_while_either_record_names_them` follows photo A and a file over
it: deleting A's record leaves the bytes for the other, and deleting both
lets them go.

[Seen.lean](storage/CovenStorage/Seen.lean) models what the uploader can count
on from other devices before deleting a candidate (§17.10). Each other device
has its writes in the order it made them, each with the slot it had read
through and whether it names the bytes, a published prefix, the slot that
added it, and whether an entry stopped it for good (§14.1). Each write reads
at least as far as the one before, and through the slot before the one that
added its device. A device has seen a slot when it published a write that read
through it, was added after it, or is gone for good. `seen_unpublished` shows
that every write a device not gone for good that has seen the slot can still
send read through it, and `missed_published` that every write of another such
device that missed the change is then published, where the reference evidence
finds it.
`unseen_copy_unpublished` follows Ben's copy that missed Ana's delete: it names
the bytes, unpublished, while nothing published does, which is why the rule
exists. `next_write_publishes_copy` shows Ben's next write sending it, and
`gone_holds_nothing` that a device gone for good holds nothing back. The model
leaves out where a write's names come from: that a write made after reading
the change names the bytes only through a record the evidence still sees is
argued in §17.10, not proved.

[Holds.lean](storage/CovenStorage/Holds.lean) models how bound uploads hold
one device's writes (§17.4). Writes are numbered as they commit, and a pass
publishes the largest prefix it may: one that ends before every queued bound
upload's first held write, and at or before a captured raise's marker. A
write staging a bound file queues an upload holding from its own write, or
from the first write a queued bound upload holds, and the write completing an
upload, or dropping its file's last reference, ends it.
`held_unpublished` shows that while a bound upload is queued, no write from
the one staging its file on is published. `held_with_completion` shows a pass
publishing a held write publishes the write that ended its upload in the
same prefix, unless a raise's marker falls between them: a held write and its
completion share a slot. The raise is published only once every write through
its marker is (§18.5). With a marker between them, `raise_waits_for_held`
shows the raise can't be published while the held write's upload is queued,
`held_with_marker` that a pass publishing the held write publishes through
the marker, and `completion_after_raise` that the completion isn't published
while the raise waits: the held write and the marker go out together, then
the raise, then the completion. The model takes the queue's order and a
slot's contents as §6.3 gives them; reading the caps and the queue in one
committed state is the code's.

[Keep.lean](storage/CovenStorage/Keep.lean) models files kept here (§17.8):
the write switching a move's files records a pin request for each one kept
here, a made pin ends its request, and the app's unpin of the file ends it
too, as do a file's record going and a switch taking the file off its stored
bytes. `requested_by_switch` shows a request appears only in the write
switching its file to the cloud, for a file kept here, and
`unkept_unrequested` that a file not kept here gets none.
`unpin_ends_request` shows the app's unpin ends the request with the pin, so
no pin of coven's follows unless a switch requests it again.
`request_ends_made_unpinned_or_gone` shows a request ends only once its pin
is made, the app has unpinned the file, its file's record is gone or the file
has left the cloud: a failed attempt and the app's own pins leave it
waiting.

The file model's `makeLocal` step is this device's one write making cloud
files device-only here (§17.3), with the app's own change in it when the
switch is a step of the app's write, and `leave` another device's published
one. `make_local_whole` shows the switch alone changes every file it names
to this device, or nothing, and `make_local_keeps_candidates` that it makes
no path a candidate: the stored paths it takes the files off stay with its
unpublished write, as a delete's do. `make_local_with_whole` and
`make_local_with_keeps_candidates` show the same of the write carrying the
app's change, which is refused whole when that change removes a switched
file's last reference; both still keep every cloud file stored and every
stored path within reach.
[DeviceOnly.lean](storage/CovenStorage/DeviceOnly.lean) models the copies
such a call writes into the person's folder and the folders it makes for
them: recorded, with which copy lies in which folder, before any of them
exist; the copies reserved while the call runs; placed, kept together by the
switch, lost to another device's switch, or removed, by the call's own
abandon or by a later write or opening once no running call reserves them.
`removal_takes_unchanged` shows removing a copy coven placed, as it left it,
leaves nothing at its path; `person_files_stay` and
`abandon_person_files_stay` that no step of coven's takes away a file the
person wrote or changed; `kept_stays` and `abandon_kept_stays` that none
takes a kept copy; and `switch_whole` that a call's switch keeps every one
of its copies, each placed before it. `made_only_by_mkdir` shows a folder
counts as made only in the step whose mkdir found nothing at its path, and
`folder_removal_made_empty` that only an empty made folder no recorded copy
needs is ever taken away; so a folder that was already there stays
(`found_folder_stays`), and so does one a recorded copy lies in
(`needed_folder_stays`). `reserved_stays` shows that while a call reserves a
copy it hasn't switched, no step but its own abandon takes the copy's
record, its file or its folder links, and `released_removable` that once the
reservation drops, the next write can take it. The person's own changes to
files and folders between steps are left free, so a folder the person puts
in place of the call's own after its mkdir counts as made, and goes if
empty.

`change_keeps_candidates` and `settle_keeps_candidates` show that no write of
this device makes stored bytes a deletion candidate: a deleted record's
coordinates stay with its unpublished write, and `publish_releases` shows
publishing it releases exactly them.

Two invariants hold over every step, other devices' writes included.
`steps_cloud_stored`: every file whose coordinates say cloud is in storage,
given that other devices' writes name only stored bytes as stored.
`steps_reachable`: every stored path is named by a record, held by the upload
queue, kept by an unpublished write of this device, or a deletion candidate, so
a retired upload's bytes and a deleted record's stay within §17.10's reach.
`encrypted_chunk_is_captured` and `file_retries_fixed` require verified
original chunks before encryption and preserve retry plaintext.

`filesRunner` supplies the file staging, completion and status comparisons in
coven-sync through `COVEN_FILES_LEAN`. The model assumes truthful source
checks, authenticated chunks and atomic local transactions, and keeps lost
values and snapshots out: they only add protection, and deleting a candidate
still needs the evidence §17.10 requires. The removed-device folder scan is
proved in [IO](io.md#removed-device-file-scan).

## Upload sessions

[UploadSession.lean](storage/CovenStorage/UploadSession.lean) models one queued
upload going up in parts (coven.md §17.9): the provider's live session, with the
parts it holds in the order they arrived, and the published object; the queue
row, with whether a file still waits for it, the confirmed parts of the session
it records, its pending failure and what that failure waits for; and the attempt
this process is running, in memory only. A part is its index. The steps are the
upload worker's: start an attempt when the upload is due, resume the recorded
session or begin one, record it, send a part, publish, complete, fail with a
class, crash, abort and forget the session of an upload no file waits for, and
retire one that records none. The provider may lose a part's reply, drop a part,
expire its session or lose the completion reply.

`steps_good` carries one invariant through every step. The provider's session
holds parts `0 … n-1` in order (`held_nodup`), and the published object is
every part once, in order (`published_whole`), after any crashes, failed
attempts and expired sessions. The part an attempt sends next is the first the
provider lacks (`next_part_new`). The recorded parts are always ones the
provider holds, so the progress `uploads()` and `coven_uploads` show, which is
the recorded session's, is never ahead of them (`progress_never_ahead`). A
crash forgets only memory (`crash_keeps`); after it, a session the provider
still has continues from the parts it holds (`resume_continues`), and one it
expired starts over from its first part (`expired_starts_over`). No step
changes the location, key and chunk hashes the staging write fixed
(`steps_keep_fixed`).

A recorded session is forgotten only by aborting it, which ends the provider's
session first (`forgotten_by_abort`), and an upload retires only once it
records none (`retire_needs_no_session`), so it retires with no session left
at the provider (`retired_no_session`). A failed abort keeps the upload queued
with its session, as the upload's failure (`failed_abort_queued`). Each
failure waits as its class says (`failure_waits_by_class`): `Automatic` for
its delay, `AppAction` for new credentials or a new connection, `Never` for
nothing. An attempt starts only when the upload is due, or the app's early
retry covers it (`start_needs_due`), and an abort only once its wait is over
(`abort_needs_ready`). `wait_ends` shows a delay ends only when it passes and a
connection wait only on a new connection, a reopened store's included, and
`never_retried` that a `Never` failure is never retried, by automatic work or
the app: no step sends anything for the upload or changes its failure or its
recorded session.

Beginning a session and recording it are one step here. A session the
provider began whose record never committed, because its reply was lost or the
device stopped first, is outside the model, as are the provider's own
behaviour, which the model takes as truthful, and the length of each delay,
which [Clocks](#hybrid-timestamps-and-retry-time) bounds.
`uploadSessionRunner` runs the worker's runs, `run` in the model, against the
upload worker over coven-storage's in-memory provider through
`COVEN_UPLOAD_SESSION_LEAN`:
`files::tests::abandoned_tests::sessions::upload_sessions_match_the_storage_model` in
coven-sync compares, after each run, the queue row, the parts its session
records, the provider's live session and its parts, the published object, the
switched file and the class of the pending failure.

## Hybrid timestamps and retry time

[Clocks.lean](storage/CovenStorage/Clocks.lean) orders a local timestamp after
all observed timestamps without waiting for the wall clock. `stamp_after_latest`,
`stamp_after_every_read`, `different_devices_distinct` and
`stamp_after_snapshot` provide the causal and uniqueness properties used by
the merge. `wall_ahead_uses_zero_counter` and `wall_behind_increments` give
the stamp's value either side of the latest time; `first_stamp` and
`first_stamp_before_epoch` cover a device with no latest timestamp.
`representable_stamp_exists` states the bounded-counter premise, and
`clock_past_range_fails` and `exhausted_fails` its two failures: the model does
not allow arithmetic overflow to become a timestamp.

Storage ages and retry delays are distinct from authored timestamps. An
object's age compares the deleting device's wall clock with the provider's time
for the object plus that time's resolution (§16.6, §20.5). With a clock no
faster than real time, an aged object really was published that long ago
(`age_never_early`); a fast clock can end the wait at once
(`fast_clock_ages_at_once`). A live retry wait runs on a clock that wall-clock
changes don't move (`wall_jump_changes_no_retry`), and its delay never passes
five minutes (`retry_delay_bounded`).
Persisted wait reopening is covered in [IO](io.md#waiting-and-reopening).
`clockRunner` compares timestamp generation with
`write_encoding::tests::lean_differential_clocks` via `COVEN_CLOCK_LEAN`: a
first stamp at the edges of the range, a stamp with the wall clock ahead, and
stamps after receiving writes and loading a snapshot with the clock behind.

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
