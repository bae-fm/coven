# IO and observable work

The Lean package in [io](io/) covers retained byte ranges, file downloads,
retry waits, the answer to the person's retry, the sync state, work
notifications and the removed-device file scan. The
[sequence model](sequence.md) covers ordered reads, blocked devices, claims,
operations, snapshot selection, the floor and history deletion.

## Retained bytes and download progress

[Trace.lean](io/CovenIO/Trace.lean) models a cache by object and byte address.
A successful cache commit retains bytes; explicit eviction removes them.
Retained or in-flight bytes cannot be fetched again. The model separates
receiving bytes from applying their contents, and bytes no commit takes are
never retained (`uncommitted_never_retained`), as a pin's prepare commits none.

[Downloads.lean](io/CovenIO/Downloads.lean) models validated file chunks,
observable progress and retry disposition. Disk failure preserves validated
progress. A pin's attempt is its prepare (§17.8): it retains and pins nothing
(`prepare_retains_nothing`), a successful one clears what was pending
(`prepared_settles`), and only the app write committing it keeps the whole
file pinned (`commit_pins`), while a record still names its stored bytes
(`unnamed_commit_keeps_nothing`). Only a pin's failure is recorded
(`failure_atomic`): a read's goes
to its caller and leaves the state as it was (`failed_read_records_nothing`),
so nothing is pending after it. Only the app's reads and pins download, so
nothing retries a download on its own (`never_on_its_own`): a failure the
app's next read or pin can clear retries then (`next_call_retries`), refused
bytes wait for an
applicable update, and a provider's outright refusal never clears
(`refusal_never_retries`). A timeout or connection failure cannot be reported
as a missing file, and a confirmed absence at the path a record's coordinates
name waits for the app, never for an upload (`absence_waits_for_the_app`):
coordinates say cloud only once storage confirmed the
bytes (§17.2). The model assumes
the byte reader, validator and local writes supply truthful outcomes; it does
not prove cipher security or filesystem atomicity.

`ioRunner` exposes retained-range and download cases;
`object_input::tests::io_differential_retained_ranges_and_refusal_eviction`
compares the retained-range behavior.
`files::tests::cache::retries::io_differential_download` sends controlled
network, authentication, content-hash and disk outcomes to the download case,
through a read and through a pin's prepare and the write committing it, and
compares completion and the persisted retry disposition.

[Keep.lean](io/CovenIO/Keep.lean) models what an attempt at a pin coven keeps
for a file kept here reads (§17.8). An app file's attempt reads coven's copy and
reads storage only if the copy doesn't check out (`copy_reads_no_storage`,
`failed_copy_downloads`); a user's file's attempt downloads (`user_downloads`);
and no attempt reads a user's original (`original_never_read`).
`files::tests::keep::io_differential_keep` runs the real attempt for an app
file whose copy checks out, one whose copy was damaged, and a user's file whose
original was moved away, and compares whether storage was read with the
runner's `keep` case.

## Waiting and reopening

[Waiting.lean](io/CovenIO/Waiting.lean),
[WaitingEvidence.lean](io/CovenIO/WaitingEvidence.lean),
[WaitingReopen.lean](io/CovenIO/WaitingReopen.lean) and
[WaitingNetwork.lean](io/CovenIO/WaitingNetwork.lean) distinguish timed retry,
changed prerequisites and work that cannot retry. Delay records retain both
the wall deadline and full wait. Reopening arms the remaining delay bounded
by that full wait; the armed wait uses monotonic time. Provider cooldowns
remain independent of an individual operation's retry delay.

The model includes a failed persistence outcome: it cannot authorize an
automatic retry. It assumes durable transactions and the clock values
supplied at the boundary. It does not promise elapsed real-time cooldowns
under arbitrary wall-clock jumps. `retry_waits::tests::io_differential_waiting`
compares Rust's decisions with `ioRunner`.
`sequence_sync::sequence_retry::tests::io_differential_waiting_evidence`
runs the operation owner's readiness and error recording across relevant,
unrelated, racing and own commits, then reopens the database both while
waiting and during an attempt. It compares readiness and persisted failure
counts after each attempted retry, including capped waits and a backward
wall-clock change.

WaitingNetwork.lean treats the app's network report as a commit changing the
network, the prerequisite of any wait whose attempt got no provider response:
the connection's, an operation's or an upload's. A wait after a
provider's answer names something else, so the report leaves it alone. The
person's retry (`retry_now`) is a commit changing whatever each wait names, so
it ends every wait but leaves the provider cooldown. Rust ends the wait by
saving it with no full wait left, so a reopen cannot bring it back. A joining
device has no handle to retry with; its `JoinControl::check_now` ends the
approval poll's wait the same way, keeping its failure count, and leaves the
cooldown, which `sequence_join::tests::check_now_ends_the_joins_wait_but_not_a_provider_cooldown`
checks. `connection_retry::tests`, the `network_report` tests of the upload and
invite owners, `files::tests::keep::a_network_report_and_the_persons_retry_end_a_kept_pins_wait`
for the pins coven keeps, and those in
`handle::sync_control::tests` check the Rust owners against these rules; no
differential runner compares them.

## The answer to the person's retry

[RetryAnswer.lean](io/CovenIO/RetryAnswer.lean) models how `retry_now` is
answered. A request is a report, so it takes the next report number. The
connection's owner acts on reports between passes and remembers how far it
acted. A pass that begins takes every waiting request the owner acted on, and
answers them when it ends: synced or failed. A request made after the owner
last acted, such as one made while a pass runs, waits for a later pass. The
connection's first attempt is the start of the first pass: a failed attempt
answers as a failed pass, and a connected one hands its requests to the
loop's first pass.

What a retry made now would do, the effect, is read from the state the
answers come from: whether a started connection will plan, and what holds the
next pass, a provider cooldown or a failure only new credentials, an update or
a reset can clear. A request made while the effect names an answer gets it at
once: cooling down, failed, or not syncing; otherwise it waits for a pass. A
request already waiting when a hold begins is answered by the owner, between
passes, with that hold. Stopping sync answers every waiting request "not
syncing"; a pass already running still answers its own requests with its
result.

`answer_final` proves an answer never changes, so each request is answered at
most once. `pass_answers_acted_on`, `running_pass_skips` and
`next_pass_answers` prove the answer comes from the first pass that begins
after the owner acted on the request, never from one already running.
`concurrent_requests_share` proves requests made together share that pass.
`held_answers` and `held_runs_no_pass` cover a held pass,
`stopped_answers_at_once` and `stop_lets_pass_answer` stopping, and
`waits_only_while_syncing` that a request waits only while a started
connection will plan. `never_stranded` proves no request is left waiting:
from every reachable state, the owner's own steps answer every request,
with no outside event.

An app offers a retry only when it would run a pass, reading the effect from
`coven_retry_effect`. `ask_answers_by_effect` proves a request stands as the
effect says in the step that takes it, `effect_answers_at_once` that when the
effect names an answer, a request made then gets that answer with no other
step, and `effect_pass_runs` that when the effect is a pass, nothing answers
the request at once and the next pass answers it. `effect_steady` proves only
starting, stopping and a change of hold change the effect.

The model's atomicity is the store's. Rust keeps whether a started connection
will plan, whether a failure holds the pass and the cooldown's end as the row
of `coven_retry_effect`, which computes the effect from them. Each event that
changes them commits that row and acts on the requests in one step on the
SQLite writer, which runs one step at a time; a request, a pass beginning and
an attempt handing its requests back read the row in such a step too. So each
event happens at once, as one step of the model, and the effect an app reads
is the one that answers. A pass begins only when the row lets it, as `begin`
requires. A cooldown's end is a write as well, the model's `held none`: the
first step after the cooldown ends writes it, the owner's when it wakes for it,
and until then the row says the cooldown holds. The failure objects a held
answer carries stay in memory beside the row, written in the same step. The
model leaves out what a pass does, keeping only how it ends.
`retry_waits::retry_answer::tests` check the requests and the row against
these rules, `internal_schema::retry_effect::tests` that the row computes the
effect and a live query follows it; the retry tests in
`storage::tests::control` and
`sequence_sync::sync_loop::tests::a_removed_device_never_retries_on_a_new_connection` check
the connection's first attempt and its loop, and that the row each writes is
how its retries are answered. No differential runner compares them.

## The sync state

[SyncState.lean](io/CovenIO/SyncState.lean) models the one state a status
screen shows (§19.1): not synced, syncing, waiting to sync, synced, offline,
offline with unsent changes, signed out, no access to storage, storage full,
storage folder missing, update needed, removed from the store, device needs
reset, clock wrong, or failed with no named cause. It is computed from
facts, never stored: whether a location is recorded and this device holds
its credentials, how the last pass that reached storage ended, whether the
running pass has read or claimed a slot, whether a download is moving, each
upload in this device's queue, whether the send queue holds a write, and the
pending records. Each fact names a candidate state or none, and the state is
the candidate of highest rank: the device's own standing first (removed,
needs reset, update needed), then signing in, then what storage said of the
account or location, then reachability, then the catch-all failure, then
activity, moving before waiting.

`one_state` proves the state is a candidate no other outranks, and the only
one, since no two states share a rank. `not_synced_iff` proves not synced
means no location is recorded, and `signed_out_without_credentials` that a
recorded location without credentials is signed out unless the device's own
standing outranks it, so the two are never confused.
`offline_unsent_iff` and `offline_iff` prove that between offline and
offline with unsent changes, whether a write or an upload is queued
decides, and `write_while_offline` that a write committed while offline
changes one into the other in its own step, with no pass.
`syncing_only_moving` proves syncing shows only while a slot or a file
moves; `quiet_pass_keeps` that a pass moving nothing leaves the state as it
was while it runs, and after it when it ends as the last pass did.
`last_synced_never_back` proves the time of the last finished sync never
goes backward, even when the clock does, and `last_synced_kept` that only a
pass reaching the read end changes it: every other event, reopening
included, keeps it.

Synced means this device's data and files are in storage.
`synced_files_stored` proves synced shows only while every upload in the
queue has its bytes stored: none moving, queued or failed. An upload queued
with nothing moving names waiting to sync, and `waiting_not_moving` proves
that state shows only while no slot and no file moves, with a wait to name:
what the first waiting upload waits for, by `Facts.uploadWait`, which takes
what holds every upload first (sync not running, a provider's cooldown, a
blocked pass), then what holds this one (the pause, for a trailing upload,
then its backoff), else its turn.

`Reason.state` maps each pending reason to one state or none, as a
function. A wait names none (`waits_name_no_state`), and so does a verdict
on an admin operation or on another member's writes, and a provider failure
that clears by itself, whose subject retries while the pass reports
reachability. A file that can't be uploaded or read names failed
(`file_failures_name_failed`): an original missing or changed, coven's copy
failing its check, stored bytes lost or refused. `bad_file_fails` proves
such a record keeps the state at failed or what outranks it. A pass held
on a wait, such as a slot waiting on another device's key or a provider's
cooldown, reached storage, so it counts as synced without moving the time
of the last finished sync (`waiting_pass_synced`).

The model takes the facts as given: that the loop records how each pass
ended and whether it moved a slot, that opening records whether storage is
set up, and that the live query reads them together in one transaction.
The `sync_state` case of `ioRunner` takes a set of facts as JSON and
answers the state's name and, for waiting to sync, its wait.

## Work notifications

[Wakes.lean](io/CovenIO/Wakes.lean) models queued work and the events that wake
its worker. `database_changes::tests::io_differential_wakes` compares the
Rust notification decisions with `ioRunner`. The model concerns which work
becomes runnable, not a proof of an operating system scheduler.

## Gone-device file scan

[Retired.lean](io/CovenIO/Retired.lean) models the folder of an uploader gone
for good, cited by §17.10: files the gone device stores, candidates found by completed
listings, and whether a listing has completed. A pass lists the folder only
until one listing completes; the app's cleanup lists it every time it runs. A
failed listing records nothing.

`listing_finds_every_file` proves that a completed listing makes every file in
the folder a candidate, and `candidates_kept` that later events never drop
one. `failed_listing_records_nothing` and `pass_lists_once` state the retry and
the rule of one listing per entry that stops the device.
`cleanup_finds_a_later_upload` shows why the app's cleanup exists: a file
uploaded after the listing when the device went is missed by every later pass
and found by the cleanup.
`sequence_sync::sequence_files::tests::io_differential_file_scan` drives the
real uploader listing on a removed S3 member's folder through uploads, passes
and cleanups that complete or fail, and compares the recorded candidates with
`ioRunner` after each event.

The premise matters: a successful listing is complete. That is a provider
assumption. A listing discovers candidates; retained references and snapshots
still determine whether any file can be deleted.

## Checking

Run `lake build` in `spec/proofs/io`. The root imports every retained module;
[Axioms.lean](io/CovenIO/Axioms.lean) audits the results. `scripts/check.sh`
rejects unfinished proofs and custom axioms and runs the exact Rust tests
above with `COVEN_IO_LEAN` pointing to the built runner. These models do not
specify serialized object bytes.
