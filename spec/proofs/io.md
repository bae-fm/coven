# IO and observable work

The Lean package in [io](io/) covers retained byte ranges, file downloads,
retry waits, what a refusal judges and when it is rechecked, the answer to the
person's retry, the sync state, work notifications and the removed-device file
scan. The
[sequence model](sequence.md) covers ordered reads, blocked devices, claims,
operations, snapshot selection, the floor and history deletion.

## Retained bytes and download progress

[Trace.lean](io/CovenIO/Trace.lean) models a cache by object and byte address.
A successful cache commit retains bytes; explicit eviction removes them.
Retained or in-flight bytes cannot be fetched again. The model separates
receiving bytes from applying their contents, and bytes no commit takes are
never retained (`uncommitted_never_retained`), as a pin's prepare commits none.

[Budget.lean](io/CovenIO/Budget.lean) models a namespace's cache budget as
`file_cache::trim_entries` enforces it. Over budget, it evicts the least
recently read entry it may, by last read and then cache file name, until its
unpinned entries are within budget or nothing more may go (`within_budget`);
within budget it evicts nothing (`within_budget_keeps_all`). A pinned entry's
bytes don't count toward the budget, so a pinned entry crowds nothing out:
evicting with it present evicts what evicting without it does
(`pinned_crowds_nothing`). A pinned entry never goes
(`pinned_kept`), nor a file's header while a chunk of the file is cached, and
another namespace's entries stay (`other_namespace_kept`). Every evicted entry
was read no later than every unpinned chunk of the namespace that stays
(`least_recent_first`). Eviction is a function of the entries, so it never
fails a read.

[Downloads.lean](io/CovenIO/Downloads.lean) models validated file chunks,
observable progress and retry disposition. Disk failure preserves validated
progress. A pin's attempt is its prepare (§17.8): it retains and pins nothing
(`prepare_retains_nothing`), a successful one clears what was pending
(`prepared_settles`), and only the app write committing it keeps the whole
file pinned (`commit_pins`), while a record still names its stored bytes
(`unnamed_commit_keeps_nothing`). Only a pin's failure is recorded
(`failure_atomic`): a read's goes
to its caller and leaves the state as it was (`failed_read_records_nothing`),
so nothing is pending after it. A read that succeeds records nothing either: one
that completes the file settles every condition, and one that stops short
settles all but a lasting one, a refusal, the file unavailable or a required
update, which stays as it was (`read_settles`). Only the app's reads and pins download, so
nothing retries a download on its own (`never_on_its_own`): a failure the
app's next read or pin can clear retries then (`next_call_retries`), refused
bytes wait for another build of coven to recheck them (below), and a
provider's outright refusal never clears (`refusal_never_retries`). A timeout
or connection failure cannot be reported as a missing file, and a confirmed
absence at the path a record's coordinates name waits for the app, never for an
upload (`absence_waits_for_the_app`): coordinates say cloud only once storage
confirmed the bytes (§17.2). The model assumes
the byte reader, validator and local writes supply truthful outcomes; it does
not prove cipher security or filesystem atomicity.

`ioRunner` exposes retained-range and download cases;
`object_input::tests::io_differential_retained_ranges_and_refusal_eviction`
compares the retained-range behavior.
`files::tests::cache::retries::io_differential_download` sends controlled
network, authentication, content-hash and disk outcomes to the download case,
through a read and through a pin's prepare and the write committing it, and
compares completion and the persisted retry disposition.
`files::tests::cache::retries::io_differential_read_settles` pins a two-chunk
file offline or over a second chunk that fails authentication, then reads its
first chunk, or the whole file once the cause is gone, and compares what stays
pending with the runner's `read_settles` case.

[Keep.lean](io/CovenIO/Keep.lean) models what an attempt at a pin coven keeps
for a file kept here reads (§17.8). An app file's attempt reads coven's copy and
reads storage only if the copy doesn't check out (`copy_reads_no_storage`,
`failed_copy_downloads`); a user's file's attempt downloads (`user_downloads`);
and no attempt reads a user's original (`original_never_read`).
The same file models where a read's bytes come from (§17.1, §17.7):
`openRead` finds the file where its record says, and judges bytes missing at
the open by the record as it then stands. `original_only_here` proves a read
reaches a user's original only while the record keeps the file on this device
and its bytes are there; `stored_reads_no_original` that a stored file reads
the cache or storage. So once the upload switches the record to storage, no
read reads the original again.
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

The model's waits are all saved ones. A wait whose save fails is outside it:
Rust holds that work until the app retries it (`RetryWaits::fail`), which
`retry_waits::tests::a_wait_that_cannot_be_persisted_holds_until_the_app_retries`
checks. Nor does the model reopen a provider cooldown: Rust saves it as a wait
of its own and reopens it by the same restart rule, and
`retry_waits::tests::a_provider_cooldown_holds_every_consumer_across_reopening`
checks that it outlasts the ordinary cap, survives reopening and holds every
consumer. The model assumes durable transactions and the clock values
supplied at the boundary. It does not promise elapsed real-time cooldowns
under arbitrary wall-clock jumps. `retry_waits::tests::io_differential_waiting`
compares Rust's decisions with `ioRunner`.
`sequence_sync::sequence_retry::tests::io_differential_waiting_evidence`
runs the operation owner's readiness and error recording across relevant,
unrelated, racing and own commits, then reopens the database both while
waiting and during an attempt. It compares readiness and persisted failure
counts after each attempted retry, including capped waits and a backward
wall-clock change.

WaitingNetwork.lean treats the network report, which coven makes as its watch
of the device's network sees a usable network appear or change, as a report
naming the network, the prerequisite of any wait whose attempt got no provider
response: the connection's, an operation's or an upload's. A wait after a
provider's answer names something else, so the report leaves it alone. The
person's retry (`retry_now`) is a report naming every wait, so it ends every
wait but leaves the provider cooldown. An owner acts on reports between
attempts, so a report during an attempt ends the wait that attempt fails
into. A report ends a wait its timer hasn't ended, and saves it ended, with
deadline now and no full wait left, keeping its failure count: a reopen then
finds it due under any wall clock (`report_survives_reopen`).
`connection_retry::tests::io_differential_network_reports` runs the
connection's wait through offline and answered failures, network reports and
the person's retry between passes and during them, time passing, the wall
clock set back and reopening, and compares whether each pass was due and the
saved failure count with the runner's `evidence` case. A joining device has
no handle to retry with; while a `JoinControl::check_often` guard lives, the
approval poll's wait ends the same way once the shortest interval held has
passed since the last check, keeping its failure count, and leaves the
cooldown. `sequence_join::tests::a_held_interval_brings_a_backed_off_joins_check_forward`,
`dropping_the_guard_returns_the_check_to_the_backoff`,
`a_guard_taken_after_its_interval_checks_at_once`,
`a_guard_taken_during_the_wait_shortens_it`,
`the_shortest_of_several_guards_decides_the_joins_check` and
`a_provider_cooldown_holds_the_check_a_guard_would_make` check it. The `network_report` tests of the upload and invite owners,
`files::tests::keep::a_network_report_and_the_persons_retry_end_a_kept_pins_wait`
for the pins coven keeps, and those in `handle::sync_control::tests` check the
other Rust owners against these rules.

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
`retry_waits::retry_answer::tests::io_differential_retry_answer` drives the
Rust owner through requests, network reports, starting and stopping, holds
beginning and ending, the owner planning and answering a held pass, passes
beginning and ending, and the connection's first attempt handing its requests
back. After each event it compares whether the event was taken (a pass
begins only when the row lets it), where each report number's request
stands, and the effect `coven_retry_effect` shows, with the runner's
`retry_answer` case. The other `retry_waits::retry_answer::tests` check the
requests and the row against these rules, `internal_schema::retry_effect::tests`
that the row computes the effect and a live query follows it; the retry tests
in `storage::tests::control` and
`sequence_sync::sync_loop::tests::a_removed_device_never_retries_on_a_new_connection` check
the connection's first attempt and its loop, and that the row each writes is
how its retries are answered.

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
pass reaching the read end, or setup, joining or restoring finishing,
changes it: every other event, reopening included, keeps it.

A store's long-lived workers end in one of the model's events. A pass ends
with `finish`; the sync loop, or the attempt to connect that starts it,
ending on a failure of its own work or a panic, in a pass or outside one,
is `halt`: the last pass shows the failure, nothing moves, and sync stops
(api.md E15). The upload worker's panic is `halt` too, with its dropped
attempts an `uploads` change, and `halted_uploads_wait` proves that after a
halt every queued upload waits with sync not running. The invite expiry, the network follower and a read-ahead change no
fact the state reads. `only_setup_changes_storage` proves no event but
opening, setup or forgetting changes whether storage is set up, so a
worker's failure never undoes a setup; `failed_worker_shows` proves that
after `finish` or `halt` with a failure naming a state, the state shown is
that one or one that outranks it, never synced.

Synced means nothing waits: this device's data and files are in storage.
`synced_files_stored` proves synced shows only while every upload in the
queue has its bytes stored: none moving, queued or failed;
`synced_writes_sent` that it shows only with the send queue empty; and
`failed_pass_not_synced` that it shows only after a pass that ended synced.
A pass held back by a wait, an upload queued with nothing moving, and a
write in the send queue name waiting to sync, and `waiting_not_moving`
proves that state shows only while no slot and no file moves, with a wait
to name: the pass's, by `Facts.passWait`, else the first waiting upload's,
by `Facts.uploadWait`, else the write's, which waits for the next pass as a
held-back pass does. Both take what holds every transfer first,
`Facts.heldAll` (sync not running, a provider's cooldown, a blocked pass);
the next pass then waits its turn, and an upload what holds it alone (the
pause, for a trailing upload, then a metered network the setting to
transfer only on unmetered networks holds it on, then its backoff), else its
turn.

Which uploads that setting holds is one definition, `Upload.heldOnMetered`:
the trailing ones, as the pause holds them. `metered_holds` proves that with
the setting on and the network metered, every upload it holds waits for
something before its backoff and its turn, and `metered_only_held` that an
upload waits for the network only then. `network_keeps_state` proves that a
change of network never changes which state shows: the state doesn't read
the network, only what a waiting upload waits for does.

[SyncReach.lean](io/CovenIO/SyncReach.lean) goes the other way, from the facts
to the state. `state_iff` proves a state shows exactly when a fact names it and
none names one above it, and `candidate_iff` says which facts can name one: the
loop, a download, an upload, a queued write, missing credentials or a pending
record. `synced_iff` unfolds that for synced: a connected device whose last
pass ended synced, with nothing moving, every upload stored, no write queued
and no record naming a state. `wait_facts` gives what each wait says of the
facts, with `pass_wait_holds` for the next pass's wait. `every_state` and
`every_wait` run the model's events from a synced device to each state and each
wait, so none of them is dead.

[StoreReach.lean](io/CovenIO/StoreReach.lean) goes from a store history to those
facts, with the [sequence](sequence.md) package's device reading the history.
`PassEnd` says how a pass ends from where the device's readers stand; Rust's
pass makes these reports, and the link to it is by name. `synced_reached` proves
a device with its keys that can open every slot can end a pass synced.
`key_wait_reached` and `unopened_reached` prove a device held at a slot, without
the key or unable to open it, never ends synced from there, however it steps,
and shows waiting to sync on the slot, or failed, or update needed.
`removal_reached` and `replacement_reached` prove that once a reader has applied
its member's removal or a replacement of its keys, the device shows removed or
device needs reset, and `device_removal_reached` and
`device_replacement_reached` the same for its own removal and a registration in
its place. The other states come from no store history: they are what a
provider or custody answers, what is recorded, or what is moving, and
`every_state` reaches them from those facts.

[FileReach.lean](io/CovenIO/FileReach.lean) does the same for files, reading the
upload queue of [`Files`](storage/CovenStorage/Files.lean). `staged_queued` and
`queue_holds_synced` prove staging a file queues its upload, and a queued upload
holds synced off. `queued_waits` proves it shows waiting to sync on an otherwise
settled device, waiting its turn. `completed_empties` and `completed_synced`
prove that once storage confirmed the last queued upload the device shows
synced. `lost_file_fails` proves a confirmed absence is a lost file, whose record
keeps the state at failed or what outranks it. What else holds a queued upload
is a setting or a failed attempt, not a step of `Files`; `wait_facts` covers
those.

`Reason.state` maps each pending reason to one state or none, as a
function. A wait names none (`waits_name_no_state`), and so does a verdict
on an admin operation or on another member's writes, and a provider failure
that clears by itself, whose subject retries while the pass reports
reachability. A file that can't be uploaded or read names failed
(`file_failures_name_failed`): an original missing or changed, coven's copy
failing its check, stored bytes lost or refused. `bad_file_fails` proves
such a record keeps the state at failed or what outranks it. A pass held
on a wait, such as a slot waiting on another device's key or a provider's
cooldown, is no failure either: it waits to sync (`waiting_pass_waits`),
without moving the time of the last finished sync.

The model takes the facts as given: that the loop records how each pass
ended and whether it moved a slot, that a loop or attempt ending outside a
pass's end records its failure and that nothing moves, that opening records whether storage is
set up, and that the live query reads them together in one transaction.
The `sync_state` case of `ioRunner` takes a set of facts as JSON and
answers the state's name and, for waiting to sync, its wait.
`sync_status::tests::io_differential_sync_state` compares the state coven
computes with the runner's for each pending reason, each provider failure
answered or not, each local failure, each way a pass ends, blocked on or
failed with each reason, each with a write queued or not, and queued
uploads under each wait, alone and behind a stored one, with the setting to
transfer only on unmetered networks on or off, the network metered or not,
and a write queued beside them or not.

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

Whose folders are listed, and by whom, mirrors `History::stops` and
`History::deletes_for`. `member_cutoff_covers` proves that a member's removal
stops every one of the member's devices, and a replacement of the member's
keys every one added before it but its author. `every_gone_folder_cleaned`
proves that every gone device's folder has a device that isn't gone to clean
it up, one of its own member's or, when that member has none, an active
admin's, as long as some active admin has a device that isn't gone. Without
one, nobody cleans up until an admin registers a device.

The premise matters: a successful listing is complete. That is a provider
assumption. A listing discovers candidates; retained references and snapshots
still determine whether any file can be deleted.

## Pending records

[PendingRecords.lean](io/CovenIO/PendingRecords.lean) models `_coven_pending`
(§19.1): one record per subject, naming the first unmet condition, the error
behind it if one was met in this run, and what ends it besides the work
settling: a slot's progress, a snapshot's coverage of a waiting write, a move's
cancellation, an operation advancing or finishing, a raise capturing an own
write. A reset ends all but the records of an operation that still has
provider access to settle.

- `time_keeps`, `repeat_keeps`: time passing changes no record, and neither
  does an attempt that meets the same condition again.
- `reopen_keeps_condition`: opening drops every record's cause and keeps its
  condition and what ends it.
- `ended_only_by_its_end`: a record is gone after an event only if the event
  settles its work, is the progress its record names, or is a reset.
- `condition_changes_only_by_save`: a record's condition changes only when an
  attempt records another.

The model is the writes to the table. That each Rust owner records the
condition its work meets, and settles or ends its records at the right
moment, is tested against the Rust, as is the download rule that a record
lasts only while a file over the bytes does.

## What a refusal judges

[Reread.lean](io/CovenIO/Reread.lean) models what a refused slot's verdict is
about (§19.1). A device judges the bytes a read returned, and nothing checks a
slot's transfer. The model takes faults on the way to change bytes each its own
way, so two different reads agree only when both returned the stored bytes. A
check that refuses the kept copy reads the slot again, and the slot is refused
only when the two reads agree; otherwise the read failed, the copy is dropped,
and a later pass judges a later read. So a refusal is a verdict on the stored
bytes (`refusal_judges_stored`): a slot whose stored bytes pass is never refused
(`honest_never_refused`), and a kept copy that is the stored bytes applies
(`stored_copy_applies`). Refusing on one read, one fault refuses an honest slot,
and every later check of the kept copy refuses it again
(`one_read_refuses_honest`); with the second read the next pass applies it
(`fault_recovers`).
`pending::tests::a_slot_garbled_on_the_way_is_read_again_not_refused` garbles
a read on its way: the pass fails as a read and the next applies the slot, and a
garbled copy an earlier build refused is applied after another build rechecks
it.

A snapshot is kept nowhere whole: its body streams once, and its announcement
names its digest. The model's announced objects judge a read whose bytes are the
announced ones at once, and read any other again, keeping only the first read's
digest; the model takes digests to be equal exactly when bytes are, and a fault
never to produce the announced bytes when storage holds other bytes. A refusal
is then a verdict on what storage holds: it is not the announced object, or the
announced object fails the check (`announced_refusal_judges_stored`), so an
honestly stored snapshot that passes is never refused
(`honest_announced_never_refused`). Refused as soon as its digest differs, one
fault makes it not the announced one (`digest_once_refuses_honest`); read again,
the next read applies it (`announced_fault_recovers`).
`files::tests::cache::bytes_changed_on_the_way_are_fetched_again_not_refused`
changes a byte of a file's header and of its chunk on the way, for a read and a
pin, and
`sequence_sync::sequence_snapshots::tests::a_snapshot_changed_on_the_way_is_read_again_not_refused`
one of a snapshot's prefix and of its body: each read fails with `Corrupted`
and records no refusal, and the next one succeeds.

A file's cached copy is kept only once it checked, and a chunk's check is its
authentication, which only the stored bytes pass. The model's cached objects
carry the copy as kept, which passed, and what it holds now; a copy that fails
is set aside and storage's reads are judged as `pass` judges them. The stored
bytes then pass (`cached_stored_passes`), so a file whose cached copy fails its
check is never refused (`damaged_copy_never_refused`), and a copy that passes
applies with no read of storage (`passing_copy_applies`). Refused on the copy,
a file whose stored bytes pass is refused (`copy_once_refuses_honest`).
`files::tests::read::a_damaged_cached_copy_is_fetched_and_written_back_not_refused`
changes a byte of a cached header and of a cached chunk on disk, in a read's
entries and in a pin's whole copy: the next read or pin fetches them and records
nothing, a pinned file stays pinned, and the read after it asks storage for
nothing.
A cache file of another size than its entry records is such a copy: it takes
back that size, and the bytes it lost read as zeros, which fail their check.
`files::tests::read::a_cache_file_of_another_size_is_fetched_and_written_back`
cuts a read's header and chunk entries and a pin's whole copy short, and grows
one past its end, with the same outcome.

A snapshot's checks read its records as this device staged them on its own disk
(§16.2), and staging is a function of the snapshot's bytes. `pass` covers them
with the stagings as its reads, the staging of the stored bytes as what is
stored, and damage on this device's disk as a fault: a check that refuses a
staging stages the snapshot again from another read, and the snapshot is
refused only when the second staging's digest is the first's.
`sequence_sync::snapshot_staged::tests::a_staged_snapshot_damaged_before_its_load_is_not_refused`
changes a staged record before a reload's load reads it, and
`sequence_sync::snapshot_staged::tests::a_staged_snapshot_damaged_while_it_stages_is_not_refused`
a staged write as its staging checks it;
`sequence_sync::sequence_files::tests::a_snapshot_staged_for_file_deletion_damaged_here_is_not_refused`
does both to the snapshot a file deletion reads. Each fails with
`DamagedDatabase` and records no refusal, and the next one loads the snapshot
or deletes the file.
`sequence_sync::snapshot_staged::tests::a_snapshot_whose_stagings_agree_is_refused`
damages both stagings alike, as a snapshot whose own records fail stages
them, and the refusal stands, at the staging's check and at the load's.
A failed removal of the staging a failed check ends is a local failure outside
the model, and changes no outcome `pass` gives:
`sequence_sync::snapshot_staged::tests::a_body_failure_is_judged_when_removing_its_staging_fails`
fails that removal under both damages at a reload, and
`sequence_sync::sequence_files::tests::a_snapshot_damaged_here_is_not_refused_when_removing_its_staging_fails`
under local damage at a file deletion's check; each reports the outcome with
the failed removal beside it.

## Rechecking a refusal

[Recheck.lean](io/CovenIO/Recheck.lean) models when a refusal is checked again
(§19.1). A refusal is a verdict on bytes that never change, judged against a
history a stuck device never moves past, so only coven's own code can change it.
The record names the build of coven that last refused the input. Any other build
rechecks it (`other_build_rechecks`), so every build that would judge it
differently does (`differing_build_rechecks`); the recording build never does
(`own_refusal_waits`), and loses nothing by waiting, since it would refuse again
(`waiting_loses_nothing`). A recheck that refuses again records the running
build, so each build rechecks at most once and never loops (`recheck_once`). A
number several builds share, such as a package version development never moves,
would strand the device: a build that would accept the input never rechecks a
refusal another build with the same number recorded (`shared_key_strands`).

The model takes a build to be distinct from every build whose code differs.
Rust draws a fresh random build identity each time it compiles `coven-sync`,
where every refusal is made, from its own checks and those of the crates below
it: a change to any of that code, or to a dependency, compiles it again.
`pending::tests::a_refused_slot_is_rechecked_once_by_another_build` runs a
device whose slot was refused over bytes that check out: the same build keeps
the slot refused, another build applies it.

## Checking

Run `lake build` in `spec/proofs/io`. The root imports every retained module;
[Axioms.lean](io/CovenIO/Axioms.lean) audits the results. `scripts/check.sh`
rejects unfinished proofs and custom axioms and runs the exact Rust tests
above with `COVEN_IO_LEAN` pointing to the built runner. These models do not
specify serialized object bytes.
