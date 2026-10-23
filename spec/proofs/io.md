# IO and observable work

The Lean package in [io](io/) covers retained byte ranges, file downloads,
retry waits, the answer to the person's retry, work notifications and the
removed-device file scan. The
[sequence model](sequence.md) covers ordered reads, blocked devices, claims,
operations, snapshot selection, the floor and history deletion.

## Retained bytes and download progress

[Trace.lean](io/CovenIO/Trace.lean) models a cache by object and byte address.
A successful cache commit retains bytes; explicit eviction removes them.
Retained or in-flight bytes cannot be fetched again. The model separates
receiving bytes from applying their contents.

[Downloads.lean](io/CovenIO/Downloads.lean) models validated file chunks,
observable progress and retry disposition. Disk failure preserves validated
progress. Storage failures have three recovery classes: automatic retry,
changed credentials or connection, and permanent refusal. A timeout or
connection failure cannot be reported as a missing file, and a confirmed absence
at a path a row names never waits for an upload (`absence_never_waits`): rows
name only stored files (§17.2). The model assumes
the byte reader, validator and local writes supply truthful outcomes; it does
not prove cipher security or filesystem atomicity.

`downloadRetryRunner` calls those decisions for
`files::tests::cache::retries::io_differential_download_retries`. The Rust test
exercises the real file reader with controlled failures and compares its
retry disposition with Lean. `ioRunner` also exposes retained-range and
download cases; `object_input::tests::io_differential_retained_ranges_and_refusal_eviction`
compares the retained-range behavior.
`files::tests::cache::retries::io_differential_download` sends controlled
network, authentication, content-hash and disk outcomes to the download case
and compares completion and the reader's persisted retry disposition.

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
the connection's, an operation's, an upload's or a download's. A wait after a
provider's answer names something else, so the report leaves it alone. The
person's retry (`retry_now`) is a commit changing whatever each wait names, so
it ends every wait but leaves the provider cooldown. Rust ends the wait by
saving it with no full wait left, so a reopen cannot bring it back. `connection_retry::tests`, the `network_report` tests of the upload,
download and access-key operation owners, and those in
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

Before a pass begins, the owner may find it held, by a provider cooldown or
by a failure only new credentials, an update or a reset can clear. It then
answers the requests it acted on at once: cooling down, or failed. Stopping
sync answers every waiting request "not syncing", and a request made while
sync is stopped is answered so at once; a pass already running still answers
its own requests with its result.

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

The model assumes what Rust keeps under one lock: a request, stopping, and a
pass taking its requests each happen at once. It does not model what a pass
does, only how it ends. `retry_waits::retry_answer::tests` check the request
list against these rules; the retry tests in `storage::tests::control` and
`sequence_sync::sync_loop::tests::a_removed_device_never_retries_on_a_new_connection` check
the connection's first attempt and its loop. No differential runner compares
them.

## Work notifications

[Wakes.lean](io/CovenIO/Wakes.lean) models queued work and the events that wake
its worker. `database_changes::tests::io_differential_wakes` compares the
Rust notification decisions with `ioRunner`. The model concerns which work
becomes runnable, not a proof of an operating system scheduler.

## Removed-device file scan

[Retired.lean](io/CovenIO/Retired.lean) models the removed uploader's folder
cited by §17.8: files the removed device stores, candidates found by completed
listings, and whether a listing has completed. A pass lists the folder only
until one listing completes; the app's cleanup lists it every time it runs. A
failed listing records nothing.

`listing_finds_every_file` proves that a completed listing makes every file in
the folder a candidate, and `candidates_kept` that later events never drop
one. `failed_listing_records_nothing` and `pass_lists_once` state the retry and
the once-per-removal rule. `cleanup_finds_a_later_upload` shows why the app's
cleanup exists: a file uploaded after the listing at removal is missed by every
later pass and found by the cleanup.
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
above with `COVEN_IO_LEAN` or `COVEN_DOWNLOAD_RETRY_LEAN` pointing to the
built runner. These models do not specify serialized object bytes.
