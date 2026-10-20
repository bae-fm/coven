# IO and observable work

The Lean package in [io](io/) covers retained byte ranges, file downloads,
retry waits, work notifications and the removed-device file scan. The
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
connection failure cannot be reported as a missing file. The model assumes
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
provider's answer names something else, so the report leaves it alone. Rust
ends the wait by saving it with no full wait left, so a reopen cannot bring it
back. `connection_retry::tests`, the `network_report` tests of the upload,
download and access-key operation owners, and those in
`handle::sync_control::tests` check the Rust owners against these rules; no
differential runner compares them.

## Work notifications

[Wakes.lean](io/CovenIO/Wakes.lean) models queued work and the events that wake
its worker. `database_changes::tests::io_differential_wakes` compares the
Rust notification decisions with `ioRunner`. The model concerns which work
becomes runnable, not a proof of an operating system scheduler.

## Removed-device file scan

[Retired.lean](io/CovenIO/Retired.lean) retains the file-folder model cited by
§17.8. A complete listing after confirmed provider cut-off records every file
in that folder. Failed listings leave the scan pending. The transaction that
records restored access invalidates the completed scan.

`scan_names_every_file` proves that a completed scan remains sound through
uploads, revocations and restored access. `retention_forgetting_counterexample`
shows why waiting until the next retention pass to forget a scan can miss a
file uploaded between a restore and another revocation. `revoked_listing_completes`
proves that a successful listing after cut-off records completion.
`sequence_sync::sequence_files::tests::io_differential_file_scan` runs the
real uploader scan through failed and successful listings, including empty
folders and repeated retention. `io_differential_folder` interleaves uploads,
listings and provider access transactions, checking that restored access
invalidates the scan in the restoring transaction.

The premises matter: cut-off prevents further publication, including
in-flight sessions, and a successful listing is complete. These are provider
assumptions. A scan discovers candidates; retained references and snapshots
still determine whether any file can be deleted.

## Checking

Run `lake build` in `spec/proofs/io`. The root imports every retained module;
[Axioms.lean](io/CovenIO/Axioms.lean) audits the results. `scripts/check.sh`
rejects unfinished proofs and custom axioms and runs the exact Rust tests
above with `COVEN_IO_LEAN` or `COVEN_DOWNLOAD_RETRY_LEAN` pointing to the
built runner. These models do not specify serialized object bytes.
