# Sync

coven syncs SQLite row changes between devices that share a library. There is no
coordinator: each device pushes the changesets it produces and pulls the ones
other devices produced, applying them with row-level last-writer-wins. The unit
of exchange is a changeset (a binary diff from SQLite's session extension)
wrapped in a metadata envelope, encrypted, and written to cloud storage under a
per-device sequence number.

The examples use a todos app. A `workspace` holds `lists`, a `list` holds
`todos`, a todo can have `todo_attachments`, and labels attach via a
`todo_labels` join. Two people, Alice and Bob, share the library.

This page covers how a local write reaches every device. Row-level gating (which
rows stay local) has its own page, [Local data](/docs/local-data); fresh-device
bootstrap from a snapshot has its own page, [Bootstrap](/docs/bootstrap).

## Change capture

The host declares the synced tables once at startup with
[`set_synced_tables`](rustdoc:fn:coven::sync::session::set_synced_tables),
passing [`SyncedTable`](rustdoc:enum:coven::sync::session::SyncedTable) values.
Every synced table must have a text `id` primary key at column 0 and an
`_updated_at TEXT NOT NULL` column. A table not listed here is local-only and
never leaves the device.

A [`SyncSession`](rustdoc:struct:coven::sync::session::SyncSession) attaches the
SQLite session extension to each declared table on the write connection. From
then on the connection records every insert, update, and delete to those tables
into an in-memory changeset. The host writes through the connection as usual;
capture is passive.

The registration is a required integration step, not a tuning knob. With no
tables registered the session attaches nothing and produces empty changesets
forever, so [`init_sync`](rustdoc:fn:coven::sync::cycle::init_sync) treats an
empty set as a hard error and refuses to start.

## The sync cycle

A background loop runs one cycle at a time.
[`run_single_sync_cycle`](rustdoc:fn:coven::sync::cycle::run_single_sync_cycle)
loads the persisted sync state each cycle (rather than holding it across calls)
and drives these steps:

1. Capture the outgoing changeset from the active session
   (`SyncSession::changeset`, `None` if nothing changed).
2. End the session by dropping it. This must happen before pulling: a still-open
   session would record the rows an incoming changeset applies and re-emit them
   as spurious local changes next cycle.
3. Apply row-level gating to the captured changeset, cutting rows that should
   stay local (see [Local data](/docs/local-data)).
4. Upload any blobs the outgoing changeset references, so a puller can fetch them
   the moment it sees the change (see [Blobs](/docs/blobs)).
5. Sign the envelope, stage the packed bytes to disk, and push them to storage
   under the device's next sequence number; on success advance `local_seq`.
6. Pull every remote changeset past the device's cursor, validate it, and apply
   it with last-writer-wins.
7. Advance the clock past every applied row's `_updated_at`.
8. Persist the updated cursors and flush the clock's high-water mark.
9. Start a new session for the next cycle, then check snapshot policy.

When Alice edits a todo title, her next cycle captures the update to `todos`,
signs and encrypts it, and writes it to storage at `changes/<alice-device>/<seq>`.
Bob's device, on its own cycle, lists the device heads, sees Alice's sequence
number is past his cursor for her device, fetches the changeset, and applies it.

[`SyncService::sync`](rustdoc:method:coven::sync::service::SyncService::sync)
captures and ends the session, gates the changeset, uploads blobs, signs the
envelope, and pulls (steps 1 through 4 and 6, plus the signing in step 5); it
returns the packed envelope and the pull result. The surrounding cycle function
stages and pushes that envelope, advances `local_seq`, persists cursors,
advances the clock, flushes the high-water mark, and checks snapshot policy.

### Push

Push stages the changeset bytes to a file before uploading. If the upload fails,
the bytes survive on disk and `staged_seq` is persisted, so the next cycle
retries the same sequence number rather than skipping it. After a successful
push the staged file is cleared and `local_seq` advances. A device's sequence
numbers start at 1; the first changeset is `local_seq + 1` over an initial
`local_seq` of 0.

The push is gated on blob uploads: if the outbox still has pending uploads, the
cycle defers the changeset so peers never learn about a row whose blob is not yet
in the cloud.

### Pull

Pull lists the device heads (one storage call), then for each device whose head
sequence is past the local cursor, fetches the changesets in `(cursor+1..=head)`
order. For each one it:

- unpacks the envelope and checks `schema_version` against the local
  [`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION);
- verifies the Ed25519 signature
  ([`verify_changeset_signature`](rustdoc:fn:coven::sync::envelope::verify_changeset_signature));
- if the library has a membership chain, checks the author can write *now*
  (a removed member or a read-only Follower is rejected);
- applies the changeset with last-writer-wins;
- downloads any blobs it references.

The cursor for that device advances to a sequence number only after the
changeset is accepted (or deliberately skipped) and its blobs downloaded. A
failed blob download leaves the cursor where it is, so the changeset re-pulls
next cycle; the pull reports this through `PullResult::asset_downloads_failed`. A
cursor is the highest sequence applied from a device, and pull only fetches
beyond it, so applies are not repeated.

## Hybrid logical clocks

`_updated_at` is a hybrid logical clock stamp, not wall-clock time. The host must
treat it as opaque: bind the string coven hands it into the row and never parse
or compare it as a date. Its format, internal to coven, is
`{millis:013}-{counter:04}-{device_id}`, for example
`1735689600000-0000-alice`. The three parts make the string sort
lexicographically in causal order: a fixed-width millisecond field, then a
counter that breaks same-millisecond ties on one device, then the device id that
breaks ties across devices.

The clock is an [`Hlc`](rustdoc:struct:coven::sync::hlc::Hlc).
[`Hlc::now`](rustdoc:method:coven::sync::hlc::Hlc::now) mints the next stamp: if
wall-clock millis moved forward it adopts them and resets the counter, otherwise
it bumps the counter, so each stamp is strictly greater than the last. The host
never calls this directly; it holds an
[`UpdatedAtStamper`](rustdoc:struct:coven::sync::hlc::UpdatedAtStamper) and calls
[`stamp`](rustdoc:method:coven::sync::hlc::UpdatedAtStamper::stamp) in its write
path. The stamper and the sync layer share one `Arc<Hlc>`.

The host opens the clock through a
[`RegisterClock`](rustdoc:struct:coven::sync::register_clock::RegisterClock)
before the first synced write.
[`RegisterClock::open`](rustdoc:method:coven::sync::register_clock::RegisterClock::open)
seeds the in-memory state to a floor of `max(persisted high-water mark,
max(_updated_at) scanned across every synced table)`, so a restart cannot mint a
stamp behind a value already on disk. The on-disk scan is the authoritative
source: the high-water mark is flushed only at cycle end and lags any local row
stamp minted between cycles.

### Advancing past pulled rows

After applying changesets, the cycle takes the greatest `_updated_at` among all
applied rows (`PullResult::max_applied_updated_at`) and calls
[`advance_past`](rustdoc:method:coven::sync::hlc::Hlc::advance_past). The next
local stamp then sorts strictly after everything just pulled.

This advance is unconditional: there is no cap to wall-clock time. An applied
row's `_updated_at` is an authoritative register value the last-writer-wins layer
already accepted and wrote to disk, not an untrusted peer's clock. Capping it
would let the next local edit mint a stamp below an already-stored row and lose
to it. The cost is that one device's far-future clock pulls every peer's clock
forward, which is correct: a value on disk outranks wall time.

This is what makes a plain wall clock insufficient. Alice creates a todo at her
12:00:00, stamped `...-alice`. Bob pulls it; his clock advances past Alice's
stamp. Bob edits the same todo five seconds later. Even if Bob's wall clock were
behind Alice's, his stamp is seeded past hers, so it is lexicographically
greater. His changeset reaches Alice, her pull applies it, and his edit wins.
Both devices converge on Bob's version.

## Last-writer-wins conflict resolution

Applying a changeset can collide with local state. SQLite reports each collision
to a conflict handler;
[`lww_conflict_handler`](rustdoc:fn:coven::sync::conflict::lww_conflict_handler)
decides what to do by comparing `_updated_at` strings. The column index is read
from `PRAGMA table_info` at apply time
([`TableSchema`](rustdoc:struct:coven::sync::conflict::TableSchema)), so adding
columns to the end of a table stays safe. The five conflict types:

- **Data** (the row exists on both sides and both edited it): compare
  `_updated_at`; the incoming row replaces the local one only if its stamp is
  greater, otherwise the local row stays.
- **Conflict** (an incoming insert hits an existing primary key): same
  comparison, newer stamp wins.
- **NotFound** (an incoming update targets a row deleted locally): the incoming
  change is dropped. Delete wins.
- **Constraint** (a foreign-key or uniqueness constraint is violated): the row is
  dropped and the changeset is marked for retry.
- **ForeignKey** (a deferred foreign-key check fails at the end of the
  changeset): same as Constraint, dropped and retried.

When `_updated_at` is missing on a Data or Conflict collision, the handler keeps
the local row and logs it, rather than guessing.

### Foreign-key retry

A child row can arrive in a changeset whose parent is in a different device's
changeset, not yet applied. The child's insert violates a foreign key and is
dropped on the first pass. Pull collects every such changeset and retries each
once after the first pass over all devices completes, by which point the parent
rows exist. If a changeset still violates a foreign key after the retry, it is
logged and skipped.

## SyncManager lifecycle

[`SyncManager`](rustdoc:struct:coven::sync::sync_manager::SyncManager) owns the
sync lifecycle. The host builds it once with
[`new`](rustdoc:method:coven::sync::sync_manager::SyncManager::new), passing the
already-opened `RegisterClock` so the manager borrows the same clock the host
stamps rows from. Construction is synchronous and infallible; the seeding it used
to do now lives in `RegisterClock::open`.

[`start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
builds the cloud home from the current config and, if sync is enabled, spawns the
loop.
[`stop_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::stop_sync)
drops the loop handle and cloud home. The pair runs when a provider is connected
or disconnected, with no app restart.
[`is_sync_ready`](rustdoc:method:coven::sync::sync_manager::SyncManager::is_sync_ready)
reports whether the loop thread is running, and
[`trigger_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::trigger_sync)
asks the loop to run a cycle now.

The loop runs on a dedicated OS thread with its own current-thread tokio runtime,
because the session holds a raw `sqlite3` pointer that is not `Send` across task
boundaries. After each cycle it emits a
[`SyncLoopStatus`](rustdoc:struct:coven::sync::sync_loop::SyncLoopStatus) over a
broadcast channel; the host observes the stream with
[`SyncLoopHandle::subscribe`](rustdoc:method:coven::sync::sync_loop::SyncLoopHandle::subscribe):

```rust
pub struct SyncLoopStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
    pub data_changed: bool,
    pub row_changes: Option<Vec<RowChange>>,
}
```

`error` carries a user-facing message when a cycle hit a hard failure, a
schema-too-old floor, asset-download failures, or schema skips. `data_changed`
is true when any changeset applied, and `row_changes` then carries those changes
for the host to map to its own domain events.

## Schema versioning

Every outgoing changeset carries the local
[`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION), a constant
bumped whenever the on-disk shape of synced tables changes. Storage may also hold
a `min_schema_version`, the floor every reader must meet. Two cases, handled
differently:

- **Hard floor.** If the local `SCHEMA_VERSION` is below storage's
  `min_schema_version`, pull returns
  [`PullError::SchemaVersionTooOld`](rustdoc:enum:coven::sync::pull::PullError)
  and syncs nothing. Its `Display` is the message shown to the user: update the
  app to keep syncing. This is permanent until the user upgrades.
- **Per-changeset skip.** A single changeset whose `schema_version` is above the
  local one is skipped: it is counted in `PullResult::skipped_schema` and its
  cursor advances past it so it is not re-fetched. The signal is transient,
  surfaced once through `SyncLoopStatus::error` and cleared next cycle. Once the
  user upgrades, a fresh snapshot reconciles the rows that were skipped.

## Backoff

One exponential formula (`backoff_secs`, `30s · 2^n`) drives two waits with
different caps. At the cycle level a successful cycle waits the base 30 seconds
before the next run; each consecutive failure doubles the wait (60s, 120s, 240s),
capped at 300 seconds. A success resets the count, and a manual `trigger_sync`
preempts the wait. At the item level, a failing blob upload's retry window grows
the same way, capped at one hour, so one stuck file does not block the others.

Most cycle errors are transient (network, a failed blob download) and recover on
the next cycle; the cycle does its best to recover and reuse the session across
them
([`SyncCycleOutcome::ErrWithSession`](rustdoc:enum:coven::sync::cycle::SyncCycleOutcome)).
Two are permanent: the schema-too-old floor (the user must upgrade) and a
membership rejection (the device is no longer a write-capable member).
