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

coven owns the SQLite connection. The host opens it once through
[`Database::open`](rustdoc:method:coven::database::Database::open), passing the
synced tables as [`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable)
values. Every synced table must have a text `id` primary key at column 0 and an
`_updated_at TEXT NOT NULL` column. A table not in the set is local-only and
never leaves the device. From then on the host runs all its SQL through
[`Database::call`](rustdoc:method:coven::database::Database::call); coven
re-exports rusqlite, so the closure works against `&coven::rusqlite::Connection`
and the host never depends on rusqlite directly.

The connection lives on one dedicated thread (an actor) natively, or on the one
Worker in the browser. Capture is the SQLite session extension, attached over
`rusqlite::session` to every declared table on that owned connection. Each
insert, update, and delete to a synced table is recorded into an in-memory
changeset. The host writes as usual; capture is passive, and there is no
host-lent pointer to a connection coven does not own.

The set is not a tuning knob. With no tables declared the session attaches
nothing and produces empty changesets forever, so
[`init_sync`](rustdoc:fn:coven::sync::cycle::init_sync) treats an empty set as a
hard error and refuses to start.

## The sync cycle

A background loop runs one cycle at a time.
[`run_single_sync_cycle`](rustdoc:fn:coven::sync::cycle::run_single_sync_cycle)
loads the persisted sync state each cycle (rather than holding it across calls)
and drives these steps:

1. Capture the outgoing changeset and reset the recorded batch
   ([`take_changeset`](rustdoc:method:coven::database::Database::take_changeset)).
   Capture stays enabled: a host write that lands later in this cycle is recorded
   into the next batch, not lost.
2. Apply row-level gating to the captured changeset, cutting rows that should
   stay local (see [Local data](/docs/local-data)).
3. Upload any blobs the outgoing changeset references, so a puller can fetch them
   the moment it sees the change (see [Blobs](/docs/blobs)).
4. Sign the envelope, stage the packed bytes to disk, and push them to storage
   under the device's next sequence number; on success advance `local_seq`.
5. Pull every remote changeset past the device's cursor, validate it, and apply
   it with last-writer-wins.
6. Advance the clock past every applied row's `_updated_at`.
7. Persist the updated cursors and flush the clock's high-water mark.
8. Check snapshot policy.

Capture is never suspended across the cycle. The only window it is off is around
each individual apply in step 5: the pull disables the session, applies one
incoming changeset synchronously, and re-enables it at once, so the applied rows
are not re-recorded as this device's own writes while a host write landing
anywhere else in the cycle still is. This is the one thing the session is ever
blind to; every other read and write goes through `Database::call` on the normal
enabled path. (An earlier design suspended capture across the whole network span;
that left a window in which a host write could be dropped, so it was removed.)

When Alice edits a todo title, her next cycle captures the update to `todos`,
signs and encrypts it, and writes it to storage at
`changes/<alice-device>/<seq>`. Bob's device, on its own cycle, lists the device
heads, sees Alice's sequence number is past his cursor for her device, fetches
the changeset, and applies it.

[`SyncService`](rustdoc:struct:coven::sync::service::SyncService) runs steps 2
through 5 (gate, upload blobs, sign the envelope, pull) over the changeset the
caller already captured. The surrounding cycle function captures the changeset
before it, then stages and pushes the returned envelope, advances `local_seq`,
persists cursors, advances the clock, and checks snapshot policy.

### Push

Push stages the changeset bytes to a file before uploading. If the upload fails,
the bytes survive on disk and `staged_seq` is persisted, so the next cycle
retries the same sequence number rather than skipping it. After a successful
push the staged file is cleared and `local_seq` advances. A device's sequence
numbers start at 1; the first changeset is `local_seq + 1` over an initial
`local_seq` of 0.

Blob-before-row ordering is the host's responsibility, not a global push gate.
The cycle publishes whatever the gate emits; it does not hold the whole changeset
back while the outbox drains. A host with rows that reference async-outbox blobs
keeps each such row's gate column off until its blobs upload, then flips it on
(typically in `on_blob_uploaded`). While the gate column is off the gate cuts the
row; when it flips on the gate re-emits the row's full subtree, so a peer never
learns of a row whose blob is not yet in the cloud. The observer can return
`DrainControl::Publish` to break the drain the moment a unit's blobs land, so the
cycle publishes that unit instead of waiting for the rest of the batch, and the
loop then runs the next cycle promptly to keep draining.

### Pull

Pull lists the device heads (one storage call), then for each device whose head
sequence is past the local cursor, fetches the changesets in `(cursor+1..=head)`
order. For each one it:

- unpacks the envelope and checks `schema_version` against the local
  [`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION);
- verifies the Ed25519 signature
  ([`verify_changeset_signature`](rustdoc:fn:coven::sync::envelope::verify_changeset_signature));
- if the library has a membership chain, checks the author can write *now* (a
  removed member or a read-only Follower is rejected);
- applies the changeset with last-writer-wins (capture disabled only around this
  one apply, then re-enabled);
- downloads any `Mirrored` blobs it references into the [cache](/docs/cache).

The cursor for that device advances to a sequence number only after the changeset
is accepted (or deliberately skipped) and its blobs downloaded. A failed blob
download leaves the cursor where it is, so the changeset re-pulls next cycle; the
pull reports this through `PullResult::asset_downloads_failed`. A cursor is the
highest sequence applied from a device, and pull only fetches beyond it, so
applies are not repeated.

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
never calls this directly. It holds the
[`UpdatedAtStamper`](rustdoc:struct:coven::sync::hlc::UpdatedAtStamper) that
`Database::open` returns and calls
[`stamp`](rustdoc:method:coven::sync::hlc::UpdatedAtStamper::stamp) in its write
path, binding the result into every synced-row write. The stamper and the sync
layer share one `Arc<Hlc>`.

`Database::open` seeds that clock before it returns, so the stamper the host gets
back is already non-optional and past every value on disk. The floor is
`max(persisted high-water mark, max(_updated_at) scanned across every synced
table)`, so a restart cannot mint a stamp behind a value already written. The
on-disk scan is the authoritative source: the high-water mark is flushed only at
cycle end and lags any local row stamp minted between cycles.

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

Conflict resolution is row-level last-writer-wins on `_updated_at`. Applying a
changeset can collide with local state; SQLite reports each collision to a
conflict handler, and
[`lww_conflict_handler`](rustdoc:fn:coven::sync::conflict::lww_conflict_handler)
decides what to do by comparing the two `_updated_at` strings. The column index
is read from `PRAGMA table_info` at apply time
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

## Schema versioning

Every outgoing changeset carries the local
[`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION), a constant
bumped whenever the on-disk shape of synced tables changes. Pull enforces it two
ways:

- **Hard floor.** If the local `SCHEMA_VERSION` is below storage's
  `min_schema_version`, pull returns
  [`PullError::SchemaVersionTooOld`](rustdoc:enum:coven::sync::pull::PullError)
  and syncs nothing. Its `Display` is the message shown to the user: update the
  app to keep syncing. This is permanent until the user upgrades.
- **Per-changeset skip.** A single changeset whose `schema_version` is above the
  local one is skipped (counted in `PullResult::skipped_schema`); the device
  leaves its cursor where it is and stops pulling that device for the cycle. The
  cursor is deliberately *not* advanced, so once the app upgrades the next cycle
  re-fetches from that sequence and applies it.

How migrations, this version number, the `min_schema_version` floor, and
snapshots fit together, with worked examples for additive vs. structural changes,
is its own page: [Schema evolution](/docs/schema-evolution).

## Lifecycle

[`SyncManager`](rustdoc:struct:coven::sync::sync_manager::SyncManager) owns the
sync lifecycle natively. The host builds it once with
[`new`](rustdoc:method:coven::sync::sync_manager::SyncManager::new), passing the
owned `Database`; the manager reads the shared `Arc<Hlc>` from it, so it advances
the same clock the host stamps rows from. Construction is synchronous and
infallible, because the clock was already seeded in `Database::open`. (In the
browser the equivalent is [`WasmSyncRuntime`](/docs/web).)

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

The keys the loop signs and encrypts with come from the OS keyring. The host
installs the keyring service and identity at startup with
[`set_keyring_service`](rustdoc:fn:coven::keys::set_keyring_service); there is no
environment-variable or dev-mode key path.

The loop runs on a dedicated OS thread with its own current-thread tokio runtime.
The connection itself lives on the `Database` actor thread, reached only through
async calls, so the loop holds nothing tied to a thread; the dedicated thread is
for stack size (aws-sdk-s3's endpoint resolution recurses deeply enough to
overflow the default secondary-thread stack in debug builds). After each cycle it
emits a [`SyncLoopStatus`](rustdoc:struct:coven::sync::sync_loop::SyncLoopStatus)
over a broadcast channel; the host observes the stream with
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
([`RowChange`](rustdoc:struct:coven::changeset::RowChange)) for the host to map
to its own domain events.

## Backoff

One exponential formula (`30s · 2^n`) drives the cycle wait. A successful cycle
waits the base 30 seconds before the next run; each consecutive failure doubles
the wait (60s, 120s, 240s), capped at 300 seconds. A success resets the count,
and a manual `trigger_sync` preempts the wait.

Most cycle errors are transient (network, a failed blob download) and recover on
the next cycle. Two are permanent: the schema-too-old floor (the user must
upgrade) and a membership rejection (the device is no longer a write-capable
member).
