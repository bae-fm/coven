# Sync

coven syncs SQLite row changes between devices that share a store. It has one
protocol: each device appends its changesets to its own immutable commit stream,
peers pull those streams, and concurrent edits merge column by column with
deletes winning over concurrent edits. The unit of exchange is one host
transaction: its SQLite changeset becomes a Store package named by an exact
signed commit.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 224" role="img" aria-label="A write is captured and sealed, appends to the device's own stream, and peers pull it, advancing one exact materialized position per stream">
<text class="hdr" x="120" y="22" text-anchor="middle">ALICE'S DEVICE</text>
<text class="hdr" x="395" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="590" y="22" text-anchor="middle">BOB PULLS</text>
<rect class="lane" x="10" y="32" width="220" height="152" rx="10"/>
<rect class="lanec" x="260" y="32" width="270" height="152" rx="10"/>
<rect class="lane" x="550" y="32" width="100" height="152" rx="10"/>
<rect class="chip" x="30" y="58" width="180" height="26" rx="7"/>
<text class="lbl s11" x="120" y="75" text-anchor="middle">write → changeset</text>
<line class="arr" x1="234" y1="71" x2="272" y2="71" marker-end="url(#fa)"/>
<text class="sub" x="330" y="52" text-anchor="middle">append-only</text>
<rect class="chipo" x="280" y="58" width="70" height="24" rx="6"/>
<text class="lbl s11" x="315" y="74" text-anchor="middle">a/1</text>
<rect class="chipo" x="358" y="58" width="70" height="24" rx="6"/>
<text class="lbl s11" x="393" y="74" text-anchor="middle">a/2</text>
<rect class="chipo" x="436" y="58" width="70" height="24" rx="6"/>
<text class="lbl s11" x="471" y="74" text-anchor="middle">a/3</text>
<rect class="chipo" x="280" y="128" width="70" height="24" rx="6"/>
<text class="lbl s11" x="315" y="144" text-anchor="middle">b/1</text>
<rect class="chipo" x="358" y="128" width="70" height="24" rx="6"/>
<text class="lbl s11" x="393" y="144" text-anchor="middle">b/2</text>
<text class="sub" x="330" y="110" text-anchor="middle">one stream per device</text>
<line class="arr" x1="512" y1="71" x2="544" y2="71" marker-end="url(#fa)"/>
<text class="lbl s11" x="600" y="75" text-anchor="middle">materialized a=3</text>
<text class="lbl s11" x="600" y="144" text-anchor="middle">materialized b=2</text>
<circle class="numc" cx="24" cy="71" r="8"/>
<text class="num" x="24" y="74.5" text-anchor="middle">1</text>
<circle class="numc" cx="268" cy="71" r="8"/>
<text class="num" x="268" y="74.5" text-anchor="middle">2</text>
<circle class="numc" cx="600" cy="48" r="8"/>
<text class="num" x="600" y="51.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="212" text-anchor="middle">1 a write is captured and sealed · 2 it appends to this device's own stream · 3 peers pull, one exact position per stream</text>
</svg>

Nothing in storage is overwritten. Publication creates exact immutable objects
for a package, commit, and head, then reads each object back before advancing durable
state. A puller records the exact hash at every materialized device sequence,
not a sequence number that could disagree with the accepted commit.

Examples use the todos app (workspaces hold lists, lists hold todos, todos
carry attachments and labels); Alice and Bob share the store.

This page covers how a local write reaches every device. Row-level gating
(which rows stay local) has its own page, [Local data](/docs/local-data);
fresh-device bootstrap from a snapshot has its own page,
[Bootstrap](/docs/bootstrap).

## One protocol

There is one protocol and no mode to select. Each device keeps one append-only
commit stream. A commit names its exact predecessor and its materialized
dependency frontier, so devices publish while offline and pull merges the
independent streams. Nothing anywhere holds a mutable global head, and no
provider coordinates a global transaction order. Storage must provide
create-once exact object slots so concurrent publishers cannot replace one
another's protocol objects.

The signed Store protocol root binds the store id, founder, schema version, and
the immutable schema-routing contract; open, join, and restore verify it before
touching storage or local state. On first open, Coven creates its complete
internal schema and initialization marker in one SQLite transaction. Later
writer and read-only opens require the marker; a missing or invalid marker fails
the open without recreating metadata. Opening works without a provider — the
store is local-only and complete until one is attached.

A single Store commit may carry an optional Store package and one package per
touched [Circle](/docs/circles) (a private audience inside the store); the
enclosing commit's coordinate is what orders and activates every package it
names. Circle packages have no independent sequence or activation of their own.

## Change capture

A missed write is a silent divergence: two devices disagree and nothing
reports it. If capture meant the host reporting its own changes, every
forgotten call site in every app would be such a miss. So coven owns the
connections and records changes itself; a host write that skips the recording
cannot happen, because the only connection that can write is the one capture
is attached to.

The host opens the store once through
`Coven::builder(config).synced_tables(...).migrations(...).open()`, declaring
its [synced tables](/docs/local-data), and from then on runs all its writes
through `handle.sql(...)`. The writer connection lives on one dedicated
thread (an actor). Each host transaction gets a SQLite session attached to every
declared table. Its insert, update, and delete operations become one changeset;
the session ends with that transaction. The host writes as usual, and there is
no host-lent pointer to a connection coven does not own.

Each declaration also states its row identity. `(table, id)` names one logical
row across every device: independently created rows use canonical UUIDv4 or
UUIDv7 ids, while `SharedKey` tables intentionally merge equal application
keys. Before the host transaction commits, coven validates introduced ids and
records a primary-key change as deletion of the old identity plus insertion of
the new one. The app rows, shared changeset, exact materialized dependency
frontier, stable `WriteId`, affected row identities, and initial `WriteStatus`
commit together or all roll back. `handle.sql` and `handle.write` return a
`WriteReceipt`; separate successful calls never combine into one Store commit.

The set is not a tuning knob. With no tables declared the session attaches
nothing and produces empty changesets forever, so sync initialization treats an
empty set as a hard error and refuses to start.

Initialization also installs signed authorization. A new store publishes its
self-signed Owner founder and causal membership head, then records the founder
and complete accepted head floor, finishing authorization before returning a
runnable session. This applies to opaque and browsable homes; browsable changes
visibility and blob paths, not authorization.

## Reads

Reads don't need capture, and they don't get it. Two read paths exist, and
both hold the invariant above the same way: they run on read-only SQLite
connections, so a write through them is refused by SQLite itself — a read
cannot bypass capture because it cannot write at all.

- **In the host's own process**: `handle.sql_read(...)`. The full handle
  opens a read-only companion connection on the same WAL database, on its own
  thread; a pure read runs there, concurrent with the writer instead of
  queued behind it, with no session attached. Read-your-writes holds for
  committed writes: a `sql_read` after an awaited `sql`/`write` sees that
  data.
- **From a second process (or a second handle)**:
  `Coven::builder(config).synced_tables(...).migrations(...).open_read_only()` returns a
  [`CovenReadHandle`](rustdoc:struct:coven::CovenReadHandle) — a same-store
  reader for something like a macOS File Provider extension that must serve
  reads while the app holds the full handle open. It takes no store lock
  and runs no migrations (it refuses a schema newer than its binary), and it
  exposes reads only: SQL, and blob reads that may fetch from the cloud into
  the device cache. WAL makes the coexistence safe: many readers, one writer,
  each read seeing the last committed state.

The write path polices itself: a `handle.sql(...)` transaction that changed
no rows at all logs a warning — that is a pure read left on the write path;
move it to `sql_read`. A write to a device-local (undeclared) table changes
rows while capturing nothing, which is routine and silent.

## The sync cycle

One background loop runs one cycle at a time. Each cycle loads durable state and:

1. Resolves authorization from causal membership heads, then refreshes
   encryption-key and device-registration state.
1. Drains blob uploads and retries the oldest prepared Store write using its
   persisted exact bytes.
1. Completes ready row-gate transitions and pulls verified remote Store commits.
1. Prepares pending writes in order, uploads and verifies their referenced
   blobs, then appends and verifies their packages and commits, activating each
   with its device head.
1. Applies remote commits whose predecessors and exact dependencies are fully
   materialized; each commit's rows, authorization state, and exact position
   advance atomically.
1. Flushes the register clock, durable file cleanup, acknowledgements, and blob
   deletion work.
1. Evaluates snapshot publication and reclamation against exact commit coverage.

A host transaction's capture session exists only inside that transaction, so a
host write can land during any network operation without joining another write.
Remote applies use the engine's apply path rather than the host transaction path
and therefore never enter the local write ledger.

When Alice edits a todo title, that call already leaves a durable pending write.
Her loop creates encrypted exact objects at
`store-v1/candidates/<family>/packages/<alice-device>/<seq>/<hash>.pkg`,
`store-v1/candidates/<family>/commits/<alice-device>/<seq>/<hash>.json`, and
`store-v1/heads/<alice-device>/<seq>.json`. Bob verifies Alice's head and commit,
waits until the named dependencies are materialized, then atomically applies the
package and records Alice's exact sequence and commit hash. The signed commit
derives `<family>` from its Store, author registration, write identity,
sequence, and predecessor. Its candidate-object manifest must exactly equal the
package and other candidate-exclusive objects reached by its closed body.

### Push

A commit stream is only trustworthy if its sequence numbers never skip and
never change meaning, even across a crash. The durable write record owns the
changeset and dependency frontier from the host commit onward. Preparation
assigns each write its device-stream sequence and predecessor, constructs the
exact signed commit and activation bytes, and persists them before any protocol
append. A retry creates and reads back the same journaled exact objects.

Before an append, the write is `Publishing`. A storage or readback failure puts
it back in `Pending`; the loop's reconnect and backoff policy owns the retry. A
missing blob, a still-local user blob, invalid package data, or invalid Store
protocol state becomes typed durable `Blocked` and holds later writes behind it.
After the head is read back, one SQLite transaction records the exact
`PublishedPosition`, advances the local materialized position, applies owned
cleanup metadata, and clears the prepared bytes from that same write record.

The host lists blocked records with `handle.blocked_writes()`. After repairing
the named prerequisite, `handle.retry_blocked_write(&write_id)` requeues the
blocked records and wakes sync. If the write must be abandoned,
`handle.discard_blocked_write(&write_id)` atomically reverses it and every later
unpublished write whose working rows depend on it. Discarded records remain
queryable with terminal `Resolved(Discarded)` status and no longer participate
in preparation.

A peer must never learn of a row whose file is not yet in the cloud. That
ordering rides the [gate](/docs/local-data), per root, not a global hold: the
cycle publishes whatever the gate emits and never holds the whole changeset
back while uploads drain. A root being made remote stays gated off
(local-only) while its blobs upload. When the last upload lands, coven flips
the gate on and breaks the drain, and the gate re-emits the root's full
subtree in that same cycle. One slow upload therefore delays only its own
root. The host's
[`BlobTransitionObserver`](/docs/blobs#observing-transitions-and-uploads) only
reports progress and completion; coven, not the host, decides when to
publish.

### Pull

Pull lists signed device heads and makes a commit ready only after its
predecessor and every exact dependency are materialized. When a semantic hash
has more than one visible copy, every copy must open to identical bytes, and
multiple valid hashes at one identity are rejected as a fork. Unreachable
immutable commits are inert, and provider listing order never chooses a winner.
For each ready commit, pull:

- parses the signed commit and checks its `schema_version` against the local
  `Database::schema_version`;
- verifies the commit, its device-stream activation head, package hash, and
  Ed25519 signatures;
- checks the author against the membership state through the exact causal
  membership grant the commit names;
- validates every row id under the table's declared identity mode; an invalid id
  holds that exact Store commit without changing rows or its materialized
  position, while other device chains continue;
- applies the package and exact materialized position in one SQLite transaction,
  advancing the clock past its stamps;
- downloads any `CacheEager` blobs it references into the [cache](/docs/cache).

The materialized ledger advances only after the package, bookkeeping, and
required blob work succeed. A failed blob download leaves the exact position
unmaterialized, so the commit is retried; the pull reports this through
`PullResult::asset_downloads_failed`.

A provider or network failure while reading a candidate or blob is a transport
failure and drives `SyncLoopStatus::Offline`. A verified blob whose plaintext
does not match its signed hash is invalid content, and failure to create or
write its local cache destination is a local filesystem failure. Those two
categories hold or fail the affected work without changing the loop to
`Offline`.

### Failure isolation

No single cloud object may stop more than its own device stream. A malformed or
missing object holds that one stream's materialized position and its successors;
every other device's stream keeps flowing.

<svg class="flow" viewBox="0 0 660 176" role="img" aria-label="A malformed commit holds only its own device's materialized position; other streams keep flowing">
<text class="hdr" x="330" y="22" text-anchor="middle">ONE PULL, TWO STREAMS</text>
<rect class="lanec" x="10" y="32" width="640" height="132" rx="10"/>
<rect class="chipo" x="40" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="100" y="69" text-anchor="middle">a/4 · applied</text>
<rect class="chipo" x="180" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="240" y="69" text-anchor="middle">a/5 · applied</text>
<rect class="chipo" x="320" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="380" y="69" text-anchor="middle">a/6 · applied</text>
<text class="sub" x="540" y="69">materialized a=6 ✓</text>
<rect class="chipo" x="40" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11" x="100" y="129" text-anchor="middle">b/7 · applied</text>
<rect class="chipd" x="180" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11" x="240" y="129" text-anchor="middle">b/8 · malformed</text>
<rect class="chipd ghost" x="320" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11 ghost" x="380" y="129" text-anchor="middle">b/9 · not fetched</text>
<text class="sub" x="540" y="129">materialized b=7 · held</text>
</svg>

- A **malformed package or commit** holds that device's position and stops pulling that
  device for the cycle; every other stream proceeds.
- An **invalid signature** (forged or corrupt) does the same, and is surfaced
  as a held Store position so the host can warn.
- A commit whose verified author is **not a write-capable member** under the
  entry it is signed against (revoked, or a read-only Follower) is skipped and
  the materialized position advances past it, surfaced as unauthorized:
  the client must not stay stuck behind an author who will never become valid.
- An **unparseable or forked head** is reported against that device and cannot
  select an alternate candidate by listing order.

## How edits merge

Applying a changeset is its own subject: the hybrid logical clock that orders
edits, the column-level three-way premerge, remove-wins deletes, and the
future-skew bound all live on the [Merge](/docs/merge) page. The cycle's part
is only *when*: each changeset is applied, and the clock advanced past its
stamps, as it lands during pull.

## Schema versioning

Devices upgrade at different times, so two schema versions are routinely live
against one store; the version stamp is what lets them coexist instead of
corrupting each other. Every outgoing Store commit carries the device's schema
version: the top rung of
the host's [migration ladder](/docs/schema-evolution), reported by
`Database::schema_version`.
Pull enforces it two ways:

- **Hard floor.** If the local version is below storage's
  `min_schema_version`, pull returns
  `PullError::SchemaVersionTooOld`
  and syncs nothing. Its `Display` is the message shown to the user: update the
  app to keep syncing. This is permanent until the user upgrades. The floor
  object is untrusted input, so it is honored only when signed by a current
  Owner; anything else is a freeze or downgrade attempt and is ignored.
- **Per-changeset skip.** A single changeset whose `schema_version` is above the
  local one is skipped (counted in `PullResult::skipped_schema`); the device
  leaves its materialized position where it is and stops pulling that device for
  the cycle. The position is deliberately *not* advanced, so once the app
  upgrades the next cycle re-fetches from that sequence and applies it.

How migrations, this version number, the `min_schema_version` floor, and
snapshots fit together, with worked examples for additive vs. structural changes,
is its own page: [Schema evolution](/docs/schema-evolution).

## Lifecycle

`CovenHandle` owns the sync lifecycle. The host calls
`handle.connect_sync()` once a provider is connected; the handle builds the
[cloud home](/docs/storage) and, if sync is enabled, spawns the loop.
`handle.stop_sync()` stops the loop after the in-flight cycle but keeps the
installed manager so `handle.start_sync()` can resume it;
`handle.disconnect_sync()` additionally drops the manager and its cloud
home. `handle.is_syncing()` reports whether the loop thread is running, and
`handle.sync_now()` asks the loop to run a cycle now.

The keys the loop signs and encrypts with are resolved from custody at each
sync start: the OS keyring by default, or whatever preset the store's
[`key_custody`](rustdoc:method:coven::CovenBuilder::key_custody) selected
before `open()` — see [Keys](/docs/keys) for the presets and what each one
protects against. Either way, the host names its keyring service once at
startup with
[`set_keyring_service`](rustdoc:fn:coven::set_keyring_service), which also
installs the platform keyring store (apple-native on macOS and iOS,
android-native on Android, windows-native on Windows; a target with no
bundled store errors). There is no environment-variable or dev-mode key
path.

The loop runs on a dedicated OS thread with its own current-thread tokio runtime.
Database access goes through async calls on the `Database` handle, so the loop
holds nothing tied to a thread; the dedicated thread is for stack size
(aws-sdk-s3's endpoint resolution recurses deeply enough to overflow the default
secondary-thread stack in debug builds). The loop stores the current
[`SyncLoopStatus`](rustdoc:enum:coven::SyncLoopStatus) in a watch channel; the
host observes it with
[`CovenHandle::subscribe_sync_status`](rustdoc:method:coven::CovenHandle::subscribe_sync_status):

```rust
pub enum SyncLoopStatus {
    Offline,
    CheckingStorage,
    Publishing,
    Synchronized(SyncLoopSuccess),
    Blocked { success: SyncLoopSuccess, writes: Vec<PendingWrite> },
    Failed { error: String },
}
```

The receiver immediately contains the current value and survives loop restarts.
Intermediate values may be coalesced, so `Synchronized.row_changes` is a refresh
hint rather than a complete event stream. `Failed` carries a user-facing
message for a whole-cycle failure. `Synchronized` and `Blocked` carry
[`SyncLoopSuccess`](rustdoc:struct:coven::SyncLoopSuccess), including alerts,
device activity, and applied row changes. `Blocked` names writes whose typed
prerequisite prevents publication.

## Backoff

A failing cycle should slow its retries, and a healthy one should not delay
a fresh edit. One exponential formula (`30s · 2^n`) drives the
cycle wait. A successful cycle
waits the base 30 seconds before the next run; each consecutive failure doubles
the wait (60s, 120s, 240s), capped at 300 seconds. A success resets the count,
and `sync_now` preempts the wait.

Provider and network transport errors leave writes retryable, set `Offline`,
and recover through the loop. Remote content mismatch and local blob-filesystem
errors are not connectivity failures; they remain typed failed or held work. A
write whose own package, blob state, or Store protocol state is invalid is
durable `Blocked` and requires `retry_blocked_write` after repair or
`discard_blocked_write`; reconnect does not silently requeue it. The
schema-too-old floor requires an app upgrade, and membership rejection means the
device is no longer a write-capable member.
