# Sync

coven syncs SQLite row changes between devices that share a store. It has one
protocol: devices capture writes locally, publish them through one accepted
Store history, and pull that history. Concurrent edits merge column by column with
deletes winning over concurrent edits. The unit of exchange is one host
transaction: its SQLite changeset becomes a Store package named by an exact
signed commit.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 238" role="img" aria-label="Local writes become immutable candidates. Conditional replacement of the current record accepts one publication order, which peers verify and replay.">
<text class="hdr" x="100" y="22" text-anchor="middle">LOCAL DEVICE</text>
<text class="hdr" x="350" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="580" y="22" text-anchor="middle">PEER</text>
<rect class="lane" x="10" y="32" width="180" height="170" rx="10"/>
<rect class="lanec" x="210" y="32" width="280" height="170" rx="10"/>
<rect class="lane" x="510" y="32" width="140" height="170" rx="10"/>
<rect class="chip" x="25" y="56" width="150" height="30" rx="7"/>
<text class="lbl s11" x="100" y="75" text-anchor="middle">durable write journal</text>
<text class="sub" x="100" y="111" text-anchor="middle">works offline</text>
<text class="sub" x="100" y="148" text-anchor="middle">prepare exact candidate</text>
<line class="arr" x1="194" y1="71" x2="223" y2="71" marker-end="url(#fa)"/>
<rect class="chipo" x="230" y="56" width="240" height="30" rx="7"/>
<text class="lbl s11" x="350" y="75" text-anchor="middle">immutable package + commit + entry</text>
<line class="arr" x1="350" y1="90" x2="350" y2="114" marker-end="url(#fa)"/>
<rect class="chip" x="230" y="120" width="240" height="30" rx="7"/>
<text class="lbl s11" x="350" y="139" text-anchor="middle">conditionally replace current record</text>
<text class="sub" x="350" y="181" text-anchor="middle">one accepted publication order</text>
<line class="arr" x1="494" y1="135" x2="523" y2="135" marker-end="url(#fa)"/>
<text class="lbl s11" x="580" y="75" text-anchor="middle">verify accepted history</text>
<text class="lbl s11" x="580" y="135" text-anchor="middle">replay causal changes</text>
<text class="sub" x="580" y="181" text-anchor="middle">commit rows + position</text>
<text class="sub" x="330" y="228" text-anchor="middle">Uploading a candidate does not publish it. Acceptance is the conditional record update.</text>
</svg>

Packages, commits, and publication entries occupy exact immutable slots. One
mutable current record, at the location bound by the signed Store root, names
the accepted history. Replacing that record requires the provider revision read
with its previous value. A competing publisher must observe and verify the
accepted change before retrying; uploaded candidates outside that history have
no effect. A puller records exact commit hashes with its materialized positions.

Examples use the todos app (workspaces hold lists, lists hold todos, todos
carry attachments and labels); Alice and Bob share the store.

This page covers how a local write reaches every device. Row-level gating
(which rows stay local) has its own page, [Local data](/docs/local-data);
fresh-device bootstrap from a snapshot has its own page,
[Bootstrap](/docs/bootstrap).

## One protocol

There is one protocol and no mode to select. A commit keeps its author's
sequence, exact predecessor, and causal dependencies. The accepted publication
order answers whether a candidate became shared and which snapshot boundary it
belongs to; causal dependencies still govern replay of row changes. A snapshot
closes the complete accepted prefix before it, so a later publication cannot
extend retired history.

Offline writes remain in the local journal. Publishing requires storage with
both create-once exact slots and provider-enforced conditional replacement.
The provider compares revisions; Coven constructs and verifies the signed
history and its authorization.

The signed Store protocol root binds the store id, founder, schema version, and
the immutable schema-routing contract; open, join, and restore verify it before
touching storage or local state. On first open, Coven creates its complete
internal schema, version ledger, and initialization marker in one SQLite
transaction. Later writer and read-only opens require the marker and an exact
known internal schema manifest. Writers apply or refuse pending Coven migrations
according to the host's explicit policy; readers always refuse them. A missing
or invalid marker fails the open without recreating metadata. Opening works
without a provider — the store is local-only and complete until one is attached.

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
`Coven::builder(store_dir, config).synced_tables(...).coven_migration_policy(...).migrations(...).open()`, declaring
its [synced tables](/docs/local-data), and from then on runs all its writes
through `handle.write(...)`. The writer connection lives on one dedicated
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
commit together or all roll back. `handle.write` returns a `WriteReceipt`;
separate successful calls never combine into one Store commit.

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

- **In the host's own process**: `handle.read(...)`. The full handle owns
  four read-only connections with one worker per connection. Independent reads
  can run concurrently with each other and the writer, with no change-capture
  session attached. Each read uses one transaction; all statements in that
  closure see the same snapshot. A `read` after an awaited `write` sees that
  committed data.
- **From a second process (or a second handle)**:
  `Coven::builder(store_dir, config).synced_tables(...).migrations(...).open_read_only()` returns a
  [`CovenReadHandle`](rustdoc:struct:coven::CovenReadHandle) — a same-store
  reader for something like a macOS File Provider extension that must serve
  reads while the app holds the full handle open. It takes no store lock
  and runs no migrations (it refuses pending Coven migrations and a host schema
  newer than its binary), and it
  exposes reads only: SQL, and blob reads that may fetch from the cloud into
  the device cache. SQLite's locking coordinates the readers with the writer,
  and each new read transaction sees committed state.

Application reads and live queries share a FIFO queue holding at most 64
waiting operations. A full queue makes callers wait for admission. Cancelling
an operation before it starts discards its closure; a running read finishes
and its result is discarded if the caller has gone away. Write dispatch keeps
its separate contract: a dispatched write can commit even after its caller
stops awaiting it.

`handle.read(fetch).process(transform).await` separates SQL from expensive
processing. The first closure fetches owned values in one transaction. Once that
transaction ends and the connection is available, a separate pool of four workers processes
the values, with its own queue of at most 64 waiting operations. Processing
receives no SQL context: all database inputs belong in the first closure.

`handle.subscribe(fetch).process(transform)` and
`handle.subscribe_reconfigurable(request, fetch).process(transform)`
apply the same split to live queries. The extracted values retain their exact
read dependencies through processing, including processing errors. Relevant
commits arriving during processing remain available for the next query run;
a result for a superseded request is discarded before delivery.

For a reconfigurable subscription, `fetch` receives `(&request, sql)` and
`transform` receives `(&request, fetched)`, both for the same request revision.
Only the processed value needs `Clone` and `PartialEq` for delivery; fetched
values can be owned types that support neither. Adding processing to a
subscription starts a fresh sequence of deliveries while retaining its request
handles.

The write path polices itself: a `handle.write(...)` callback that prepares no
`INSERT`, `UPDATE`, or `DELETE` statement is rejected as a pure read on the
write path; move it to `read`. A write to a device-local (undeclared) table is
still a write even though its rows do not enter a synced changeset.

## The sync cycle

One background loop runs one cycle at a time. Each cycle loads durable state and:

1. Resolves authorization from accepted history, then refreshes
   encryption-key and device-registration state.
1. Drains blob uploads and retries the oldest prepared Store write using its
   persisted exact bytes.
1. Completes ready row-gate transitions and pulls verified remote Store commits.
1. Prepares pending writes in order, uploads and verifies their referenced
   blobs, then creates their exact packages, commits, and publication entries.
   Conditional replacement of the current record accepts each publication.
1. Applies remote commits whose predecessors and exact dependencies are fully
   materialized. Installation commits rows, authorization state, and exact
   positions together with replay of retained local writes.
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
`store-v1/candidates/<family>/commits/<alice-device>/<seq>/<hash>.json`.
The publication entry names that exact commit; the conditional record update
makes it part of accepted history. Bob verifies that acceptance and Alice's
commit, waits until the named dependencies are materialized, then applies the
package and records Alice's exact sequence and commit hash. The signed commit
derives `<family>` from its Store, author registration, write identity,
sequence, and predecessor. Its candidate-object manifest must exactly equal the
package and other candidate-exclusive objects reached by its closed body.

### Push

A commit stream is only trustworthy if an accepted sequence never changes
meaning, even across a crash. The durable write record owns the original write
and its stable `WriteId`. Preparation reserves the author's sequence, constructs
the exact signed candidate and publication attempt, and persists them before
upload. Retrying an unchanged attempt uses those same exact objects.

If another publisher wins, Coven verifies and installs that accepted history.
Within the same snapshot boundary it can prepare a new publication entry for
the existing commit. Crossing a snapshot boundary requires rebuilding the
unaccepted candidate against the new baseline while retaining the write identity
and author reservation. A lost response is settled by reading accepted history;
the presence of an uploaded package alone never proves publication.

Before an append, the write is `Publishing`. A storage or readback failure puts
it back in `Pending`; the loop's reconnect and backoff policy owns the retry. A
missing blob, a still-local user blob, invalid package data, or invalid Store
protocol state becomes typed durable `Blocked` and holds later writes behind it.
After acceptance is verified, local completion records `PublishedWrite::Commit`
with the exact commit, or `PublishedWrite::Snapshot` when accepted snapshot
coverage proves completion after the commit's history has retired. The latter
names the reserved author position and accepted snapshot without inventing an
exact candidate hash. Publication evidence, replay state, owned cleanup metadata,
and the write's completion are recorded atomically.

The host lists blocked records with `handle.blocked_writes()`. After repairing
the named prerequisite, `handle.retry_blocked_write(&write_id)` requeues the
blocked records and wakes sync. If the write must be abandoned,
`handle.discard_blocked_write(&write_id)` atomically reverses it and every later
unpublished write whose working rows depend on it. Discarded records remain
queryable with terminal `Resolved(Discarded)` status and no longer participate
in preparation.

A retained private-only write that conflicts with accepted shared history becomes
`LocalOnlyBlocked(RebaseConflict)`. The conflict identifies its `WriteId`, affected
rows, and reason. The failed apply preserves the private write and its dependent
suffix. `retry_blocked_write` returns that write to `LocalOnly`, so it remains
private; explicit discard reverses the dependent unpublished suffix as above.
Private rows already folded into the baseline have no remaining write receipt;
their conflicts report the row and accepted commit without inventing a `WriteId`.

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

Pull reads the current publication record at its root-bound location and
verifies the immutable accepted entries back to its installed boundary. If
history has been retired, it prepares the accepted snapshot and retained
evidence needed to replace that baseline. A commit becomes ready only after its
predecessor and every exact dependency are materialized. Unaccepted immutable
candidates are inert, and provider listing order never chooses a winner.
For each ready commit, pull:

- parses the signed commit and checks its `schema_version` against the local
  `Database::schema_version`;
- verifies the commit's publication acceptance, package hash, and
  Ed25519 signatures;
- checks the author against the membership state through the exact causal
  membership grant the commit names;
- validates every row id under the table's declared identity mode; an invalid id
  holds that exact Store commit without advancing its materialized position;
- prepares the package for atomic installation with exact materialized
  positions, advancing the clock past its stamps.

The materialized ledger advances only after package installation and its
bookkeeping succeed. Background eager-cache filling has its own progress and
failure state; it does not make a committed row apply wait for a cache download.
Join and restore have their own required blob work before returning a store;
see [Bootstrap](/docs/bootstrap).

A provider or network failure while reading a candidate or blob is a transport
failure and drives `SyncLoopStatus::Offline`. A verified blob whose plaintext
does not match its signed hash is invalid content, and failure to create or
write its local cache destination is a local filesystem failure. Those two
categories hold or fail the affected work without changing the loop to
`Offline`.

### Failure boundaries

Publication acceptance and local materialization are different facts. A commit
can be accepted but held locally because its schema, package, dependencies, or
required files cannot be applied. Pull reports the exact held coordinate and
reason. It never advances that commit's materialized position to hide the error.

<svg class="flow" viewBox="0 0 660 194" role="img" aria-label="Accepted publications are prepared for one database transaction. Successful installation commits rows and positions together; a held installation preserves the previous state.">
<text class="hdr" x="330" y="22" text-anchor="middle">ACCEPTANCE AND LOCAL INSTALLATION</text>
<rect class="lanec" x="10" y="36" width="190" height="118" rx="10"/>
<text class="lbl s11" x="105" y="69" text-anchor="middle">accepted publications</text>
<text class="sub" x="105" y="96" text-anchor="middle">verify + prepare packages</text>
<line class="arr" x1="204" y1="94" x2="238" y2="94" marker-end="url(#fa)"/>
<rect class="chip" x="245" y="62" width="165" height="64" rx="8"/>
<text class="lbl s11" x="327" y="87" text-anchor="middle">database transaction</text>
<text class="sub" x="327" y="108" text-anchor="middle">rows + positions + local replay</text>
<line class="arr" x1="414" y1="77" x2="449" y2="65" marker-end="url(#fa)"/>
<line class="arr" x1="414" y1="111" x2="449" y2="135" marker-end="url(#fa)"/>
<rect class="chipo" x="455" y="46" width="195" height="38" rx="7"/>
<text class="lbl s11" x="552" y="70" text-anchor="middle">applied → commit together</text>
<rect class="chipd" x="455" y="116" width="195" height="38" rx="7"/>
<text class="lbl s11" x="552" y="140" text-anchor="middle">held → preserve prior state</text>
<text class="sub" x="330" y="181" text-anchor="middle">A failed installation does not leave some of its rows or positions committed.</text>
</svg>

Invalid accepted-history evidence can fail the pull before package preparation.
Package preparation can hold individual commits and their dependents. Once
prepared work reaches database installation, a constraint or private/shared
replay conflict rolls that transaction back; earlier rows in that transaction
are not reported as applied. Snapshot adoption also preserves the previous
installed state if its replacement cannot be committed.

There is no promise that a malformed accepted object affects only one author's
stream: dependencies and snapshot boundaries connect the accepted history.
Authorization failure is not permission to skip a commit and advance past it.

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
- **Per-package hold.** A Store package whose `schema_version` is above the
  local one produces `HeldStorePositionReason::NewerSchema` in the pull's
  `held_positions`. Its materialized position does not advance, and dependent
  work waits. After an app upgrade, pull can prepare and apply the package.

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
android-native on Android, windows-native on Windows, and Secret Service on
Linux). There is no environment-variable or dev-mode key path.

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
    Blocked { success: SyncLoopSuccess, operations: Vec<BlockedOperation> },
    Failed { error: SyncLoopFailure },
}
```

The receiver immediately contains the current value and survives loop restarts.
Intermediate values may be coalesced, so `Synchronized.row_changes` is a refresh
hint rather than a complete event stream. `Failed` preserves the typed cause
of a whole-cycle failure and its display message. `Synchronized` and `Blocked` carry
[`SyncLoopSuccess`](rustdoc:struct:coven::SyncLoopSuccess), including alerts,
device activity, and applied row changes. `Blocked` names host writes, Circle
operations, and reclaim operations whose typed prerequisites prevent progress.

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
