# Sync

coven syncs SQLite row changes between devices that share a store. There is
no coordinator: each device appends the changesets it produces to its own
stream in storage and pulls the streams other devices produced. Concurrent
edits merge column by column, and deletes win over concurrent edits. The unit
of exchange is a changeset (a binary diff from SQLite's session extension)
wrapped in a metadata envelope, signed, encrypted, and written under a
per-device sequence number.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 224" role="img" aria-label="A write is captured and sealed, appends to the device's own stream, and peers pull it, advancing one cursor per stream">
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
<text class="lbl s11" x="600" y="75" text-anchor="middle">cursor a=3</text>
<text class="lbl s11" x="600" y="144" text-anchor="middle">cursor b=2</text>
<circle class="numc" cx="24" cy="71" r="8"/>
<text class="num" x="24" y="74.5" text-anchor="middle">1</text>
<circle class="numc" cx="268" cy="71" r="8"/>
<text class="num" x="268" y="74.5" text-anchor="middle">2</text>
<circle class="numc" cx="600" cy="48" r="8"/>
<text class="num" x="600" y="51.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="212" text-anchor="middle">1 a write is captured and sealed · 2 it appends to this device's own stream · 3 peers pull, one cursor per stream</text>
</svg>

Nothing in storage is ever overwritten: a device only appends to its own
stream, so there is no write contention to coordinate. A puller tracks one
cursor per stream, the highest sequence it has applied from that device.

Examples use the todos app (workspaces hold lists, lists hold todos, todos
carry attachments and labels); Alice and Bob share the store.

This page covers how a local write reaches every device. Row-level gating
(which rows stay local) has its own page, [Local data](/docs/local-data);
fresh-device bootstrap from a snapshot has its own page,
[Bootstrap](/docs/bootstrap).

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
thread (an actor). Capture is the SQLite session extension, attached to every
declared table on that owned connection: each insert, update, and delete to a
synced table is recorded into an in-memory changeset. The host writes as
usual. Capture is passive, and there is no host-lent pointer to a connection
coven does not own.

Each declaration also states its row identity. `(table, id)` names one logical
row across every device: independently created rows use canonical UUIDv4 or
UUIDv7 ids, while `SharedKey` tables intentionally merge equal application
keys. Before the host transaction commits, coven validates introduced ids and
records a primary-key change as deletion of the old identity plus insertion of
the new one. The rows and pending journal commit together or both roll back.

The set is not a tuning knob. With no tables declared the session attaches
nothing and produces empty changesets forever, so
[`init_sync_over_storage`](rustdoc:fn:coven::sync::cycle::init_sync_over_storage)
treats an empty set as a hard error and refuses to start.

Initialization also requires a signed, owner-anchored membership chain. A new
store publishes its self-signed Owner founder and signed head, then records the
founder and complete accepted head floor together before it returns a runnable
session. This applies to both opaque and browsable cloud homes; browsable changes
object visibility and blob path naming, not authorization.

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
  `Coven::builder(config).open_read_only()` returns a
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

Everything this page promises is enforced in one loop that runs the same
steps in the same order every cycle. A background loop runs one cycle at a
time.
The initialized sync session loads the persisted sync state each cycle (rather
than holding it across calls) and drives these steps:

1. Capture the outgoing changeset and reset the recorded batch. Capture stays
   enabled: a host write that lands later in this cycle is recorded into the next
   batch, not lost.
2. Apply row-level gating to the captured changeset, cutting rows that should
   stay local (see [Local data](/docs/local-data)).
3. Upload any blobs the outgoing changeset references, so a puller can fetch them
   the moment it sees the change (see [Blobs](/docs/blobs)).
4. Sign the envelope, stage the packed bytes to disk, and push them to storage
   under the device's next sequence number; on success advance `local_seq`.
5. Pull every remote changeset past the device's cursor, validate it, and apply
   it: the [merge](/docs/merge) runs a column-level premerge, then row
   arbitration, and the clock advances past each applied changeset's stamps
   as it lands.
6. Persist the updated cursors and flush the clock's high-water mark.
7. Check snapshot policy.

Capture is never suspended across the cycle. The only window it is off is around
each individual apply in step 5: the pull disables the session, applies one
incoming changeset synchronously, and re-enables it at once, so the applied rows
are not re-recorded as this device's own writes while a host write landing
anywhere else in the cycle still is. This is the one thing the session is ever
blind to; every other read and write goes through the handle SQL path on the
normal enabled path. (An earlier design suspended capture across the whole
network span; that left a window in which a host write could be dropped, so it
was removed.)

When Alice edits a todo title, her next cycle captures the update to `todos`,
signs and encrypts it, and writes it to storage at
`changes/<alice-device>/<seq>`. Bob's device, on its own cycle, lists the device
heads, sees Alice's sequence number is past his cursor for her device, fetches
the changeset, and applies it.

### Push

A device's stream is only trustworthy if its sequence numbers never skip and
never change meaning, even across a crash. So push stages the changeset bytes
to a file before uploading. If the upload fails,
the bytes survive on disk and `staged_seq` is persisted, so the next cycle
retries the same sequence number rather than skipping it. After a successful
push the staged file is cleared and `local_seq` advances. A device's sequence
numbers start at 1; the first changeset is `local_seq + 1` over an initial
`local_seq` of 0.

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

Pull lists the device heads (one storage call), then for each device whose head
sequence is past the local cursor, fetches the changesets in `(cursor+1..=head)`
order. For each one it:

- unpacks the envelope and checks its `schema_version` against the local
  [`Database::schema_version`](rustdoc:method:coven::database::Database::schema_version);
- verifies the Ed25519 signature
  ([`verify_changeset_signature`](rustdoc:fn:coven::sync::envelope::verify_changeset_signature));
- checks the author could write under the exact membership entry the changeset
  is signed against;
- validates every row id under the table's declared identity mode; an invalid id
  holds that exact Store commit without changing rows or its materialized
  position, while other device chains continue;
- applies the changeset (the [merge](/docs/merge), with capture disabled only
  around this one apply), advancing the clock past its stamps;
- downloads any `CacheEager` blobs it references into the [cache](/docs/cache).

The cursor for that device advances to a sequence number only after the
changeset is accepted (or deliberately skipped) and its blobs downloaded. A
failed blob download leaves the cursor where it is, so the changeset re-pulls
next cycle; the pull reports this through
`PullResult::asset_downloads_failed`.

### One bad object stops one stream

The failure rule throughout pull: no single cloud object may stop more than
its own stream.

<svg class="flow" viewBox="0 0 660 176" role="img" aria-label="A malformed changeset holds only its own device's cursor; other streams keep flowing">
<text class="hdr" x="330" y="22" text-anchor="middle">ONE PULL, TWO STREAMS</text>
<rect class="lanec" x="10" y="32" width="640" height="132" rx="10"/>
<rect class="chipo" x="40" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="100" y="69" text-anchor="middle">a/4 · applied</text>
<rect class="chipo" x="180" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="240" y="69" text-anchor="middle">a/5 · applied</text>
<rect class="chipo" x="320" y="52" width="120" height="26" rx="6"/>
<text class="lbl s11" x="380" y="69" text-anchor="middle">a/6 · applied</text>
<text class="sub" x="540" y="69">cursor a=6 ✓</text>
<rect class="chipo" x="40" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11" x="100" y="129" text-anchor="middle">b/7 · applied</text>
<rect class="chipd" x="180" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11" x="240" y="129" text-anchor="middle">b/8 · malformed</text>
<rect class="chipd ghost" x="320" y="112" width="120" height="26" rx="6"/>
<text class="lbl s11 ghost" x="380" y="129" text-anchor="middle">b/9 · not fetched</text>
<text class="sub" x="540" y="129">cursor b=7 · held</text>
</svg>

- A **malformed envelope** holds that device's cursor and stops pulling that
  device for the cycle; every other stream proceeds.
- An **invalid signature** (forged or corrupt) does the same, and is surfaced
  in `PullResult::invalid_signatures` so the host can warn.
- A changeset whose verified author is **not a write-capable member** under the
  entry it is signed against (revoked, or a read-only Follower) is skipped and
  the cursor advances past it, surfaced in `PullResult::rejected_unauthorized`:
  the client must not stay stuck behind an author who will never become valid.
- An **unparseable head object** is skipped like a bad-signature head; it never
  wedges the listing.

## How edits merge

Applying a changeset is its own subject: the hybrid logical clock that orders
edits, the column-level three-way premerge, remove-wins deletes, and the
future-skew bound all live on the [Merge](/docs/merge) page. The cycle's part
is only *when*: each changeset is applied, and the clock advanced past its
stamps, as it lands during pull.

## Schema versioning

Devices upgrade at different times, so two schema versions are routinely live
against one store; the version stamp is what lets them coexist instead of
corrupting each other. Every outgoing changeset carries the device's schema
version: the top rung of
the host's [migration ladder](/docs/schema-evolution), reported by
[`Database::schema_version`](rustdoc:method:coven::database::Database::schema_version).
Pull enforces it two ways:

- **Hard floor.** If the local version is below storage's
  `min_schema_version`, pull returns
  [`PullError::SchemaVersionTooOld`](rustdoc:enum:coven::sync::pull::PullError)
  and syncs nothing. Its `Display` is the message shown to the user: update the
  app to keep syncing. This is permanent until the user upgrades. The floor
  object is untrusted input, so it is honored only when signed by a current
  Owner; anything else is a freeze or downgrade attempt and is ignored.
- **Per-changeset skip.** A single changeset whose `schema_version` is above the
  local one is skipped (counted in `PullResult::skipped_schema`); the device
  leaves its cursor where it is and stops pulling that device for the cycle. The
  cursor is deliberately *not* advanced, so once the app upgrades the next cycle
  re-fetches from that sequence and applies it.

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
secondary-thread stack in debug builds). Each cycle emits a
[`SyncLoopStatus`](rustdoc:enum:coven::SyncLoopStatus) over a broadcast channel;
the host observes the stream with
[`CovenHandle::subscribe_sync_status`](rustdoc:method:coven::CovenHandle::subscribe_sync_status):

```rust
pub enum SyncLoopStatus {
    Started,
    Succeeded(SyncLoopSuccess),
    Failed { error: String },
}
```

`Failed` carries a user-facing message for a whole-cycle failure. `Succeeded`
carries [`SyncLoopSuccess`](rustdoc:struct:coven::SyncLoopSuccess), including
alerts, device activity, and applied row changes for the host to map to its own
domain events.

## Backoff

A failing cycle should slow its retries, and a healthy one should not delay
a fresh edit. One exponential formula (`30s · 2^n`) drives the
cycle wait. A successful cycle
waits the base 30 seconds before the next run; each consecutive failure doubles
the wait (60s, 120s, 240s), capped at 300 seconds. A success resets the count,
and a manual `trigger_sync` preempts the wait.

Most cycle errors are transient (network, a failed blob download) and recover on
the next cycle. Two are permanent: the schema-too-old floor (the user must
upgrade) and a membership rejection (the device is no longer a write-capable
member).
