# Sync

coven syncs SQLite row changes between devices that share a library. There is
no coordinator: each device appends the changesets it produces to its own
stream in storage and pulls the streams other devices produced. Concurrent
edits merge column by column, and deletes win over concurrent edits. The unit
of exchange is a changeset (a binary diff from SQLite's session extension)
wrapped in a metadata envelope, signed, encrypted, and written under a
per-device sequence number.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 196" role="img" aria-label="Each device appends to its own changeset stream in the cloud; a puller keeps one cursor per stream">
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
</svg>

Nothing in storage is ever overwritten: a device only appends to its own
stream, so there is no write contention to coordinate. A puller tracks one
cursor per stream, the highest sequence it has applied from that device.

Examples use the todos app (workspaces hold lists, lists hold todos, todos
carry attachments and labels); Alice and Bob share the library.

This page covers how a local write reaches every device. Row-level gating
(which rows stay local) has its own page, [Local data](/docs/local-data);
fresh-device bootstrap from a snapshot has its own page,
[Bootstrap](/docs/bootstrap).

## Change capture

The problem: miss a single write and two devices disagree forever, silently.
If capture meant the host reporting its own changes, every forgotten call
site in every app would be one of those silent divergences. So coven doesn't
ask. It owns the connection and records changes itself, and a host write that
skips the recording is impossible rather than discouraged.

The host opens the library once through
`Coven::builder(config).synced_tables(...).migrations(...).open()`, declaring
its [synced tables](/docs/local-data), and from then on runs all its SQL
through `handle.sql(...)`. The connection lives on one dedicated thread (an
actor). Capture is the SQLite session extension, attached to every declared
table on that owned connection: each insert, update, and delete to a synced
table is recorded into an in-memory changeset. The host writes as usual.
Capture is passive, and there is no host-lent pointer to a connection coven
does not own.

The set is not a tuning knob. With no tables declared the session attaches
nothing and produces empty changesets forever, so
[`init_sync_over_storage`](rustdoc:fn:coven::sync::cycle::init_sync_over_storage)
treats an empty set as a hard error and refuses to start.

## The sync cycle

Every guarantee on this page needs a place to live, and that place is one
deterministic loop: the same steps, in the same order, every cycle. A
background loop runs one cycle at a time.
[`run_single_sync_cycle`](rustdoc:fn:coven::sync::cycle::run_single_sync_cycle)
loads the persisted sync state each cycle (rather than holding it across calls)
and drives these steps:

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
   it (a column-level premerge, then row arbitration). The clock advances past
   each applied changeset's stamps as it lands.
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

Blob-before-row ordering rides the gate, owned by coven, not a global push gate.
The cycle publishes whatever the gate emits; it does not hold the whole changeset
back while the outbox drains. A gated root stays gated-off (local-only) while its
blobs upload; coven's `make_remote` flips the gate on the instant the last upload
lands, within the upload drain, and breaks the drain so the cycle publishes that
root. While the gate is off the gate cuts the root's rows; when it flips on the
gate re-emits the root's full subtree, so a peer never learns of a row whose blob
is not yet in the cloud. The host's
[`BlobTransitionObserver`](/docs/blobs#observing-transitions-and-uploads) only
reports progress and completion; coven, not the host, decides when to publish.

### Pull

Pull lists the device heads (one storage call), then for each device whose head
sequence is past the local cursor, fetches the changesets in `(cursor+1..=head)`
order. For each one it:

- unpacks the envelope and checks its `schema_version` against the local
  [`Database::schema_version`](rustdoc:method:coven::database::Database::schema_version);
- verifies the Ed25519 signature
  ([`verify_changeset_signature`](rustdoc:fn:coven::sync::envelope::verify_changeset_signature));
- if the library has a membership chain, checks the author could write under
  the exact membership entry the changeset is signed against;
- applies the changeset (premerge, then row arbitration, with capture disabled
  only around this one apply), advancing the clock past its stamps;
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

## Hybrid logical clocks

The problem: your laptop's clock runs three minutes behind your phone. You
edit a todo on the phone, walk to the desk, and fix a typo in it on the
laptop. Ordered by wall clock, the typo fix is "older" and loses to the edit
it was fixing. Any ordering for edits has to survive clocks that drift, sit
offline for weeks, or lie, and it must guarantee one thing above all: if you
pull my edit and then change it, your change wins. A hybrid logical clock
provides exactly that.

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
never calls this directly. It calls `sql.stamp()` inside `handle.sql` or
`handle.write`, binding the result into every synced-row write. The SQL context
and the sync layer share one `Arc<Hlc>`.

The handle open path seeds that clock before it returns, so every stamp minted
through the handle is already past every value on disk. The floor is
`max(persisted high-water mark, max(_updated_at) scanned across every synced
table)`, so a restart cannot mint a stamp behind a value already written. The
on-disk scan is the authoritative source: the high-water mark is flushed only at
cycle end and lags any local row stamp minted between cycles.

### Advancing past pulled rows

As each changeset applies, the cycle takes the greatest `_updated_at` among its
applied rows and calls
[`advance_past`](rustdoc:method:coven::sync::hlc::Hlc::advance_past), so an
edit made between two applies already sorts after the rows the first apply
landed. The next local stamp then sorts strictly after everything pulled so
far: pull, then edit, and the edit wins.

The advance is bounded the same way arbitration is (below): a stamp the
arbiter refused as grossly future never ratchets the clock either, because
only applied rows feed the advance.

Concretely: Alice creates a todo at her 12:00:00, stamped `...-alice`. Bob
pulls it; his clock advances past Alice's stamp. Bob edits the same todo five
seconds later. Even if Bob's wall clock were behind Alice's, his stamp is
seeded past hers, so it is lexicographically greater. His changeset reaches
Alice, her pull applies it, and his edit wins. Both devices converge on Bob's
version: pull-then-edit wins, whatever the wall clocks say.

## How concurrent edits merge

Two devices edit while apart; both changesets eventually apply everywhere.
Merge runs in two stages inside apply.

**Stage one: column-level three-way premerge.** An UPDATE changeset carries,
per column it changed, the value it moved *from* (the base) and the value it
moved *to*. When an incoming update loses row arbitration, the premerge folds
into the local row every column the incoming update moved away from a base the
local row still holds: the local device never touched that column, so the
incoming edit to it survives. When the incoming update *wins*, it only writes
the columns it changed in the first place. Either way, concurrent edits to
different columns of one row both land.

<svg class="flow" viewBox="0 0 660 190" role="img" aria-label="Base row; phone edits title, laptop edits body; the merged row holds both edits">
<text class="sub" x="330" y="20" text-anchor="middle">base row</text>
<rect class="chip" x="205" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="46" text-anchor="middle">title: “Milk”</text>
<rect class="chip" x="330" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="46" text-anchor="middle">body: “2%”</text>
<line class="arrd" x1="240" y1="62" x2="140" y2="92" marker-end="url(#fam)"/>
<line class="arrd" x1="420" y1="62" x2="520" y2="92" marker-end="url(#fam)"/>
<text class="sub" x="120" y="84" text-anchor="middle">phone edits title</text>
<text class="sub" x="540" y="84" text-anchor="middle">laptop edits body</text>
<rect class="chipa" x="35" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="97" y="116" text-anchor="middle">title: “Milk run”</text>
<rect class="chip" x="160" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="222" y="116" text-anchor="middle">body: “2%”</text>
<rect class="chip" x="375" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="437" y="116" text-anchor="middle">title: “Milk”</text>
<rect class="chipa" x="500" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="562" y="116" text-anchor="middle">body: “oat, 2%”</text>
<line class="arr" x1="160" y1="132" x2="270" y2="158" marker-end="url(#fa)"/>
<line class="arr" x1="500" y1="132" x2="390" y2="158" marker-end="url(#fa)"/>
<text class="sub" x="330" y="148" text-anchor="middle">merge</text>
<rect class="chipa" x="205" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="178" text-anchor="middle">title: “Milk run”</text>
<rect class="chipa" x="330" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="178" text-anchor="middle">body: “oat, 2%”</text>
</svg>

**Stage two: row arbitration.** For every collision the premerge did not fold
in, [`arbitrate_row_conflict`](rustdoc:fn:coven::sync::conflict::arbitrate_row_conflict)
compares the two `_updated_at` stamps and the later writer wins. Concurrent
edits to the *same* column therefore resolve to the later stamp. The
`_updated_at` column index is read from `PRAGMA table_info` at apply time, so
adding columns to the end of a table stays safe.

Two special cases:

- **Deletes are remove-wins.** A hard delete carries only the row's pre-delete
  stamp and cannot be reconstructed from a later partial update, so an
  incoming delete always wins, and an incoming update targeting a locally
  deleted row is dropped. The row stays gone.
- **Grossly-future stamps are refused.** A member is trusted, so arbitration is
  robustness, not a security boundary; still, a buggy client or broken clock
  could stamp a row far in the future and win every conflict forever. The
  receiver bounds an incoming stamp to its own wall clock plus an offline
  allowance
  ([`MAX_FUTURE_SKEW_MS`](rustdoc:const:coven::sync::hlc::MAX_FUTURE_SKEW_MS),
  30 days) and refuses to let a grossly-future stamp win or ratchet its clock.

### Constraints and foreign keys

A child row can arrive in a changeset whose parent is in a different device's
changeset, not yet applied. The child's insert violates a foreign key and is
dropped on the first pass. Pull collects every such changeset and retries each
once after the first pass over all devices completes, by which point the parent
rows exist. If a changeset still violates a foreign key after the retry, it is
logged and skipped.

Non-foreign-key constraint conflicts (a uniqueness violation, a CHECK failure)
are different: retrying cannot make them valid, so the conflicting rows are
omitted, the affected tables are surfaced in
`ApplyResult::constraint_conflict_tables`, and the changeset is not retried.

## Schema versioning

Devices upgrade at different times, so two schema versions are routinely live
against one library; the version stamp is what lets them coexist instead of
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
  object is untrusted input, so with a membership chain present it is honored
  only when signed by a current Owner; anything else is a freeze or downgrade
  attempt and is ignored.
- **Per-changeset skip.** A single changeset whose `schema_version` is above the
  local one is skipped (counted in `PullResult::skipped_schema`); the device
  leaves its cursor where it is and stops pulling that device for the cycle. The
  cursor is deliberately *not* advanced, so once the app upgrades the next cycle
  re-fetches from that sequence and applies it.

How migrations, this version number, the `min_schema_version` floor, and
snapshots fit together, with worked examples for additive vs. structural changes,
is its own page: [Schema evolution](/docs/schema-evolution).

## Lifecycle

`CovenHandle` owns the sync lifecycle natively. The host calls
`handle.connect_sync(...)` once a provider is connected; the handle builds the
cloud home and, if sync is enabled, spawns the loop. `handle.stop_sync()` drops
the loop handle and cloud home, `handle.is_syncing()` reports whether the loop
thread is running, and `handle.sync_now()` asks the loop to run a cycle now.

The keys the loop signs and encrypts with come from the OS keyring. The host
installs the keyring service and identity at startup with
[`set_keyring_service`](rustdoc:fn:coven::keys::set_keyring_service); there is no
environment-variable or dev-mode key path.

The loop runs on a dedicated OS thread with its own current-thread tokio runtime.
Database access goes through async calls on the `Database` handle, so the loop
holds nothing tied to a thread; the dedicated thread is for stack size
(aws-sdk-s3's endpoint resolution recurses deeply enough to overflow the default
secondary-thread stack in debug builds). After each cycle it emits a
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
([`RowChange`](rustdoc:struct:coven::changeset::RowChange)) for the host to map
to its own domain events.

## Backoff

A failing cycle should not hammer a struggling provider, and a healthy one
should not lag a fresh edit. One exponential formula (`30s · 2^n`) drives the
cycle wait. A successful cycle
waits the base 30 seconds before the next run; each consecutive failure doubles
the wait (60s, 120s, 240s), capped at 300 seconds. A success resets the count,
and a manual `trigger_sync` preempts the wait.

Most cycle errors are transient (network, a failed blob download) and recover on
the next cycle. Two are permanent: the schema-too-old floor (the user must
upgrade) and a membership rejection (the device is no longer a write-capable
member).
