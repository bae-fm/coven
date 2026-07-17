# Overview

Sync normally means a backend: a server you run and pay for, and a database
that holds every user's data in the clear.

coven syncs without the server. Devices exchange end-to-end-encrypted changes
through storage the user already has, and merge them locally.

The data is SQLite. You keep your schema; coven owns the connections and runs
your queries through them, so it can
[capture](https://www.sqlite.org/sessionintro.html) each change with SQLite's
session extension, encrypt and sign it, move it through the user's storage,
and apply remote changes back.

## The round trip

No server is needed because nothing in the loop below requires one. A write
is captured, sealed, and parked in storage; every other device picks it up
from there. The storage never has to understand the data, which is what lets
it be storage the user already has.

<div style="margin: 1.5rem 0; padding: 24px 28px; background: var(--vp-code-block-bg); border-radius: 8px;">
<svg viewBox="11 18 666 232" width="100%" role="img" aria-label="On your device, your app's write is captured, signed, and encrypted by coven, then pushed to storage you own. On a teammate's device, coven pulls it back, verifies, decrypts, and applies it into their app." style="font-family: var(--vp-font-family-mono); overflow: visible;">
  <defs>
    <marker id="rt-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" style="fill: var(--coven-a);" />
    </marker>
  </defs>

  <!-- app containers: the host app, with coven nested inside it -->
  <rect x="12" y="24"  width="424" height="92" rx="12" style="fill: none; stroke: var(--vp-c-divider);" />
  <text x="28" y="42" style="fill: var(--vp-c-text-2); font-size: 11px; letter-spacing: 0.5px;">your app</text>
  <text x="330" y="42" text-anchor="middle" style="fill: var(--coven-a); font-size: 11px; letter-spacing: 0.5px;">coven</text>

  <rect x="12" y="156" width="424" height="92" rx="12" style="fill: none; stroke: var(--vp-c-divider);" />
  <text x="28" y="174" style="fill: var(--vp-c-text-2); font-size: 11px; letter-spacing: 0.5px;">their app</text>
  <text x="330" y="174" text-anchor="middle" style="fill: var(--coven-a); font-size: 11px; letter-spacing: 0.5px;">coven</text>

  <!-- app boxes (host code, tinted fill to set apart from coven/storage) -->
  <rect x="28"  y="52"  width="132" height="52" rx="8" style="fill: var(--vp-c-tip-soft); stroke: var(--vp-c-divider);" />
  <rect x="28"  y="184" width="132" height="52" rx="8" style="fill: var(--vp-c-tip-soft); stroke: var(--vp-c-divider);" />

  <!-- coven boxes (accent-bordered = the coven layer) -->
  <rect x="236" y="52"  width="188" height="52" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--coven-a); stroke-opacity: 0.55;" />
  <rect x="236" y="184" width="188" height="52" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--coven-a); stroke-opacity: 0.55;" />

  <!-- storage (remote, shared) -->
  <rect x="508" y="52" width="168" height="184" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--vp-c-divider); stroke-dasharray: 5 4;" />

  <!-- arrows -->
  <line x1="160" y1="78"  x2="232" y2="78"  style="stroke: var(--coven-a); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="424" y1="78"  x2="504" y2="78"  style="stroke: var(--coven-a); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="508" y1="210" x2="428" y2="210" style="stroke: var(--coven-a); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="236" y1="210" x2="164" y2="210" style="stroke: var(--coven-a); stroke-width: 2;" marker-end="url(#rt-arrow)" />

  <!-- labels -->
  <text x="94"  y="83"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">INSERT todo</text>

  <text x="330" y="74"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">capture changes</text>
  <text x="330" y="92"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">sign + encrypt</text>

  <text x="592" y="138" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">storage you own</text>
  <text x="592" y="160" text-anchor="middle" style="fill: var(--vp-c-text-2); font-size: 12px;">▒ ciphertext ▒</text>

  <text x="330" y="206" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">verify + decrypt</text>
  <text x="330" y="224" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">apply changes</text>

  <text x="94"  y="215" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">refresh UI</text>
</svg>
</div>

The provider only ever holds ciphertext. It never sees a todo title, a file,
or who is allowed to write.

## In your code

The integration is one builder call and a handful of methods on the handle it
returns. Two beats give the flavor; the whole tour, from open to invite, is
the [Example](/docs/example).

**Open the store.** Declare the tables that sync and the migration ladder
that builds your schema; `open` installs or verifies Coven's one current
internal schema, runs your ladder, and returns one handle. Tables you don't
list stay local to the device.

```rust
use coven::{Coven, Migration, RowIdentity, SyncedTable, WritePolicy};

let handle = Coven::builder(config)
    .write_policy(WritePolicy::MergeConcurrent)
    .synced_tables(vec![
        SyncedTable::new("todos", RowIdentity::IndependentUuid),
        SyncedTable::new("todo_attachments", RowIdentity::IndependentUuid),
    ])
    .migrations(vec![Migration::sql(1, "initial", MY_SCHEMA)])
    .open()?;
```

The identity argument states what equal ids mean across devices. Use
`IndependentUuid` with canonical UUIDv4 or UUIDv7 ids for independently created
rows. Use `SharedKey` only when equal application keys intentionally name and
merge as one logical row. Changing a primary key removes the old identity and
inserts the new validated identity atomically.

**Write normally, through the handle.** Your closure gets a transaction;
coven captures what changed when it commits. Synced rows carry an
`_updated_at` you mint with `sql.stamp()`; that stamp is how edits order
across devices.

```rust
let id = uuid::Uuid::new_v4().to_string();
let receipt = handle.sql(move |sql| {
    sql.execute(
        "INSERT INTO todos (id, title, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params![id, title, sql.stamp()],
    )?;
    Ok(())
}).await?;
```

The returned `WriteReceipt` names this transaction in coven's durable write
ledger. Its initial status is `LocalOnly` when the transaction changed no shared
rows, otherwise `Pending`. Separate calls produce separate write ids and Store
commits.

**Read on the read connection.** Pure reads go through `handle.sql_read`,
which runs on a read-only companion connection: no change capture, and reads
run concurrently with the writer instead of queuing behind it. The connection
is read-only at the SQLite layer, so a write inside the closure is refused. A
read issued after an awaited write sees that write.

```rust
let titles: Vec<String> = handle.sql_read(|conn| {
    let mut stmt = conn.prepare("SELECT title FROM todos ORDER BY _updated_at")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    Ok(rows.collect::<Result<_, _>>()?)
}).await?;
```

Everything else follows the same ownership boundary: `handle.write` commits a
row and its file bytes in one transaction, `handle.pending_writes` reconstructs
unpublished writes after restart, `handle.connect_sync` starts the background
loop, `handle.subscribe_sync_status` exposes its current state, and
`handle.invite_member` adds a teammate.

## Who owns what

The integration stays small because the boundary is strict: coven owns what
sync needs to be correct, and the host owns the product.

coven owns the sync layer and the database connections:

- The SQLite connections: one writer, where coven runs the change-capture
  session and keeps its own bookkeeping, and a read-only companion for
  queries. You run your writes through the first and your reads through the
  second; there is no connection coven does not own.
- Capturing local changes and applying remote ones.
- Encrypting, signing, and verifying everything that leaves the device.
- Moving rows and files through your storage.
- Membership, invites, and recovery codes.

You own the app:

- Your schema and your queries, run through coven's connections.
- Which tables sync and which stay local.
- Where user-provided blob files live on disk.
- Provider configuration and credentials.
- All UI and product policy.

## Topics

In reading order; each page builds on the ones before it:

- [Local data](/docs/local-data): the store on one device: schema
  conventions, which tables sync, which rows share.
- [Sync](/docs/sync-model): change capture, the cycle, push and pull.
- [Merge](/docs/merge): the clock, and how concurrent edits land on every
  device.
- [Storage](/docs/storage): the `CloudHome` contract and the providers.
- [Blobs](/docs/blobs): files that rows carry, where their bytes live, and how
  they move.
- [Cache](/docs/cache): the device-local copies of remote files: budgets,
  pinning, eviction.
- [Sharing](/docs/sharing): membership, roles, invite, join, revoke.
- [Bootstrap](/docs/bootstrap): snapshots and how a new device joins or
  restores.
- [Encryption](/docs/encryption): the keys, what is encrypted, what the
  provider sees.
- [Keys](/docs/keys): where each key lives on the device, the custody
  presets, and what a host has to set up per platform.
- [Schema evolution](/docs/schema-evolution): migrating the synced schema while
  devices upgrade at different times.

## Status

coven is pre-1.0.
