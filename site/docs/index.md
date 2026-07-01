# Overview

coven syncs apps that keep their data in SQLite. You keep your schema; coven owns
the connection and runs your queries through it, so it can
[capture](https://www.sqlite.org/sessionintro.html) each change with SQLite's
session extension, encrypt and sign it, move it through storage you already
control, and apply remote changes back into SQLite. No coordination server.

## The round trip

<div style="margin: 1.5rem 0; padding: 24px 28px; background: var(--vp-code-block-bg); border-radius: 8px;">
<svg viewBox="11 18 666 232" width="100%" role="img" aria-label="On your device, your app's write is captured, signed, and encrypted by coven, then pushed to storage you own. On a teammate's device, coven pulls it back, verifies, decrypts, and applies it into their app." style="font-family: var(--vp-font-family-mono); overflow: visible;">
  <defs>
    <marker id="rt-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" style="fill: var(--vp-c-brand-1);" />
    </marker>
  </defs>

  <!-- app containers: the host app, with coven nested inside it -->
  <rect x="12" y="24"  width="424" height="92" rx="12" style="fill: none; stroke: var(--vp-c-divider);" />
  <text x="28" y="42" style="fill: var(--vp-c-text-2); font-size: 11px; letter-spacing: 0.5px;">your app</text>
  <text x="330" y="42" text-anchor="middle" style="fill: var(--vp-c-brand-1); font-size: 11px; letter-spacing: 0.5px;">coven</text>

  <rect x="12" y="156" width="424" height="92" rx="12" style="fill: none; stroke: var(--vp-c-divider);" />
  <text x="28" y="174" style="fill: var(--vp-c-text-2); font-size: 11px; letter-spacing: 0.5px;">their app</text>
  <text x="330" y="174" text-anchor="middle" style="fill: var(--vp-c-brand-1); font-size: 11px; letter-spacing: 0.5px;">coven</text>

  <!-- app boxes (host code, tinted fill to set apart from coven/storage) -->
  <rect x="28"  y="52"  width="132" height="52" rx="8" style="fill: var(--vp-c-tip-soft); stroke: var(--vp-c-divider);" />
  <rect x="28"  y="184" width="132" height="52" rx="8" style="fill: var(--vp-c-tip-soft); stroke: var(--vp-c-divider);" />

  <!-- coven boxes (brand-bordered = the coven layer) -->
  <rect x="236" y="52"  width="188" height="52" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--vp-c-brand-1); stroke-opacity: 0.55;" />
  <rect x="236" y="184" width="188" height="52" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--vp-c-brand-1); stroke-opacity: 0.55;" />

  <!-- storage (remote, shared) -->
  <rect x="508" y="52" width="168" height="184" rx="8" style="fill: var(--vp-c-bg-soft); stroke: var(--vp-c-divider); stroke-dasharray: 5 4;" />

  <!-- arrows -->
  <line x1="160" y1="78"  x2="232" y2="78"  style="stroke: var(--vp-c-brand-1); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="424" y1="78"  x2="504" y2="78"  style="stroke: var(--vp-c-brand-1); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="508" y1="210" x2="428" y2="210" style="stroke: var(--vp-c-brand-1); stroke-width: 2;" marker-end="url(#rt-arrow)" />
  <line x1="236" y1="210" x2="164" y2="210" style="stroke: var(--vp-c-brand-1); stroke-width: 2;" marker-end="url(#rt-arrow)" />

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

**Open the library.** coven owns the connection. Hand it the tables that sync
and your schema; it returns one handle. Tables you don't list stay local to the
device.

```rust
use coven::{Coven, SyncedTable};

let handle = Coven::builder(config)
    .synced_tables(vec![
        SyncedTable::new("todos"),
        SyncedTable::new("todo_attachments"),
    ])
    .open(|conn| { conn.execute_batch(MY_SCHEMA)?; Ok(()) })?;
```

**Write normally, through the handle.** coven captures the change. Synced rows
carry an `_updated_at` you mint with `SqlContext::stamp()`.

```rust
handle.sql(move |sql| {
    sql.connection().execute(
        "INSERT INTO todos (id, title, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params![id, title, sql.stamp()],
    )?;
    Ok(())
}).await?;
```

**Store a row plus host-provided blob bytes** with one batch.

```rust
handle.write(|w| {
    let blob = w.put_blob("attachments", blob_id, bytes);
    w.sql(move |sql| {
        sql.connection().execute(
            "INSERT INTO todo_attachments (id, todo_id, blob_id, _updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            coven::rusqlite::params![attachment_id, todo_id, blob.id(), sql.stamp()],
        )?;
        Ok(())
    })
}).await?;
```

**Start sync and react to remote changes.** Once a cloud provider is connected,
start sync through the handle and subscribe to the status stream.

```rust
handle.connect_sync(Some(encryption_service)).await?;
handle.sync_now();

let mut status = handle.subscribe_sync_status()?;
while let Ok(s) = status.recv().await {
    if let Some(changes) = s.row_changes {
        refresh_ui(&changes);
    }
}
```

**Share and revoke.** Invite a member with a code. Removing them rotates the
library key.

```rust
let code = handle.invite_member(teammate_pubkey, MemberRole::Member).await?;
```

## Who owns what

coven owns the sync layer and the database connection:

- The one SQLite connection. coven opens it, runs the change-capture session on
  it, and keeps its own bookkeeping there. You run your queries through it.
- Capturing local changes and applying remote ones.
- Encrypting, signing, and verifying everything that leaves the device.
- Moving rows and files through your storage.
- Membership, invites, and recovery codes.

You own the app:

- Your schema and your queries, run through coven's connection.
- Which tables sync and which stay local.
- Where blob files live on disk.
- Provider configuration and credentials.
- All UI and product policy.

## Topics

- [Example](/docs/example): opening the database, declaring the synced tables,
  writing through the connection.
- [Sync](/docs/sync-model): change capture, the cycle, conflict resolution,
  schema versions.
- [Local data](/docs/local-data): gating, what stays on one device.
- [Bootstrap](/docs/bootstrap): snapshots and how a new device joins.
- [Sharing](/docs/sharing): the membership chain, roles, invite, join, revoke.
- [Encryption](/docs/encryption): the keys, what is encrypted, what the provider
  sees.
- [Storage](/docs/storage): the `CloudHome` trait, providers, ranged reads.
- [Blobs](/docs/blobs): large files, the blob source, upload outbox, tombstone
  deletes.
- [Cache](/docs/cache): the device-local blob store, pinning, eviction.
- [Web](/docs/web): the `coven-wasm` browser crate and current unsupported
  browser operations.

## Status

coven is pre-1.0.
