# Overview

Sync normally means a backend: a server you run and pay for, and a database
that holds every user's data in the clear.

coven syncs without the server. Devices exchange end-to-end-encrypted changes
through cloud storage the household already pays for, and merge them locally.

The data is SQLite. You keep your schema and write ordinary SQL. coven owns the
connections and runs your SQL through them, so it can record each change as it
commits, encrypt and sign it, publish it to storage, read everyone else's, and
merge them into your tables. Rows can carry files, such as songs, photos or
documents, which sync with them.

coven is built for one person's devices or a household of up to about twenty
people; four or five is typical. It is not built for high write rates or large
groups.

The examples on these pages follow one household. Ana, Ben and Carol share a
store called Household. Ana and Ben also share a circle called Gifts, which
Carol can't read. Each has a phone and a laptop. Dan joins later.

## The round trip

No server is needed because nothing in the loop below requires one. A write
commits on the device at once. A later sync pass encrypts it, signs it and
publishes it as the next numbered *slot* of the store's sequence in storage.
Every other device reads the slots in the same order, checks each signature,
and merges the writes into its own database. Storage never has to understand
the data, which is what lets it be storage the user already has.

<div style="margin: 1.5rem 0; padding: 24px 28px; background: var(--vp-code-block-bg); border-radius: 8px;">
<svg viewBox="11 18 666 232" width="100%" role="img" aria-label="On your device, your app's write is captured, signed, and encrypted by coven, then published to storage you own. On another member's device, coven reads it, verifies, decrypts, and merges it into their app." style="font-family: var(--vp-font-family-mono); overflow: visible;">
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
  <text x="94"  y="83"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">INSERT note</text>

  <text x="330" y="74"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">record the write</text>
  <text x="330" y="92"  text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">sign + encrypt</text>

  <text x="592" y="138" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">storage you own</text>
  <text x="592" y="160" text-anchor="middle" style="fill: var(--vp-c-text-2); font-size: 12px;">▒ seq/1, seq/2 … ▒</text>

  <text x="330" y="206" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">verify + decrypt</text>
  <text x="330" y="224" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">merge the write</text>

  <text x="94"  y="215" text-anchor="middle" style="fill: var(--vp-c-text-1); font-size: 13px;">refresh UI</text>
</svg>
</div>

The provider holds encrypted bytes. It sees object sizes, counts and timing,
and the kind of each slot; it never sees a note's title, a file's contents, or
which device wrote a slot. What it can see is listed on the
[Encryption](/docs/encryption#what-the-provider-sees) page.

## In your code

The integration is one builder and a handful of methods on the handle it
returns. Three steps give the flavor; the whole tour, from creating a store to
inviting someone, is the [Example](/docs/example).

**Create the store.** Name the keychain service once at startup, then declare
the tables that sync and the migrations that build your schema. Tables you
don't declare stay on the device.

```rust
use coven::{Coven, Migration, RowIdentity, StoreLayout, SyncedTable};

coven::set_keyring_service("com.example.notes")?;

let layout = StoreLayout::new(app_dir);
let handle = Coven::builder(layout.clone())
    .synced_tables(vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid),
        SyncedTable::new("tags", RowIdentity::SharedKey),
    ])
    .migrations(vec![Migration::addition(1, "initial", SCHEMA)])
    .create_store("Household")
    .await?;
```

On later launches the same builder calls `open(store_id)` instead;
`layout.stores()` lists the stores on this device. Creating and opening need no
network: the store is complete on the device before any storage is set up.

**Write and read normally, through the handle.** A write is one SQLite
transaction. coven records what it changed in the same transaction, so a
committed write is never lost, even if the app crashes a moment later. There is
no timestamp column to maintain; coven stamps each write itself.

```rust
use coven::UuidText;
use uuid::Uuid;

let note = UuidText(Uuid::now_v7());
handle.write(move |sql| {
    sql.execute("INSERT INTO notes (id, title) VALUES (?1, ?2)", (note, "Groceries"))?;
    Ok(())
}).await?;

let mut titles = handle.subscribe(|sql| {
    Ok(sql.query("SELECT title FROM notes ORDER BY title", [], |row| row.get::<_, String>(0))?)
});
while let Ok(titles) = titles.next().await {
    show(titles);
}
```

**Connect storage.** Setting up storage creates the store there, returns the
person's restore code, and starts syncing. On later launches,
`handle.start_sync()` starts it again.

```rust
handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await?;
let setup = handle
    .setup_oauth_storage(
        StorageConfig::Dropbox { location: DropboxLocation::SharedFolder { namespace_id } },
        "Ana's phone",
    )
    .await?;
show_blurred_qr_code(setup.restore_code.as_str());
```

## Who owns what

The integration stays small because the boundary is strict: coven owns what
sync needs to be correct, and the app owns the product.

coven owns:

- the SQLite connections: one writer, where coven records each change and keeps
  its own bookkeeping, and several read-only connections for the app's queries;
- recording local changes and merging everyone else's;
- encrypting, signing and checking everything that leaves the device;
- the store's sequence in storage, snapshots, and deleting old history;
- members, devices, circles, invites and restore codes;
- moving files, the cache, and pinning.

The app owns:

- its schema and its queries, run through coven's connections;
- which tables sync and which stay on the device;
- where the user's own files live on disk;
- the provider sign-in screen and the provider's app registration;
- all UI and product policy, including what to show for a lost value or a
  pending condition.

## Topics

In reading order; each page builds on the ones before it:

- [Local data](/docs/local-data): the store on one device: opening it, which
  tables sync, keys, writing and reading.
- [Sync](/docs/sync-model): the sequence, how a write claims a slot, a sync
  pass, and what the app sees while it runs.
- [Merge](/docs/merge): timestamps, how concurrent edits land on every device,
  lost values, and the rules your SQL schema brings.
- [Storage](/docs/storage): what coven needs from a provider, and how Dropbox,
  OneDrive, iCloud and S3 services supply it.
- [Files](/docs/files): files that rows carry: attaching, uploading, status and
  reading.
- [Cache](/docs/cache): the device's copies of uploaded files: budgets,
  pinning and progress.
- [Sharing](/docs/sharing): members, roles and devices; restore codes,
  invites and removal.
- [Members and profiles](/docs/profiles): showing who is who, with a profile
  table each member writes for themselves.
- [Circles](/docs/circles): rows only some members can read.
- [Snapshots and joining](/docs/bootstrap): snapshots, the floor, deleting
  old history, and how a new or returning device catches up.
- [Encryption](/docs/encryption): the keys, what is encrypted, and what the
  provider sees.
- [Keys](/docs/keys): where keys live on the device, custody choices, and what
  each platform needs.
- [Schema evolution](/docs/schema-evolution): changing the schema while
  devices update at different times.

## Status

coven is pre-1.0.
