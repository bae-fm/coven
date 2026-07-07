---
layout: home

hero:
    name: coven
    text: Serverless sync
    tagline: Build private, local-first apps that scale to the cloud with nothing to run
    image:
        src: /favicon.svg
        alt: coven
    actions:
        - theme: brand
          text: Get started
          link: /docs/

features:
    - icon:
          light: /icons/harddrive-light.svg
          dark: /icons/harddrive-dark.svg
          width: 28
          height: 28
      title: Local first
      details: 'The database lives on the device. Updates never touch the network, sync follows in the background.'
      link: /docs/
    - icon:
          light: /icons/cloud-light.svg
          dark: /icons/cloud-dark.svg
          width: 28
          height: 28
      title: Serverless
      details: 'Devices sync through storage users bring: Google Drive, Dropbox, OneDrive, iCloud, or S3. Nothing to deploy or operate.'
      link: /docs/storage
    - icon:
          light: /icons/database-light.svg
          dark: /icons/database-dark.svg
          width: 28
          height: 28
      title: Pick what syncs
      details: 'SQLite in a sync harness: choose what tables and rows to sync, relations follow automatically.'
      link: /docs/local-data
    - icon:
          light: /icons/image-light.svg
          dark: /icons/image-dark.svg
          width: 28
          height: 28
      title: Blobs, too
      details: 'Rows carry files. A file commits in the row''s transaction and syncs alongside it.'
      link: /docs/blobs
    - icon:
          light: /icons/layers-light.svg
          dark: /icons/layers-dark.svg
          width: 28
          height: 28
      title: Beyond the disk
      details: 'Files can live in the cloud and stream back when read. Pin what should stay offline.'
      link: /docs/cache
    - icon:
          light: /icons/devices-light.svg
          dark: /icons/devices-dark.svg
          width: 28
          height: 28
      title: Everyone writes
      details: 'Any device edits anything, offline included, and concurrent edits merge on their own.'
      link: /docs/sync-model
---

<div class="home-body">

## What it looks like

Your app opens one handle, declares which tables sync, and runs its own SQL.
Everything below is the real API.

### Open a library

A synced table is ordinary SQLite with two conventions: a text `id` primary
key, and an `_updated_at` column that coven stamps. Migrations are a plain
versioned ladder.

```rust
use coven::{Coven, Migration, SyncedTable};

const SCHEMA: &str = "
    CREATE TABLE notes (
        id          TEXT PRIMARY KEY,
        body        TEXT NOT NULL,
        _updated_at TEXT NOT NULL
    );
";

let handle = Coven::builder(config)
    .synced_tables(vec![SyncedTable::new("notes")])
    .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
    .open()?;
```

### Write rows

Run your own SQL in a transaction on the connection coven owns. The change is
captured and journaled for every other device, online or not.

```rust
handle.sql(|sql| {
    sql.tx().execute(
        "INSERT INTO notes (id, body, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params!["note-1", "Pick up milk", sql.stamp()],
    )?;
    Ok(())
})
.await?;
```

### Attach files

`handle.write` commits file bytes and row changes as one unit; the row never
exists without its file. The bytes ride the same encrypted pipeline as the
rows. (The `photos` table declares its blob namespace with
`SyncedTable::carries_blob`; see [Blobs](/docs/blobs).)

```rust
handle.write(
    move |batch| {
        batch.put_blob("photos", "sunset-01", jpeg_bytes);
        Ok(())
    },
    |sql| {
        sql.tx().execute(
            "INSERT INTO photos (id, caption, _updated_at) VALUES (?1, ?2, ?3)",
            coven::rusqlite::params!["sunset-01", "Golden hour", sql.stamp()],
        )?;
        Ok(())
    },
)
.await?;
```

### Connect storage

Point the library's config at a provider and connect. The library key lives on
your devices; the provider only ever sees ciphertext. A library with no cloud
home skips this step entirely; local-first means local works.

```rust
let encryption = EncryptionService::new(&library_key)?;
handle.connect_sync(Some(encryption)).await?;
handle.sync_now();
```

## How it works

1. **Capture.** Your SQL runs on the connection coven owns; the SQLite session
   extension records exactly which rows and columns changed.
2. **Stamp.** Each changeset carries hybrid-logical-clock timestamps, so edits
   order consistently across devices without a shared clock.
3. **Seal.** Changesets are signed by their author and encrypted with the
   library key before they leave the device.
4. **Exchange.** Every device appends to its own stream of objects in your
   storage and pulls everyone else's. Streams are append-only and per-author:
   nothing is overwritten, so there is no write contention to coordinate.
5. **Merge.** Concurrent edits merge column by column against their common
   ancestor; deletes win over concurrent edits.

Sharing works the same way: membership is an append-only chain of signed
entries, and the library key is wrapped to each member's public key. Inviting
someone adds an entry. Removing someone rotates the key.

## Bring your own storage

<p class="providers">
  <span>Amazon S3</span>
  <span>Google Drive</span>
  <span>Dropbox</span>
  <span>OneDrive</span>
  <span>iCloud (CloudKit)</span>
</p>

Any S3-compatible endpoint works too; the S3 provider takes a custom endpoint.
Storage is a single `CloudHome` trait, so a new provider is one implementation
away. [How storage works →](/docs/storage)

## Read on

- [Sync model](/docs/sync-model): how changes move and merge
- [Bootstrap](/docs/bootstrap): creating, joining, and restoring a library
- [Sharing](/docs/sharing): invites, roles, and revocation
- [Encryption](/docs/encryption): keys, chunking, and signatures
- [Storage](/docs/storage): the `CloudHome` contract and the providers
- [Example](/docs/example): a complete host, end to end

</div>

<style>
:root {
    --vp-home-hero-name-color: transparent;
    --vp-home-hero-name-background: linear-gradient(120deg, #3f8f8b 30%, #345f5d);
    --vp-home-hero-image-background-image: linear-gradient(-45deg, #3f8f8b66 50%, #345f5d66 50%);
    --vp-home-hero-image-filter: blur(56px);
}

.dark {
    --vp-home-hero-name-background: linear-gradient(120deg, #8fd9d4 30%, #4a9a95);
}

/* Smaller eyebrow, bigger jumbo (VitePress sizes both at 32/48/56px
   across its breakpoints). */
.VPHero .name {
    font-size: 26px;
    line-height: 34px;
}

.VPHero .text {
    font-size: 40px;
    line-height: 48px;
}

.VPHero .tagline {
    font-size: 16px;
    line-height: 24px;
}

@media (min-width: 640px) {
    .VPHero .name {
        font-size: 30px;
        line-height: 38px;
    }

    .VPHero .text {
        font-size: 56px;
        line-height: 64px;
    }

    .VPHero .tagline {
        font-size: 18px;
        line-height: 26px;
    }
}

@media (min-width: 960px) {
    .VPHero .name {
        font-size: 34px;
        line-height: 42px;
    }

    .VPHero .text {
        font-size: 64px;
        line-height: 72px;
    }

    .VPHero .tagline {
        font-size: 20px;
        line-height: 28px;
    }
}

/* Wider gap between the feature cards (VitePress default is 16px:
   8px item padding against a -8px items margin). */
.VPFeatures .items {
    margin: -11px;
}

.VPFeatures .item {
    padding: 11px;
}

.home-body {
    max-width: 688px;
    margin: 0 auto;
    padding: 16px 24px 96px;
}

.home-body .providers {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 16px 0;
}

.home-body .providers span {
    padding: 6px 14px;
    border-radius: 20px;
    background-color: var(--vp-c-bg-soft);
    border: 1px solid var(--vp-c-divider);
    color: var(--vp-c-text-1);
    font-size: 14px;
    font-weight: 500;
}
</style>
