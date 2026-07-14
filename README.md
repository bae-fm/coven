# coven

Sync normally means a backend: a server you run and pay for, and a database
that holds every user's data in the clear.

coven syncs without the server. Devices exchange end-to-end-encrypted changes
through storage the user already has (Google Drive, Dropbox, OneDrive, iCloud,
or any S3-compatible endpoint), and merge them locally.

The data is SQLite. You keep your schema; coven owns the connections and runs
your queries through them, so it can capture each change with SQLite's session
extension, encrypt and sign it, move it through the user's storage, and apply
remote changes back.

Docs: <https://coven.bae.fm>

## What it looks like

Open a store, declaring the tables that sync and the migration ladder that
builds your schema. Tables you don't declare stay local to the device.

```rust
use coven::{Coven, Migration, SyncedTable};

let handle = Coven::builder(config)
    .synced_tables(vec![
        SyncedTable::new("notes"),
        SyncedTable::new("photos").carries_blob(photo_blob_decl),
    ])
    .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
    .open()?;
```

Write ordinary SQL through the handle. Your closure gets a transaction; coven
captures what changed when it commits. Synced rows carry an `_updated_at`
minted with `sql.stamp()`, the register concurrent edits are ordered by.

```rust
handle.sql(move |sql| {
    sql.tx().execute(
        "INSERT INTO notes (id, body, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params![id, body, sql.stamp()],
    )?;
    Ok(())
}).await?;
```

Pure reads go through `handle.sql_read`, which runs on a read-only companion
connection: no change capture, and reads run concurrently with the writer
instead of queuing behind it.

Connect storage when there is somewhere to sync to. A store with no cloud
home is complete on its own. Identity and keys live in the OS keyring: the
host names its keyring service once at startup with `set_keyring_service`,
which installs the platform keyring store (apple-native on macOS/iOS,
android-native on Android; a target with no bundled store errors).

```rust
handle.connect_sync(Some(encryption_service)).await?;
handle.sync_now();
```

`handle.write` commits a row and its file bytes in one transaction,
`handle.subscribe_sync_status` streams what each cycle applied, and
`handle.invite_member` adds a member. The whole tour is the
[example](https://coven.bae.fm/docs/example).

## Design

- **Local first.** Every device has the full database. Writes commit locally;
  a background cycle pushes and pulls sealed changesets through the storage.
- **Everyone writes.** No primary. Each device appends to its own stream, and
  concurrent edits merge column by column over a hybrid logical clock; deletes
  win over concurrent edits.
- **Pick what syncs.** Undeclared tables never leave the device, and a boolean
  gate column can keep chosen rows of a synced table local; the gate follows
  the schema's foreign keys down to descendants.
- **Files ride with rows.** A blob commits in its row's transaction and syncs
  encrypted. Bytes can live locally or in the cloud, with per-namespace cache
  budgets and pinning on each device.
- **Cryptographic membership.** Members are keypairs; membership changes are
  signed per-owner streams; the store keyring is sealed to each member, and
  removing one appends a key generation the removed member never receives.
- **Untrusted storage.** The provider holds ciphertext and signed control
  objects; every changeset is verified on pull.

## Workspace

- `coven`: the native Rust package and public API.
- `coven-core`: the shared engine crate used by platform packages.
- `coven-wasm`: the browser package. Browser support is isolated there and
  returns explicit unsupported errors for operations whose browser backend is
  not implemented.

## Development

coven is greenfield software. Repository changes implement the intended design
directly; they do not preserve earlier development-state APIs, storage formats,
or behavior through compatibility shims, legacy readers or writers, fallback
paths, or compatibility migrations unless explicitly requested. The
application-schema migration API shown above is product functionality, not a
compatibility promise for coven's own pre-release internals.

## Status

Pre-1.0.

## License

Apache-2.0. See [LICENSE](LICENSE).
