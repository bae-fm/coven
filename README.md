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

Coven gives each device an immutable signed commit stream. Commits carry their
causal dependencies; devices accept dependency-ready commits and converge by
merging concurrent row changes deterministically.

Docs: <https://coven.bae.fm>

## What it looks like

Open a store, declaring the tables that sync and the migration ladder that
builds your schema. Tables you don't declare stay local to the device.

```rust
use coven::{Coven, Migration, RowIdentity, SyncedTable};

let handle = Coven::builder(store_dir, config)
    .synced_tables(vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid),
        SyncedTable::new("photos", RowIdentity::IndependentUuid)
            .carries_blob(photo_blob_decl),
    ])
    .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
    .open()?;
```

`(table, id)` identifies one logical row across every device. Independently
created rows use canonical UUIDv4 or UUIDv7 ids; `RowIdentity::SharedKey` is for
application keys whose equal values intentionally merge as one row. Changing a
primary key removes the old identity and inserts the new validated identity in
the same transaction.

Write ordinary SQL through the handle. Your closure gets a transaction; coven
captures what changed when it commits. Synced rows carry an `_updated_at`
minted with `sql.stamp()`, the register concurrent edits are ordered by.

```rust
let id = uuid::Uuid::new_v4().to_string();
let receipt = handle.write(move |sql| {
    sql.execute(
        "INSERT INTO notes (id, body, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params![id, body, sql.stamp()],
    )?;
    Ok(())
}).await?;
```

The receipt identifies this transaction in coven's durable write ledger.
`LocalOnly` will never publish; `Pending` means the shared changes are committed
locally and waiting for their Store commit. Separate `write` calls always receive
separate write ids and Store commits.

Pure reads go through `handle.read`, which runs on a read-only companion
connection: no change capture, and reads run concurrently with the writer
instead of queuing behind it.

Connect storage when there is somewhere to sync to. A store with no cloud
home is complete on its own. Identity and keys live in the OS keyring: the
host names its keyring service once at startup with `set_keyring_service`,
which installs the platform keyring store (apple-native on macOS/iOS,
android-native on Android; a target with no bundled store errors).

```rust
handle.connect_sync().await?;
handle.sync_now();
```

`handle.write_with_blobs` commits a row and its file bytes in one transaction.
`handle.pending_writes` reconstructs unpublished write state after restart;
`blocked_writes`, `retry_blocked_write`, and `discard_blocked_write` expose the
explicit recovery path for a write stopped by a missing blob or invalid package
or protocol state. `handle.subscribe_sync_status` exposes the current loop
state. Device sharing starts with `handle.begin_device_invite`, which admits
the identity in an exact join request and returns a recipient-sealed device
invitation. The whole tour is the
[example](https://coven.bae.fm/docs/example).

`Offline` means a provider or network operation failed. Invalid remote blob
content and local cache filesystem failures remain typed failed or held work;
they never masquerade as a connectivity loss.

## Design

- **Local first.** Every device has the full database. Writes commit locally;
  a background cycle pushes and pulls sealed changesets through the storage.
- **Causal ordering.** Each device has an append-only stream. Pull verifies
  signed predecessors and dependencies, then merges concurrent edits column by
  column; provider listing order never decides the result.
- **Pick what syncs.** Undeclared tables never leave the device, and a boolean
  gate column can keep chosen rows of a synced table local; the gate follows
  the schema's foreign keys down to descendants.
- **Files ride with rows.** A blob commits in its row's transaction and syncs
  encrypted. Bytes can live locally or in the cloud, with per-namespace cache
  budgets and pinning on each device.
- **Cryptographic membership.** Members are keypairs. Membership changes are
  signed causal owner streams. The store keyring is sealed to each member, and
  a removed member never receives the replacement generation.
- **Untrusted storage.** The provider holds ciphertext and signed control
  objects; every changeset is verified on pull.

## Workspace

- `coven`: the Rust package containing the public API and its retained domain
  owners.

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
