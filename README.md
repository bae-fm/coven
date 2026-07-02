# coven

End-to-end encrypted, multi-writer SQLite sync over bring-your-own storage, with
an encrypted blob store and a cryptographic membership model. No coordination
server.

coven captures changes via the SQLite session extension, stamps them with a
hybrid logical clock, signs them per author, encrypts them, and pushes/pulls them
through a pluggable `CloudHome` (S3, Google Drive, Dropbox, OneDrive, iCloud,
local). Conflicts resolve by row-level last-writer-wins on an `_updated_at`
column. Membership is an append-only Ed25519-signed chain; the per-library
symmetric key is wrapped to each member's X25519 key. Blobs referenced by rows
sync as encrypted opaque files through a cloud outbox.

The host owns its schema and domain; coven owns the data — the SQLite connection
and the blob store. A library is local-first: rows and blobs live on the device
and reach the cloud only when you connect a provider. A native host talks to
coven through `CovenHandle` and never reaches into coven's storage or sync
internals directly.

This workspace has three crates:

- `coven`: the native Rust package and public API.
- `coven-core`: the shared engine crate used by platform packages.
- `coven-wasm`: the browser package. Browser support is isolated there and
  returns explicit unsupported errors for operations whose browser backend is not
  implemented.

## Integration

- Declare your synced tables on `Coven::builder(config).synced_tables(...)`, at
  startup, before sync starts. Each synced table has an `id` text primary key at
  column 0 and an `_updated_at TEXT NOT NULL` column. `SyncedTable::new` syncs
  rows; `remote_root()` also makes blobs on those rows and descendants always
  Remote. Tables you don't list stay local-only.
- Open one native handle with `Coven::builder(config).open(|conn| ...)`; coven
  opens SQLite, runs its bookkeeping migration, then runs your schema migration.
- Declare which rows carry blobs per table with `SyncedTable::carries_blob` (a
  `BlobDecl`: namespace, provenance, cache fill, encryption scope), and
  optionally pass a `BlobTransitionObserver` to the builder.
- Register identity/OAuth at startup: `set_keyring_service`,
  `set_oauth_client_creds`.
- Run app SQL through `handle.sql(...)`. Use `handle.write(...)` when a row write
  and host-provided blob bytes must commit together. Read blobs through
  `handle.read_blob`, pin through `handle.pin`, and drive sync/membership through
  handle methods.

```rust
let handle = Coven::builder(config)
    .synced_tables(vec![SyncedTable::new("files").carries_blob(file_blob_decl)])
    .open(|conn| {
        conn.execute_batch(APP_SCHEMA)?;
        Ok(())
    })?;

// Rows: your app SQL on the connection coven owns.
handle.sql(|sql| {
    sql.connection().execute(
        "INSERT INTO files (id, name, _updated_at) VALUES (?1, ?2, ?3)",
        coven::rusqlite::params![id, name, sql.stamp()],
    )?;
    Ok(())
}).await?;

// Rows plus host-provided blob bytes: one batch.
handle.write(|w| {
    let blob = w.put_blob("files", blob_id, bytes);
    w.sql(move |sql| {
        sql.connection().execute(
            "INSERT INTO files (id, blob_id, _updated_at) VALUES (?1, ?2, ?3)",
            coven::rusqlite::params![file_id, blob.id(), sql.stamp()],
        )?;
        Ok(())
    })
}).await?;

// Sync is optional. A library with no cloud home never calls these.
handle.connect_sync(Some(encryption_service)).await;
handle.sync_now();
```

## Blobs

A blob is opaque bytes a row references — a photo, an audio file, a cover image.
Each blob declares two orthogonal properties and has one runtime state:

- **Provenance** — where the bytes live while the blob is *Local*. *User-provided*:
  the user's own file at a path coven references. *Host-provided*: data the host
  hands coven, kept in coven's own local store at `storage/local/<namespace>/<id>`.
- **Cache fill** (`CacheEager` / `CacheLazy`) — how a device gets the bytes while
  the blob is *Remote*: `CacheEager` fetches into the cache on pull (cover art),
  `CacheLazy` on first read (audio).
- **Locality** — *Local* (bytes on-device) or *Remote* (bytes in the cloud, each
  device's copy a cache copy). `make_remote` uploads the bytes and turns a gated
  root's gate on; `make_local` brings them back to a local file and turns it off.
  A `remote_root()` table is already Remote and rejects those transitions.

The **cache** is a Remote-only mechanism: re-fetchable copies of Remote blobs under
`storage/cache/<namespace>/…` (evictable, against a per-namespace budget) and
`storage/pinned/<namespace>/…` (kept). A Local blob is never in the cache, and
`CacheEager`/`CacheLazy`/pin/budget describe a blob only while it is Remote. An
*asset* (a cover, an artist image) rides its subject's gate but never keeps the
subject alive. The `blob` module documents the full model.

## Status

Pre-1.0.

## License

Apache-2.0. See [LICENSE](LICENSE).
