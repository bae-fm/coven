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
and the blob store. A library is local-first: rows and blobs live on the device,
and reach the cloud only when you connect a provider. A host talks to coven
through one handle — `CovenHandle` natively, or the `CovenLibrary` facade in the
browser — and never reaches into coven's storage or sync internals directly.

## Integration

- Declare your synced tables with `sync::session::set_synced_tables`, **once at
  startup, before sync starts** (and before any restore/join, which applies
  changesets too). Each must have an `id` text primary key at column 0 and an
  `_updated_at TEXT NOT NULL` column (the HLC/LWW timestamp). This is required,
  not optional: with no tables registered, `sync::cycle::init_sync` aborts and
  logs an error rather than silently running a no-op (snapshot-only) sync. A
  table you *don't* list stays local-only — that's how you keep device-local
  state (per-device pin/cache columns, local paths) out of sync.
- Apply `db::MIGRATION_SQL` (creates `sync_cursors`, `sync_state`,
  `cloud_outbox`) and implement `db::SyncBookkeeping` + `db::RawDbHandle` on your
  database — coven imposes no SQLite driver.
- Declare which rows carry blobs per table with
  `sync::session::SyncedTable::carries_blob` (a `BlobDecl`: namespace, provenance,
  cache fill, encryption scope), and optionally pass a
  `blob::BlobTransitionObserver` to the `SyncManager`.
- Register identity/OAuth at startup: `keys::set_keyring_service`,
  `oauth::set_oauth_client_creds`.
- Hold a `CovenHandle`, built once over the open `Database`. Run app SQL through
  `handle.database()`, read and write blobs through `handle.read_blob` /
  `store_blob` / `pin`, and — when a provider is connected — drive sync through
  `handle.connect_sync` / `sync_now`. The handle owns the `SyncManager`, so you
  never build or thread it (or the storage) yourself; reach the connected
  manager's `SyncLoopStatus { row_changes, .. }` through `handle.sync_manager()`
  to react to pulled changes.

```rust
// Build the handle once over the open Database; then only call methods on it.
let handle = CovenHandle::new(
    db, library_dir, config_provider, key_service, clock, observer,
);

// Rows: your app SQL on the connection coven owns.
handle.database().call(|conn| { /* INSERT/SELECT ... */ Ok(()) }).await?;

// Blobs: read/store by descriptor. coven resolves locality (the user's file,
// its local store, the cache, or a cloud fetch) and at-rest encryption.
let bytes = handle.read_blob(&cover).await?;
handle.store_blob("images", "release-1", &bytes).await?;

// Sync is optional. A library with no cloud home never calls these.
handle.connect_sync(encryption_service).await;
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
