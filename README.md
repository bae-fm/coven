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

The host owns its schema and domain; coven owns the sync layer.

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
- Provide a `blob::BlobPlan` (which row-changes carry blobs, where they live
  locally, how each is scoped for encryption) and optionally a
  `blob::BlobUploadObserver`.
- Register identity/OAuth at startup: `keys::set_keyring_service`,
  `oauth::set_oauth_client_creds`.
- Construct a `sync::sync_manager::SyncManager` and drive it; subscribe to its
  `SyncLoopStatus { row_changes, .. }` to react to pulled changes.

## Status

Pre-1.0.
