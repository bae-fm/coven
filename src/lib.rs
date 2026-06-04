//! coven — end-to-end encrypted, multi-writer, bring-your-own-storage SQLite
//! sync, with an encrypted blob store and a cryptographic membership model.
//!
//! The host app owns its SQLite schema and domain. coven owns the sync layer:
//! changesets captured via the SQLite session extension, HLC-stamped
//! and signed per author, encrypted and pushed/pulled through a pluggable
//! `CloudHome`, conflict-resolved by row-level last-writer-wins on `_updated_at`.
//! An append-only Ed25519-signed membership chain wraps the per-library
//! symmetric key to each member.
//!
//! Integration contract for the host:
//! - Every synced table has an `id` text primary key at column 0 and an
//!   `_updated_at TEXT NOT NULL` column. `_updated_at` is coven's last-writer-
//!   wins register, an opaque Hybrid Logical Clock stamp the host mints with an
//!   [`UpdatedAtStamper`] and binds into every synced-row write. The host obtains
//!   the stamper from a [`sync::register_clock::RegisterClock`] (see the startup
//!   sequence below) and injects it into its database write path. The host must
//!   not parse or compare the stamp as a wall-clock time; coven advances the
//!   clock past pulled rows so a later local write always sorts causally after
//!   them, which a wall clock cannot guarantee under skew. Changeset envelopes
//!   and membership entries also carry an HLC stamp for ordering/debuggability,
//!   but it is not authorization-load-bearing — pull authorizes by signature and
//!   current write-capable membership, and revocation is enforced by key
//!   rotation.
//! - The host applies [`db::MIGRATION_SQL`] to create coven's bookkeeping tables
//!   and implements [`db::SyncBookkeeping`] + [`db::RawDbHandle`].
//! - Startup sequence, in order:
//!   1. Register the synced-table list via [`sync::session::set_synced_tables`]
//!      (required — [`sync::cycle::init_sync`] aborts if it's empty).
//!   2. Open the register clock:
//!      [`sync::register_clock::RegisterClock::open`]`(device_id, &db)`. It scans
//!      `MAX(_updated_at)` across the registered synced tables (via the raw write
//!      handle) and reads the persisted high-water mark to seed the register
//!      floor — so a restart cannot mint a stamp behind a row already on disk,
//!      including one whose stamp never reached the flushed high-water mark
//!      between cycles. Registration must precede this so the scan sees the
//!      tables. coven derives the floor itself; the host implements no
//!      `MAX(_updated_at)` query.
//!   3. Inject the clock's [`sync::register_clock::RegisterClock::updated_at_stamper`]
//!      into the database write path, before any synced-row write.
//!   4. When (and only when) a cloud provider is connected, build the
//!      [`sync::sync_manager::SyncManager`] lazily, passing it the same
//!      `RegisterClock` (it borrows the clock's `Arc<Hlc>`, so the host's stamps
//!      and coven's advance-on-pull share one register). `SyncManager::new` is
//!      synchronous and infallible — all seeding happened in step 2. The host
//!      also supplies a [`blob::BlobPlan`] and an optional
//!      [`blob::BlobUploadObserver`].
//!
//!   A local-only library that never connects a provider stops after step 3: it
//!   stamps and writes rows without ever building a `SyncManager`, an
//!   `EncryptionService`, or a cloud config.

pub mod blob;
pub mod changeset;
pub mod clock;
pub mod config;
pub mod db;
pub mod encryption;
pub mod id_provider;
pub mod join_code;
pub mod keys;
pub mod library_dir;
pub mod oauth;
pub mod storage;
pub mod sync;

pub use sync::hlc::UpdatedAtStamper;
pub use sync::register_clock::RegisterClock;
