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
//! - coven OWNS the SQLite connection. The host opens it once with
//!   [`database::Database::open`]`(path, synced_tables, device_id, migrate)`:
//!   coven runs its own bookkeeping migration, then the host's `migrate` closure
//!   for the app's tables, seeds the register clock off the rows on disk,
//!   attaches the capture session, and spawns the connection thread. The host
//!   runs all of its own SQL through [`database::Database::call`]`(|conn| …)`;
//!   coven captures those writes through the attached session.
//! - Every synced table has an `id` text primary key at column 0 and an
//!   `_updated_at TEXT NOT NULL` column, and is declared as a
//!   [`sync::session::SyncedTable`] in the set passed to `Database::open`. A plain
//!   [`sync::session::SyncedTable::new`] table syncs unconditionally;
//!   [`sync::session::SyncedTable::gated_by`] marks a *gated root* whose boolean
//!   gate column decides, per row, whether that row and its declared
//!   FK-descendants are shared. See [`sync::gate`] for the gating semantics.
//! - `_updated_at` is coven's last-writer-wins register, an opaque Hybrid Logical
//!   Clock stamp the host mints with the [`UpdatedAtStamper`] that
//!   `Database::open` returns (non-optional, already seeded) and binds into every
//!   synced-row write. The host must not parse or compare the stamp as a
//!   wall-clock time; coven advances the clock past pulled rows so a later local
//!   write always sorts causally after them, which a wall clock cannot guarantee
//!   under skew. Changeset envelopes and membership entries also carry an HLC
//!   stamp for ordering/debuggability, but it is not authorization-load-bearing —
//!   pull authorizes by signature and current write-capable membership, and
//!   revocation is enforced by key rotation.
//! - When (and only when) a cloud provider is connected, the host builds the
//!   [`sync::sync_manager::SyncManager`] lazily, passing it the same `Database`
//!   handle and synced-table set. `SyncManager::new` is synchronous and
//!   infallible — all seeding happened in `Database::open`. The host also
//!   supplies a [`blob::BlobPlan`] and an optional [`blob::BlobUploadObserver`].
//!
//!   A local-only library that never connects a provider simply stamps and
//!   writes rows through `Database::call` without ever building a `SyncManager`,
//!   an `EncryptionService`, or a cloud config.

pub mod blob;
pub mod changeset;
pub mod clock;
pub mod config;
pub mod database;
pub mod db;
pub mod encryption;
pub mod id_provider;
pub mod join_code;
pub mod keys;
pub mod library_dir;
pub mod oauth;
#[cfg(feature = "share-proxy")]
pub mod share;
pub mod storage;
pub mod sync;
// Browser-only storage setup: installing the OPFS-backed SQLite VFS that makes
// the wasm `Database` durable across page loads. Documented at the module.
#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Headless proof that the wasm `Database` persists on OPFS through coven's own
// API. Inside the crate (not `tests/`) because it drives the crate-private
// `take_changeset_and_suspend` capture path. Worker-only — see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_opfs_test;

pub use database::Database;
pub use sync::hlc::UpdatedAtStamper;

/// The exact `rusqlite` coven owns the connection through. The host runs its app
/// SQL via [`Database::call`]`(|conn| …)` against this same crate — use
/// `coven::rusqlite::{params, Row, …}` rather than depending on `rusqlite`
/// directly, so the host can never drift onto a `libsqlite3-sys` version that
/// conflicts with coven's.
pub use rusqlite;
