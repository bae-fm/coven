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
//!   wins register, an opaque Hybrid Logical Clock stamp the host obtains from
//!   [`sync::sync_manager::SyncManager::stamp_updated_at`] and binds into every
//!   synced-row write. The host must not parse or compare it as a wall-clock
//!   time; coven advances the clock past pulled rows so a later local write
//!   always sorts causally after them, which a wall clock cannot guarantee
//!   under skew. Changeset envelopes and membership entries also carry an HLC
//!   stamp for ordering/debuggability, but it is not authorization-load-
//!   bearing — pull authorizes by signature and current write-capable
//!   membership, and revocation is enforced by key rotation.
//! - The host applies [`db::MIGRATION_SQL`] to create coven's bookkeeping tables
//!   and implements [`db::SyncBookkeeping`] + [`db::RawDbHandle`].
//! - The host registers the synced-table list at startup via
//!   [`sync::session::set_synced_tables`] (required — [`sync::cycle::init_sync`]
//!   aborts if it's empty), and supplies a [`blob::BlobPlan`] and an optional
//!   [`blob::BlobUploadObserver`].

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
