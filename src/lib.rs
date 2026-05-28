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
//!   `_updated_at TEXT NOT NULL` column (the HLC/LWW timestamp).
//! - The host applies [`db::MIGRATION_SQL`] to create coven's bookkeeping tables
//!   and implements [`db::SyncBookkeeping`] + [`db::RawDbHandle`].
//! - The host supplies the synced-table list, a [`blob::BlobPlan`], and an
//!   optional [`blob::BlobUploadObserver`].

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
