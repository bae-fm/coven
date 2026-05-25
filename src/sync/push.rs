//! Push-related types for the sync system.
//!
//! The actual push orchestration happens in `SyncService::sync()`, which
//! returns an `OutgoingChangeset` for the caller to encrypt and upload.
//! This module holds the shared types and the schema version constant.

/// Current schema version -- a monotonically increasing tag attached to
/// outgoing changesets. Receivers reject changesets whose schema_version
/// is higher than they support, so this must be bumped any time the on-disk
/// shape of synced tables changes.
pub const SCHEMA_VERSION: u32 = 4;

/// An outgoing changeset ready to be pushed to sync storage.
pub struct OutgoingChangeset {
    /// The packed envelope + changeset bytes (plaintext, ready for encryption).
    pub packed: Vec<u8>,
    /// The sequence number for this changeset.
    pub seq: u64,
}
