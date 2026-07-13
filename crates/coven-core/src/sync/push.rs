//! Push-related types for the sync system.
//!
//! The actual push orchestration happens in [`super::service::sync`], which
//! returns an `OutgoingChangeset` for the caller to encrypt and upload.
//! This module holds the shared types.
//!
//! The outgoing changeset's `schema_version` is not a constant here: it is
//! [`crate::database::Database::schema_version`], the applied `PRAGMA user_version`
//! from the host's migration ladder, so a device can only stamp a version it has
//! actually migrated to.

/// An outgoing changeset ready to be pushed to sync storage.
pub struct OutgoingChangeset {
    /// The packed envelope + changeset bytes (plaintext, ready for encryption).
    pub packed: Vec<u8>,
    /// The sequence number for this changeset.
    pub seq: u64,
    /// Local-store blob cleanup that must wait until this changeset is durably
    /// published.
    pub deferred_local_blob_drops: Vec<super::service::DeferredLocalBlobDrop>,
    /// make_remote intents this changeset's inline host-blob uploads consumed. Their
    /// deletion is deferred to the same transaction that durably records the drop
    /// intents, so a crash before that commit leaves the intent live (carrying its
    /// `retain_pinned`) and the retry re-derives the disposition from it instead of
    /// defaulting it.
    pub consumed_make_remote_intents: Vec<(String, String)>,
}
