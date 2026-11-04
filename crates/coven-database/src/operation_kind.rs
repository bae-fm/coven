//! The journal kind of each operation coven runs, and the `started_by` of
//! those coven starts on its own. Sync gives each kind its steps; the
//! database finds invites' rows itself, for `coven_invites` and to end them
//! when the installation stops for good.

/// An invite's kind, as schema SQL spells it.
macro_rules! invite_kind {
    () => {
        "invite"
    };
}
pub(crate) use invite_kind;

/// An administrative change claimed in the store's sequence (§11).
pub const SEQUENCE: &str = "sequence";
/// An invite, from its code to its end, and the `started_by` of the
/// operations it starts (§13.2).
pub const INVITE: &str = invite_kind!();
/// Granting or revoking a provider account's access (§14.2).
pub const PROVIDER_ACCESS: &str = "provider-access";
/// Advancing the floor and deleting the history below it (§16.6).
pub const SEQUENCE_RETENTION: &str = "sequence-retention";
/// An explicit reload of audiences from their snapshots (§16.5).
pub const SEQUENCE_RELOAD: &str = "sequence-reload";
/// Scanning the store's files for those nothing protects (§17.10).
pub const SEQUENCE_FILES: &str = "sequence-files";
/// The kind a breaking migration commits with its schema (§18.3).
pub const SCHEMA_RAISE: &str = "schema-raise";
/// The `started_by` of an operation coven starts on its own.
pub const COVEN: &str = "coven";
