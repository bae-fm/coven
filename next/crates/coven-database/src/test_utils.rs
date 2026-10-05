//! A file-free schema fixture for tests opening the production database graph.

use crate::{Migration, RowIdentity, SyncedTable};

/// Declarations for a UUID-keyed notes table in the store audience.
pub fn notes_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)]
}

/// The corresponding app migration sequence.
pub fn notes_migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '')")]
}
