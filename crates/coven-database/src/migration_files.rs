//! This device's file references follow a migration's renames (§18): the
//! references of a renamed table, and of a renamed file column, take the new
//! names in the migration's own transaction, as the merge state does
//! (`migration_state.rs`). Files are keyed by their own ids, which no rename
//! changes.

use crate::migration_names::MigrationMatch;
use crate::sqlite::DatabaseConnection;
use crate::DbError;

pub(crate) fn carry(db: &DatabaseConnection, names: &MigrationMatch) -> Result<(), DbError> {
    let columns: Vec<_> = names.columns.iter().filter(|((_, old), new)| old != *new).collect();
    // Stage names first, so a swap of two names can't collide on a key.
    for (index, ((table, old), _)) in columns.iter().enumerate() {
        crate::file_protection::rename_reference_column(
            db,
            table,
            old,
            &format!("coven_migration_column_{index}"),
        )?;
    }
    for (index, ((table, _), new)) in columns.iter().enumerate() {
        crate::file_protection::rename_reference_column(
            db,
            table,
            &format!("coven_migration_column_{index}"),
            new,
        )?;
    }
    let tables: Vec<_> = names.tables.iter().filter(|(old, new)| old != new).collect();
    for (index, (old, _)) in tables.iter().enumerate() {
        crate::file_protection::rename_reference_table(
            db,
            old,
            &format!("coven_migration_table_{index}"),
        )?;
    }
    for (index, (_, new)) in tables.iter().enumerate() {
        crate::file_protection::rename_reference_table(
            db,
            &format!("coven_migration_table_{index}"),
            new,
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "migration_files_tests.rs"]
mod tests;
