//! This device's file records follow a migration's renames (§18): the
//! pending attachments, originals, row files, file references and abandoned
//! attachments of a renamed table, and the originals of a renamed file
//! column, take the new names in the migration's own transaction, as the
//! merge state does (`migration_state.rs`). The queued and carried pins'
//! captured references take them too, so the reference a pending attachment
//! gives after the rename stays valid through its completion (§17.2).

use crate::migration_names::MigrationMatch;
use crate::sqlite::DatabaseConnection;
use crate::{DbError, FileRef, FileRefJournal};
use rusqlite::params;

pub(crate) fn carry(db: &DatabaseConnection, names: &MigrationMatch) -> Result<(), DbError> {
    let columns: Vec<_> = names
        .columns
        .iter()
        .filter(|((_, old), new)| old != *new)
        .collect();
    // Stage names first, so a swap of two names can't collide on a key.
    for (index, ((table, old), _)) in columns.iter().enumerate() {
        rename_column(db, table, old, &format!("coven_migration_column_{index}"))?;
    }
    for (index, ((table, _), new)) in columns.iter().enumerate() {
        rename_column(db, table, &format!("coven_migration_column_{index}"), new)?;
    }
    let tables: Vec<_> = names
        .tables
        .iter()
        .filter(|(old, new)| old != new)
        .collect();
    for (index, (old, _)) in tables.iter().enumerate() {
        rename_table(db, old, &format!("coven_migration_table_{index}"))?;
    }
    for (index, (_, new)) in tables.iter().enumerate() {
        rename_table(db, &format!("coven_migration_table_{index}"), new)?;
    }
    // Captured references name the row by its final names.
    let uploads = db.query("SELECT id,reference FROM _coven_file_uploads", [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
    })?;
    for (id, bytes) in uploads {
        if let Some(file) = renamed(&bytes, names)? {
            db.internal_execute(
                "UPDATE _coven_file_uploads SET reference=?2 WHERE id=?1",
                params![id, file],
            )?;
        }
    }
    let requests = db.query(
        "SELECT namespace,file_id,reference FROM _coven_pin_requests",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Vec<u8>>(2)?,
            ))
        },
    )?;
    for (namespace, id, bytes) in requests {
        if let Some(file) = renamed(&bytes, names)? {
            db.internal_execute(
                "UPDATE _coven_pin_requests SET reference=?3 WHERE namespace=?1 AND file_id=?2",
                params![namespace, id, file],
            )?;
        }
    }
    Ok(())
}

/// A captured reference under the migration's new names, encoded, or `None`
/// when neither its table nor its file column was renamed.
fn renamed(bytes: &[u8], names: &MigrationMatch) -> Result<Option<Vec<u8>>, DbError> {
    let mut file = FileRef::decode(bytes)?;
    let old = (file.table().to_owned(), file.column().to_owned());
    let table = names.tables.get(&old.0).unwrap_or(&old.0).clone();
    let column = names.columns.get(&old).unwrap_or(&old.1).clone();
    if (&table, &column) == (&old.0, &old.1) {
        return Ok(None);
    }
    file.rename(table, column);
    file.encode().map(Some)
}

fn rename_column(
    db: &DatabaseConnection,
    table: &str,
    old: &str,
    new: &str,
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_user_files SET column_name=?3 WHERE table_name=?1 AND column_name=?2",
        params![table, old, new],
    )?;
    Ok(())
}

fn rename_table(db: &DatabaseConnection, old: &str, new: &str) -> Result<(), DbError> {
    // Each of this device's records that names a row by its table.
    for table in [
        "_coven_pending_attachments",
        "_coven_user_files",
        "_coven_row_files",
        "_coven_row_file_refs",
        "_coven_abandoned_attachments",
    ] {
        db.internal_execute(
            &format!("UPDATE {table} SET table_name=?2 WHERE table_name=?1"),
            params![old, new],
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "migration_files_tests.rs"]
mod tests;
