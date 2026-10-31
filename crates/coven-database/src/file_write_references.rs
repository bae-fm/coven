//! Which files a write leaves with no reference (§17.2), read from the
//! merge's own result for the write's rows, so the write itself deletes
//! their records.

use crate::{
    file_protection::{loss_files, state_files},
    file_table::Record,
    merge_store::MergeStore,
    sqlite::DatabaseConnection,
    write_rows::AppKey,
    write_schema::WriteSchema,
    DbError,
};
use coven_format::{
    dismissal::Dismissal,
    value::Value,
    write::{RowChange, Step, WriteRecord},
};
use coven_foundation::id_source::FileId;
use coven_merge::{RowId, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// The files whose last reference `record` removes: each one an app row it
/// changes, or whose lost values it dismisses, referenced before, that no
/// row references once it applies, and that this device still has a record
/// of. A row's references once the record applies are what merging the
/// record gives each of its audiences (§8.2): their cells and live lost
/// values, with its frozen lost values, less what the record dismisses. A
/// row that removal rules took out keeps its cells, so its own lost record
/// adds none (§8.4). Merging here persists nothing.
pub(crate) fn unreferenced(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    store: &MergeStore<'_>,
    record: &WriteRecord,
) -> Result<BTreeSet<FileId>, DbError> {
    let dismissals: Vec<&Dismissal> = record.parts.iter().flat_map(|p| &p.dismissals).collect();
    let keys: BTreeSet<AppKey> = record
        .parts
        .iter()
        .flat_map(|part| &part.rows)
        .map(|change| &change.row)
        .chain(dismissals.iter().map(|dismissal| &dismissal.row))
        .filter(|row| !schema.file_columns(&row.table).is_empty())
        .map(|row| (row.table.clone(), row.key.clone()))
        .collect();
    let mut before = BTreeSet::new();
    for key in &keys {
        for file in db.query(
            "SELECT file FROM _coven_file_references WHERE table_name=?1 AND key=?2",
            params![key.0, key.1],
            |r| r.get::<_, FileId>(0),
        )? {
            before.insert(file);
        }
    }
    if before.is_empty() {
        return Ok(BTreeSet::new());
    }
    let updates = store.apply(record)?;
    let mut after = BTreeSet::new();
    for key in &keys {
        after.extend(files_after(db, schema, store, key, &updates, &dismissals)?);
    }
    let mut unreferenced = BTreeSet::new();
    for file in before.difference(&after) {
        let elsewhere = db
            .query(
                "SELECT table_name,key FROM _coven_file_references WHERE file=?1",
                [file],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )?
            .into_iter()
            .any(|key| !keys.contains(&key));
        if !elsewhere && Record::read(db, *file)?.is_some() {
            unreferenced.insert(*file);
        }
    }
    Ok(unreferenced)
}

/// The deletion of each of `files`' records, as row changes of coven's
/// files table, the first at step `sequence` and each next one after it.
pub(crate) fn deletions(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    files: &BTreeSet<FileId>,
    mut sequence: u32,
) -> Result<Vec<(RowChange, Step)>, DbError> {
    let mut deletions = Vec::new();
    for file in files {
        let record = Record::read(db, *file)?
            .ok_or(DbError::Invariant { what: "an unreferenced file to delete has its record" })?;
        let row = crate::file_table::row(*file, record.audience)?;
        let change = crate::dismissal::deletion(db, store, &store.row(&row)?.state)?;
        let step = Step { sequence, change: change.change.clone(), key_update: false };
        sequence = sequence
            .checked_add(1)
            .ok_or(DbError::Invariant { what: "a write's steps fit their counter" })?;
        deletions.push((change, step));
    }
    Ok(deletions)
}

/// The files one app row references once the write's `updates` apply:
/// each of its audiences' merged state, the write's rows as merging gives
/// them, and its frozen lost values, less the cells `dismissals` name.
fn files_after(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    store: &MergeStore<'_>,
    key: &AppKey,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
    dismissals: &[&Dismissal],
) -> Result<BTreeSet<FileId>, DbError> {
    let columns = schema.file_columns(&key.0);
    let mut rows: BTreeSet<RowId> = db
        .query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",
            params![key.0, key.1],
            crate::row_queries::read_identity,
        )?
        .into_iter()
        .collect();
    rows.extend(updates.keys().filter(|row| row.table == key.0 && row.key == key.1).cloned());
    let kept = |row: &RowId, column: &str, write: WriteId| {
        !dismissals
            .iter()
            .any(|dismissal| crate::dismissal::dismisses_cell(dismissal, row, column, write))
    };
    let mut files = BTreeSet::new();
    for row in &rows {
        match updates.get(row) {
            Some(update) => {
                state_files(&columns, &update.state, |c, w| kept(row, c, w), &mut files)
            }
            None => {
                let state = store.row(row)?.state;
                state_files(&columns, &state, |c, w| kept(row, c, w), &mut files);
            }
        }
    }
    let mut losses = Vec::new();
    db.for_each(
        &format!(
            "{} WHERE l.table_name=?1 AND l.key=?2 AND l.frozen=1",
            crate::loss_record::SELECT
        ),
        params![key.0, key.1],
        |r| {
            losses.push(crate::loss_record::read(r)?);
            Ok::<_, DbError>(())
        },
    )?;
    for loss in &losses {
        loss_files(schema, loss, |c, w| kept(&loss.row, c, w), &mut files);
    }
    Ok(files.into_iter().map(|(file, _)| file).collect())
}

#[cfg(test)]
#[path = "file_write_references_tests.rs"]
mod tests;
