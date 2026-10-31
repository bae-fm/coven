//! New files over existing files' bytes (§17.2): those a write's moves
//! give rows in their new audience, and those the app stages with
//! `existing_file`. Only the device holding a file's bytes makes one.

use super::FileWrite;
use crate::{
    file_location::Place,
    file_table::{self, Record},
    write_rows::{AppKey, AppValues, AppView},
    DbError,
};
use coven_foundation::id_source::FileId;
use coven_merge::Audience;
use std::collections::BTreeMap;

impl FileWrite<'_> {
    /// Make the record of each new file over an existing file's bytes a row
    /// names, in the audience of the rows naming it, with the existing
    /// file's namespace, size, place and key (§17.2). An existing file this
    /// device has no record of fails the write with `FileAbsent`, and a new
    /// file no row names with `FileUnreferenced`.
    pub(super) fn make_existing(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<(), DbError> {
        if self.existing.is_empty() {
            return Ok(());
        }
        let db = self.database;
        let mut audiences = self.audiences(
            captured,
            &self.existing.iter().map(|file| file.id).collect(),
        )?;
        for file in &self.existing {
            let record =
                Record::read(db, file.from)?.ok_or(DbError::FileAbsent { file: file.from })?;
            let Some(audience) = audiences.remove(&file.id) else {
                return Err(DbError::FileUnreferenced {
                    namespace: record.namespace,
                    file: file.id,
                });
            };
            self.make_copy(file.id, &record, audience)?;
        }
        Ok(())
    }

    /// Give each row the write moves to another audience a new file there
    /// for each file it names in its old one, over the same bytes, and name
    /// it in the row (§15.4, §17.2). A row removal rules took out keeps its
    /// values in coven's records; its new files are named in the move's
    /// insert instead.
    pub(super) fn copy_moved(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<Copies, DbError> {
        let db = self.database;
        let mut copies = Copies::default();
        let before = AppView::before(db, self.schema, captured)?;
        let after = AppView::after(db, self.schema);
        let store = crate::merge_store::MergeStore::new(db, &before);
        let (moved, _) = crate::write_record::moved(
            db,
            self.schema,
            &before,
            &after,
            &store,
            captured.keys().cloned(),
        )?;
        for (old, new) in moved {
            let columns = self.schema.file_columns(&old.table);
            if columns.is_empty() {
                continue;
            }
            let table = self.schema.table(&old.table);
            let state = store.row(&old)?;
            let shown = state.loss.is_none();
            let values: AppValues = if shown {
                match crate::write_rows::read_values(db, table, &old.key)? {
                    Some(values) => values,
                    None => continue,
                }
            } else {
                state
                    .state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                    .collect()
            };
            for column in columns {
                let Some(file) = values.get(column).and_then(file_table::id) else {
                    continue;
                };
                let Some(record) = Record::read(db, file)? else {
                    continue;
                };
                if record.audience != old.audience || record.audience == new.audience {
                    continue;
                }
                let copy = match copies.made.get(&(file, new.audience.clone())) {
                    Some(copy) => *copy,
                    None => {
                        let copy = self.copy(&record, new.audience.clone())?;
                        copies.made.insert((file, new.audience.clone()), copy);
                        copy
                    }
                };
                if shown {
                    let changed = db.internal_execute(
                        &format!(
                            "UPDATE main.{} SET {}=?1 WHERE {}",
                            crate::sql::identifier(&table.name),
                            crate::sql::identifier(column),
                            crate::write_rows::key_predicate(table)
                        ),
                        rusqlite::params_from_iter(
                            std::iter::once(rusqlite::types::Value::Text(copy.0.to_string()))
                                .chain(crate::write_rows::key_parameters(&old.key)?),
                        ),
                    )?;
                    if changed != 1 {
                        return Err(rusqlite::Error::StatementChangedRows(changed).into());
                    }
                } else {
                    copies.taken_out.insert((file, new.audience.clone()), copy);
                }
            }
        }
        Ok(copies)
    }

    /// A new file in `audience` over `record`'s bytes, given a new id.
    fn copy(&self, record: &Record, audience: Audience) -> Result<FileId, DbError> {
        let id = FileId(self.ids.new_id());
        self.make_copy(id, record, audience)?;
        Ok(id)
    }

    /// Make file `id` in `audience` over `record`'s bytes: the same size,
    /// namespace, place and content hash (§17.2). Only a device holding a
    /// file's bytes makes one over them: a file on another device only fails
    /// the write with `NotThisDevice`, and a file on this device only shares
    /// its bytes here and any upload, at once, or, for bytes this write
    /// stages, its own or those `record` was made over, once the write keeps
    /// them and queues their upload.
    fn make_copy(&self, id: FileId, record: &Record, audience: Audience) -> Result<(), DbError> {
        let db = self.database;
        crate::write_commit::check_membership(db, self.device, &audience)?;
        match record.place {
            Place::DeviceOnly(device) if device != self.device => {
                return Err(DbError::NotThisDevice { file: record.id });
            }
            _ => {}
        }
        Record {
            id,
            audience,
            ..record.clone()
        }
        .insert(db)?;
        let bytes = self.staged_bytes(record.id);
        if self.staged.iter().any(|file| file.id == bytes) {
            self.copies.borrow_mut().entry(bytes).or_default().push(id);
        } else if record.place == Place::DeviceOnly(self.device) {
            super::uploads::share(db, record.id, id)?;
        }
        Ok(())
    }

    /// The file this write stages whose bytes `file` is: itself, or the one
    /// it made `file` over.
    pub(super) fn staged_bytes(&self, file: FileId) -> FileId {
        self.copies
            .borrow()
            .iter()
            .find(|(_, copies)| copies.contains(&file))
            .map_or(file, |(staged, _)| *staged)
    }
}

/// New files moves made, by the file each shares bytes with and the
/// audience it was made in.
pub(crate) type Copied = BTreeMap<(FileId, Audience), FileId>;

/// The new files a write's moves made, and those a taken-out row's insert
/// names instead of the files of its old audience.
#[derive(Default)]
pub(super) struct Copies {
    made: Copied,
    pub(crate) taken_out: Copied,
}

#[cfg(test)]
#[path = "file_write_copies_tests.rs"]
mod tests;
