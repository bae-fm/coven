//! Bound references (§17.4): the write setting one goes out only once its
//! file is stored, so other devices get the row and the stored file together.

use super::FileWrite;
use crate::{
    file_location::Place,
    file_table::{self, Record},
    write_rows::{AppKey, AppView},
    Binding, DbError, Provenance,
};
use coven_format::value::Value;
use std::collections::BTreeMap;

impl FileWrite<'_> {
    /// Check each bound reference the app's SQL sets, before coven's own
    /// moves name new files (§17.4). It names an app file within the
    /// column's limit, and one stored already, or uploading for a write
    /// that waits for it: one this write stages for the cloud, whose upload
    /// then holds this write, or one an earlier write's bound upload stores.
    /// A reference naming no file of its row's audience is left to the
    /// reference check, which refuses it.
    pub(super) fn check_bound(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<(), DbError> {
        let db = self.database;
        let view = AppView::after(db, self.schema);
        for (key, row) in captured {
            let bound: Vec<(&str, u64)> = self
                .schema
                .declaration(&key.0)
                .files
                .iter()
                .filter_map(|column| match column.binding {
                    Binding::Bound { limit } => Some((column.column.as_str(), limit)),
                    Binding::Trailing => None,
                })
                .collect();
            if bound.is_empty() {
                continue;
            }
            let table = self.schema.table(&key.0);
            let Some(values) = crate::write_rows::read_values(db, table, &key.1)? else {
                continue;
            };
            for (column, limit) in bound {
                let Some(value) = values.get(column).filter(|value| **value != Value::Null) else {
                    continue;
                };
                // A reference this write leaves as it was is not set by it.
                if row.old.get(column) == Some(value) {
                    continue;
                }
                let Some(file) = file_table::id(value) else {
                    continue;
                };
                let Some(record) = Record::read(db, file)? else {
                    continue;
                };
                if record.audience != view.audience_of(table, &values)? {
                    continue;
                }
                let kind = &self
                    .schema
                    .namespace(&record.namespace)
                    .ok_or_else(|| DbError::UndeclaredNamespace {
                        namespace: record.namespace.clone(),
                    })?
                    .kind;
                if *kind == Provenance::UserProvided {
                    return Err(DbError::NamespaceKind {
                        namespace: record.namespace,
                        kind: Provenance::UserProvided,
                    });
                }
                if record.size > limit {
                    return Err(DbError::BoundFileTooLarge {
                        table: table.name.clone(),
                        column: column.to_owned(),
                        size: record.size,
                        limit,
                    });
                }
                match record.place {
                    Place::Cloud(_) => {}
                    Place::DeviceOnly(_) if self.stages_for_cloud(record.id) => {
                        self.held.borrow_mut().insert(record.id);
                    }
                    Place::DeviceOnly(device)
                        if device == self.device
                            && crate::file_queue::held_upload_of(db, record.id)? => {}
                    Place::DeviceOnly(_) => {
                        return Err(DbError::DeviceOnly { file: record.id });
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether this write stages `file` for the cloud.
    fn stages_for_cloud(&self, file: coven_foundation::id_source::FileId) -> bool {
        self.staged
            .iter()
            .any(|staged| staged.id == file && staged.place != crate::Place::DeviceOnly)
    }
}

#[cfg(test)]
#[path = "file_write_bound_tests.rs"]
mod tests;
