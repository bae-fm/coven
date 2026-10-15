//! This device's records of user-provided originals (`_coven_user_files`):
//! kept while a merge row names the original, hidden or not (§14.7, §16.5,
//! `RetentionSafety.LocalSources`).

use super::FileWrite;
use crate::{
    file_row,
    write_rows::{AppKey, AppView},
    DbError, Provenance,
};
use std::collections::BTreeSet;

impl FileWrite<'_> {
    /// Retain an original's recorded facts while a current row still names it,
    /// including rows temporarily removed by constraints and rows a circle
    /// deletion hides: that deletion is not a row delete and can still drop
    /// (§14.7). Historical generations cannot keep them. Audience moves keep a
    /// file's identity. The upload queue keeps its own source facts and owned
    /// copies independently of rows (§16.5). Nothing physically removes a
    /// deleted circle's rows, so a hidden row's record outlives the deletion's
    /// finality, and a deleted generation forgets the record before the
    /// deleting write is final: both differ from §16.5's rule
    /// (`implemented_keeps_hidden_forever`, `implemented_releases_returning_row`).
    pub(crate) fn retain_rows(&self, keys: BTreeSet<AppKey>) -> Result<(), DbError> {
        let db = self.database;
        // Each file identity is checked once; retaining its app row would keep
        // every downloaded payload alive until this whole write finishes.
        let visible = AppView::after(db, self.schema).without_row_cache();
        let store = crate::merge_store::MergeStore::new(db, &visible).without_row_cache();
        for key in keys {
            crate::file_protection::refresh(db, self.schema, &key)?;
            crate::eager_work::note(db, self.schema, self.device, &key)?;
            let Some(file) = &self.schema.declaration(&key.0).files else {
                continue;
            };
            if file.provenance != Provenance::UserProvided {
                continue;
            }
            let identities = db.query(
                "SELECT identity FROM _coven_user_files
                 WHERE table_name=?1 AND key=?2 AND column_name=?3",
                (&key.0, &key.1, &file.id),
                |r| r.get::<_, Vec<u8>>(0),
            )?;
            if identities.is_empty() {
                continue;
            }
            let mut retained = BTreeSet::new();
            for (audience, generation) in db.query(
                "SELECT audience,max(generation) FROM _coven_rows
                 WHERE table_name=?1 AND key=?2 GROUP BY audience",
                (&key.0, &key.1),
                |r| {
                    Ok((
                        crate::write_encoding::audience(&r.get::<_, String>(0)?)?,
                        crate::write_encoding::counter(r.get(1)?),
                    ))
                },
            )? {
                if generation % 2 == 0 {
                    continue;
                }
                let row = store.row(&coven_merge::RowId {
                    table: key.0.clone(),
                    key: key.1.clone(),
                    audience,
                })?;
                let values = row
                    .state
                    .cells()
                    .iter()
                    .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
                    .collect();
                if let Some(identity) = file_row::identity(file, &values)? {
                    retained.insert(identity);
                }
            }
            if identities
                .iter()
                .all(|identity| !retained.contains(identity))
            {
                self.forget_original(&key, &file.id)?;
            }
        }
        Ok(())
    }

    pub(super) fn forget_original(&self, key: &AppKey, column: &str) -> Result<(), DbError> {
        self.database.internal_execute(
            "DELETE FROM _coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3",
            (&key.0, &key.1, column),
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "file_write_sources_tests.rs"]
mod tests;
