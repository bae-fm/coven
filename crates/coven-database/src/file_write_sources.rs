//! Release local original-source records when no live row can name them.

use super::FileWrite;
use crate::{
    file_row,
    sqlite::DatabaseConnection,
    write_rows::{AppKey, AppView},
    write_schema::WriteSchema,
    DbError, Provenance,
};
use coven_merge::Audience;
use std::collections::BTreeSet;

impl FileWrite<'_> {
    /// Refresh the file facts of the rows a write touched, then forget each
    /// original no row can name any more (`retain`). Historical generations
    /// cannot keep a record; audience moves keep a file's identity. The upload
    /// queue keeps its own source facts and owned copies independently of
    /// rows (§17.7).
    pub(crate) fn retain_rows(&self, keys: BTreeSet<AppKey>) -> Result<(), DbError> {
        for key in &keys {
            crate::file_protection::refresh(self.database, self.schema, key)?;
            crate::eager_work::note(self.database, self.schema, self.device, key)?;
        }
        retain(self.database, self.schema, keys)
    }
}

/// A live row retains its named original even while constraints hide it.
fn retain(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    keys: impl IntoIterator<Item = AppKey>,
) -> Result<(), DbError> {
    // Each file identity is checked once; retaining its app row would keep
    // every downloaded payload alive until this whole write finishes.
    let visible = AppView::after(db, schema).without_row_cache();
    let store = crate::merge_store::MergeStore::new(db, &visible).without_row_cache();
    let mut deletions = None;
    for key in keys {
        let Some(file) = &schema.declaration(&key.0).files else {
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
            if let Audience::Circle(circle) = &audience {
                if deletions.is_none() {
                    deletions = Some(crate::store_state_tables::deleted_circles(db)?);
                }
                if deletions.as_ref().is_some_and(|d| d.contains_key(circle)) {
                    continue;
                }
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
        let oracle = crate::write_metadata::DatabaseMetadata::new(db);
        db.for_each(
            "SELECT record FROM _coven_published_rows WHERE table_name=?1 AND key=?2",
            (&key.0, &key.1),
            |r| {
                let state = coven_format::merge_fields::decode_merge_row(
                    &r.get::<_, Vec<u8>>(0)?,
                    &oracle,
                )?
                .state;
                oracle.check()?;
                if !state.present() {
                    return Ok::<_, DbError>(());
                }
                if let Audience::Circle(circle) = &state.row().audience {
                    if deletions.is_none() {
                        deletions = Some(crate::store_state_tables::deleted_circles(db)?);
                    }
                    if deletions.as_ref().is_some_and(|d| d.contains_key(circle)) {
                        return Ok(());
                    }
                }
                let values = state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                    .collect();
                if let Some(identity) = file_row::identity(file, &values)? {
                    retained.insert(identity);
                }
                Ok(())
            },
        )?;
        if identities
            .iter()
            .all(|identity| !retained.contains(identity))
        {
            forget(db, &key, &file.id)?;
        }
    }
    Ok(())
}

fn forget(db: &DatabaseConnection, key: &AppKey, column: &str) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3",
        (&key.0, &key.1, column),
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "file_write_sources_tests.rs"]
mod tests;
