//! This device's records of user-provided originals (`_coven_user_files`):
//! kept while a row naming the original is current or can become current
//! again, and forgotten once none can (§14.7, §16.5,
//! `RetentionSafety.LocalSources`).
//!
//! A row a circle deletion hides is current again if the deletion drops, so
//! it keeps the record until that deletion is kept and final; the row is then
//! cleaned up on this device and never names the original again
//! (`hidden_released_when_final`). A deleted row returns if a rejudging
//! excludes its deleting write (§10), so it keeps the record while that
//! write's window verdict can change: while the write has a landing record
//! (`deleted_kept_until_final`). This device's own deletes are never rejudged
//! here, and a received write's record goes once its verdict is final.

use super::FileWrite;
use crate::{
    file_row,
    internal_schema::WRITE_DEVICE_SQL,
    sqlite::DatabaseConnection,
    write_encoding::audience_text,
    write_rows::{AppKey, AppView},
    write_schema::WriteSchema,
    DbError, EntryId, Provenance,
};
use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

impl FileWrite<'_> {
    /// Refresh the file facts of the rows a write touched, then forget each
    /// original no row can name any more (`retain`). Historical generations
    /// cannot keep a record; audience moves keep a file's identity. The upload
    /// queue keeps its own source facts and owned copies independently of
    /// rows (§16.5).
    pub(crate) fn retain_rows(&self, keys: BTreeSet<AppKey>) -> Result<(), DbError> {
        for key in &keys {
            crate::file_protection::refresh(self.database, self.schema, key)?;
            crate::eager_work::note(self.database, self.schema, self.device, key)?;
        }
        retain(self.database, self.schema, keys)
    }

    pub(super) fn forget_original(&self, key: &AppKey, column: &str) -> Result<(), DbError> {
        forget(self.database, key, column)
    }
}

/// Forget each of `keys`' recorded originals that no row can name any more.
/// A row keeps the identity it names while it is current, including while
/// constraints take it out or a deletion not yet final hides it; a row a kept
/// and final circle deletion hides is cleaned up and keeps nothing. A deleted
/// row keeps every record of its key while its deleting write's verdict can
/// still change, since the row it would bring back is not read here.
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
        let mut held = false;
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
                held |= deletion_open(db, &key, &audience, generation)?;
                continue;
            }
            if let Audience::Circle(circle) = &audience {
                if deletions.is_none() {
                    deletions = Some(final_deletions(db)?);
                }
                if deletions.as_ref().is_some_and(|d| d.contains(circle)) {
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
        if !held
            && identities
                .iter()
                .all(|identity| !retained.contains(identity))
        {
            forget(db, &key, &file.id)?;
        }
    }
    Ok(())
}

/// The kept circle deletions that are final: they hide their rows for good,
/// so those rows are cleaned up and keep no original (§14.7, `cleanup`).
fn final_deletions(db: &DatabaseConnection) -> Result<BTreeSet<CircleId>, DbError> {
    let deleted = crate::store_log_tables::deleted_circles(db)?;
    let open =
        crate::local_effects::provisional(db, &deleted.values().copied().collect::<BTreeSet<_>>())?;
    Ok(deleted
        .into_iter()
        .filter(|(_, entry)| !open.contains(entry))
        .map(|(circle, _)| circle)
        .collect())
}

/// What can still keep an original's record before a horizon advance: kept
/// circle deletions not yet final, and received writes whose window verdict
/// is still recorded. Only an advance settles either (§9, §10).
pub(crate) struct Open {
    deletions: BTreeMap<CircleId, EntryId>,
    writes: BTreeSet<WriteId>,
}

impl Open {
    pub(crate) fn before(db: &DatabaseConnection) -> Result<Self, DbError> {
        let mut deletions = crate::store_log_tables::deleted_circles(db)?;
        let open = crate::local_effects::provisional(
            db,
            &deletions.values().copied().collect::<BTreeSet<_>>(),
        )?;
        deletions.retain(|_, entry| open.contains(entry));
        Ok(Self {
            deletions,
            writes: crate::write_landing::recorded_writes(db)?,
        })
    }

    /// In the advance's transaction, after its releases: forget the records
    /// whose rows a deletion it made final cleans up, or a delete whose
    /// verdict it settled removes for good (`hidden_released_when_final`,
    /// `deleted_released_when_final`).
    pub(crate) fn release(
        self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
    ) -> Result<(), DbError> {
        let open = crate::local_effects::provisional(
            db,
            &self.deletions.values().copied().collect::<BTreeSet<_>>(),
        )?;
        let mut keys = BTreeSet::new();
        for (circle, entry) in &self.deletions {
            if open.contains(entry) {
                continue;
            }
            keys.extend(db.query(
                "SELECT DISTINCT r.table_name,r.key FROM _coven_rows r
                 JOIN _coven_user_files u ON u.table_name=r.table_name AND u.key=r.key
                 WHERE r.audience=?1",
                [audience_text(&Audience::Circle(*circle))],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )?);
        }
        let recorded = crate::write_landing::recorded_writes(db)?;
        for write in self.writes.difference(&recorded) {
            keys.extend(db.query(
                &format!(
                    "SELECT DISTINCT r.table_name,r.key FROM _coven_writes w
                     JOIN _coven_rows r ON r.write_id=w.id
                     JOIN _coven_user_files u ON u.table_name=r.table_name AND u.key=r.key
                     WHERE {WRITE_DEVICE_SQL}=?1 AND w.number=?2"
                ),
                params![
                    write.device.0.to_be_bytes().as_slice(),
                    write.number.to_be_bytes().as_slice()
                ],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )?);
        }
        retain(db, schema, keys)
    }
}

/// Whether the write that made this deleting generation can still be
/// rejudged: a received write keeps its landing record until its window
/// verdict is final (§10, `write_landing::release`).
fn deletion_open(
    db: &DatabaseConnection,
    key: &AppKey,
    audience: &Audience,
    generation: u64,
) -> Result<bool, DbError> {
    db.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM _coven_rows r JOIN _coven_writes w ON w.id=r.write_id
             JOIN _coven_write_landings l ON l.device={WRITE_DEVICE_SQL} AND l.number=w.number
             WHERE r.table_name=?1 AND r.key=?2 AND r.audience=?3 AND r.generation=?4)"
        ),
        params![
            key.0,
            key.1,
            audience_text(audience),
            generation.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )
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
