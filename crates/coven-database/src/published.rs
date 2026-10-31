//! The counted data beneath this installation's provisional write queue.
//!
//! While originals await slots, preserve the published merge state separately.
//! Incoming slots rebuild from it, then fold the current queue once. App rows
//! are materialized only after both folds, in the enclosing transaction.

use crate::write_encoding::{audience_text, decoded, encoded};
use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError,
};
use coven_format::{merge_fields, snapshot_rows::MergeRow};
use coven_merge::{RowId, RowState, RowUpdate};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn exists(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_published)", [], |r| r.get(0))
}

/// Preserve each affected row once, before provisional changes reach it.
pub(crate) fn capture_rows<'a>(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    rows: impl IntoIterator<Item = &'a RowId>,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_published(singleton) VALUES(1) ON CONFLICT DO NOTHING",
        [],
    )?;
    for row in rows {
        let audience = audience_text(&row.audience);
        let kept:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_published_rows WHERE table_name=?1 AND key=?2 AND audience=?3)",params![row.table,row.key,audience],|r|r.get(0))?;
        if kept {
            continue;
        }
        #[cfg(any(test, feature = "test-utils"))]
        db.record_work(|work| work.published_rows += 1);
        let stored = store.row_without_losses(row)?;
        let visible = stored.state.present() && stored.loss.is_none();
        db.internal_execute("INSERT INTO _coven_published_rows(table_name,key,audience,record,visible) VALUES(?1,?2,?3,?4,?5)",params![row.table,row.key,audience,encoded(merge_fields::encode_merge_row(&MergeRow{state:stored.state}))?,visible])?;
        db.for_each(&format!("{} WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3",crate::loss_record::SELECT),params![row.table,row.key,audience],|r| {
            let loss=crate::loss_record::read(r)?;
            db.internal_execute("INSERT INTO _coven_published_losses(table_name,key,audience,record) VALUES(?1,?2,?3,?4)",params![row.table,row.key,audience,encoded(merge_fields::encode_loss(&loss))?])?;
            Ok::<_,DbError>(())
        })?;
        db.internal_execute("INSERT INTO _coven_published_creations SELECT * FROM _coven_creations WHERE table_name=?1 AND key=?2 AND audience=?3",params![row.table,row.key,audience])?;
        db.internal_execute("INSERT INTO _coven_published_file_refs SELECT device,file FROM _coven_row_file_refs WHERE table_name=?1 AND key=?2 ON CONFLICT DO NOTHING",params![row.table,row.key])?;
    }
    Ok(())
}

pub(crate) fn capture(db: &DatabaseConnection, store: &MergeStore<'_>) -> Result<(), DbError> {
    let rows=db.query("SELECT table_name,key,audience FROM _coven_rows UNION SELECT table_name,key,audience FROM _coven_lost",[],crate::row_queries::read_identity)?;
    capture_rows(db, store, &rows)
}

/// Migrations preserve the entire counted projection before changing its schema.
pub(crate) fn before_migration(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
) -> Result<(), DbError> {
    let store = MergeStore::from_schema(db, schema).without_row_cache();
    capture(db, &store)
}

pub(crate) fn clear(db: &DatabaseConnection) -> Result<(), DbError> {
    for table in [
        "_coven_published_rows",
        "_coven_published_losses",
        "_coven_published_creations",
        "_coven_published_file_refs",
        "_coven_published",
    ] {
        db.internal_execute(&format!("DELETE FROM {table}"), [])?;
    }
    Ok(())
}

/// Restore only merge metadata and staged values; the previously visible app
/// tables remain untouched until the final materialization.
pub(crate) fn restore(
    db: &DatabaseConnection,
    schema: &WriteSchema,
) -> Result<BTreeSet<RowId>, DbError> {
    if !exists(db)? {
        return Ok(BTreeSet::new());
    }
    let rows=db.query("SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses",[],crate::row_queries::read_identity)?;
    let mut touched = BTreeSet::new();
    #[cfg(any(test, feature = "test-utils"))]
    db.record_work(|work| work.published_rows += rows.len());
    for row in rows {
        let audience = audience_text(&row.audience);
        for table in ["_coven_references", "_coven_cells", "_coven_claims"] {
            db.internal_execute(&format!("DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3)"),params![row.table,row.key,audience])?;
        }
        for table in ["_coven_rows", "_coven_lost", "_coven_creations"] {
            db.internal_execute(
                &format!("DELETE FROM {table} WHERE table_name=?1 AND key=?2 AND audience=?3"),
                params![row.table, row.key, audience],
            )?;
        }
        if schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
            touched.insert(row);
        }
    }
    // The oracle reads the originals' metadata, which provisional application
    // never changes. It is independent of the rows being restored.
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    db.for_each(
        "SELECT record FROM _coven_published_rows ORDER BY audience,table_name,key",
        [],
        |r| {
            let bytes: Vec<u8> = r.get(0)?;
            let merged = merge_fields::decode_merge_row(&bytes, &oracle);
            oracle.check()?;
            let merged = merged?;
            let row = merged.state.row().clone();
            if !schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
                return Ok::<_, DbError>(());
            }
            crate::download_stage::put_values(db, &row, &merged.state)?;
            crate::write_commit::persist(
                db,
                &BTreeMap::from([(
                    row.clone(),
                    RowUpdate { state: merged.state, lost_changes: Vec::new() },
                )]),
                |row| {
                    Ok(crate::merge_store::StoredRow {
                        state: RowState::new(row.clone()),
                        ordinal: None,
                        loss: None,
                        lost_ids: BTreeMap::new(),
                    })
                },
                |id| crate::snapshot_state::ordinal(db, id),
            )?;
            if schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
                touched.insert(row);
            }
            Ok::<_, DbError>(())
        },
    )?;
    db.for_each("SELECT record FROM _coven_published_losses ORDER BY id", [], |r| {
        let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
        crate::loss_record::put(db, &loss, None)?;
        crate::file_protection::retain_loss(db, schema, &loss)
    })?;
    db.internal_execute(
        "INSERT INTO _coven_creations SELECT * FROM _coven_published_creations",
        [],
    )?;
    clear(db)?;
    Ok(touched)
}
