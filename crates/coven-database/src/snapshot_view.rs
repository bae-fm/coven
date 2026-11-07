//! Snapshot projection of counted rows, independent of queued local originals.
use crate::write_encoding::decoded;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{authorship::Creation, loss::Loss, merge_fields, snapshot_rows::MergeRow};
use coven_merge::{Audience, RowId, Rule};
use std::collections::{BTreeMap, BTreeSet};

/// Each counted row of `audience`.
pub(crate) fn rows<E: From<DbError>>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    mut visit: impl FnMut(MergeRow) -> Result<(), E>,
) -> Result<(), E> {
    let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema).without_row_cache();
    db.for_each("SELECT table_name,key FROM _coven_rows WHERE audience=?1 UNION SELECT table_name,key FROM _coven_published_rows WHERE audience=?1 ORDER BY table_name,key",[audience],|r| {
        let id=RowId {table:r.get(0).map_err(DbError::from)?,key:r.get(1).map_err(DbError::from)?,audience:audience.clone()};
        let state=match crate::published::kept_state(db,&id)? {
            Some(state)=>state,
            None=>store.row_without_losses(&id)?.state,
        };
        if state.generations().is_empty() {return Ok(());}
        visit(MergeRow {state})
    })
}

/// Each counted loss of `audience`: the stored ones, and the removal losses
/// of the kept rows `removed` takes out (`published::removals`).
pub(crate) fn losses<E: From<DbError>>(
    db: &DatabaseConnection,
    removed: &BTreeMap<RowId, BTreeSet<Rule>>,
    audience: &Audience,
    mut visit: impl FnMut(Loss) -> Result<(), E>,
) -> Result<(), E> {
    db.batch(
        "CREATE TEMP TABLE _coven_snapshot_losses (
        id INTEGER PRIMARY KEY,table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,
        generation BLOB NOT NULL,column_id INTEGER,value BLOB NOT NULL,set_by BLOB NOT NULL,
        frozen INTEGER NOT NULL,replacement_kind TEXT NOT NULL,replaced_by BLOB NOT NULL);",
    )?;
    db.internal_execute("INSERT INTO temp._coven_snapshot_losses SELECT * FROM _coven_lost l WHERE l.audience=?1 AND NOT EXISTS(SELECT 1 FROM _coven_published_rows p WHERE p.table_name=l.table_name AND p.key=l.key AND p.audience=l.audience) AND NOT EXISTS(SELECT 1 FROM _coven_published_losses p WHERE p.table_name=l.table_name AND p.key=l.key AND p.audience=l.audience) AND NOT EXISTS(SELECT 1 FROM _coven_published_dropped p WHERE p.table_name=l.table_name AND p.key=l.key AND p.audience=l.audience)",[audience])?;
    db.for_each(
        "SELECT record FROM _coven_published_losses WHERE audience=?1 ORDER BY id",
        [audience],
        |r| {
            let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
            crate::loss_record::put_table(
                db,
                &loss,
                None,
                crate::loss_record::LossTable::SnapshotView,
            )
        },
    )?;
    for (row, rules) in removed.iter().filter(|(row, _)| row.audience == *audience) {
        let state = crate::published::kept_state(db, row)?
            .ok_or(DbError::Invariant { what: "a kept row's removal names a kept record" })?;
        let loss = coven_format::loss::Loss::removed(&state, rules.clone());
        crate::loss_record::put_table(
            db,
            &loss,
            None,
            crate::loss_record::LossTable::SnapshotView,
        )?;
    }
    crate::loss_record::visit_table(
        db,
        audience,
        crate::loss_record::LossTable::SnapshotView,
        &mut visit,
    )?;
    db.batch("DROP TABLE temp._coven_snapshot_losses")?;
    Ok(())
}

pub(crate) fn creations<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(Creation) -> Result<(), E>,
) -> Result<(), E> {
    db.for_each("SELECT c.table_name,c.key,c.write_id,c.creator FROM _coven_creations c WHERE c.audience=?1 AND NOT EXISTS(SELECT 1 FROM _coven_published_rows p WHERE p.table_name=c.table_name AND p.key=c.key AND p.audience=c.audience) UNION ALL SELECT table_name,key,write_id,creator FROM _coven_published_creations WHERE audience=?1 ORDER BY table_name,key",[audience],|r| {
        visit(Creation {
            row:RowId {table:r.get(0).map_err(DbError::from)?,key:r.get(1).map_err(DbError::from)?,audience:audience.clone()},
            write:decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(2).map_err(DbError::from)?)).map_err(DbError::from)?,
            creator:decoded(merge_fields::decode_change_id(&r.get::<_,Vec<u8>>(3).map_err(DbError::from)?)).map_err(DbError::from)?,
        })
    })
}
