//! Snapshot projection of counted rows, independent of queued local originals.
use crate::write_encoding::{audience_text, decoded};
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    authorship::Creation,
    loss::Loss,
    merge_fields,
    snapshot_rows::{MergeRow, SyncedRow},
};
use coven_merge::{Audience, RowId};

pub(crate) fn rows<E: From<DbError>>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    mut visit: impl FnMut(MergeRow, Option<SyncedRow>) -> Result<(), E>,
) -> Result<(), E> {
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema).without_row_cache();
    db.for_each("SELECT table_name,key FROM _coven_rows WHERE audience=?1 UNION SELECT table_name,key FROM _coven_published_rows WHERE audience=?1 ORDER BY table_name,key",[audience_text(audience)],|r| {
        let id=RowId {table:r.get(0).map_err(DbError::from)?,key:r.get(1).map_err(DbError::from)?,audience:audience.clone()};
        let kept=db.query("SELECT record,visible FROM _coven_published_rows WHERE table_name=?1 AND key=?2 AND audience=?3",rusqlite::params![id.table,id.key,audience_text(audience)],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,bool>(1)?)))?.pop();
        let (row,visible)=if let Some((bytes,visible))=kept {
            let row=merge_fields::decode_merge_row(&bytes,&oracle).map_err(DbError::from)?;
            oracle.check()?;
            (row,visible)
        } else {
            let stored=store.row_without_losses(&id)?;
            let visible=stored.state.present() && stored.loss.is_none();
            (MergeRow {state:stored.state},visible)
        };
        if row.state.generations().is_empty() {return Ok(());}
        let synced=if visible {
            let raw=row.state.cells().iter().map(|(name,cell)|(name.clone(),cell.value.value.clone())).collect();
            let values=crate::removal_sql::evaluate_values(db,schema.table(&id.table),&raw)?;
            let columns=values.into_iter().map(|(name,value)| {
                let column=row.state.cells().get(&name).map(|cell|cell.value.clone()).unwrap_or(coven_merge::ColumnValue {value,parents:Default::default()});
                (name,column)
            }).collect();
            Some(SyncedRow {row:id,columns})
        } else {None};
        visit(row,synced)
    })
}

pub(crate) fn losses<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(Loss) -> Result<(), E>,
) -> Result<(), E> {
    db.batch(
        "CREATE TEMP TABLE _coven_snapshot_losses (
        id INTEGER PRIMARY KEY,table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,
        generation BLOB NOT NULL,column_id INTEGER,value BLOB NOT NULL,set_by BLOB NOT NULL,
        frozen INTEGER NOT NULL,replacement_kind TEXT NOT NULL,replaced_by BLOB NOT NULL);",
    )?;
    db.internal_execute("INSERT INTO temp._coven_snapshot_losses SELECT * FROM _coven_lost l WHERE l.audience=?1 AND NOT EXISTS(SELECT 1 FROM _coven_published_rows p WHERE p.table_name=l.table_name AND p.key=l.key AND p.audience=l.audience) AND NOT EXISTS(SELECT 1 FROM _coven_published_losses p WHERE p.table_name=l.table_name AND p.key=l.key AND p.audience=l.audience)",[audience_text(audience)])?;
    db.for_each(
        "SELECT record FROM _coven_published_losses WHERE audience=?1 ORDER BY id",
        [audience_text(audience)],
        |r| {
            let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
            crate::loss_record::put_table(db, &loss, None, "temp._coven_snapshot_losses")
        },
    )?;
    crate::loss_record::visit_table(db, audience, "temp._coven_snapshot_losses", &mut visit)?;
    db.batch("DROP TABLE temp._coven_snapshot_losses")?;
    Ok(())
}

pub(crate) fn creations<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(Creation) -> Result<(), E>,
) -> Result<(), E> {
    db.for_each("SELECT c.table_name,c.key,c.write_id,c.creator FROM _coven_creations c WHERE c.audience=?1 AND NOT EXISTS(SELECT 1 FROM _coven_published_rows p WHERE p.table_name=c.table_name AND p.key=c.key AND p.audience=c.audience) UNION ALL SELECT table_name,key,write_id,creator FROM _coven_published_creations WHERE audience=?1 ORDER BY table_name,key",[audience_text(audience)],|r| {
        visit(Creation {
            row:RowId {table:r.get(0).map_err(DbError::from)?,key:r.get(1).map_err(DbError::from)?,audience:audience.clone()},
            write:decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(2).map_err(DbError::from)?)).map_err(DbError::from)?,
            creator:decoded(merge_fields::decode_change_id(&r.get::<_,Vec<u8>>(3).map_err(DbError::from)?)).map_err(DbError::from)?,
        })
    })
}
