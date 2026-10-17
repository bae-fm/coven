//! Carry the counted projection through the same schema identities as app rows.
use crate::{
    migration_names::MigrationMatch,
    sqlite::DatabaseConnection,
    write_encoding::{decoded, encoded},
    write_schema::WriteSchema,
    DbError,
};
use coven_format::{
    loss::{Loss, LossValues},
    merge_fields,
    snapshot_rows::MergeRow,
};
use coven_merge::{ColumnValue, RowId};
use rusqlite::params;

fn column(
    names: &MigrationMatch,
    schema: &WriteSchema,
    table: &str,
    name: String,
    mut value: ColumnValue<coven_format::value::Value>,
) -> Option<(String, ColumnValue<coven_format::value::Value>)> {
    let target = names.tables.get(table)?;
    let name = names.columns.get(&(table.to_owned(), name))?.clone();
    value.parents = value
        .parents
        .into_iter()
        .filter_map(|(key, mut parent)| {
            let key = names.foreign_key(table, &key, None)?;
            let target = schema.table(target);
            if !target
                .foreign_keys
                .iter()
                .any(|fk| schema.foreign_key(target, fk) == key)
            {
                return None;
            }
            parent.row.table = names.tables.get(&parent.row.table)?.clone();
            Some((key, parent))
        })
        .collect();
    Some((name, value))
}

pub(crate) fn carry(
    db: &DatabaseConnection,
    names: &MigrationMatch,
    schema: &WriteSchema,
) -> Result<(), DbError> {
    if !crate::published::exists(db)? {
        return Ok(());
    }
    // Staging makes table-name swaps independent of update order.
    db.batch("CREATE TEMP TABLE _coven_carried_rows AS SELECT * FROM _coven_published_rows; DELETE FROM _coven_published_rows")?;
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    db.for_each("SELECT record,visible FROM temp._coven_carried_rows",[],|r| {
        let merged=merge_fields::decode_merge_row(&r.get::<_,Vec<u8>>(0)?,&oracle)?;oracle.check()?;
        let old=merged.state.row().clone();
        let Some(table)=names.tables.get(&old.table) else {return Ok::<_,DbError>(());};
        let row=RowId {table:table.clone(),..old.clone()};
        let state=merged.state.map_schema(row.clone(),|name,value|column(names,schema,&old.table,name,value));
        let visible:bool=r.get(1)?;
        db.internal_execute("INSERT INTO _coven_published_rows(table_name,key,audience,record,visible) VALUES(?1,?2,?3,?4,?5)",params![row.table,row.key,crate::write_encoding::audience_text(&row.audience),encoded(merge_fields::encode_merge_row(&MergeRow{state}))?,visible])?;
        Ok::<_,DbError>(())
    })?;
    db.batch("DROP TABLE temp._coven_carried_rows; CREATE TEMP TABLE _coven_carried_creations AS SELECT * FROM _coven_published_creations; CREATE INDEX temp._coven_carried_creations_table ON _coven_carried_creations(table_name); DELETE FROM _coven_published_creations")?;
    for (old, new) in &names.tables {
        db.internal_execute("INSERT INTO _coven_published_creations SELECT ?2,key,audience,write_id,creator FROM temp._coven_carried_creations WHERE table_name=?1",params![old,new])?;
    }
    db.batch("DROP TABLE temp._coven_carried_creations; CREATE TEMP TABLE _coven_carried_losses AS SELECT record FROM _coven_published_losses; DELETE FROM _coven_published_losses")?;
    db.for_each("SELECT record FROM temp._coven_carried_losses",[],|r| {
        let loss=decoded(merge_fields::decode_loss(&r.get::<_,Vec<u8>>(0)?))?;
        let losses=carry_loss(names,schema,loss);
        for loss in losses {
            db.internal_execute("INSERT INTO _coven_published_losses(table_name,key,audience,record) VALUES(?1,?2,?3,?4)",params![loss.row.table,loss.row.key,crate::write_encoding::audience_text(&loss.row.audience),encoded(merge_fields::encode_loss(&loss))?])?;
        }
        Ok::<_,DbError>(())
    })?;
    db.batch("DROP TABLE temp._coven_carried_losses")
}

fn carry_loss(names: &MigrationMatch, schema: &WriteSchema, mut loss: Loss) -> Vec<Loss> {
    if loss.frozen {
        return vec![loss];
    }
    let old = loss.row.table.clone();
    if !names.tables.contains_key(&old) {
        return vec![crate::migration_state::frozen(loss)];
    }
    let dropped = match &loss.values {
        LossValues::Cell { column, .. } => {
            !names.columns.contains_key(&(old.clone(), column.clone()))
        }
        LossValues::Row(cells) => cells
            .keys()
            .any(|c| !names.columns.contains_key(&(old.clone(), c.clone()))),
    };
    let mut kept = Vec::new();
    if dropped {
        kept.push(crate::migration_state::frozen(loss.clone()));
        if matches!(loss.values, LossValues::Cell { .. }) {
            return kept;
        }
    }
    loss.row.table = names.tables[&old].clone();
    loss.values = match loss.values {
        LossValues::Cell {
            column: name,
            mut cell,
        } => {
            let (name, value) =
                column(names, schema, &old, name, cell.value).expect("surviving column");
            cell.value = value;
            LossValues::Cell { column: name, cell }
        }
        LossValues::Row(cells) => LossValues::Row(
            cells
                .into_iter()
                .filter_map(|(name, mut cell)| {
                    let (name, value) = column(names, schema, &old, name, cell.value)?;
                    cell.value = value;
                    Some((name, cell))
                })
                .collect(),
        ),
    };
    // Active removal losses are recomputed under the new constraints. Their
    // retained values are needed until the counted fold materializes them.
    kept.push(loss);
    kept
}
