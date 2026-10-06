//! Persist exactly the row updates produced by coven-merge, with their record.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded};
use crate::DbError;
use coven_format::{merge_fields, value::Value, write::WriteRecord, Object};
use coven_merge::{LostChange, RowId, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::BTreeMap;

pub(crate) fn commit(
    database: &DatabaseConnection,
    record: &WriteRecord,
    store: &MergeStore<'_>,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
) -> Result<(), DbError> {
    let bytes = encoded(Object::Write(record.clone()).encode())?;
    let write_id: i64 = database.query_row(
        "INSERT INTO coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3) RETURNING id",
        params![
            encoded(merge_fields::encode_timestamp(&record.header.timestamp))?,
            record.header.position.number.to_be_bytes().as_slice(),
            encoded(merge_fields::encode_write_positions(
                &record.header.had_read
            ))?
        ],
        |r| r.get(0),
    )?;
    database.internal_execute("INSERT INTO coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",params![record.header.position.device.0.to_be_bytes().as_slice(),record.header.position.number.to_be_bytes().as_slice()])?;
    database.internal_execute(
        "INSERT INTO coven_uploads(device,number,record) VALUES(?1,?2,?3)",
        params![
            record.header.position.device.0.to_be_bytes().as_slice(),
            record.header.position.number.to_be_bytes().as_slice(),
            bytes
        ],
    )?;
    let ordinal = |id: WriteId| {
        if id == record.header.position {
            write_id
        } else {
            store.write_ordinal(id)
        }
    };
    for (row, update) in updates {
        let old = store.row(row)?;
        let audience = audience_text(&row.audience);
        for (generation, writer) in update.state.generations() {
            if old.state.generations().get(generation) != Some(writer) {
                database.internal_execute("INSERT INTO coven_rows(table_name,key,audience,generation,write_id) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(table_name,key,audience,generation) DO UPDATE SET write_id=excluded.write_id",params![row.table,row.key,audience,generation.to_be_bytes().as_slice(),ordinal(*writer)])?;
            }
        }
        let advanced = old.state.generation() != update.state.generation();
        if advanced {
            if let Some(ordinal) = old.ordinal {
                database.internal_execute("DELETE FROM coven_cells WHERE row_id=?1", [ordinal])?;
                database
                    .internal_execute("DELETE FROM coven_references WHERE row_id=?1", [ordinal])?;
                database.internal_execute("DELETE FROM coven_claims WHERE row_id=?1", [ordinal])?;
            }
            if let Some(loss) = old.loss {
                database.internal_execute("DELETE FROM coven_lost WHERE id=?1", [loss])?;
            }
        }
        if update.state.present() {
            let row_ordinal:i64 = database.query_row("SELECT id FROM coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4",params![row.table,row.key,audience,update.state.generation().to_be_bytes().as_slice()],|r| r.get(0))?;
            for (name, cell) in update.state.cells() {
                if !advanced && old.state.cells().get(name) == Some(cell) {
                    continue;
                }
                let column = column(database, &row.table, name)?;
                database.internal_execute("INSERT INTO coven_cells(column_id,row_id,write_id) VALUES(?1,?2,?3) ON CONFLICT(column_id,row_id) DO UPDATE SET write_id=excluded.write_id",params![column,row_ordinal,ordinal(cell.write)])?;
                let previous = old.state.cells().get(name).map(|c| &c.value.parents);
                if !advanced {
                    if let Some(previous) = previous {
                        for key in previous
                            .keys()
                            .filter(|key| !cell.value.parents.contains_key(*key))
                        {
                            let key = crate::row_queries::foreign_key(database, &row.table, key)?;
                            database.internal_execute("DELETE FROM coven_references WHERE row_id=?1 AND column_id=?2 AND foreign_key_id=?3",params![row_ordinal,column,key])?;
                        }
                    }
                }
                for (key, parent) in &cell.value.parents {
                    if !advanced && previous.and_then(|p| p.get(key)) == Some(parent) {
                        continue;
                    }
                    let key = crate::row_queries::foreign_key(database, &row.table, key)?;
                    database.internal_execute("INSERT INTO coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(row_id,column_id,foreign_key_id) DO UPDATE SET parent_table=excluded.parent_table,parent_key=excluded.parent_key,parent_audience=excluded.parent_audience,parent_generation=excluded.parent_generation",params![row_ordinal,column,key,parent.row.table,parent.row.key,audience_text(&parent.row.audience),parent.generation.to_be_bytes().as_slice()])?;
                }
            }
        }
        for change in &update.lost_changes {
            match change {
                LostChange::Remove(key) => {
                    database.internal_execute(
                        "DELETE FROM coven_lost WHERE id=?1",
                        [old.lost_ids[key]],
                    )?;
                }
                LostChange::Put(key, value) => {
                    let bytes = encoded(merge_fields::encode_column_value(&value.value))?;
                    let setter = encoded(merge_fields::encode_write_id(&key.write))?;
                    let replaced_by = encoded(merge_fields::encode_write_id(&value.replaced_by))?;
                    if let Some(id) = old.lost_ids.get(key) {
                        database.internal_execute(
                            "UPDATE coven_lost SET value=?1,set_by=?2,replaced_by=?3 WHERE id=?4",
                            params![bytes, setter, replaced_by, id],
                        )?;
                    } else {
                        let column = column(database, &row.table, &key.column)?;
                        database.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replaced_by) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![row.table,row.key,audience,value.incarnation.to_be_bytes().as_slice(),column,bytes,setter,replaced_by])?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn column(database: &DatabaseConnection, table: &str, column: &str) -> Result<i64, DbError> {
    database.internal_execute(
        "INSERT INTO coven_columns(table_name,column_name) VALUES(?1,?2) ON CONFLICT DO NOTHING",
        params![table, column],
    )?;
    database.query_row(
        "SELECT id FROM coven_columns WHERE table_name=?1 AND column_name=?2",
        params![table, column],
        |r| r.get(0),
    )
}

#[cfg(test)]
#[path = "write_commit_tests.rs"]
mod tests;
