//! Indexed loading of the merge's own row state and causal metadata.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, decoded};
use crate::write_rows::AppView;
use crate::DbError;
use coven_format::{merge_fields, snapshot_rows::AppliedWrite, value::Value, write::WriteRecord};
use coven_merge::{
    Cell, ColumnValue, LostKey, LostValue, MergeError, RowId, RowState, RowUpdate, Timestamp,
    WriteId, WriteOracle,
};
use rusqlite::params;
use std::cell::RefCell;
use std::collections::BTreeMap;

struct StoredWrite {
    ordinal: i64,
    record: AppliedWrite,
}

struct WriteMetadata<'a> {
    database: &'a DatabaseConnection,
    writes: RefCell<BTreeMap<WriteId, StoredWrite>>,
}

impl<'a> WriteMetadata<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection) -> Self {
        Self {
            database,
            writes: RefCell::new(BTreeMap::new()),
        }
    }
    pub(crate) fn load(&self, id: WriteId) -> Result<(), DbError> {
        if self.writes.borrow().contains_key(&id) {
            return Ok(());
        }
        let applied=self.database.query_row("SELECT id,timestamp,had_read FROM coven_writes WHERE substr(timestamp,9,8)=?1 AND number=?2",params![id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()],|r| Ok(StoredWrite { ordinal:r.get(0)?, record:AppliedWrite { id,timestamp:decoded(merge_fields::decode_timestamp(&r.get::<_,Vec<u8>>(1)?))?,had_read:decoded(merge_fields::decode_write_positions(&r.get::<_,Vec<u8>>(2)?))? } }))?;
        self.writes.borrow_mut().insert(id, applied);
        Ok(())
    }
    pub(crate) fn ordinal(&self, id: WriteId) -> i64 {
        self.writes.borrow()[&id].ordinal
    }
    pub(crate) fn stamp(&self, id: WriteId) -> Timestamp {
        self.writes.borrow()[&id].record.timestamp
    }
    fn retain(&self, row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<WriteId> {
        let timestamp = decoded(merge_fields::decode_timestamp(
            &row.get::<_, Vec<u8>>(offset + 1)?,
        ))?;
        let id = WriteId {
            device: timestamp.device(),
            number: counter(row.get(offset + 2)?),
        };
        let applied = StoredWrite {
            ordinal: row.get(offset)?,
            record: AppliedWrite {
                id,
                timestamp,
                had_read: decoded(merge_fields::decode_write_positions(
                    &row.get::<_, Vec<u8>>(offset + 3)?,
                ))?,
            },
        };
        self.writes.borrow_mut().insert(id, applied);
        Ok(id)
    }
}

impl WriteOracle for WriteMetadata<'_> {
    fn timestamp(&self, id: WriteId) -> Option<Timestamp> {
        self.writes.borrow().get(&id).map(|w| w.record.timestamp)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let writes = self.writes.borrow();
        let reader_metadata = writes
            .get(&reader)
            .ok_or(MergeError::MissingWrite(reader))?;
        Ok(if reader.device == earlier.device {
            earlier.number < reader.number
        } else {
            reader_metadata.record.had_read.covers(earlier)
        })
    }
}

#[derive(Clone)]
pub(crate) struct StoredRow {
    pub(crate) state: RowState<Value>,
    pub(crate) ordinal: Option<i64>,
    pub(crate) loss: Option<i64>,
    pub(crate) lost_ids: BTreeMap<LostKey, i64>,
}

pub(crate) struct MergeStore<'a> {
    database: &'a DatabaseConnection,
    values: &'a AppView<'a>,
    metadata: WriteMetadata<'a>,
    rows: RefCell<BTreeMap<RowId, StoredRow>>,
}

impl<'a> MergeStore<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection, app: &'a AppView<'a>) -> Self {
        Self {
            database,
            values: app,
            metadata: WriteMetadata::new(database),
            rows: RefCell::new(BTreeMap::new()),
        }
    }

    pub(crate) fn stamp(&self, id: WriteId) -> Timestamp {
        self.metadata.stamp(id)
    }
    pub(crate) fn write_ordinal(&self, id: WriteId) -> i64 {
        self.metadata.ordinal(id)
    }

    pub(crate) fn row(&self, id: &RowId) -> Result<StoredRow, DbError> {
        if let Some(row) = self.rows.borrow().get(id) {
            return Ok(row.clone());
        }
        #[cfg(test)]
        self.database.record_merge_load(&id.table);
        let audience = audience_text(&id.audience);
        let generations = self.database.query("SELECT r.id,r.generation,w.id,w.timestamp,w.number,w.had_read FROM coven_rows r JOIN coven_writes w ON w.id=r.write_id WHERE r.table_name=?1 AND r.key=?2 AND r.audience=?3 ORDER BY r.generation", params![id.table, id.key, audience], |r| Ok((r.get::<_,i64>(0)?, counter(r.get(1)?), self.metadata.retain(r,2)?)))?;
        let current = generations
            .last()
            .map(|(ordinal, generation, _)| (*ordinal, *generation));
        let mut cells = BTreeMap::new();
        let mut loss = None;
        if let Some((ordinal, generation)) = current.filter(|(_, g)| g % 2 == 1) {
            let retained = self.database.query("SELECT id,value FROM coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS NULL AND replacement_kind='rules' AND retired=0", params![id.table,id.key,audience,generation.to_be_bytes().as_slice()], |r| Ok((r.get::<_,i64>(0)?, decoded(merge_fields::decode_columns(&r.get::<_,Vec<u8>>(1)?))?)))?;
            assert!(
                retained.len() <= 1,
                "present row has more than one removal record: {id:?}"
            );
            let values = if let Some((ordinal, columns)) = retained.into_iter().next() {
                loss = Some(ordinal);
                columns.into_iter().map(|(n, v)| (n, v.value)).collect()
            } else {
                let app = self
                    .values
                    .row(&(id.table.clone(), id.key.clone()))?
                    .expect("present row must be in app or coven_lost");
                assert_eq!(
                    app.audience, id.audience,
                    "visible row audience differs from merge state"
                );
                app.values
            };
            let mut references = BTreeMap::<String, BTreeMap<_, _>>::new();
            for (column,key,parent) in self.database.query("SELECT c.column_name,f.identity,v.parent_table,v.parent_key,v.parent_audience,v.parent_generation FROM coven_references v JOIN coven_foreign_keys f ON f.id=v.foreign_key_id JOIN coven_columns c ON c.id=v.column_id WHERE v.row_id=?1", [ordinal], |r| Ok((r.get::<_,String>(0)?,decoded(merge_fields::decode_foreign_key(&r.get::<_,Vec<u8>>(1)?))?, coven_merge::Parent { row: RowId { table:r.get(2)?,key:r.get(3)?,audience:crate::write_encoding::audience(&r.get::<_,String>(4)?)? }, generation:counter(r.get(5)?) })))? {
                references.entry(column).or_default().insert(key,parent);
            }
            for (name, write) in self.database.query("SELECT c.column_name,w.id,w.timestamp,w.number,w.had_read FROM coven_cells v JOIN coven_columns c ON c.id=v.column_id JOIN coven_writes w ON w.id=v.write_id WHERE v.row_id=?1", [ordinal], |r| Ok((r.get::<_,String>(0)?,self.metadata.retain(r,1)?)))? {
                let parents = references.remove(&name).unwrap_or_default();
                cells.insert(name.clone(), Cell { write, value: ColumnValue { value: values[&name].clone(), parents } });
            }
            assert!(references.is_empty(), "reference names a missing cell");
        }
        let mut lost = BTreeMap::new();
        let mut lost_ids = BTreeMap::new();
        for (ordinal, generation, column, value, set_by, replaced_by) in self.database.query("SELECT l.id,l.generation,c.column_name,l.value,l.set_by,l.replaced_by FROM coven_lost l JOIN coven_columns c ON c.id=l.column_id WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3 AND l.column_id IS NOT NULL AND l.replacement_kind='write' AND l.retired=0", params![id.table,id.key,audience], |r| Ok((r.get::<_,i64>(0)?, counter(r.get(1)?),r.get::<_,String>(2)?,decoded(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(3)?))?,decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(4)?))?,decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(5)?))?)))? {
            self.metadata.load(set_by)?;
            self.metadata.load(replaced_by)?;
            let key = LostKey { column, write:set_by };
            lost_ids.insert(key.clone(), ordinal);
            lost.insert(key, LostValue { incarnation:generation, value, replaced_by });
        }
        let state = coven_merge::RowState::from_parts(
            id.clone(),
            generations.into_iter().map(|(_, g, w)| (g, w)).collect(),
            cells,
            lost,
            &self.metadata,
        )
        .expect("stored merge state satisfies its invariants");
        let row = StoredRow {
            state,
            ordinal: current.map(|(id, _)| id),
            loss,
            lost_ids,
        };
        self.rows.borrow_mut().insert(id.clone(), row.clone());
        Ok(row)
    }

    pub(crate) fn apply(
        &self,
        record: &WriteRecord,
    ) -> Result<BTreeMap<RowId, RowUpdate<Value>>, DbError> {
        let mut past = record.header.had_read.clone();
        if record.header.position.number > 1 {
            past.0.push(WriteId {
                device: record.header.position.device,
                number: record.header.position.number - 1,
            });
            past.0.sort_by_key(|w| w.device);
        }
        for id in &past.0 {
            self.metadata.load(*id)?;
        }
        let write = coven_merge::Write {
            id: record.header.position,
            timestamp: record.header.timestamp,
            had_read: past,
            changes: record
                .parts
                .iter()
                .flat_map(|p| &p.rows)
                .map(|c| (c.row.clone(), c.change.clone()))
                .collect(),
        };
        let invalid = |error| DbError::InvalidWrite {
            write: write.id,
            error,
        };
        // Skipped and excluded parts can leave no rows; their applied metadata
        // must obey the same causal rules as writes with visible changes.
        write.validate_metadata(&self.metadata).map_err(invalid)?;
        let mut updates = BTreeMap::new();
        for row in write.changes.keys() {
            let old = self.row(row)?;
            let update = coven_merge::apply(&old.state, &write, &self.metadata).map_err(invalid)?;
            updates.insert(row.clone(), update);
        }
        Ok(updates)
    }
}

#[cfg(test)]
#[path = "merge_store_tests.rs"]
mod tests;
