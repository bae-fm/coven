//! Indexed loading of the merge's own row state and causal metadata.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::row_cache::{value_weight, RowCache, Weight};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, decoded, slot_counter};
use crate::write_metadata::StoredWrite;
use crate::write_rows::AppView;
use crate::DbError;
use coven_format::{merge_fields, value::Value, write::WriteRecord};
use coven_merge::{
    Audience, Cell, ColumnValue, LostKey, LostValue, MergeError, RowId, RowState, RowUpdate,
    Timestamp, WriteId, WriteOracle,
};
use rusqlite::params;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

/// The newest write by timestamp that a write had read: one in a slot it had
/// read through, or one of its author's own earlier writes. It walks the
/// writes from the newest timestamp down and stops at the first it had read,
/// so it reads only the newer writes it hadn't. SQLite would otherwise read
/// every write in the slots read through by their slot index and sort them,
/// so applying the 5,000th slot of a restore read 5,000 writes (§21.2).
pub(crate) fn newest_read() -> String {
    format!(
        "SELECT timestamp,number FROM _coven_writes INDEXED BY sqlite_autoindex__coven_writes_1 \
         WHERE (slot<=?1 OR ({WRITE_DEVICE_SQL}=?2 AND number<?3)) ORDER BY timestamp DESC LIMIT 1"
    )
}

struct WriteMetadata<'a> {
    database: &'a DatabaseConnection,
    writes: RefCell<BTreeMap<WriteId, StoredWrite>>,
}

impl<'a> WriteMetadata<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection) -> Self {
        Self { database, writes: RefCell::new(BTreeMap::new()) }
    }
    pub(crate) fn load(&self, id: WriteId) -> Result<(), DbError> {
        if self.writes.borrow().contains_key(&id) {
            return Ok(());
        }
        let applied = crate::write_metadata::stored_write(self.database, id)?
            .ok_or(DbError::from(rusqlite::Error::QueryReturnedNoRows))?;
        self.writes.borrow_mut().insert(id, applied);
        Ok(())
    }
    pub(crate) fn ordinal(&self, id: WriteId) -> i64 {
        self.writes.borrow()[&id].ordinal
    }
    pub(crate) fn stamp(&self, id: WriteId) -> Timestamp {
        self.writes.borrow()[&id].timestamp
    }
    fn retain(&self, row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<WriteId> {
        let timestamp =
            decoded(merge_fields::decode_timestamp(&row.get::<_, Vec<u8>>(offset + 1)?))?;
        let id = WriteId { device: timestamp.device(), number: counter(row.get(offset + 2)?)? };
        let applied = StoredWrite {
            ordinal: row.get(offset)?,
            timestamp,
            read_through: counter(row.get(offset + 3)?)?,
            slot: row.get::<_, Option<Vec<u8>>>(offset + 4)?.map(slot_counter).transpose()?,
        };
        self.writes.borrow_mut().insert(id, applied);
        Ok(id)
    }
}

impl WriteOracle for WriteMetadata<'_> {
    type Error = MergeError;
    fn timestamp(&self, id: WriteId) -> Result<Option<Timestamp>, MergeError> {
        Ok(self.writes.borrow().get(&id).map(|w| w.timestamp))
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let writes = self.writes.borrow();
        let reader_metadata = writes.get(&reader).ok_or(MergeError::MissingWrite(reader))?;
        Ok(coven_merge::had_read(
            reader,
            reader_metadata.read_through,
            earlier,
            writes.get(&earlier).and_then(|w| w.slot),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct StoredRow {
    pub(crate) state: RowState<Value>,
    pub(crate) ordinal: Option<i64>,
    pub(crate) loss: Option<i64>,
    pub(crate) lost_ids: BTreeMap<LostKey, i64>,
}

impl Weight for StoredRow {
    fn weight(&self) -> usize {
        let cells: usize = self
            .state
            .cells()
            .iter()
            .map(|(name, cell)| name.len() + value_weight(&cell.value.value))
            .sum();
        let lost: usize = self
            .state
            .lost()
            .iter()
            .map(|(key, lost)| key.column.len() + value_weight(&lost.value.value))
            .sum();
        cells + lost
    }
}

pub(crate) struct MergeStore<'a> {
    database: &'a DatabaseConnection,
    values: RowValues<'a>,
    metadata: WriteMetadata<'a>,
    rows: RowCache<RowId, StoredRow>,
    /// The generations `generation` read, which a later read of the same
    /// row's state starts from.
    generations: RowCache<RowId, Generations>,
    /// The merge rows each app key has, by table and key.
    keys: RowCache<(String, Vec<u8>), KeyRows>,
}

/// A row's stored generations, oldest first: each one's `_coven_rows` id,
/// its number, and the write that started it.
#[derive(Clone)]
struct Generations(Vec<(i64, u64, WriteId)>);

impl Weight for Generations {
    fn weight(&self) -> usize {
        std::mem::size_of_val(self.0.as_slice())
    }
}

/// The merge rows one app key has stored, one for each audience.
#[derive(Clone)]
pub(crate) struct KeyRows(pub(crate) BTreeSet<RowId>);

impl Weight for KeyRows {
    fn weight(&self) -> usize {
        self.0.iter().map(|row| row.table.len() + row.key.len()).sum()
    }
}

enum RowValues<'a> {
    App(&'a AppView<'a>),
    Schema(&'a crate::schema::Schema),
    Snapshot(&'a crate::schema::Schema, &'a BTreeSet<Audience>),
    Staged(&'a crate::schema::Schema),
}

impl<'a> MergeStore<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection, app: &'a AppView<'a>) -> Self {
        Self {
            database,
            values: RowValues::App(app),
            metadata: WriteMetadata::new(database),
            rows: RowCache::all(),
            generations: RowCache::all(),
            keys: RowCache::all(),
        }
    }

    /// Read values and stored references without resolving app references.
    pub(crate) fn from_schema(
        database: &'a DatabaseConnection,
        schema: &'a crate::schema::Schema,
    ) -> Self {
        Self {
            database,
            values: RowValues::Schema(schema),
            metadata: WriteMetadata::new(database),
            rows: RowCache::all(),
            generations: RowCache::all(),
            keys: RowCache::all(),
        }
    }

    pub(crate) fn stamp(&self, id: WriteId) -> Timestamp {
        self.metadata.stamp(id)
    }

    /// Load the writes that started `state`'s generations, read from a
    /// record outside this store, so their stamps can be read.
    pub(crate) fn know(&self, state: &RowState<Value>) -> Result<(), DbError> {
        for write in state.generations().values() {
            self.metadata.load(*write)?;
        }
        Ok(())
    }
    pub(crate) fn from_snapshot(
        database: &'a DatabaseConnection,
        schema: &'a crate::schema::Schema,
        audiences: &'a BTreeSet<Audience>,
    ) -> Self {
        Self {
            database,
            values: RowValues::Snapshot(schema, audiences),
            metadata: WriteMetadata::new(database),
            rows: RowCache::all(),
            generations: RowCache::all(),
            keys: RowCache::all(),
        }
    }

    pub(crate) fn from_staged(
        database: &'a DatabaseConnection,
        schema: &'a crate::schema::Schema,
    ) -> Self {
        Self {
            database,
            values: RowValues::Staged(schema),
            metadata: WriteMetadata::new(database),
            rows: RowCache::all(),
            generations: RowCache::all(),
            keys: RowCache::all(),
        }
    }

    /// Immutable reader snapshots and staged rows can be reread without keeping
    /// every row value in memory while a connected region is materialized.
    pub(crate) fn without_row_cache(mut self) -> Self {
        self.rows = RowCache::Off;
        self.generations = RowCache::Off;
        self.keys = RowCache::Off;
        self
    }

    /// Keep the rows most recently read, up to `row_cache::RECENT` bytes of
    /// their values, so a region of a streamed write is read once for each
    /// run of uses without being held whole (§21.2).
    pub(crate) fn with_recent_rows(mut self) -> Self {
        self.rows = RowCache::recent(crate::row_cache::RECENT);
        self.generations = RowCache::recent(crate::row_cache::RECENT);
        self.keys = RowCache::recent(crate::row_cache::RECENT);
        self
    }

    /// The merge rows stored for an app key, one for each audience it has
    /// been in.
    pub(crate) fn key_rows(&self, table: &str, key: &[u8]) -> Result<BTreeSet<RowId>, DbError> {
        let id = (table.to_owned(), key.to_vec());
        if let Some(rows) = self.keys.get(&id) {
            return Ok(rows.0);
        }
        let rows: BTreeSet<RowId> = self
            .database
            .query(
                "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",
                params![table, key],
                crate::row_queries::read_identity,
            )?
            .into_iter()
            .collect();
        if self.keys.keeps() {
            self.keys.insert(id, KeyRows(rows.clone()));
        }
        Ok(rows)
    }

    /// An empty cache of other rows derived from this store's, keeping what
    /// this store keeps.
    pub(crate) fn row_cache_like<V: Clone + Weight>(&self) -> RowCache<RowId, V> {
        self.rows.like()
    }

    /// Whether values come from the transaction's download stage.
    pub(crate) fn staged(&self) -> bool {
        matches!(self.values, RowValues::Staged(_))
    }

    pub(crate) fn retain_materialization_values(
        &self,
        region: &BTreeSet<RowId>,
    ) -> Result<(), DbError> {
        if matches!(self.values, RowValues::Staged(_)) {
            for row in region {
                let state = self.row(row)?.state;
                crate::download_stage::retain_values(self.database, row, &state)?;
            }
        }
        Ok(())
    }

    pub(crate) fn write_ordinal(&self, id: WriteId) -> i64 {
        self.metadata.ordinal(id)
    }

    pub(crate) fn row(&self, id: &RowId) -> Result<StoredRow, DbError> {
        if let Some(row) = self.rows.get(id) {
            return Ok(row);
        }
        let row = self.read_row(id, true)?;
        if self.rows.keeps() {
            self.rows.insert(id.clone(), row.clone());
        }
        Ok(row)
    }

    /// A row's stored generation, zero for a row never held. Nothing changes
    /// it before the store's write commits, so a local write's capture, its
    /// record and its merge share one read of each row's.
    pub(crate) fn generation(&self, id: &RowId) -> Result<u64, DbError> {
        if let Some(generation) = self.rows.peek(id, |row| row.state.generation()) {
            return Ok(generation);
        }
        let generations = self.generations(id, true)?;
        Ok(generations.0.last().map_or(0, |(_, generation, _)| *generation))
    }

    /// A row's generations: those a generation read kept, or read now, and
    /// kept when `keep`. A read of the row's whole state keeps that instead.
    fn generations(&self, id: &RowId, keep: bool) -> Result<Generations, DbError> {
        if let Some(generations) = self.generations.get(id) {
            return Ok(generations);
        }
        let generations = Generations(self.database.query("SELECT r.id,r.generation,w.id,w.timestamp,w.number,w.read_through,w.slot FROM _coven_rows r JOIN _coven_writes w ON w.id=r.write_id WHERE r.table_name=?1 AND r.key=?2 AND r.audience=?3 ORDER BY r.generation", params![id.table, id.key, &id.audience], |r| Ok((r.get::<_,i64>(0)?, counter(r.get(1)?)?, self.metadata.retain(r,2)?)))?);
        if keep && self.generations.keeps() {
            self.generations.insert(id.clone(), generations.clone());
        }
        Ok(generations)
    }

    /// Snapshot merge records and loss validation read generations and winning
    /// cells independently of the loss section, without loading prior losses.
    pub(crate) fn row_without_losses(&self, id: &RowId) -> Result<StoredRow, DbError> {
        self.read_row(id, false)
    }

    fn read_row(&self, id: &RowId, with_losses: bool) -> Result<StoredRow, DbError> {
        #[cfg(test)]
        self.database.record_merge_load(&id.table);
        let audience = &id.audience;
        let generations = self.generations(id, false)?.0;
        let current = generations.last().map(|(ordinal, generation, _)| (*ordinal, *generation));
        let mut cells = BTreeMap::new();
        let mut loss = None;
        if let Some((ordinal, generation)) =
            current.filter(|(_, g)| coven_merge::generation_present(*g))
        {
            let retained = crate::loss_record::removed(self.database, id, generation)?;
            let staged = match self.values {
                RowValues::Staged(schema) => {
                    crate::download_stage::values(self.database, schema, id)?
                }
                _ => None,
            };
            let raw_staged = staged.is_some();
            let mut values = if let Some(values) = staged {
                loss = retained.as_ref().map(|removed| removed.loss);
                values
            } else if let Some(removed) = retained {
                loss = Some(removed.loss);
                removed.columns.into_iter().map(|(n, v)| (n, v.value)).collect()
            } else {
                match self.values {
                    RowValues::App(app) => {
                        let app = app
                            .row(&(id.table.clone(), id.key.clone()))?
                            .ok_or(DbError::DamagedDatabase)?;
                        if app.audience != id.audience {
                            return Err(DbError::DamagedDatabase);
                        }
                        app.values
                    }
                    RowValues::Snapshot(schema, audiences) if audiences.contains(&id.audience) => {
                        crate::snapshot_install::values(self.database, schema, id)?
                    }
                    RowValues::Schema(schema)
                    | RowValues::Snapshot(schema, _)
                    | RowValues::Staged(schema) => crate::write_rows::read_values(
                        self.database,
                        &schema.tables[&id.table.to_ascii_lowercase()],
                        &id.key,
                    )?
                    .ok_or(DbError::DamagedDatabase)?,
                }
            };
            // Staged cells already contain written values. A correction for
            // an earlier displayed NULL must not replace a new target.
            if !raw_staged {
                values.extend(crate::written_values::load(self.database, ordinal)?);
            }
            let mut references = BTreeMap::<String, BTreeMap<_, _>>::new();
            for (column,key,parent) in self.database.query("SELECT c.column_name,f.identity,v.parent_table,v.parent_key,v.parent_audience,v.parent_generation FROM _coven_references v JOIN _coven_foreign_keys f ON f.id=v.foreign_key_id JOIN _coven_columns c ON c.id=v.column_id WHERE v.row_id=?1", [ordinal], |r| Ok((r.get::<_,String>(0)?,decoded(merge_fields::decode_foreign_key(&r.get::<_,Vec<u8>>(1)?))?, coven_merge::Parent { row: RowId { table:r.get(2)?,key:r.get(3)?,audience:crate::write_encoding::audience(r, 4)? }, generation:counter(r.get(5)?)? })))? {
                references.entry(column).or_default().insert(key,parent);
            }
            for (name, write) in self.database.query("SELECT c.column_name,w.id,w.timestamp,w.number,w.read_through,w.slot FROM _coven_cells v JOIN _coven_columns c ON c.id=v.column_id JOIN _coven_writes w ON w.id=v.write_id WHERE v.row_id=?1", [ordinal], |r| Ok((r.get::<_,String>(0)?,self.metadata.retain(r,1)?)))? {
                let parents = references.remove(&name).unwrap_or_default();
                cells.insert(name.clone(), Cell { write, value: ColumnValue { value: values[&name].clone(), parents } });
            }
            assert!(references.is_empty(), "reference names a missing cell");
        }
        let mut lost = BTreeMap::new();
        let mut lost_ids = BTreeMap::new();
        if with_losses {
            let records = self.database.query("SELECT l.id,l.generation,c.column_name,l.value,l.set_by,l.replaced_by FROM _coven_lost l JOIN _coven_columns c ON c.id=l.column_id WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3 AND l.column_id IS NOT NULL AND l.replacement_kind='write' AND l.frozen=0", params![id.table,id.key,audience], |r| Ok((r.get::<_,i64>(0)?, counter(r.get(1)?)?,r.get::<_,String>(2)?,decoded(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(3)?))?,decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(4)?))?,decoded(merge_fields::decode_write_id(&r.get::<_,Vec<u8>>(5)?))?)))?;
            for (ordinal, generation, column, value, set_by, replaced_by) in records {
                self.metadata.load(set_by)?;
                self.metadata.load(replaced_by)?;
                let key = LostKey { column, write: set_by };
                lost_ids.insert(key.clone(), ordinal);
                lost.insert(key, LostValue { incarnation: generation, value, replaced_by });
            }
        }
        let state = coven_merge::RowState::from_parts(
            id.clone(),
            generations.into_iter().map(|(_, g, w)| (g, w)).collect(),
            cells,
            lost,
            &self.metadata,
        )
        .expect("stored merge state satisfies its invariants");
        Ok(StoredRow { state, ordinal: current.map(|(id, _)| id), loss, lost_ids })
    }

    pub(crate) fn apply(
        &self,
        record: &WriteRecord,
    ) -> Result<BTreeMap<RowId, RowUpdate<Value>>, DbError> {
        let latest = self
            .database
            .query(
                &newest_read(),
                params![
                    record.header.read_through.to_be_bytes().as_slice(),
                    record.header.position.device,
                    record.header.position.number.to_be_bytes().as_slice()
                ],
                |r| {
                    let stamp = decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(0)?))?;
                    Ok(WriteId { device: stamp.device(), number: counter(r.get(1)?)? })
                },
            )?
            .pop();
        if let Some(latest) = latest {
            self.metadata.load(latest)?;
        }
        let past = coven_merge::SlotPast::new(
            record.header.position,
            record.header.read_through,
            |id: &WriteId| self.metadata.writes.borrow().get(id).and_then(|w| w.slot),
            latest,
        );
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
        let invalid = |error| DbError::InvalidWrite { write: write.id, error };
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
