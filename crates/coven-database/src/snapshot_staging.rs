//! A snapshot's checked records, staged on the input connection while its
//! body streams, until one load or file check reads them (§16.2, §21.2).
//! Rows, cells, references, source records and creations are staged as
//! columns of tables of their own, so installing reads fields rather than
//! decoding records again; the few other records keep their frames.

use super::InputDatabase;
use crate::database::{start_call, Database, DatabaseInner};
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_layout::{SnapshotLayout, Staged, TableLayout};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded, sql_value};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    admin::SnapshotId,
    merge_fields,
    sealed_snapshot::SnapshotObjectPrefix,
    snapshot::{SnapshotHeader, SnapshotRecord},
    snapshot_rows::{AuthoredChange, MergeRow, SyncedRow},
    snapshot_stream::SnapshotChunkDecoder,
    value::Value,
    write::WriteHeader,
};
use rusqlite::{params, params_from_iter};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Created with the input connection, in its temporary storage, so staged
/// records go with the connection: no restart finds them. That storage spills
/// to files once the connection's cache fills; opening refuses a SQLite built
/// to keep it in memory. Only the snapshot loader names these tables.
pub(super) fn create_tables(db: &DatabaseConnection) -> Result<(), DbError> {
    db.batch("PRAGMA temp_store=FILE;")?;
    SnapshotMetadata::create_table(db)
}

/// One staging's number, whether its records are still on the input
/// connection, and the tables that hold them. It changes only while the
/// input writer is held.
pub(crate) struct Staging {
    pub(crate) id: i64,
    held: AtomicBool,
    tables: Vec<String>,
}

/// A staging's tables of its own, beside its rows' tables.
pub(crate) fn table(staging: i64, name: &str) -> String {
    format!("temp._coven_staged_{staging}_{name}")
}

fn create_staged(
    db: &DatabaseConnection,
    layout: &SnapshotLayout,
    staging: i64,
) -> Result<Vec<String>, DbError> {
    let mut tables = Vec::new();
    for table in &layout.tables {
        for rows in [Staged::Synced, Staged::Merged] {
            db.batch(&table.create_staged(staging, rows))?;
            tables.push(table.staged(staging, rows));
        }
    }
    let name = |name: &str| table(staging, name).trim_start_matches("temp.").to_owned();
    db.batch(&format!(
        "CREATE TEMP TABLE {}(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,generation BLOB NOT NULL,device BLOB NOT NULL,number BLOB NOT NULL);
         CREATE TEMP TABLE {}(table_name TEXT NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(table_name,column_name)) WITHOUT ROWID;
         CREATE TEMP TABLE {}(write_id BLOB NOT NULL,audience TEXT NOT NULL,header BLOB NOT NULL,complete INTEGER NOT NULL,PRIMARY KEY(write_id,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE {}(write_id BLOB NOT NULL,audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,record BLOB NOT NULL,admission BLOB);
         CREATE TEMP TABLE {}(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,write_id BLOB NOT NULL,creator BLOB NOT NULL);
         CREATE TEMP TABLE {}(section INTEGER NOT NULL,evidence INTEGER NOT NULL CHECK(evidence IN (0,1)),record BLOB NOT NULL);",
        name("generations"),
        name("columns"),
        name("parts"),
        name("authored"),
        name("creations"),
        name("records"),
    ))?;
    for name in [
        "generations",
        "columns",
        "parts",
        "authored",
        "creations",
        "records",
    ] {
        tables.push(table(staging, name));
    }
    Ok(tables)
}

impl Staging {
    pub(crate) fn held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }
    /// A load or file check read these records and removed them.
    pub(crate) fn taken(&self) {
        self.held.store(false, Ordering::SeqCst);
    }
}

/// Drop a staging's tables whole.
pub(crate) fn remove(db: &DatabaseConnection, staging: &Staging) -> Result<(), DbError> {
    for table in &staging.tables {
        db.batch(&format!("DROP TABLE {table}"))?;
    }
    SnapshotMetadata::remove(db, staging.id)
}

/// The statements staging one snapshot runs for its records, written once
/// for its tables.
struct Inserts {
    layout: SnapshotLayout,
    /// Each synced table's synced and merge row inserts, in the layout's
    /// order.
    rows: Vec<[String; 2]>,
    generations: String,
    columns: String,
    parts: String,
    complete: String,
    authored: String,
    creations: String,
    records: String,
}

impl Inserts {
    fn new(layout: SnapshotLayout, staging: i64) -> Self {
        let rows = layout
            .tables
            .iter()
            .map(|table| {
                [Staged::Synced, Staged::Merged].map(|rows| {
                    format!(
                        "INSERT INTO {}({}) VALUES({})",
                        table.staged(staging, rows),
                        table.names().join(","),
                        vec!["?"; table.width()].join(",")
                    )
                })
            })
            .collect();
        let insert = |name: &str, columns: &str| {
            let count = columns.split(',').count();
            format!(
                "INSERT INTO {}({columns}) VALUES({})",
                table(staging, name),
                (1..=count)
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        Self {
            layout,
            rows,
            generations: insert(
                "generations",
                "table_name,key,audience,generation,device,number",
            ),
            columns: insert("columns", "table_name,column_name"),
            parts: format!(
                "INSERT INTO {}(write_id,audience,header,complete) VALUES(?1,?2,?3,0)",
                table(staging, "parts")
            ),
            complete: format!(
                "UPDATE {} SET complete=1 WHERE write_id=?1 AND audience=?2",
                table(staging, "parts")
            ),
            authored: insert(
                "authored",
                "write_id,audience,table_name,key,record,admission",
            ),
            creations: insert("creations", "table_name,key,audience,write_id,creator"),
            records: insert("records", "section,evidence,record"),
        }
    }
}

impl InputDatabase {
    fn begin_staging(&self, schema: &WriteSchema) -> Result<(Arc<Staging>, Inserts), DbError> {
        let id = self.stagings.fetch_add(1, Ordering::SeqCst);
        let layout = SnapshotLayout::new(schema);
        let tables = self.write(|db| create_staged(db, &layout, id))?;
        Ok((
            Arc::new(Staging {
                id,
                held: AtomicBool::new(true),
                tables,
            }),
            Inserts::new(layout, id),
        ))
    }

    /// Remove a staging's records, if a load or file check hasn't taken them.
    fn remove_staging(&self, staging: &Staging) -> Result<(), DbError> {
        super::with_locked(&self.writer, |writer| {
            if !staging.held() {
                return Ok(());
            }
            writer.transaction(|db| remove(db, staging))?;
            staging.taken();
            Ok(())
        })
    }
}

/// The records of one staged snapshot. The last owner to drop removes them
/// unless a load or file check took them, or `discard` already did.
struct StagedRows {
    database: Database,
    staging: Arc<Staging>,
    /// The first record that failed the loader's checks as it staged, and
    /// its section: the load or file check that reads the records refuses
    /// the body with it where it reaches that section.
    refusal: Mutex<Option<(u8, DbError)>>,
}

impl Drop for StagedRows {
    fn drop(&mut self) {
        if !self.staging.held() {
            return;
        }
        // Only an abandoned staging gets here: a cancelled or panicking
        // caller. Its connection's temporary storage goes when the store
        // closes, which waits for this removal.
        let staging = self.staging.clone();
        let remove = move |inner: &DatabaseInner| {
            if let Err(error) = inner.call(|inner| inner.inputs.remove_staging(&staging)) {
                tracing::error!(%error, "removing an abandoned snapshot staging failed");
            }
            Ok::<_, DbError>(())
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            drop(start_call(self.database.inner.clone(), remove));
        } else if let Some(inner) = self
            .database
            .inner
            .read()
            .expect("database lock poisoned")
            .as_ref()
        {
            let _removed = remove(inner);
        }
    }
}

/// A snapshot being staged: sync hands it each batch of opened plaintext
/// as the body streams. Each batch is decoded, checked and committed in one
/// input transaction, outside the application writer (§21.2).
pub struct SnapshotStaging {
    rows: Arc<StagedRows>,
    expected: SnapshotId,
    /// Absent after a failed batch: a staging that failed is discarded.
    state: Option<StagingState>,
    /// The batches staged so far, and the time staging them took.
    batches: usize,
    time: std::time::Duration,
}

/// What staging keeps between batches beside its tables: the decoder, the
/// statements that stage records, the records staged so far, the columns the
/// snapshot names, each source part's header, and the first record the
/// loader's checks refuse.
struct StagingState {
    decoder: SnapshotChunkDecoder,
    inserts: Inserts,
    records: i64,
    columns: BTreeSet<(String, String)>,
    parts: BTreeMap<(Vec<u8>, String), WriteHeader>,
    refusal: Option<(u8, DbError)>,
}

/// A staged snapshot whose stream checked out to its end marker: its header,
/// and its records on the input connection.
pub struct StagedSnapshot {
    rows: Arc<StagedRows>,
    header: SnapshotHeader,
}

/// What a load or file check reads a staged snapshot from. It keeps the
/// records until then; reading them removes them.
#[derive(Clone)]
pub struct StagedRecords {
    rows: Arc<StagedRows>,
    header: SnapshotHeader,
}

impl StagedRecords {
    /// The staging, when `database` staged it: records on another store's
    /// input connection are not there to read.
    pub(crate) fn staging(&self, database: &Database) -> Result<&Arc<Staging>, DbError> {
        if !Arc::ptr_eq(&self.rows.database.inner, &database.inner) {
            return Err(DbError::Invariant {
                what: "a snapshot is loaded by the store that staged it",
            });
        }
        Ok(&self.rows.staging)
    }
    pub(crate) fn id(&self) -> i64 {
        self.rows.staging.id
    }
    pub(crate) fn header(&self) -> &SnapshotHeader {
        &self.header
    }
    /// The first record staging found failing the loader's checks, and its
    /// section.
    pub(crate) fn refusal(&self) -> Option<(u8, DbError)> {
        self.rows
            .refusal
            .lock()
            .expect("staging refusal lock poisoned")
            .take()
    }
}

impl Database {
    /// Begin staging the snapshot `id`, whose signed prefix is `prefix`
    /// (§16.2). Nothing outside the loader reads what it stages.
    pub async fn stage_snapshot(
        &self,
        id: SnapshotId,
        prefix: SnapshotObjectPrefix,
    ) -> Result<SnapshotStaging, DbError> {
        let (staging, inserts) = self
            .call(|inner| inner.inputs.begin_staging(&inner.access.write_schema))
            .await?;
        Ok(SnapshotStaging {
            rows: Arc::new(StagedRows {
                database: self.clone(),
                staging,
                refusal: Mutex::new(None),
            }),
            expected: id,
            state: Some(StagingState {
                decoder: SnapshotChunkDecoder::new(prefix),
                inserts,
                records: 0,
                columns: BTreeSet::new(),
                parts: BTreeMap::new(),
                refusal: None,
            }),
            batches: 0,
            time: std::time::Duration::ZERO,
        })
    }

    /// Where the input connection keeps temporary tables (`PRAGMA temp_store`):
    /// 1 is in files.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn input_temp_store(&self) -> Result<u8, DbError> {
        self.call(|inner| {
            super::with_locked(&inner.inputs.writer, |writer| {
                writer.query_row("PRAGMA temp_store", [], |row| row.get(0))
            })
        })
        .await
    }

    /// The records staged on the input connection now, for every snapshot:
    /// the rows of every staging's tables, and their applied writes.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn staged_snapshot_records(&self) -> Result<u64, DbError> {
        self.call(|inner| {
            super::with_locked(&inner.inputs.writer, |writer| {
                let tables = writer.query(
                    "SELECT name FROM temp.sqlite_schema WHERE type='table' AND name GLOB '_coven_staged_*'",
                    [],
                    |row| row.get::<_, String>(0),
                )?;
                let mut count: i64 = writer.query_row(
                    "SELECT count(*) FROM temp._coven_snapshot_writes",
                    [],
                    |row| row.get(0),
                )?;
                for table in tables {
                    count += writer.query_row(
                        &format!("SELECT count(*) FROM temp.{}", crate::sql::identifier(&table)),
                        [],
                        |row| row.get::<_, i64>(0),
                    )?;
                }
                Ok::<_, DbError>(count)
            })
        })
        .await
        .map(|count| count as u64)
    }
}

impl SnapshotStaging {
    /// Decode one batch of opened plaintext and commit its records together.
    /// After a failure, discard this staging.
    pub async fn stage(&mut self, plaintext: Vec<u8>) -> Result<(), DbError> {
        let mut state = self.state.take().ok_or(DbError::Invariant {
            what: "a snapshot staging that failed is discarded",
        })?;
        let staging = self.rows.staging.clone();
        let expected = self.expected.clone();
        let (state, time) = self
            .rows
            .database
            .call(move |inner| {
                let started = inner.clock.elapsed();
                inner.inputs.write(|db| {
                    stage(
                        db,
                        &inner.access.write_schema,
                        &expected,
                        &staging,
                        &mut state,
                        &plaintext,
                    )
                })?;
                Ok::<_, DbError>((state, inner.clock.elapsed() - started))
            })
            .await?;
        self.state = Some(state);
        self.batches += 1;
        self.time += time;
        Ok(())
    }

    /// Refuse a stream without its header, declared records and end marker,
    /// then check its applied writes. Nothing more stages after this; after a
    /// failure, discard this staging.
    pub async fn finish(&mut self) -> Result<StagedSnapshot, DbError> {
        let state = self.state.take().ok_or(DbError::Invariant {
            what: "a snapshot staging that failed is discarded",
        })?;
        let staging = self.rows.staging.clone();
        let expected = self.expected.clone();
        let records = state.records;
        let header = self
            .rows
            .database
            .call(move |inner| {
                inner.inputs.write(|db| {
                    state.decoder.finish().map_err(SnapshotError::Format)?;
                    let header = state
                        .decoder
                        .header()
                        .ok_or_else(|| invalid("snapshot has no header"))?
                        .clone();
                    check_header(&inner.access.write_schema, &expected, &header)?;
                    SnapshotMetadata::staged(db, staging.id).validate(header.covers)?;
                    Ok((header, state.refusal))
                })
            })
            .await?;
        let (header, refusal) = header;
        *self
            .rows
            .refusal
            .lock()
            .expect("staging refusal lock poisoned") = refusal;
        tracing::info!(
            audience = ?header.id.audience,
            records,
            batches = self.batches,
            time = ?self.time,
            "staged snapshot"
        );
        Ok(StagedSnapshot {
            rows: self.rows.clone(),
            header,
        })
    }

    /// Remove everything staged so far (§16.2).
    pub async fn discard(self) -> Result<(), DbError> {
        discard(&self.rows).await
    }
}

impl StagedSnapshot {
    /// The snapshot's identity, schema version, coverage and counts.
    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }
    /// What a load or file check reads; reading removes the records.
    pub fn records(&self) -> StagedRecords {
        StagedRecords {
            rows: self.rows.clone(),
            header: self.header.clone(),
        }
    }
    /// Remove the records unless a load or file check already took them.
    pub async fn discard(self) -> Result<(), DbError> {
        discard(&self.rows).await
    }
}

async fn discard(rows: &StagedRows) -> Result<(), DbError> {
    // Only the input writer changes this, and nothing holding this staging
    // is reading it now.
    if !rows.staging.held() {
        return Ok(());
    }
    let staging = rows.staging.clone();
    rows.database
        .call(move |inner| inner.inputs.remove_staging(&staging))
        .await
}

fn stage(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    expected: &SnapshotId,
    staging: &Staging,
    state: &mut StagingState,
    mut bytes: &[u8],
) -> Result<(), DbError> {
    if !staging.held() {
        return Err(DbError::Invariant {
            what: "a snapshot is staged only until it is read or discarded",
        });
    }
    #[cfg(any(test, feature = "test-utils"))]
    if state.records == 0 {
        db.record_work(|work| work.snapshot_reads += 1);
    }
    let metadata = SnapshotMetadata::staged(db, staging.id);
    let StagingState {
        decoder,
        inserts,
        records,
        columns,
        parts,
        refusal,
    } = state;
    loop {
        let headed = decoder.header().is_some();
        let record = decoder.next_record(&mut bytes, &metadata);
        metadata.check()?;
        let record = record.map_err(SnapshotError::Format)?;
        if !headed {
            if let Some(header) = decoder.header() {
                check_header(schema, expected, header)?;
            }
        }
        let Some(record) = record else {
            return Ok(());
        };
        if let SnapshotRecord::Write(write) = &record {
            metadata.stage(write)?;
        }
        *records += 1;
        // Once a record is refused the body is, but it is still decoded to
        // its end, so a damaged body is refused for that.
        if refusal.is_some() {
            continue;
        }
        let header = decoder.header().expect("records follow the header");
        let additions = header.schema_version < *schema.versions.end();
        let staged = RecordStaging {
            db,
            schema,
            inserts,
        };
        let section = section(&record);
        if let Err(refused) = staged.record(record, decoder.frame(), additions, columns, parts)? {
            *refusal = Some((section, refused));
        }
    }
}

/// A record's section of the snapshot (D7).
fn section(record: &SnapshotRecord) -> u8 {
    match record {
        SnapshotRecord::Synced(_) => 0,
        SnapshotRecord::Write(_) => 1,
        SnapshotRecord::Column(_) => 2,
        SnapshotRecord::Merge(_) => 3,
        SnapshotRecord::Loss(_) => 4,
        SnapshotRecord::Authored(_) => 5,
        SnapshotRecord::Creation(_) => 6,
        SnapshotRecord::Evidence(_) => 7,
        SnapshotRecord::Admin(_) => 8,
        SnapshotRecord::KeyCopy(_) => 9,
        SnapshotRecord::Announcement(_) => 10,
        SnapshotRecord::Claim(_) => 11,
    }
}

/// Where one staging's records go.
struct RecordStaging<'a> {
    db: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    inserts: &'a Inserts,
}

/// A record the loader's checks accept, or the error refusing it.
type Checked = Result<(), DbError>;

impl RecordStaging<'_> {
    /// Stage one record. Failing to write it fails the batch; a record the
    /// loader refuses is the inner error, which refuses the whole body.
    fn record(
        &self,
        record: SnapshotRecord,
        frame: &[u8],
        additions: bool,
        columns: &mut BTreeSet<(String, String)>,
        parts: &mut BTreeMap<(Vec<u8>, String), WriteHeader>,
    ) -> Result<Checked, DbError> {
        match &record {
            SnapshotRecord::Synced(row) => return self.synced(row, additions),
            SnapshotRecord::Write(_) => return Ok(Ok(())),
            SnapshotRecord::Column(column) => {
                self.db.internal_execute(
                    &self.inserts.columns,
                    params![column.table, column.column],
                )?;
                columns.insert((column.table.clone(), column.column.clone()));
                return Ok(Ok(()));
            }
            SnapshotRecord::Merge(row) => return self.merged(row, columns),
            SnapshotRecord::Authored(authored) => return self.authored(authored, parts),
            SnapshotRecord::Creation(creation) => {
                if let Err(refused) = self.schema.row_table(&creation.row) {
                    return Ok(Err(refused));
                }
                self.db.internal_execute(
                    &self.inserts.creations,
                    params![
                        creation.row.table,
                        creation.row.key,
                        audience_text(&creation.row.audience),
                        encoded(merge_fields::encode_write_id(&creation.write))?,
                        encoded(merge_fields::encode_change_id(&creation.creator))?
                    ],
                )?;
                return Ok(Ok(()));
            }
            SnapshotRecord::Loss(_)
            | SnapshotRecord::Evidence(_)
            | SnapshotRecord::Admin(_)
            | SnapshotRecord::KeyCopy(_)
            | SnapshotRecord::Announcement(_)
            | SnapshotRecord::Claim(_) => {}
        }
        let section = section(&record);
        self.db
            .internal_execute(&self.inserts.records, params![section, section == 7, frame])?;
        Ok(Ok(()))
    }

    /// A synced row's schema checks, then its values and parents.
    fn synced(&self, row: &SyncedRow, additions: bool) -> Result<Checked, DbError> {
        let keyed = match check_synced(self.schema, row, additions) {
            Ok(keyed) => keyed,
            Err(refused) => return Ok(Err(refused)),
        };
        let at = self.inserts.layout.position(&row.row.table);
        let layout = &self.inserts.layout.tables[at];
        let mut values = vec![rusqlite::types::Value::Null; layout.width()];
        values[TableLayout::KEY] = row.row.key.clone().into();
        values[TableLayout::AUDIENCE] = audience_text(&row.row.audience).into();
        values[TableLayout::SYNCED] = layout
            .columns
            .iter()
            .map(|name| {
                if row.columns.contains_key(name) {
                    '1'
                } else {
                    '0'
                }
            })
            .collect::<String>()
            .into();
        values[TableLayout::MERGED] = 0.into();
        values[TableLayout::EARLIER] = 0.into();
        values[TableLayout::KEYED] = keyed.into();
        values[TableLayout::PRESENT] = 0.into();
        for (j, name) in layout.columns.iter().enumerate() {
            if let Some(column) = row.columns.get(name) {
                values[layout.value_at(j)] = sql_value(&column.value);
            }
        }
        put_parents(layout, &mut values, |j| row.columns.get(&layout.columns[j]));
        self.db
            .internal_execute(&self.inserts.rows[at][0], params_from_iter(values))?;
        Ok(Ok(()))
    }

    /// A merge row's checks, then its generations, cells and parents.
    /// Installing compares it with its synced row.
    fn merged(
        &self,
        merged: &MergeRow,
        columns: &BTreeSet<(String, String)>,
    ) -> Result<Checked, DbError> {
        if let Err(refused) = check_merged(self.schema, merged, columns) {
            return Ok(Err(refused));
        }
        let state = &merged.state;
        let row = state.row();
        let at = self.inserts.layout.position(&row.table);
        let layout = &self.inserts.layout.tables[at];
        let generation = state.generation();
        let writer = state.generations()[&generation];
        let mut values = vec![rusqlite::types::Value::Null; layout.width()];
        values[TableLayout::KEY] = row.key.clone().into();
        values[TableLayout::AUDIENCE] = audience_text(&row.audience).into();
        values[TableLayout::MERGED] = 1.into();
        values[TableLayout::PRESENT] = state.present().into();
        values[TableLayout::GENERATION] = generation.to_be_bytes().to_vec().into();
        values[TableLayout::DEVICE] = writer.device.0.to_be_bytes().to_vec().into();
        values[TableLayout::NUMBER] = writer.number.to_be_bytes().to_vec().into();
        values[TableLayout::EARLIER] = (state.generations().len() as i64 - 1).into();
        values[TableLayout::KEYED] = 1.into();
        for (j, name) in layout.columns.iter().enumerate() {
            if let Some(cell) = state.cells().get(name) {
                values[layout.value_at(j)] = sql_value(&cell.value.value);
                values[layout.setter_at(j)] = cell.write.device.0.to_be_bytes().to_vec().into();
                values[layout.setter_at(j) + 1] = cell.write.number.to_be_bytes().to_vec().into();
            }
        }
        put_parents(layout, &mut values, |j| {
            state
                .cells()
                .get(&layout.columns[j])
                .map(|cell| &cell.value)
        });
        self.db
            .internal_execute(&self.inserts.rows[at][1], params_from_iter(values))?;
        for (earlier, write) in state.generations() {
            if *earlier == generation {
                continue;
            }
            self.db.internal_execute(
                &self.inserts.generations,
                params![
                    row.table,
                    row.key,
                    audience_text(&row.audience),
                    earlier.to_be_bytes().as_slice(),
                    write.device.0.to_be_bytes().as_slice(),
                    write.number.to_be_bytes().as_slice()
                ],
            )?;
        }
        Ok(Ok(()))
    }

    /// A source record and the part it belongs to. Every record of a part
    /// carries the part's one header.
    fn authored(
        &self,
        authored: &AuthoredChange,
        parts: &mut BTreeMap<(Vec<u8>, String), WriteHeader>,
    ) -> Result<Checked, DbError> {
        let write = encoded(merge_fields::encode_write_id(&authored.header.position))?;
        let row = authored.change.row();
        let audience = audience_text(&row.audience);
        // Equal headers encode to equal bytes, and decoding gives the value
        // back, so comparing values compares the bytes the part keeps.
        match parts.get(&(write.clone(), audience.clone())) {
            Some(known) if *known != authored.header => {
                return Ok(Err(invalid("original source header changed")));
            }
            Some(_) => {}
            None => {
                self.db.internal_execute(
                    &self.inserts.parts,
                    params![
                        write,
                        audience,
                        encoded(merge_fields::encode_write_header(&authored.header))?
                    ],
                )?;
                parts.insert((write.clone(), audience.clone()), authored.header.clone());
            }
        }
        if authored.complete {
            self.db
                .internal_execute(&self.inserts.complete, params![write, audience])?;
        }
        self.db.internal_execute(
            &self.inserts.authored,
            params![
                write,
                audience,
                row.table,
                row.key,
                authored.change.encode()?,
                crate::write_sources::stored_verdict(&authored.verdict)?
            ],
        )?;
        Ok(Ok(()))
    }
}

/// The loader's checks of a synced row against the schema, and whether it
/// has every key column, from which its key is checked here.
fn check_synced(schema: &WriteSchema, row: &SyncedRow, additions: bool) -> Result<bool, DbError> {
    let table = schema.row_table(&row.row)?;
    for (name, value) in &row.columns {
        schema.row_column(table, &row.row.audience, name, value)?;
    }
    if crate::file_table::is_files(&table.name) {
        crate::file_table::check_cells(
            &row.columns.iter().map(|(n, v)| (n.as_str(), v)).collect(),
            true,
        )?;
    }
    let disagree = || invalid("synced values disagree with their key or schema");
    if !additions && row.columns.len() != table.columns.len() {
        return Err(disagree());
    }
    let key: Option<crate::write_rows::AppValues> = crate::write_rows::key_columns(table)
        .iter()
        .map(|column| {
            row.columns
                .get(&column.name)
                .map(|value| (column.name.clone(), value.value.clone()))
        })
        .collect();
    let Some(key) = key else {
        // A key column left to its default is checked once SQLite gives it.
        return Ok(false);
    };
    if crate::write_rows::row_key(table, &key)? != row.row.key {
        return Err(disagree());
    }
    Ok(true)
}

/// The loader's checks of a merge row on its own.
fn check_merged(
    schema: &WriteSchema,
    merged: &MergeRow,
    columns: &BTreeSet<(String, String)>,
) -> Result<(), DbError> {
    let state = &merged.state;
    let row = state.row();
    let table = schema.row_columns(
        row,
        state.present(),
        state.cells().iter().map(|(name, cell)| (name, &cell.value)),
    )?;
    for foreign in &table.foreign_keys {
        let mut setters = foreign.columns.iter().filter_map(|column| {
            state
                .cells()
                .get(crate::write_rows::column_name(table, column))
        });
        if let Some(first) = setters.next() {
            if setters.any(|cell| cell.write != first.write) {
                return Err(invalid("multi-column reference setters disagree"));
            }
        }
    }
    if state.generations().is_empty() {
        return Err(invalid("merge row has no generations"));
    }
    for name in state.cells().keys() {
        if !columns.contains(&(row.table.clone(), name.clone())) {
            return Err(invalid("merge cell has no column record"));
        }
    }
    Ok(())
}

/// Stage the parents each column's value carries, by reference pair.
fn put_parents<'a>(
    layout: &TableLayout,
    values: &mut [rusqlite::types::Value],
    column: impl Fn(usize) -> Option<&'a coven_merge::ColumnValue<Value>>,
) {
    for (q, (j, key)) in layout.pairs.iter().enumerate() {
        let at = layout.parent_at(q);
        match column(*j).and_then(|value| value.parents.get(key)) {
            Some(parent) => {
                values[at] = parent.row.key.clone().into();
                values[at + 1] = audience_text(&parent.row.audience).into();
                values[at + 2] = parent.generation.to_be_bytes().to_vec().into();
            }
            None => {
                values[at] = rusqlite::types::Value::Null;
                values[at + 1] = rusqlite::types::Value::Null;
                values[at + 2] = rusqlite::types::Value::Null;
            }
        }
    }
}

fn check_header(
    schema: &WriteSchema,
    expected: &SnapshotId,
    header: &SnapshotHeader,
) -> Result<(), DbError> {
    if header.id != *expected {
        return Err(invalid(
            "snapshot identity differs from the requested object",
        ));
    }
    if !schema.versions.contains(&header.schema_version) {
        return Err(SnapshotError::Schema {
            snapshot: header.schema_version,
            database: *schema.versions.end(),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_staging_tests.rs"]
mod tests;
