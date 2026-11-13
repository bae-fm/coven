//! A snapshot's checked records, staged on the input connection while its
//! body streams, until one load or file check reads them (§16.2, §21.2).
//! Rows, cells, references, source records and creations are staged as
//! columns of tables of their own, so installing reads fields rather than
//! decoding records again; the few other records keep their frames.

use super::InputDatabase;
use crate::database::{start_call, Database, DatabaseInner};
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_layout::{SnapshotLayout, TableLayout};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{encoded, sql_value};
use crate::write_metadata::MetadataFailure;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    admin::SnapshotId,
    merge_fields,
    sealed_snapshot::SnapshotObjectPrefix,
    snapshot::{section, SnapshotHeader, SnapshotRecord},
    snapshot_rows::{AuthoredRecord, MergeRow},
    snapshot_stream::SnapshotChunkDecoder,
    value::Value,
    write::WriteHeader,
};
use coven_foundation::locks::read_unpoisoned;
use rusqlite::{params, params_from_iter};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Created with the input connection, in its temporary storage, so staged
/// records go with the connection: no restart finds them. That storage spills
/// to files once the connection's cache fills; opening refuses a SQLite built
/// to keep it in memory. Only the snapshot loader names these tables.
pub(super) fn create_tables(db: &DatabaseConnection) -> Result<(), DbError> {
    db.batch("PRAGMA temp_store=FILE;")?;
    crate::snapshot_metadata::create_table(db)
}

/// One staging's number and the tables that hold its records on the input
/// connection.
struct StagedTables {
    id: i64,
    tables: Vec<String>,
}

/// A staging's tables of its own, beside its rows' tables.
pub(crate) fn table(staging: i64, name: &str) -> String {
    format!("temp._coven_staged_{staging}_{name}")
}

#[path = "staged_damage.rs"]
mod damage;
use damage::with_staged_digest;
#[cfg(any(test, feature = "test-utils"))]
pub use damage::StagedDamage;
pub use damage::StagedDigest;

fn create_staged(
    db: &DatabaseConnection,
    layout: &SnapshotLayout,
    staging: i64,
) -> Result<Vec<String>, DbError> {
    let mut tables = Vec::new();
    for table in &layout.tables {
        db.batch(&table.create_staged(staging))?;
        tables.push(table.staged(staging));
    }
    let name = |name: &str| table(staging, name).trim_start_matches("temp.").to_owned();
    db.batch(&format!(
        "CREATE TEMP TABLE {}(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,generation BLOB NOT NULL,device BLOB NOT NULL,number BLOB NOT NULL);
         CREATE TEMP TABLE {}(table_name TEXT NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(table_name,column_name)) WITHOUT ROWID;
         CREATE TEMP TABLE {}(write_id BLOB NOT NULL,audience TEXT NOT NULL,header BLOB NOT NULL,PRIMARY KEY(write_id,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE {}(write_id BLOB NOT NULL,audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,record BLOB NOT NULL,admission BLOB,stop BLOB);
         CREATE TEMP TABLE {}(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,write_id BLOB NOT NULL,creator BLOB NOT NULL);
         CREATE TEMP TABLE {}(section INTEGER NOT NULL,evidence INTEGER NOT NULL CHECK(evidence IN (0,1)),record BLOB NOT NULL);",
        name("generations"),
        name("columns"),
        name("parts"),
        name("authored"),
        name("creations"),
        name("records"),
    ))?;
    for name in ["generations", "columns", "parts", "authored", "creations", "records"] {
        tables.push(table(staging, name));
    }
    Ok(tables)
}

/// Drop a staging's tables whole.
fn remove(db: &DatabaseConnection, staging: &StagedTables) -> Result<(), DbError> {
    for table in &staging.tables {
        db.batch(&format!("DROP TABLE {table}"))?;
    }
    crate::snapshot_metadata::remove(db, staging.id)
}

/// Drop the tables of each of `stagings` that still holds its records, in
/// the transaction of the operation that ends them.
fn remove_all(db: &DatabaseConnection, stagings: &[SnapshotStaging]) -> Result<(), DbError> {
    stagings
        .iter()
        .filter_map(|staging| staging.tables.as_ref())
        .try_for_each(|tables| remove(db, tables))
}

/// The statements staging one snapshot runs for its records, written once
/// for its tables.
struct Inserts {
    layout: SnapshotLayout,
    /// Each synced table's row insert, in the layout's order.
    rows: Vec<String>,
    generations: String,
    columns: String,
    parts: String,
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
                format!(
                    "INSERT INTO {}({}) VALUES({})",
                    table.staged(staging),
                    table.names().join(","),
                    vec!["?"; table.width()].join(",")
                )
            })
            .collect();
        let insert = |name: &str, columns: &str| {
            let count = columns.split(',').count();
            format!(
                "INSERT INTO {}({columns}) VALUES({})",
                table(staging, name),
                (1..=count).map(|i| format!("?{i}")).collect::<Vec<_>>().join(",")
            )
        };
        Self {
            layout,
            rows,
            generations: insert("generations", "table_name,key,audience,generation,device,number"),
            columns: insert("columns", "table_name,column_name"),
            parts: insert("parts", "write_id,audience,header"),
            authored: insert("authored", "write_id,audience,table_name,key,record,admission,stop"),
            creations: insert("creations", "table_name,key,audience,write_id,creator"),
            records: insert("records", "section,evidence,record"),
        }
    }
}

impl InputDatabase {
    fn begin_staging(&self, schema: &WriteSchema) -> Result<(StagedTables, Inserts), DbError> {
        let id = self.stagings.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let layout = SnapshotLayout::new(schema);
        let tables = self.write(|db| create_staged(db, &layout, id))?;
        Ok((StagedTables { id, tables }, Inserts::new(layout, id)))
    }

    /// The damage a test asked for to the staging finishing now, if any.
    #[cfg(any(test, feature = "test-utils"))]
    fn next_finish_damage(&self) -> Option<StagedDamage> {
        let mut damage = coven_foundation::locks::lock_unpoisoned(&self.damage_finishing);
        let (made, count) = (*damage)?;
        *damage = (count > 1).then_some((made, count - 1));
        Some(made)
    }

    /// Remove a staging's records.
    fn remove_staging(&self, staging: &StagedTables) -> Result<(), DbError> {
        super::with_locked(&self.writer, |writer| writer.transaction(|db| remove(db, staging)))
    }

    /// Run one load or file check over `stagings`, the last phase of each:
    /// `consume` reads their records in one input transaction, which removes
    /// them as it commits. When it fails, the records are removed before the
    /// failure is reported, so nothing staged outlives its operation (§16.2).
    pub(crate) fn read_staged<T, S: AsMut<[SnapshotStaging]>>(
        &self,
        stagings: &mut S,
        consume: impl FnOnce(
            &crate::input_preparation::PreparedInputs<'_>,
            &mut S,
        ) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        super::with_locked(&self.writer, |writer| {
            #[cfg(any(test, feature = "test-utils"))]
            if let Some(made) = coven_foundation::locks::lock_unpoisoned(&self.damage_staged).take()
            {
                let first = stagings.as_mut().iter().find_map(|staging| staging.tables.as_ref());
                let first = first.map(|tables| tables.id).ok_or(DbError::Invariant {
                    what: "a test damages a staging that holds its records",
                })?;
                writer.transaction(|db| damage::damage(db, first, made))?;
            }
            let read = writer.transaction(|db| {
                stagings.as_mut().iter().try_for_each(|staging| damage::unchanged(db, staging))?;
                let inputs = crate::input_preparation::PreparedInputs::new(db)?;
                let result = consume(&inputs, stagings)?;
                inputs.finish()?;
                remove_all(db, stagings.as_mut())?;
                Ok(result)
            });
            let result = match read {
                Ok(result) => Ok(result),
                Err(error) => {
                    let error = with_staged_digest(writer, stagings.as_mut(), error);
                    let removed = writer.transaction(|db| {
                        #[cfg(any(test, feature = "test-utils"))]
                        if self.fail_removal.swap(false, std::sync::atomic::Ordering::SeqCst) {
                            return Err(damage::removal_failure());
                        }
                        remove_all(db, stagings.as_mut())
                    });
                    match removed {
                        Ok(()) => Err(error),
                        // The stagings keep their tables, which dropping
                        // them removes.
                        Err(removal) => {
                            return Err(DbError::StagingCleanup {
                                operation: Box::new(error),
                                cleanup: Box::new(removal),
                            })
                        }
                    }
                }
            };
            stagings.as_mut().iter_mut().for_each(SnapshotStaging::taken);
            result
        })
    }
}

/// One snapshot's staging, from its first batch to the load or file check
/// that reads its records, or the discard that removes them (§16.2). It is
/// the one holder of what it stages for that whole operation: its steps are
/// its phases, and the load takes it whole. Dropped while still holding
/// records, as by a cancelled or panicking caller, it removes them.
pub struct SnapshotStaging {
    database: Database,
    expected: SnapshotId,
    /// Absent once the load or file check that read them removed them.
    tables: Option<StagedTables>,
    phase: Phase,
    /// The batches staged so far, and the time staging them took.
    batches: usize,
    time: std::time::Duration,
}

/// Where a staging is in its operation.
enum Phase {
    /// The body streams in: sync hands it each batch of opened plaintext,
    /// decoded, checked and committed in one input transaction, outside
    /// the application writer (§21.2). The state, near a kilobyte with its
    /// decoder, lives on the heap: each batch moves it to the blocking pool
    /// and back, and every future holding the staging carries what is inline.
    Streaming(Box<StagingState>),
    /// The stream checked out to its end marker.
    Staged {
        header: SnapshotHeader,
        /// The digest of the records as staging ended, which every load or
        /// file check compares with what the disk holds before it reads
        /// any (§16.2).
        digest: StagedDigest,
        /// The first record that failed the loader's checks as it staged,
        /// and its section: the load or file check refuses the body with it
        /// where it reaches that section.
        refusal: Option<(usize, DbError)>,
    },
    /// A batch or the end of the stream failed; only discarding is left.
    Failed,
}

/// What staging keeps between batches beside its tables: the decoder, the
/// statements that stage records, the records staged so far, the columns the
/// snapshot names, the source part whose changes are arriving, and the first
/// record the loader's checks refuse.
struct StagingState {
    decoder: SnapshotChunkDecoder,
    inserts: Inserts,
    records: i64,
    columns: BTreeSet<(String, String)>,
    part: Option<StagedPart>,
    refusal: Option<(usize, DbError)>,
}

/// The part of section 4 whose changes follow it (D7.3). Its row is staged
/// with its first change, which names its audience.
struct StagedPart {
    write: Vec<u8>,
    header: WriteHeader,
    staged: bool,
}

impl Drop for SnapshotStaging {
    fn drop(&mut self) {
        let Some(tables) = self.tables.take() else {
            return;
        };
        // Only an abandoned staging gets here: a cancelled or panicking
        // caller. Its connection's temporary storage goes when the store
        // closes, which waits for this removal. Nobody waits for the removal,
        // so a bug in it goes to the store's bug sink.
        let bugs = self.database.bugs.clone();
        let remove = move |inner: &DatabaseInner| {
            let removed =
                coven_foundation::task::caught_sync("remove an abandoned snapshot staging", || {
                    inner.call(|inner| inner.inputs.remove_staging(&tables))
                });
            match removed {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if !coven_foundation::task::report_carried(bugs.as_ref(), &error) {
                        tracing::error!(%error, "removing an abandoned snapshot staging failed");
                    }
                }
                Err(bug) => coven_foundation::task::report(bugs.as_ref(), &bug),
            }
            Ok::<_, DbError>(())
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let removing = start_call(
                "remove an abandoned snapshot staging",
                &self.database.bugs,
                self.database.inner.clone(),
                remove,
            );
            #[cfg(test)]
            coven_foundation::locks::lock_unpoisoned(&self.database.writer.abandoned_stagings)
                .push(removing);
            #[cfg(not(test))]
            drop(removing);
        } else if let Some(inner) = read_unpoisoned(&self.database.inner).as_ref() {
            let _removed = remove(inner);
        }
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
        let (tables, inserts) =
            self.call(|inner| inner.inputs.begin_staging(&inner.access.write_schema)).await?;
        Ok(SnapshotStaging {
            database: self.clone(),
            expected: id,
            tables: Some(tables),
            phase: Phase::Streaming(Box::new(StagingState {
                decoder: SnapshotChunkDecoder::new(prefix),
                inserts,
                records: 0,
                columns: BTreeSet::new(),
                part: None,
                refusal: None,
            })),
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

    /// Make `damage` to a staging before the next load or file check reads
    /// it, as damage to this device's disk would (§19.1).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn damage_next_staged_read(&self, damage: StagedDamage) -> Result<(), DbError> {
        self.call(move |inner| {
            *coven_foundation::locks::lock_unpoisoned(&inner.inputs.damage_staged) = Some(damage);
            Ok::<_, DbError>(())
        })
        .await
    }

    /// Make `damage` to each of the next `count` stagings as it finishes,
    /// before finishing checks it, as damage to this device's disk would
    /// (§19.1). Damage to two stagings of one snapshot alike stands for a
    /// snapshot whose own records fail.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn damage_staged_finishes(
        &self,
        damage: StagedDamage,
        count: u32,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            *coven_foundation::locks::lock_unpoisoned(&inner.inputs.damage_finishing) =
                Some((damage, count));
            Ok::<_, DbError>(())
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

/// The failure that ends `stagings` before their load or file check runs,
/// reported once each has removed its records, beside any removal that
/// failed (§16.2).
pub(crate) async fn refuse(stagings: Vec<SnapshotStaging>, error: DbError) -> DbError {
    let mut error = error;
    for staging in stagings {
        if let Err(cleanup) = staging.discard().await {
            error =
                DbError::StagingCleanup { operation: Box::new(error), cleanup: Box::new(cleanup) };
        }
    }
    error
}

impl SnapshotStaging {
    fn id(&self) -> Result<i64, DbError> {
        self.tables.as_ref().map(|tables| tables.id).ok_or(DbError::Invariant {
            what: "a staged snapshot is read once, before it is discarded",
        })
    }

    /// Decode one batch of opened plaintext and commit its records together.
    /// After a failure, discard this staging.
    pub async fn stage(&mut self, plaintext: Vec<u8>) -> Result<(), DbError> {
        let staging = self.id()?;
        let Phase::Streaming(mut state) = std::mem::replace(&mut self.phase, Phase::Failed) else {
            return Err(DbError::Invariant {
                what: "a snapshot stages only until its stream ends or fails",
            });
        };
        let expected = self.expected.clone();
        let (state, time) = self
            .database
            .call(move |inner| {
                let started = inner.clock.elapsed();
                inner.inputs.write(|db| {
                    stage(
                        db,
                        &inner.access.write_schema,
                        &expected,
                        staging,
                        &mut state,
                        &plaintext,
                    )
                })?;
                Ok::<_, DbError>((state, inner.clock.elapsed() - started))
            })
            .await?;
        self.phase = Phase::Streaming(state);
        self.batches += 1;
        self.time += time;
        Ok(())
    }

    /// Refuse a stream without its header, declared records and end marker,
    /// then check its applied writes. Nothing more stages after this; after a
    /// failure, discard this staging.
    pub async fn finish(&mut self) -> Result<(), DbError> {
        let staging = self.id()?;
        let Phase::Streaming(state) = std::mem::replace(&mut self.phase, Phase::Failed) else {
            return Err(DbError::Invariant {
                what: "a snapshot stages only until its stream ends or fails",
            });
        };
        let expected = self.expected.clone();
        let records = state.records;
        let tables = StagedTables {
            id: staging,
            tables: self.tables.as_ref().map(|tables| tables.tables.clone()).unwrap_or_default(),
        };
        let (header, refusal, digest) = self
            .database
            .call(move |inner| {
                #[cfg(any(test, feature = "test-utils"))]
                if let Some(made) = inner.inputs.next_finish_damage() {
                    inner.inputs.write(|db| damage::damage(db, staging, made))?;
                }
                inner.inputs.write(|db| {
                    state.decoder.finish().map_err(SnapshotError::Format)?;
                    let header = state
                        .decoder
                        .header()
                        .ok_or_else(|| invalid("snapshot has no header"))?
                        .clone();
                    check_header(&inner.access.write_schema, &expected, &header)?;
                    SnapshotMetadata::staged(db, staging).validate(header.covers)?;
                    let digest = damage::digest(db, &tables)?;
                    Ok((header, state.refusal, digest))
                })
            })
            .await?;
        tracing::info!(
            audience = ?header.id.audience,
            records,
            batches = self.batches,
            time = ?self.time,
            "staged snapshot"
        );
        self.phase = Phase::Staged { header, digest, refusal };
        Ok(())
    }

    /// Remove everything staged so far (§16.2).
    pub async fn discard(mut self) -> Result<(), DbError> {
        let Some(tables) = self.tables.take() else {
            return Ok(());
        };
        self.database.call(move |inner| inner.inputs.remove_staging(&tables)).await
    }

    /// Whether `database` staged it: records on another store's input
    /// connection are not there to read.
    pub(crate) fn staged_by(&self, database: &Database) -> bool {
        Arc::ptr_eq(&self.database.inner, &database.inner)
    }

    /// Authenticate its staged write evidence for the load or file check
    /// reading it, in that operation's input transaction (D7.5).
    pub(crate) fn prepare<'a>(
        &mut self,
        inputs: &crate::input_preparation::PreparedInputs<'a>,
        authorship: &dyn crate::Authorship,
    ) -> Result<crate::snapshot_preparation::PreparedSnapshot<'a>, DbError> {
        let staging = self.id()?;
        let Phase::Staged { header, refusal, .. } = &mut self.phase else {
            return Err(DbError::Invariant {
                what: "a snapshot is read once its stream checked out to its end",
            });
        };
        inputs.snapshot(staging, header.clone(), refusal.take(), authorship)
    }

    /// Its records are gone, with the transaction that removed them.
    fn taken(&mut self) {
        self.tables = None;
    }
}

fn stage(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    expected: &SnapshotId,
    staging: i64,
    state: &mut StagingState,
    mut bytes: &[u8],
) -> Result<(), DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    if state.records == 0 {
        db.record_work(|work| work.snapshot_reads += 1);
    }
    let metadata = SnapshotMetadata::staged(db, staging);
    let StagingState { decoder, inserts, records, columns, part, refusal } = state;
    loop {
        let headed = decoder.header().is_some();
        let record =
            decoder.next_record(&mut bytes, &metadata).map_err(MetadataFailure::snapshot)?;
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
        let staged = RecordStaging { db, schema, inserts };
        let section = record.section();
        if let Err(refused) = staged.record(record, decoder.frame(), columns, part)? {
            *refusal = Some((section, refused));
        }
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
        columns: &mut BTreeSet<(String, String)>,
        part: &mut Option<StagedPart>,
    ) -> Result<Checked, DbError> {
        match &record {
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
            SnapshotRecord::Authored(authored) => return self.authored(authored, part),
            SnapshotRecord::Creation(creation) => {
                if let Err(refused) = self.schema.row_table(&creation.row) {
                    return Ok(Err(refused));
                }
                self.db.internal_execute(
                    &self.inserts.creations,
                    params![
                        creation.row.table,
                        creation.row.key,
                        &creation.row.audience,
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
        let section = record.section();
        let evidence = section == section::EVIDENCE;
        self.db
            .internal_execute(&self.inserts.records, params![section as i64, evidence, frame])?;
        Ok(Ok(()))
    }

    /// A merge row's checks, then its generations, cells and parents.
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
        values[TableLayout::AUDIENCE] = (&row.audience).into();
        values[TableLayout::PRESENT] = state.present().into();
        values[TableLayout::GENERATION] = generation.to_be_bytes().to_vec().into();
        values[TableLayout::DEVICE] = writer.device.0.to_be_bytes().to_vec().into();
        values[TableLayout::NUMBER] = writer.number.to_be_bytes().to_vec().into();
        for (j, name) in layout.columns.iter().enumerate() {
            if let Some(cell) = state.cells().get(name) {
                values[layout.value_at(j)] = sql_value(&cell.value.value);
                values[layout.setter_at(j)] = cell.write.device.0.to_be_bytes().to_vec().into();
                values[layout.setter_at(j) + 1] = cell.write.number.to_be_bytes().to_vec().into();
            }
        }
        put_parents(layout, &mut values, |j| {
            state.cells().get(&layout.columns[j]).map(|cell| &cell.value)
        });
        self.db.internal_execute(&self.inserts.rows[at], params_from_iter(values))?;
        for (earlier, write) in state.generations() {
            if *earlier == generation {
                continue;
            }
            self.db.internal_execute(
                &self.inserts.generations,
                params![
                    row.table,
                    row.key,
                    &row.audience,
                    earlier.to_be_bytes().as_slice(),
                    write.device,
                    write.number.to_be_bytes().as_slice()
                ],
            )?;
        }
        Ok(Ok(()))
    }

    /// A source part, or a kept change of the part before it. A change
    /// holds in full exactly the columns that were references under its
    /// write's schema, as this device would keep it (D7.3).
    fn authored(
        &self,
        authored: &AuthoredRecord,
        part: &mut Option<StagedPart>,
    ) -> Result<Checked, DbError> {
        let change = match authored {
            AuthoredRecord::Part(new) => {
                *part = Some(StagedPart {
                    write: encoded(merge_fields::encode_write_id(&new.header.position))?,
                    header: new.header.clone(),
                    staged: false,
                });
                return Ok(Ok(()));
            }
            AuthoredRecord::Change(change) => change,
        };
        let part = part.as_mut().ok_or(DbError::Invariant {
            what: "the snapshot decoder gives a kept change after its part",
        })?;
        let row = change.record.row();
        let audience = &row.audience;
        if !part.staged {
            self.db.internal_execute(
                &self.inserts.parts,
                params![
                    part.write,
                    audience,
                    encoded(merge_fields::encode_write_header(&part.header))?
                ],
            )?;
            part.staged = true;
        }
        let form = crate::write_sources::KeptForm::new(self.schema, part.header.schema_version);
        if !form.conforms(&change.record) {
            return Ok(Err(invalid("kept change holds its columns unlike its schema")));
        }
        self.db.internal_execute(
            &self.inserts.authored,
            params![
                part.write,
                audience,
                row.table,
                row.key,
                change.record.encode()?,
                crate::write_sources::stored_verdict(&change.verdict)?,
                change.stop.map(|stop| stop.get().to_be_bytes().to_vec())
            ],
        )?;
        Ok(Ok(()))
    }
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
        let mut setters = foreign
            .columns
            .iter()
            .filter_map(|column| state.cells().get(crate::write_rows::column_name(table, column)));
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
                values[at + 1] = (&parent.row.audience).into();
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
        return Err(invalid("snapshot identity differs from the requested object"));
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
