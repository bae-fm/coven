//! A snapshot's checked records, staged on the input connection while its
//! body streams, until one load or file check reads them (§16.2, §21.2).

use super::InputDatabase;
use crate::database::{start_call, Database, DatabaseInner};
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    admin::SnapshotId,
    sealed_snapshot::SnapshotObjectPrefix,
    snapshot::{SnapshotHeader, SnapshotRecord},
    snapshot_stream::SnapshotChunkDecoder,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Created with the input connection, in its temporary storage, so staged
/// records go with the connection: no restart finds them. That storage spills
/// to files once the connection's cache fills; opening refuses a SQLite built
/// to keep it in memory. Only the snapshot loader names these tables.
pub(super) fn create_tables(db: &DatabaseConnection) -> Result<(), DbError> {
    db.batch(
        "PRAGMA temp_store=FILE;
         CREATE TEMP TABLE _coven_staged_records(staging INTEGER NOT NULL,ordinal INTEGER NOT NULL,evidence INTEGER NOT NULL CHECK(evidence IN (0,1)),record BLOB NOT NULL,PRIMARY KEY(staging,ordinal));
         CREATE INDEX temp._coven_staged_evidence ON _coven_staged_records(staging,evidence,ordinal);",
    )?;
    SnapshotMetadata::create_table(db)
}

/// One staging's number, and whether its records are still on the input
/// connection. It changes only while the input writer is held.
pub(crate) struct Staging {
    pub(crate) id: i64,
    held: AtomicBool,
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

pub(crate) fn remove(db: &DatabaseConnection, staging: &Staging) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM temp._coven_staged_records WHERE staging=?1",
        [staging.id],
    )?;
    SnapshotMetadata::remove(db, staging.id)
}

impl InputDatabase {
    fn begin_staging(&self) -> Arc<Staging> {
        Arc::new(Staging {
            id: self.stagings.fetch_add(1, Ordering::SeqCst),
            held: AtomicBool::new(true),
        })
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
    decoder: Option<SnapshotChunkDecoder>,
    next: i64,
    /// The batches staged so far, and the time staging them took.
    batches: usize,
    time: std::time::Duration,
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
}

impl Database {
    /// Begin staging the snapshot `id`, whose signed prefix is `prefix`
    /// (§16.2). Nothing outside the loader reads what it stages.
    pub async fn stage_snapshot(
        &self,
        id: SnapshotId,
        prefix: SnapshotObjectPrefix,
    ) -> Result<SnapshotStaging, DbError> {
        let staging = self
            .call(|inner| Ok::<_, DbError>(inner.inputs.begin_staging()))
            .await?;
        Ok(SnapshotStaging {
            rows: Arc::new(StagedRows {
                database: self.clone(),
                staging,
            }),
            expected: id,
            decoder: Some(SnapshotChunkDecoder::new(prefix)),
            next: 0,
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

    /// The records staged on the input connection now, for every snapshot.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn staged_snapshot_records(&self) -> Result<u64, DbError> {
        self.call(|inner| {
            super::with_locked(&inner.inputs.writer, |writer| {
                writer.query_row(
                    "SELECT (SELECT count(*) FROM temp._coven_staged_records)+(SELECT count(*) FROM temp._coven_snapshot_writes)",
                    [],
                    |row| row.get::<_, i64>(0),
                )
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
        let mut decoder = self.decoder.take().ok_or(DbError::Invariant {
            what: "a snapshot staging that failed is discarded",
        })?;
        let staging = self.rows.staging.clone();
        let expected = self.expected.clone();
        let next = self.next;
        let (decoder, next, time) = self
            .rows
            .database
            .call(move |inner| {
                let started = inner.clock.elapsed();
                let next = inner.inputs.write(|db| {
                    stage(
                        db,
                        &inner.access.write_schema,
                        &expected,
                        &staging,
                        &mut decoder,
                        next,
                        &plaintext,
                    )
                })?;
                Ok::<_, DbError>((decoder, next, inner.clock.elapsed() - started))
            })
            .await?;
        self.decoder = Some(decoder);
        self.next = next;
        self.batches += 1;
        self.time += time;
        Ok(())
    }

    /// Refuse a stream without its header, declared records and end marker,
    /// then check its applied writes. Nothing more stages after this; after a
    /// failure, discard this staging.
    pub async fn finish(&mut self) -> Result<StagedSnapshot, DbError> {
        let decoder = self.decoder.take().ok_or(DbError::Invariant {
            what: "a snapshot staging that failed is discarded",
        })?;
        let staging = self.rows.staging.clone();
        let expected = self.expected.clone();
        let header = self
            .rows
            .database
            .call(move |inner| {
                inner.inputs.write(|db| {
                    decoder.finish().map_err(SnapshotError::Format)?;
                    let header = decoder
                        .header()
                        .ok_or_else(|| invalid("snapshot has no header"))?
                        .clone();
                    check_header(&inner.access.write_schema, &expected, &header)?;
                    SnapshotMetadata::staged(db, staging.id).validate(header.covers)?;
                    // Installing reads applied writes from the records again.
                    SnapshotMetadata::remove(db, staging.id)?;
                    Ok(header)
                })
            })
            .await?;
        tracing::info!(
            audience = ?header.id.audience,
            records = self.next,
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
    decoder: &mut SnapshotChunkDecoder,
    mut next: i64,
    mut bytes: &[u8],
) -> Result<i64, DbError> {
    if !staging.held() {
        return Err(DbError::Invariant {
            what: "a snapshot is staged only until it is read or discarded",
        });
    }
    #[cfg(any(test, feature = "test-utils"))]
    if next == 0 {
        db.record_work(|work| work.snapshot_reads += 1);
    }
    let metadata = SnapshotMetadata::staged(db, staging.id);
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
            return Ok(next);
        };
        if let SnapshotRecord::Write(write) = &record {
            metadata.stage(write)?;
        }
        db.internal_execute(
            "INSERT INTO temp._coven_staged_records(staging,ordinal,evidence,record) VALUES(?1,?2,?3,?4)",
            (
                staging.id,
                next,
                matches!(record, SnapshotRecord::Evidence(_)),
                decoder.frame(),
            ),
        )?;
        next += 1;
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
