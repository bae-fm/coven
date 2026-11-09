//! File references used to prove that storage deletion cannot strand a row.

use super::Database;
use crate::file_protection::value as retain_value;
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{
    DbError, DownloadedPartStream, DownloadedWriteStream, FileEvidence, Slot, UnprovenFiles,
};
use coven_format::{value::Value, write::RowChange};
use coven_foundation::id_source::{DeviceId, ObjectId};
use coven_merge::{ColumnValue, Operation};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

/// Local references read in one database transaction.
#[cfg(test)]
pub(crate) struct FileRetention {
    /// Fixed paths named by rows, losses, published rows, waiting writes or
    /// this device's upload queue.
    pub references: BTreeSet<(DeviceId, ObjectId)>,
}

impl Database {
    /// A snapshot whose absence was confirmed by an exact read or deletion.
    pub async fn snapshot_absent(
        &self,
        id: coven_format::admin::SnapshotId,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_absent_snapshots WHERE audience=?1 AND device=?2 AND number=?3)",
                params![&id.audience, id.device, id.number.to_be_bytes()], |row| row.get(0)))
        }).await
    }

    /// Confirm that a snapshot no longer protects its checked file references.
    /// The references become candidates in the same transaction as that fact.
    pub async fn record_absent_snapshot(
        &self,
        id: coven_format::admin::SnapshotId,
        files: BTreeSet<(DeviceId, ObjectId)>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            let number = std::num::NonZeroU64::new(id.number).ok_or(DbError::DamagedDatabase)?;
            let path = coven_format::path::ObjectPath::snapshot(
                inner.access.directory.id(),
                id.audience.clone(),
                id.device,
                number,
            );
            inner.access.with_writer(|writer| writer.transaction(|db| {
                for (device, object) in files { insert_candidate(db, device, object)?; }
                db.internal_execute("INSERT INTO _coven_absent_snapshots(audience,device,number) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                    params![&id.audience, id.device, id.number.to_be_bytes()])?;
                Ok(())
            }))?;
            inner.inputs.forget_path(path.as_str())
        }).await
    }
    /// Every fixed path local data protects, in one read transaction.
    #[cfg(test)]
    pub(crate) async fn retained_files(&self) -> Result<FileRetention, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|db| {
                let references = db.query("SELECT device,object FROM _coven_row_file_refs UNION SELECT device,object FROM _coven_published_file_refs UNION SELECT device,object FROM _coven_queued_file_refs UNION SELECT device,object FROM _coven_file_uploads", [], |r| Ok((r.get::<_, DeviceId>(0)?,r.get::<_, ObjectId>(1)?)))?.into_iter().collect();
                Ok(FileRetention { references })
            })
        })
        .await
    }

    /// Stream the stored paths a write's records name from checked plaintext. The caller
    /// must authenticate the complete object before using this result. It
    /// names the audience and key of each part it skipped: a skipped part, or
    /// a schema version this version cannot interpret, proves no file absent.
    pub async fn write_file_references<R: std::io::Read + Send + 'static>(
        &self,
        write: DownloadedWriteStream<R>,
        skipped: BTreeSet<(coven_merge::Audience, coven_foundation::id_source::KeyId)>,
    ) -> Result<FileEvidence, DbError> {
        self.call(move |inner| {
            let mut references = BTreeSet::new();
            write.header.encode()?;
            let version = write.header.header.schema_version;
            if !inner.access.write_schema.versions.contains(&version) {
                return Ok(FileEvidence::Unproven(UnprovenFiles::Schema(version)));
            }
            if write.header.parts.len() != write.parts.len() {
                return Err(crate::SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            let opened = write
                .parts
                .iter()
                .filter(|part| matches!(part, DownloadedPartStream::Opened(_)))
                .count();
            if (opened == write.parts.len()) != skipped.is_empty() {
                return Err(crate::SnapshotError::Inconsistent(
                    "skipped parts disagree with the parts named skipped",
                )
                .into());
            }
            if !skipped.is_empty() {
                return Ok(FileEvidence::Unproven(UnprovenFiles::Parts(skipped)));
            }
            for (header, part) in write.header.parts.into_iter().zip(write.parts) {
                let DownloadedPartStream::Opened(input) = part else {
                    unreachable!("every part opened");
                };
                crate::download_stream::read_part(input, header, |frame| {
                    if let coven_format::dismissal::WriteFrame::Change(row) = frame {
                        retain_change(&row, &mut references)?;
                    }
                    Ok::<_, DbError>(())
                })?;
            }
            Ok(FileEvidence::Complete(references))
        })
        .await
    }

    /// The schema versions whose writes and snapshots this version interprets.
    pub async fn write_schema_versions(&self) -> Result<RangeInclusive<u32>, DbError> {
        self.call(move |inner| Ok(inner.access.write_schema.versions.clone())).await
    }

    /// The candidates of these uploaders, each with whether local data still
    /// protects it (§17.10). Other uploaders' candidates are not read: their
    /// files are deleted by another device until ownership changes.
    pub async fn file_candidates(
        &self,
        uploaders: BTreeSet<DeviceId>,
    ) -> Result<Vec<FileCandidate>, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|db| {
                let mut candidates = Vec::new();
                for device in uploaders {
                    candidates.extend(db.query(
                        "SELECT object,EXISTS(SELECT 1 FROM _coven_file_protection p WHERE p.device=c.device AND p.object=c.object),since FROM _coven_file_candidates c WHERE c.device=?1 ORDER BY c.object",
                        [device],
                        |r| {
                            Ok(FileCandidate {
                                device,
                                object: r.get(0)?,
                                protected: r.get(1)?,
                                since: crate::store_state_tables::slot(r, 2)?,
                            })
                        },
                    )?);
                }
                Ok(candidates)
            })
        })
        .await
    }

    /// The slot count each of these devices had read through when it made its
    /// last published write, for those that published one. A device's
    /// writes are numbered as it commits them and its applied slot never
    /// goes back, so its last published write read furthest, and every
    /// write it made before is published too (§6.3, §17.10).
    pub async fn published_reads(
        &self,
        devices: BTreeSet<DeviceId>,
    ) -> Result<BTreeMap<DeviceId, u64>, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|db| {
                let mut reads = BTreeMap::new();
                for device in devices {
                    if let Some(read) = db
                        .query(
                            &format!("SELECT read_through FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND slot IS NOT NULL ORDER BY number DESC LIMIT 1"),
                            [device],
                            |r| crate::write_encoding::counter(r.get(0)?),
                        )?
                        .pop()
                    {
                        reads.insert(device, read);
                    }
                }
                Ok(reads)
            })
        })
        .await
    }

    /// Settle candidates in one transaction. A protected one goes only while
    /// local data still protects it, so releasing that protection adds it
    /// again; a deleted one goes after storage confirmed its deletion.
    pub async fn settle_file_candidates(
        &self,
        protected: Vec<(DeviceId, ObjectId)>,
        deleted: Vec<(DeviceId, ObjectId)>,
    ) -> Result<(), DbError> {
        if protected.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (device, object) in protected {
                        db.internal_execute(
                            "DELETE FROM _coven_file_candidates WHERE device=?1 AND object=?2 AND EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=?1 AND object=?2)",
                            params![device, object],
                        )?;
                    }
                    for (device, object) in deleted {
                        db.internal_execute(
                            "DELETE FROM _coven_file_candidates WHERE device=?1 AND object=?2",
                            params![device, object],
                        )?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Paths named by a retained object this device confirmed deleted: its
    /// protection ended with the object (§17.10).
    #[cfg(test)]
    pub async fn add_file_candidates(
        &self,
        files: BTreeSet<(DeviceId, ObjectId)>,
    ) -> Result<(), DbError> {
        if files.is_empty() {
            return Ok(());
        }
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (device, object) in files {
                        insert_candidate(db, device, object)?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// The counted entry stopping `device` for good at which this device
    /// last listed its file folder, if it did (§17.10).
    pub async fn file_scan(&self, device: DeviceId) -> Result<Option<Slot>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                Ok(db
                    .query("SELECT slot FROM _coven_file_scans WHERE device=?1", [device], |r| {
                        crate::store_state_tables::slot(r, 0)
                    })?
                    .pop())
            })
        })
        .await
    }

    /// Record a complete listing of the file folder of an uploader gone for
    /// good, taken once the entry at `stop` stopped it, with every path it
    /// found as a candidate. Returns false, recording nothing, when this
    /// device hasn't applied a counted cut-off at `stop` naming the device or
    /// its member: the history the caller judged by is not the one applied
    /// here. Whether a replacement of the member's keys stopped the device,
    /// which depends on the slot that added it and the entry's author, the
    /// caller's history decides (§14.1). Nothing the device writes after the
    /// stop counts, so any listing taken after it is sound, however often it
    /// is taken (§17.10).
    pub async fn complete_file_scan(
        &self,
        device: DeviceId,
        stop: Slot,
        objects: BTreeSet<ObjectId>,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let kept: bool = db.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_cutoffs c JOIN _coven_devices d ON d.device=?1 WHERE c.slot=?2 AND (c.device=d.device OR c.member=d.member))",
                        params![device, stop.get().to_be_bytes()],
                        |r| r.get(0),
                    )?;
                    if !kept {
                        return Ok(false);
                    }
                    for object in objects {
                        insert_candidate(db, device, object)?;
                    }
                    db.internal_execute(
                        "INSERT INTO _coven_file_scans(device,slot) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET slot=excluded.slot",
                        params![
                            device,
                            stop.get().to_be_bytes()
                        ],
                    )?;
                    Ok(true)
                })
            })
        })
        .await
    }
}

/// A fixed file path awaiting a storage-deletion decision (§17.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileCandidate {
    /// The uploader named by its path.
    pub device: DeviceId,
    /// The stored object's id, the last part of its path.
    pub object: ObjectId,
    /// A local row, loss, published row, queued write or upload still names it.
    pub protected: bool,
    /// This device's applied slot when the path last lost its local
    /// protection, or a later one: a write that read through it saw every
    /// change that left the path unnamed (§17.10).
    pub since: Slot,
}

fn insert_candidate(
    db: &crate::sqlite::DatabaseConnection,
    device: DeviceId,
    object: ObjectId,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_file_candidates(device,object,since) VALUES(?1,?2,(SELECT applied FROM _coven_sequence WHERE singleton=1)) ON CONFLICT DO UPDATE SET since=max(since,excluded.since)",
        params![device, object],
    )?;
    Ok(())
}

/// Count every candidate from the applied slot `slot`, as though each lost
/// its protection there.
pub(crate) fn count_candidates_from(
    db: &crate::sqlite::DatabaseConnection,
    slot: u64,
) -> Result<(), DbError> {
    db.internal_execute("UPDATE _coven_file_candidates SET since=?1", [slot.to_be_bytes()])?;
    Ok(())
}

/// Stored files named by the records and losses of the audience snapshot
/// just read into this transaction. A location that won't decode is the
/// snapshot's own failure.
pub(crate) fn snapshot_references(
    db: &crate::sqlite::DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    audience: &coven_merge::Audience,
) -> Result<BTreeSet<(DeviceId, ObjectId)>, DbError> {
    let mut references = BTreeSet::new();
    crate::snapshot_install::file_locations(db, schema, audience, |value| {
        retain_value(value, &mut references)
    })
    .and_then(|()| {
        crate::loss_record::visit(db, audience, |loss| {
            crate::file_protection::loss(&loss, &mut references);
            Ok::<_, DbError>(())
        })
    })
    .map_err(|error| match error {
        DbError::DamagedDatabase => {
            crate::snapshot_error::invalid("snapshot file location is invalid")
        }
        error => error,
    })?;
    Ok(references)
}

fn retain_change(
    row: &RowChange,
    references: &mut BTreeSet<(DeviceId, ObjectId)>,
) -> Result<(), DbError> {
    if let Operation::Insert(columns) | Operation::Update(columns) = &row.change.operation {
        retain_columns(&row.row.table, columns, references)?;
    }
    if let Some(column) = crate::file_protection::location(&row.row.table) {
        if let Some(value) = row.old.get(column) {
            retain_value(value, references)?;
        }
    }
    Ok(())
}

fn retain_columns(
    table: &str,
    columns: &BTreeMap<String, ColumnValue<Value>>,
    references: &mut BTreeSet<(DeviceId, ObjectId)>,
) -> Result<(), DbError> {
    if let Some(column) = crate::file_protection::location(table) {
        if let Some(value) = columns.get(column) {
            retain_value(&value.value, references)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "database_file_retention_tests.rs"]
pub(crate) mod tests;
