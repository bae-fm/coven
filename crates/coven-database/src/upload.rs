//! Original authored plaintext, with format and keys fixed by its first upload attempt.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, encoded};
use crate::DbError;
use coven_format::chunks::CHUNK_SIZE;
use coven_format::sealed_write::WriteObjectPrefix;
use coven_format::write_stream::{PartHeader, WriteHeaderFrame};
use coven_foundation::id_source::DeviceId;
use coven_merge::WriteId;
use rusqlite::params;
use std::rc::Rc;

/// A waiting write's plaintext, fixed at commit, read under one committed
/// reader transaction.
pub struct UploadPlaintext<'a> {
    /// Identity, causality and the audience stream boundaries.
    pub header: WriteHeaderFrame,
    /// The exact plaintext header frame, sealed as one chunk.
    pub header_frame: Vec<u8>,
    /// Each audience's record stream, in header order, cut into 64 KiB chunks.
    pub parts: UploadParts<'a>,
}

/// A captured write ready to seal. Sealing and transport belong to sync;
/// these are its input streams and the keys it seals with.
pub struct WaitingUpload<'a> {
    /// The plaintext fixed at commit.
    pub plaintext: UploadPlaintext<'a>,
    /// Format and keys fixed by the first attempt.
    pub keys: WriteObjectPrefix,
}

/// A captured queue prefix whose writes all have their first-attempt keys
/// fixed, in queue order. Only [`crate::Database::prepare_uploads`] makes
/// one, and [`crate::Database::read_uploads`] reads its writes' plaintext.
pub struct PreparedUploads {
    writes: Vec<PreparedUpload>,
}

/// One captured write: its queue row and the keys it seals with.
struct PreparedUpload {
    rowid: i64,
    write: WriteId,
    keys: WriteObjectPrefix,
}

impl PreparedUploads {
    /// How many writes the prefix captured.
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// Whether the prefix captured no write.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

/// A queue read failed in the database or in its consumer.
#[derive(Debug, thiserror::Error)]
#[cfg(any(test, feature = "test-utils"))]
pub enum UploadReadError<E> {
    /// Reading the committed queue state failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The consumer failed while reading or using the stream.
    #[error("upload consumer: {0}")]
    Consumer(E),
}

#[cfg(any(test, feature = "test-utils"))]
impl<E: coven_foundation::task::CarriesBug> coven_foundation::task::CarriesBug
    for UploadReadError<E>
{
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Database(error) => error.bug(),
            Self::Consumer(error) => error.bug(),
        }
    }
}

/// An incremental SQLite BLOB reader. It never collects the waiting write.
#[derive(Clone)]
pub struct UploadBytes<'a> {
    blob: Rc<rusqlite::blob::Blob<'a>>,
    offset: usize,
    end: usize,
}

impl<'a> UploadBytes<'a> {
    pub(crate) fn new(blob: rusqlite::blob::Blob<'a>) -> Self {
        let end = blob.len();
        Self { blob: Rc::new(blob), offset: 0, end }
    }

    fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, DbError> {
        self.check_length(offset, length)?;
        let mut bytes = vec![0; length];
        self.blob.read_at_exact(&mut bytes, offset)?;
        Ok(bytes)
    }

    fn check_length(&self, offset: usize, length: usize) -> Result<(), DbError> {
        if offset > self.end || length > self.end - offset {
            return Err(DbError::Invariant {
                what: "upload reads stay inside the queued plaintext",
            });
        }
        Ok(())
    }
}

impl Iterator for UploadBytes<'_> {
    type Item = Result<Vec<u8>, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.end {
            return None;
        }
        let length = (self.end - self.offset).min(CHUNK_SIZE);
        let result = self.read(self.offset, length);
        self.offset = if result.is_ok() { self.offset + length } else { self.end };
        Some(result)
    }
}

/// Audience stream descriptors paired with their bounded plaintext readers.
#[derive(Clone)]
pub struct UploadParts<'a> {
    bytes: UploadBytes<'a>,
    headers: std::vec::IntoIter<PartHeader>,
}

impl<'a> Iterator for UploadParts<'a> {
    type Item = (PartHeader, UploadBytes<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let header = self.headers.next()?;
        let offset = self.bytes.offset;
        self.bytes.offset += header.plaintext_length as usize;
        Some((
            header,
            UploadBytes { blob: self.bytes.blob.clone(), offset, end: self.bytes.offset },
        ))
    }
}

impl UploadParts<'_> {
    /// Each part's authorship root (D11), read from the queued plaintext
    /// without consuming these streams. `hasher` gives the keyed leaf hasher
    /// of a part, by its index in header order, under its first-attempt key.
    pub fn roots<E: From<DbError> + From<coven_format::Error>>(
        &self,
        mut hasher: impl FnMut(usize, &PartHeader) -> Result<coven_crypto::AuthorshipHasher, E>,
    ) -> Result<Vec<[u8; 32]>, E> {
        let mut roots = Vec::new();
        for (index, (header, mut bytes)) in self.clone().enumerate() {
            let hasher = hasher(index, &header)?;
            let mut builder = coven_format::authorship::RootBuilder::new(&hasher);
            crate::download_stream::visit_part::<E>(
                header,
                |chunk| {
                    let plain = bytes.next().ok_or(DbError::DamagedDatabase)??;
                    if plain.len() != chunk.len() {
                        return Err(DbError::DamagedDatabase.into());
                    }
                    chunk.copy_from_slice(&plain);
                    Ok(())
                },
                |frame| {
                    let record = match &frame {
                        coven_format::dismissal::WriteFrame::Change(row) => {
                            coven_format::write::PartRecord::Change(row)
                        }
                        coven_format::dismissal::WriteFrame::Execution(execution) => {
                            coven_format::write::PartRecord::Execution(execution)
                        }
                        coven_format::dismissal::WriteFrame::Dismissal(_) => return Ok(()),
                    };
                    for leaf in coven_format::authorship::record_leaves(record) {
                        builder.push(&leaf)?;
                    }
                    Ok(())
                },
            )?;
            roots.push(builder.finish());
        }
        Ok(roots)
    }
}

/// A write waiting in `_coven_uploads`: its row, its id, and the keys its
/// first attempt fixed, when that is known.
struct QueueEntry {
    rowid: i64,
    write: WriteId,
    keys: Option<Vec<u8>>,
}

impl QueueEntry {
    /// An entry selected as `rowid,device,number` from `_coven_uploads`,
    /// then its upload keys' `sealing_keys`, or NULL for none known.
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            rowid: row.get(0)?,
            write: WriteId { device: row.get::<_, DeviceId>(1)?, number: counter(row.get(2)?)? },
            keys: row.get(3)?,
        })
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn oldest(database: &DatabaseConnection) -> Result<Option<QueueEntry>, DbError> {
    Ok(database
        .query(
            "SELECT u.rowid,u.device,u.number,k.sealing_keys FROM _coven_uploads u LEFT JOIN _coven_upload_keys k ON k.device=u.device AND k.number=u.number ORDER BY u.rowid LIMIT 1",
            [],
            QueueEntry::read,
        )?
        .pop())
}

/// The oldest waiting write and the keys its first attempt fixed, if any.
#[cfg(test)]
pub(crate) fn oldest_keys(
    database: &DatabaseConnection,
) -> Result<Option<(WriteId, Option<WriteObjectPrefix>)>, DbError> {
    let Some(entry) = oldest(database)? else {
        return Ok(None);
    };
    let keys =
        entry.keys.as_deref().map(WriteObjectPrefix::decode).transpose().map_err(DbError::from)?;
    Ok(Some((entry.write, keys)))
}

fn plaintext<'a>(
    database: &'a DatabaseConnection,
    rowid: i64,
    write: WriteId,
) -> Result<(WriteHeaderFrame, Vec<u8>, UploadBytes<'a>), DbError> {
    let mut bytes = database.upload_plaintext(rowid)?;
    let prefix = bytes.read(0, coven_format::FRAME_PREFIX_LEN)?;
    let length = encoded(coven_format::frame_length(&prefix))?;
    let header_frame = bytes.read(0, length)?;
    let header = encoded(WriteHeaderFrame::decode(&header_frame))?;
    let total =
        header.parts.iter().try_fold(length as u64, |n, p| n.checked_add(p.plaintext_length));
    if total != Some(bytes.end as u64) || header.header.position != write {
        return Err(DbError::DamagedDatabase);
    }
    bytes.offset = length;
    Ok((header, header_frame, bytes))
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn read<R, E>(
    database: &DatabaseConnection,
    consume: impl FnOnce(UploadPlaintext<'_>) -> Result<R, E>,
) -> Result<Option<R>, UploadReadError<E>> {
    let Some(entry) = oldest(database)? else {
        return Ok(None);
    };
    consume(read_plaintext(database, entry.rowid, entry.write)?)
        .map(Some)
        .map_err(UploadReadError::Consumer)
}

fn read_plaintext(
    database: &DatabaseConnection,
    rowid: i64,
    write: WriteId,
) -> Result<UploadPlaintext<'_>, DbError> {
    let (header, header_frame, bytes) = plaintext(database, rowid, write)?;
    let parts = UploadParts { bytes, headers: header.parts.clone().into_iter() };
    Ok(UploadPlaintext { header, header_frame, parts })
}

fn batch(
    database: &DatabaseConnection,
    device: DeviceId,
    through: u64,
) -> Result<Vec<QueueEntry>, DbError> {
    database.query(
        "SELECT u.rowid,u.device,u.number,k.sealing_keys FROM _coven_uploads u
        LEFT JOIN _coven_upload_keys k ON k.device=u.device AND k.number=u.number
        WHERE u.device=?1 AND u.number<=?2 ORDER BY u.number",
        params![device, through.to_be_bytes()],
        QueueEntry::read,
    )
}

/// Fix the keys of every write in `device`'s queue through `through` that
/// has none yet, keeping the keys earlier attempts fixed.
pub(crate) fn prepare_batch<E: From<DbError> + From<coven_format::Error>>(
    database: &mut DatabaseConnection,
    device: DeviceId,
    through: u64,
    mut select: impl FnMut(&WriteHeaderFrame) -> Result<WriteObjectPrefix, E>,
) -> Result<PreparedUploads, E> {
    let mut writes = Vec::new();
    let mut selected = Vec::new();
    for entry in batch(database, device, through)? {
        let keys = match entry.keys {
            Some(keys) => WriteObjectPrefix::decode(&keys).map_err(DbError::from)?,
            None => {
                let (header, _, _) = plaintext(database, entry.rowid, entry.write)?;
                let keys = select(&header)?;
                selected.push((entry.write, keys.encode()?));
                keys
            }
        };
        writes.push(PreparedUpload { rowid: entry.rowid, write: entry.write, keys });
    }
    if !selected.is_empty() {
        database.stream_transaction(|database| {
            for (write, keys) in selected {
                keep_keys(database, write, &keys)?;
            }
            Ok(())
        })?;
    }
    Ok(PreparedUploads { writes })
}

/// Fix the keys a queued write is sealed with, at its first attempt.
fn keep_keys(database: &DatabaseConnection, write: WriteId, keys: &[u8]) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_upload_keys(device,number,sealing_keys) VALUES(?1,?2,?3)",
        params![write.device, write.number.to_be_bytes(), keys],
    )?;
    Ok(())
}

/// Read each prepared write's plaintext, with the keys it seals with.
pub(crate) fn read_batch<E: From<DbError>>(
    database: &DatabaseConnection,
    prepared: PreparedUploads,
    mut consume: impl FnMut(WaitingUpload<'_>) -> Result<(), E>,
) -> Result<(), E> {
    for PreparedUpload { rowid, write, keys } in prepared.writes {
        consume(WaitingUpload { plaintext: read_plaintext(database, rowid, write)?, keys })?;
    }
    Ok(())
}

/// Queue this device's write `write`, its plaintext `record` fixed at
/// commit, to wait for its upload, with the rows its dismissals name,
/// `dismissed`.
pub(crate) fn queue<'a>(
    database: &DatabaseConnection,
    write: WriteId,
    record: &[u8],
    dismissed: impl IntoIterator<Item = &'a coven_merge::RowId>,
) -> Result<(), DbError> {
    let number = write.number.to_be_bytes();
    database.internal_execute(
        "INSERT INTO _coven_uploads(device,number,record) VALUES(?1,?2,?3)",
        params![write.device, number.as_slice(), record],
    )?;
    for row in dismissed {
        database.internal_execute(
            "INSERT INTO _coven_queued_dismissals(device,number,table_name,key,audience) VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
            params![write.device, number.as_slice(), row.table, row.key, &row.audience],
        )?;
    }
    Ok(())
}

/// The rows the dismissals of this device's queued write `write` name.
pub(crate) fn dismissed(
    database: &DatabaseConnection,
    write: WriteId,
) -> Result<Vec<coven_merge::RowId>, DbError> {
    database.query(
        "SELECT table_name,key,audience FROM _coven_queued_dismissals WHERE device=?1 AND number=?2",
        params![write.device, write.number.to_be_bytes().as_slice()],
        crate::row_queries::read_identity,
    )
}

/// The writes `device`'s landed claim `claim` carried leave the queue.
pub(crate) fn forget_claimed(
    database: &DatabaseConnection,
    device: DeviceId,
    claim: u64,
) -> Result<(), DbError> {
    database.internal_execute(
        "DELETE FROM _coven_uploads WHERE device=?1 AND number IN
         (SELECT number FROM _coven_upload_claims WHERE device=?1 AND claim=?2)",
        params![device, claim.to_be_bytes().as_slice()],
    )?;
    Ok(())
}

/// The header of this device's write `write` while it waits in the queue.
pub(crate) fn queued(
    database: &DatabaseConnection,
    write: WriteId,
) -> Result<Option<WriteHeaderFrame>, DbError> {
    let Some(rowid) = database
        .query(
            "SELECT rowid FROM _coven_uploads WHERE device=?1 AND number=?2",
            params![write.device, write.number.to_be_bytes()],
            |r| r.get::<_, i64>(0),
        )?
        .pop()
    else {
        return Ok(None);
    };
    Ok(Some(plaintext(database, rowid, write)?.0))
}

/// Read every waiting plaintext, including attempted entries.
/// The caller holds a reader transaction for a consistent queue view.
pub(crate) fn each_plaintext(
    database: &DatabaseConnection,
    mut consume: impl FnMut(WriteHeaderFrame, UploadParts<'_>) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let entries = database.query(
        "SELECT rowid,device,number,NULL FROM _coven_uploads ORDER BY rowid",
        [],
        QueueEntry::read,
    )?;
    for entry in entries {
        let UploadPlaintext { header, parts, .. } =
            read_plaintext(database, entry.rowid, entry.write)?;
        consume(header, parts)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn prepare(
    database: &mut DatabaseConnection,
    log: &crate::StoreState,
    select: impl FnOnce(
        &crate::StoreState,
        u32,
        &WriteHeaderFrame,
    ) -> Result<WriteObjectPrefix, DbError>,
) -> Result<Option<WriteId>, DbError> {
    database.stream_transaction(|database| {
        let Some(entry) = oldest(database)? else {
            return Ok(None);
        };
        if entry.keys.is_some() {
            return Ok(Some(entry.write));
        }
        let (header, _, _) = plaintext(database, entry.rowid, entry.write)?;
        let keys = select(log, database.schema_version()?, &header)?;
        if keys.part_keys.len() != header.parts.len() {
            return Err(DbError::Invariant { what: "sealing selects one key per write part" });
        }
        keep_keys(database, entry.write, &keys.encode()?)?;
        Ok(Some(entry.write))
    })
}

/// The newest of this device's waiting writes it may send now: none past a
/// raise's captured prefix (§18.5), and none from the first write a bound
/// upload holds on (§17.4). `None` with no write waiting. The caller holds
/// one committed read, so a write is never sent past a hold it was queued
/// with.
pub(crate) fn sendable_through(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Option<u64>, DbError> {
    let newest =
        db.query_row("SELECT max(number) FROM _coven_uploads WHERE device=?1", [device], |r| {
            r.get::<_, Option<Vec<u8>>>(0)
        })?;
    let Some(newest) = newest.map(counter).transpose()? else {
        return Ok(None);
    };
    let captured = crate::raise_capture::captured_through(db)?;
    let held = crate::file_queue::first_held(db)?.map(|first| first - 1);
    Ok(Some([captured, held].into_iter().flatten().fold(newest, u64::min)))
}

/// The first own queued write at or before `through` not yet confirmed
/// stored. A snapshot covering own writes through `through` is sent only
/// once this is none: capture may precede confirmation, but neither settles
/// a queue slot (§16.1).
pub(crate) fn unconfirmed(
    db: &DatabaseConnection,
    through: WriteId,
) -> Result<Option<WriteId>, DbError> {
    Ok(db
        .query(
            &format!("SELECT u.number FROM _coven_uploads u JOIN _coven_writes w ON {WRITE_DEVICE_SQL}=u.device AND w.number=u.number WHERE u.device=?1 AND u.number<=?2 AND w.slot IS NULL ORDER BY u.number LIMIT 1"),
            params![
                through.device,
                through.number.to_be_bytes().as_slice()
            ],
            |r| {
                Ok(WriteId {
                    device: through.device,
                    number: counter(r.get(0)?)?,
                })
            },
        )?
        .pop())
}

#[cfg(test)]
#[path = "upload_tests.rs"]
pub(crate) mod tests;
