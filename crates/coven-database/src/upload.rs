//! A waiting write's plaintext and the keys fixed by its first upload attempt.

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

/// The oldest waiting write, read under one committed reader transaction.
/// Sealing and transport belong to sync; these are its input streams.
pub struct WaitingUpload<'a> {
    /// Identity, causality and the audience stream boundaries.
    pub header: WriteHeaderFrame,
    /// The exact plaintext header frame, sealed as one chunk.
    pub header_frame: Vec<u8>,
    /// Each audience's record stream, in header order, cut into 64 KiB chunks.
    pub parts: UploadParts<'a>,
    /// Fixed by the first attempt; absence means migrations may still convert it.
    pub keys: Option<WriteObjectPrefix>,
}

/// A queue read failed in the database or in its consumer.
#[derive(Debug, thiserror::Error)]
pub enum UploadReadError<E> {
    /// Reading the committed queue state failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The consumer failed while reading or using the stream.
    #[error("upload consumer: {0}")]
    Consumer(E),
}

/// An incremental SQLite BLOB reader. It never collects the waiting write.
pub struct UploadBytes<'a> {
    blob: Rc<rusqlite::blob::Blob<'a>>,
    offset: usize,
    end: usize,
}

impl<'a> UploadBytes<'a> {
    pub(crate) fn new(blob: rusqlite::blob::Blob<'a>) -> Self {
        let end = blob.len();
        Self {
            blob: Rc::new(blob),
            offset: 0,
            end,
        }
    }

    fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, DbError> {
        self.check_length(offset, length)?;
        let mut bytes = vec![0; length];
        self.blob.read_at_exact(&mut bytes, offset)?;
        Ok(bytes)
    }

    fn check_length(&self, offset: usize, length: usize) -> Result<(), DbError> {
        if offset > self.end || length > self.end - offset {
            return Err(DbError::DamagedDatabase);
        }
        Ok(())
    }

    pub(crate) fn read_chunk(&mut self, bytes: &mut [u8]) -> Result<(), DbError> {
        self.check_length(self.offset, bytes.len())?;
        self.blob.read_at_exact(bytes, self.offset)?;
        self.offset += bytes.len();
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
        self.offset = if result.is_ok() {
            self.offset + length
        } else {
            self.end
        };
        Some(result)
    }
}

/// Audience stream descriptors paired with their bounded plaintext readers.
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
            UploadBytes {
                blob: self.bytes.blob.clone(),
                offset,
                end: self.bytes.offset,
            },
        ))
    }
}

struct Oldest {
    rowid: i64,
    write: WriteId,
    keys: Option<Vec<u8>>,
}

fn oldest(database: &DatabaseConnection) -> Result<Option<Oldest>, DbError> {
    Ok(database
        .query(
            "SELECT rowid,device,number,sealing_keys FROM _coven_uploads ORDER BY rowid LIMIT 1",
            [],
            |r| {
                Ok(Oldest {
                    rowid: r.get(0)?,
                    write: WriteId {
                        device: DeviceId(counter(r.get(1)?)),
                        number: counter(r.get(2)?),
                    },
                    keys: r.get(3)?,
                })
            },
        )?
        .pop())
}

fn plaintext<'a>(
    database: &'a DatabaseConnection,
    entry: &Oldest,
) -> Result<(WriteHeaderFrame, Vec<u8>, UploadBytes<'a>), DbError> {
    let mut bytes = database.upload_plaintext(entry.rowid)?;
    let prefix = bytes.read(0, coven_format::FRAME_PREFIX_LEN)?;
    let length = encoded(coven_format::frame_length(&prefix))?;
    let header_frame = bytes.read(0, length)?;
    let header = encoded(WriteHeaderFrame::decode(&header_frame))?;
    let total = header
        .parts
        .iter()
        .try_fold(length as u64, |n, p| n.checked_add(p.plaintext_length));
    if total != Some(bytes.end as u64) || header.header.position != entry.write {
        return Err(DbError::DamagedDatabase);
    }
    bytes.offset = length;
    Ok((header, header_frame, bytes))
}

pub(crate) fn read<R, E>(
    database: &DatabaseConnection,
    consume: impl FnOnce(WaitingUpload<'_>) -> Result<R, E>,
) -> Result<Option<R>, UploadReadError<E>> {
    let Some(entry) = oldest(database)? else {
        return Ok(None);
    };
    let (header, header_frame, bytes) = plaintext(database, &entry)?;
    let keys = entry
        .keys
        .as_deref()
        .map(WriteObjectPrefix::decode)
        .transpose()
        .map_err(DbError::from)?;
    let parts = UploadParts {
        bytes,
        headers: header.parts.clone().into_iter(),
    };
    let upload = WaitingUpload {
        header,
        header_frame,
        parts,
        keys,
    };
    consume(upload).map(Some).map_err(UploadReadError::Consumer)
}

/// Read every waiting plaintext, including attempted entries.
/// The caller holds a reader transaction for a consistent queue view.
pub(crate) fn each_plaintext(
    database: &DatabaseConnection,
    mut consume: impl FnMut(WriteHeaderFrame, UploadParts<'_>) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let entries = database.query(
        "SELECT rowid,device,number FROM _coven_uploads ORDER BY rowid",
        [],
        |row| {
            Ok(Oldest {
                rowid: row.get(0)?,
                write: WriteId {
                    device: DeviceId(counter(row.get(1)?)),
                    number: counter(row.get(2)?),
                },
                keys: None,
            })
        },
    )?;
    for entry in entries {
        let (header, _, bytes) = plaintext(database, &entry)?;
        let parts = UploadParts {
            bytes,
            headers: header.parts.clone().into_iter(),
        };
        consume(header, parts)?;
    }
    Ok(())
}

pub(crate) fn prepare(
    database: &DatabaseConnection,
    select: impl FnOnce(&crate::StoreLog, u32, &WriteHeaderFrame) -> Result<WriteObjectPrefix, DbError>,
) -> Result<Option<WriteId>, DbError> {
    database.stream_transaction(|database| {
        let Some(entry) = oldest(database)? else {
            return Ok(None);
        };
        if entry.keys.is_some() {
            return Ok(Some(entry.write));
        }
        let (header, _, _) = plaintext(database, &entry)?;
        let log = crate::store_log_tables::read(database)?;
        let keys = select(&log, database.schema_version()?, &header)?;
        if keys.part_keys.len() != header.parts.len() {
            return Err(DbError::DamagedDatabase);
        }
        database.internal_execute(
            "UPDATE _coven_uploads SET sealing_keys=?1 WHERE rowid=?2",
            params![keys.encode()?, entry.rowid],
        )?;
        Ok(Some(entry.write))
    })
}

pub(crate) fn succeeded(database: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
    database.stream_transaction(|database| {
        let exists = database.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_uploads WHERE device=?1 AND number=?2)",
            params![
                write.device.0.to_be_bytes().as_slice(),
                write.number.to_be_bytes().as_slice()
            ],
            |r| r.get::<_, bool>(0),
        )?;
        if !exists {
            return Ok(false);
        }
        let entry = oldest(database)?
            .filter(|e| e.write == write)
            .ok_or(DbError::UploadNotOldest { write })?;
        if entry.keys.is_none() {
            return Err(DbError::UploadNotAttempted { write });
        }
        database.internal_execute("DELETE FROM _coven_uploads WHERE rowid=?1", [entry.rowid])?;
        Ok(true)
    })
}

pub(crate) fn session(
    database: &DatabaseConnection,
    write: WriteId,
) -> Result<Option<Vec<u8>>, DbError> {
    database.query_row(
        "SELECT (SELECT session FROM _coven_write_upload_sessions WHERE device=?1 AND number=?2)",
        params![
            write.device.0.to_be_bytes().as_slice(),
            write.number.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )
}

pub(crate) fn keep_session(
    database: &DatabaseConnection,
    write: WriteId,
    bytes: Vec<u8>,
) -> Result<(), DbError> {
    database.stream_transaction(|database| {
        if oldest(database)?.is_none_or(|e| e.write != write || e.keys.is_none()) {
            return Err(DbError::UploadNotOldest { write });
        }
        database.internal_execute(
            "INSERT INTO _coven_write_upload_sessions(device,number,session) VALUES(?1,?2,?3) ON CONFLICT(device,number) DO UPDATE SET session=excluded.session",
            params![
                write.device.0.to_be_bytes().as_slice(),
                write.number.to_be_bytes().as_slice(),
                bytes
            ],
        )?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "upload_tests.rs"]
pub(crate) mod tests;
