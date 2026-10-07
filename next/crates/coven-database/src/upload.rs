//! A waiting write's immutable plaintext and first sealed upload attempt.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, encoded};
use crate::DbError;
use coven_format::chunks::CHUNK_SIZE;
use coven_format::write_stream::{PartHeader, WriteHeaderFrame};
use coven_foundation::id_source::DeviceId;
use coven_merge::WriteId;
use rusqlite::params;
use std::rc::Rc;

/// The oldest waiting write, read under one committed reader transaction.
/// Sealing and transport belong to sync; these are its input streams.
pub enum WaitingUpload<'a> {
    /// No attempt has fixed this write's sealed bytes yet.
    Plaintext {
        /// Identity, causality and the audience stream boundaries.
        header: WriteHeaderFrame,
        /// The exact plaintext header frame, sealed as one chunk.
        header_frame: Vec<u8>,
        /// Each audience's record stream, in header order, cut into 64 KiB chunks.
        parts: UploadParts<'a>,
    },
    /// Every attempt must send these already fixed bytes.
    Sealed {
        /// The object path's device and number.
        write: WriteId,
        /// The original sealed object, read incrementally.
        bytes: UploadBytes<'a>,
    },
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
        if offset > self.end || length > self.end - offset {
            return Err(DbError::DamagedDatabase);
        }
        let mut bytes = vec![0; length];
        self.blob.read_at_exact(&mut bytes, offset)?;
        Ok(bytes)
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

pub(crate) enum UploadValue {
    Plaintext,
    Sealed,
}

impl UploadValue {
    pub(crate) fn column(&self) -> (&'static str, &'static str) {
        match self {
            Self::Plaintext => ("coven_uploads", "record"),
            Self::Sealed => ("coven_upload_seals", "sealed_bytes"),
        }
    }
}

struct Oldest {
    rowid: i64,
    write: WriteId,
    sealed: Option<i64>,
}

fn oldest(database: &DatabaseConnection) -> Result<Option<Oldest>, DbError> {
    Ok(database
        .query(
            "SELECT u.rowid,u.device,u.number,s.rowid FROM coven_uploads u
         LEFT JOIN coven_upload_seals s USING(device,number) ORDER BY u.rowid LIMIT 1",
            [],
            |r| {
                Ok(Oldest {
                    rowid: r.get(0)?,
                    write: WriteId {
                        device: DeviceId(counter(r.get(1)?)),
                        number: counter(r.get(2)?),
                    },
                    sealed: r.get(3)?,
                })
            },
        )?
        .pop())
}

fn plaintext<'a>(
    database: &'a DatabaseConnection,
    entry: &Oldest,
) -> Result<(WriteHeaderFrame, Vec<u8>, UploadBytes<'a>), DbError> {
    let mut bytes = database.upload_value(UploadValue::Plaintext, entry.rowid)?;
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
    let upload = match entry.sealed {
        Some(rowid) => WaitingUpload::Sealed {
            write: entry.write,
            bytes: database.upload_value(UploadValue::Sealed, rowid)?,
        },
        None => {
            let (header, header_frame, bytes) = plaintext(database, &entry)?;
            let parts = UploadParts {
                bytes,
                headers: header.parts.clone().into_iter(),
            };
            WaitingUpload::Plaintext {
                header,
                header_frame,
                parts,
            }
        }
    };
    consume(upload).map(Some).map_err(UploadReadError::Consumer)
}

pub(crate) fn keep(
    database: &DatabaseConnection,
    write: WriteId,
    bytes: Vec<u8>,
) -> Result<Vec<u8>, DbError> {
    database.transaction(|database| {
        let entry = oldest(database)?
            .filter(|e| e.write == write)
            .ok_or(DbError::UploadNotOldest { write })?;
        if let Some(rowid) = entry.sealed {
            return database.query_row(
                "SELECT sealed_bytes FROM coven_upload_seals WHERE rowid=?1",
                [rowid],
                |r| r.get(0),
            );
        }
        let (header, frame, _) = plaintext(database, &entry)?;
        let expected = check_length(database, frame.len(), &header.parts)? as u64;
        if bytes.len() as u64 != expected {
            return Err(DbError::UploadLength {
                write,
                expected,
                actual: bytes.len() as u64,
            });
        }
        database.internal_execute(
            "INSERT INTO coven_upload_seals(device,number,sealed_bytes) VALUES(?1,?2,?3)",
            params![
                write.device.0.to_be_bytes().as_slice(),
                write.number.to_be_bytes().as_slice(),
                &bytes
            ],
        )?;
        Ok(bytes)
    })
}

pub(crate) fn check_length(
    database: &DatabaseConnection,
    header_length: usize,
    parts: &[PartHeader],
) -> Result<usize, DbError> {
    let lengths: Vec<_> = parts.iter().map(|part| part.plaintext_length).collect();
    let sealed = encoded(coven_format::sealed_write::sealed_length(
        header_length,
        &lengths,
    ))?;
    let length = database.check_value_length("sealed write", sealed)?;
    // SQLite limits the complete row too. Three fields (two eight-byte keys
    // and the BLOB) need at most 24 bytes for the keys and record header.
    database.check_value_length("sealed write row", sealed + 24)?;
    Ok(length)
}

pub(crate) fn succeeded(database: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
    database.transaction(|database| {
        let exists = database.query_row(
            "SELECT EXISTS(SELECT 1 FROM coven_uploads WHERE device=?1 AND number=?2)",
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
        if entry.sealed.is_none() {
            return Err(DbError::UploadNotSealed { write });
        }
        database.internal_execute("DELETE FROM coven_uploads WHERE rowid=?1", [entry.rowid])?;
        Ok(true)
    })
}

#[cfg(test)]
#[path = "upload_tests.rs"]
mod tests;
