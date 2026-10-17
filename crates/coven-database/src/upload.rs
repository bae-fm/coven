//! Original authored plaintext, with format and keys fixed by its first upload attempt.

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
    /// Format and keys fixed by the first attempt; plaintext is fixed at commit.
    pub keys: Option<WriteObjectPrefix>,
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
        self.offset = if result.is_ok() {
            self.offset + length
        } else {
            self.end
        };
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
            UploadBytes {
                blob: self.bytes.blob.clone(),
                offset,
                end: self.bytes.offset,
            },
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

struct Oldest {
    rowid: i64,
    write: WriteId,
    keys: Option<Vec<u8>>,
}

#[cfg(any(test, feature = "test-utils"))]
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

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn read<R, E>(
    database: &DatabaseConnection,
    consume: impl FnOnce(WaitingUpload<'_>) -> Result<R, E>,
) -> Result<Option<R>, UploadReadError<E>> {
    let Some(entry) = oldest(database)? else {
        return Ok(None);
    };
    consume(read_entry(database, &entry)?)
        .map(Some)
        .map_err(UploadReadError::Consumer)
}

fn read_entry<'a>(
    database: &'a DatabaseConnection,
    entry: &Oldest,
) -> Result<WaitingUpload<'a>, DbError> {
    let (header, header_frame, bytes) = plaintext(database, entry)?;
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
    Ok(WaitingUpload {
        header,
        header_frame,
        parts,
        keys,
    })
}

fn batch(
    database: &DatabaseConnection,
    device: DeviceId,
    through: u64,
) -> Result<Vec<Oldest>, DbError> {
    database.query("SELECT rowid,device,number,sealing_keys FROM _coven_uploads WHERE device=?1 AND number<=?2 ORDER BY number",
        params![device.0.to_be_bytes(), through.to_be_bytes()], |row| Ok(Oldest {rowid: row.get(0)?,
            write: WriteId {device: DeviceId(counter(row.get(1)?)), number: counter(row.get(2)?)}, keys: row.get(3)?}))
}

pub(crate) fn prepare_batch<E: From<DbError> + From<coven_format::Error>>(
    database: &DatabaseConnection,
    device: DeviceId,
    through: u64,
    mut select: impl FnMut(&WriteHeaderFrame) -> Result<WriteObjectPrefix, E>,
) -> Result<Vec<WriteHeaderFrame>, E> {
    let mut headers = Vec::new();
    let mut selected = Vec::new();
    for entry in batch(database, device, through)? {
        let (header, _, _) = plaintext(database, &entry)?;
        if entry.keys.is_none() {
            selected.push((entry.rowid, select(&header)?.encode()?));
        }
        headers.push(header);
    }
    if !selected.is_empty() {
        database.stream_transaction(|database| {
            for (row, keys) in selected {
                database.internal_execute(
                    "UPDATE _coven_uploads SET sealing_keys=?1 WHERE rowid=?2",
                    params![keys, row],
                )?;
            }
            Ok(())
        })?;
    }
    Ok(headers)
}

pub(crate) fn read_batch<E: From<DbError>>(
    database: &DatabaseConnection,
    device: DeviceId,
    through: u64,
    mut consume: impl FnMut(WaitingUpload<'_>) -> Result<(), E>,
) -> Result<(), E> {
    for entry in batch(database, device, through)? {
        consume(read_entry(database, &entry)?)?;
    }
    Ok(())
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

#[cfg(test)]
pub(crate) fn prepare(
    database: &DatabaseConnection,
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
        let (header, _, _) = plaintext(database, &entry)?;
        let keys = select(log, database.schema_version()?, &header)?;
        if keys.part_keys.len() != header.parts.len() {
            return Err(DbError::Invariant {
                what: "sealing selects one key per write part",
            });
        }
        database.internal_execute(
            "UPDATE _coven_uploads SET sealing_keys=?1 WHERE rowid=?2",
            params![keys.encode()?, entry.rowid],
        )?;
        Ok(Some(entry.write))
    })
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
            "SELECT u.number FROM _coven_uploads u JOIN _coven_writes w ON substr(w.timestamp,9,8)=u.device AND w.number=u.number WHERE u.device=?1 AND u.number<=?2 AND w.slot IS NULL ORDER BY u.number LIMIT 1",
            params![
                through.device.0.to_be_bytes().as_slice(),
                through.number.to_be_bytes().as_slice()
            ],
            |r| {
                Ok(WriteId {
                    device: through.device,
                    number: counter(r.get(0)?),
                })
            },
        )?
        .pop())
}

#[cfg(test)]
#[path = "upload_tests.rs"]
pub(crate) mod tests;
