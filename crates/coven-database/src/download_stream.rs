//! Incremental write parts supplied by synchronization.

use crate::{DownloadedPart, DownloadedWrite, SnapshotError};
use coven_format::chunks::CHUNK_SIZE;
use coven_format::dismissal::WriteFrame;
use coven_format::write::WritePart;
use coven_format::write_stream::{PartDecoder, WriteHeaderFrame};
use std::io::Read;

/// A checked write header and its audience streams. The database commits only
/// after sync confirms authentication. Parts follow the header's order and count.
pub struct DownloadedWriteStream<R> {
    /// Authenticated identity, causality and plaintext stream boundaries.
    pub header: WriteHeaderFrame,
    /// Opened plaintext or an explicit unreadable audience, in header order.
    pub parts: Vec<DownloadedPartStream<R>>,
}

/// Whether sync could decrypt the corresponding part in the header.
pub enum DownloadedPartStream<R> {
    /// A complete plaintext part stream, starting at its first byte.
    Opened(R),
    /// Sync authenticated the part but the device does not have its key.
    Skipped,
}

impl<R: Read> DownloadedWriteStream<R> {
    pub(crate) fn apply(
        self,
        database: &crate::sqlite::DatabaseConnection,
        before: &crate::sqlite::DatabaseConnection,
        schema: &crate::write_schema::WriteSchema,
        now: std::time::SystemTime,
        files: &crate::file_write::FileWrite<'_>,
        store_log: &coven_format::value::EntryPositions,
        authenticate: impl FnOnce() -> Result<(), crate::DbError>,
    ) -> Result<crate::ApplyOutcome, crate::DbError> {
        database.stream_transaction(|database| {
            if crate::store_log::positions(database)? != *store_log {
                return Err(crate::DbError::StoreLogEntriesChanged);
            }
            self.header.encode()?;
            if self.header.parts.len() != self.parts.len() {
                return Err(SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            if crate::download::positions(database)?.covers(self.header.header.position) {
                return Ok(crate::ApplyOutcome::AlreadyApplied);
            }
            if let Some(wait) = crate::download::prerequisite(database, now, &self.header.header)? {
                return Ok(crate::ApplyOutcome::Waiting(wait));
            }
            crate::download_stage::begin(database)?;
            let deleted = crate::store_log_tables::deleted_circles(database)?;
            for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
                let DownloadedPartStream::Opened(input) = stream else {
                    continue;
                };
                let audience = header.audience.clone();
                read_part(input, header, &self.header.header, |frame| {
                    crate::download_stage::frame(
                        database,
                        schema,
                        &self.header.header,
                        &audience,
                        frame,
                    )
                })?;
            }
            authenticate()?;
            crate::download_stage::finish(
                database,
                before,
                schema,
                &self.header.header,
                &deleted,
                files,
            )?;
            files.before_commit()?;
            Ok(crate::ApplyOutcome::Applied)
        })
    }

    pub(crate) fn read(self) -> Result<DownloadedWrite, SnapshotError> {
        if self.header.parts.len() != self.parts.len() {
            return Err(SnapshotError::Inconsistent(
                "write stream count differs from its header",
            ));
        }
        let mut parts = Vec::new();
        for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
            let input = match stream {
                DownloadedPartStream::Opened(input) => input,
                DownloadedPartStream::Skipped => {
                    parts.push(DownloadedPart::Skipped(header.audience));
                    continue;
                }
            };
            let audience = header.audience.clone();
            let mut rows = Vec::new();
            let mut dismissals = Vec::new();
            read_part(input, header, &self.header.header, |frame| {
                match frame {
                    WriteFrame::Change(row) => rows.push(row),
                    WriteFrame::Dismissal(dismissal) => dismissals.push(dismissal),
                }
                Ok::<_, SnapshotError>(())
            })?;
            parts.push(DownloadedPart::Opened(WritePart {
                audience,
                rows,
                dismissals,
            }));
        }
        Ok(DownloadedWrite {
            header: self.header.header,
            parts,
        })
    }
}

/// Decode and validate one part without collecting its row frames. Every
/// consumer checks the same chunk lengths, record counts, ordering and EOF.
pub(crate) fn read_part<R: Read, E: From<SnapshotError> + From<coven_format::Error>>(
    mut input: R,
    header: coven_format::write_stream::PartHeader,
    write: &coven_format::write::WriteHeader,
    mut consume: impl FnMut(WriteFrame) -> Result<(), E>,
) -> Result<(), E> {
    visit_part(
        header,
        |chunk| {
            input
                .read_exact(chunk)
                .map_err(|error| SnapshotError::Read(error).into())
        },
        |frame| {
            if let WriteFrame::Dismissal(dismissal) = &frame {
                dismissal.validate_past(write)?;
            }
            consume(frame)
        },
    )?;
    let mut trailing = [0];
    loop {
        match input.read(&mut trailing) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(coven_format::Error::TrailingBytes.into()),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(SnapshotError::Read(error).into()),
        }
    }
}

/// Visit a part from either an I/O stream or the upload queue's SQLite BLOB.
/// The source fills each requested chunk exactly; the decoder checks framing,
/// ordering, audience and record count before completing the part.
pub(crate) fn visit_part<E: From<coven_format::Error>>(
    header: coven_format::write_stream::PartHeader,
    mut read_chunk: impl FnMut(&mut [u8]) -> Result<(), E>,
    mut consume: impl FnMut(WriteFrame) -> Result<(), E>,
) -> Result<(), E> {
    let mut left = header.plaintext_length;
    let mut decoder = PartDecoder::new(header)?;
    let mut chunk = [0; CHUNK_SIZE];
    while left > 0 {
        let length = left.min(CHUNK_SIZE as u64) as usize;
        read_chunk(&mut chunk[..length])?;
        for frame in decoder.chunk(&chunk[..length])? {
            consume(frame)?;
        }
        left -= length as u64;
    }
    decoder.finish()?;
    Ok(())
}
