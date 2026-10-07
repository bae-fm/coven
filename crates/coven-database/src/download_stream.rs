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
            let mut chunk = [0; CHUNK_SIZE];
            for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
                let DownloadedPartStream::Opened(mut input) = stream else {
                    continue;
                };
                let audience = header.audience.clone();
                let mut left = header.plaintext_length;
                let mut decoder = PartDecoder::new(header)?;
                while left > 0 {
                    let length = left.min(CHUNK_SIZE as u64) as usize;
                    input
                        .read_exact(&mut chunk[..length])
                        .map_err(SnapshotError::Read)?;
                    for frame in decoder.chunk(&chunk[..length])? {
                        crate::download_stage::frame(
                            database,
                            schema,
                            &self.header.header,
                            &audience,
                            frame,
                        )?;
                    }
                    left -= length as u64;
                }
                decoder.finish()?;
                loop {
                    match input.read(&mut chunk[..1]) {
                        Ok(0) => break,
                        Ok(_) => return Err(coven_format::Error::TrailingBytes.into()),
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) => return Err(SnapshotError::Read(error).into()),
                    }
                }
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
        let mut chunk = [0; CHUNK_SIZE];
        for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
            let mut input = match stream {
                DownloadedPartStream::Opened(input) => input,
                DownloadedPartStream::Skipped => {
                    parts.push(DownloadedPart::Skipped(header.audience));
                    continue;
                }
            };
            let audience = header.audience.clone();
            let mut left = header.plaintext_length;
            let mut decoder = PartDecoder::new(header)?;
            let mut rows = Vec::new();
            let mut dismissals = Vec::new();
            while left > 0 {
                let length = left.min(CHUNK_SIZE as u64) as usize;
                input
                    .read_exact(&mut chunk[..length])
                    .map_err(SnapshotError::Read)?;
                for frame in decoder.chunk(&chunk[..length])? {
                    match frame {
                        WriteFrame::Change(row) => rows.push(row),
                        WriteFrame::Dismissal(dismissal) => {
                            dismissal.validate_past(&self.header.header)?;
                            dismissals.push(dismissal);
                        }
                    }
                }
                left -= length as u64;
            }
            decoder.finish()?;
            loop {
                match input.read(&mut chunk[..1]) {
                    Ok(0) => break,
                    Ok(_) => return Err(coven_format::Error::TrailingBytes.into()),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(SnapshotError::Read(error)),
                }
            }
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
