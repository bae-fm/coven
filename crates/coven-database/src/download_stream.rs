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
    /// The object's first complete publication time, from provider metadata (§10).
    pub stored_at: coven_format::value::StorageTime,
}

/// Whether sync could decrypt the corresponding part in the header.
pub enum DownloadedPartStream<R> {
    /// A complete plaintext part stream, starting at its first byte.
    Opened(R),
    /// Sync authenticated the part but the device does not have its key.
    Skipped,
}

impl<R: Read> DownloadedWriteStream<R> {
    pub(crate) fn apply<P: crate::PendingValue>(
        self,
        database: &crate::sqlite::DatabaseConnection,
        before: &crate::sqlite::DatabaseConnection,
        schema: &crate::write_schema::WriteSchema,
        files: &crate::file_write::FileWrite<'_>,
        store_log: &coven_format::value::EntryPositions,
        authenticate: impl FnOnce() -> Result<(), crate::DbError>,
    ) -> Result<crate::ApplyOutcome, crate::DbError> {
        crate::download::source_wait(database.stream_transaction(|database| {
            if crate::store_log::positions(database)? != *store_log {
                return Err(crate::DbError::StoreLogEntriesChanged);
            }
            let original_header = self.header.clone();
            original_header.encode()?;
            if self.header.parts.len() != self.parts.len() {
                return Err(SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            if crate::download::positions(database)?.covers(self.header.header.position) {
                crate::write_commit::retain_metadata(
                    database,
                    &coven_format::snapshot_rows::AppliedWrite {
                        id: self.header.header.position,
                        timestamp: self.header.header.timestamp,
                        had_read: self.header.header.had_read.clone(),
                    },
                )?;
                for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
                    let DownloadedPartStream::Opened(input) = stream else {
                        continue;
                    };
                    let audience = header.audience.clone();
                    read_part(input, header, &self.header.header, |frame| {
                        let rows = match frame {
                            WriteFrame::Change(row) => vec![row],
                            WriteFrame::Dismissal(_) => Vec::new(),
                        };
                        crate::download::retain_source_rows(
                            database,
                            &DownloadedWrite {
                                header: self.header.header.clone(),
                                parts: vec![DownloadedPart::Opened(WritePart {
                                    audience: audience.clone(),
                                    rows,
                                    dismissals: Vec::new(),
                                })],
                            },
                        )
                    })?;
                    crate::write_sources::complete(database, &self.header.header, &audience)?;
                }
                authenticate()?;
                return Ok(crate::ApplyOutcome::AlreadyApplied);
            }
            if let Some(wait) = crate::download::prerequisite(database, &self.header.header)? {
                return Ok(crate::ApplyOutcome::Waiting(wait));
            }
            for (part, stream) in self.header.parts.iter().zip(&self.parts) {
                if matches!(stream, DownloadedPartStream::Opened(_))
                    && crate::entry_effects::reloading(database, &part.audience)?
                {
                    return Ok(crate::ApplyOutcome::Waiting(crate::WriteWait::Reload(
                        part.audience.clone(),
                    )));
                }
            }
            crate::pending::clear_log(
                database,
                crate::LogObject::Write(self.header.header.position),
            )?;
            let opened: Vec<_> = self
                .header
                .parts
                .iter()
                .zip(&self.parts)
                .filter(|(_, stream)| matches!(stream, DownloadedPartStream::Opened(_)))
                .map(|(part, _)| part.audience.clone())
                .collect();
            let late = crate::write_landing::land::<P>(
                database,
                schema,
                &self.header.header,
                self.stored_at,
                &opened,
                true,
            )?;
            crate::download_stage::begin(database)?;
            let deleted = crate::store_log_tables::deleted_circles(database)?;
            for (header, stream) in self.header.parts.into_iter().zip(self.parts) {
                let DownloadedPartStream::Opened(input) = stream else {
                    continue;
                };
                let audience = header.audience.clone();
                read_part(input, header, &self.header.header, |frame| {
                    if late {
                        // Authored evidence and retained parts only: a late
                        // write has no effects or losses (§10).
                        crate::retained_writes::frame(
                            database,
                            &self.header.header,
                            &audience,
                            &frame,
                        )?;
                        if let WriteFrame::Change(row) = frame {
                            crate::write_sources::validate(database, &self.header.header, &row)?;
                            crate::write_sources::retain(database, &self.header.header, &row)?;
                            crate::write_sources::admit(
                                database,
                                &self.header.header,
                                &row.row,
                                &coven_format::snapshot_rows::ChangeVerdict::Dropped,
                            )?;
                        }
                        return Ok(());
                    }
                    crate::download_stage::frame(
                        database,
                        schema,
                        &self.header.header,
                        &audience,
                        frame,
                    )
                })?;
                crate::write_sources::complete(database, &self.header.header, &audience)?;
            }
            authenticate()?;
            crate::write_headers::retain(database, &original_header)?;
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
        }))
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
            match &frame {
                WriteFrame::Dismissal(dismissal) => dismissal.validate_past(write)?,
                WriteFrame::Change(row) => row.sources.validate_past(write)?,
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
