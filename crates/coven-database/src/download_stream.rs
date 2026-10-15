//! Incremental write parts supplied by synchronization.

use crate::{DownloadedPart, DownloadedWrite, SnapshotError};
use coven_format::chunks::CHUNK_SIZE;
use coven_format::dismissal::WriteFrame;
use coven_format::write::{PartRecord, WritePart};
use coven_format::write_stream::{PartDecoder, WriteHeaderFrame};
use std::io::Read;

/// A checked write header and its audience streams. The database commits only
/// after sync confirms authentication. Parts follow the header's order and count.
pub struct DownloadedWriteStream<R> {
    /// Authenticated identity, causality and plaintext stream boundaries.
    pub header: WriteHeaderFrame,
    /// Opened plaintext or an explicit unreadable audience, in header order.
    pub parts: Vec<DownloadedPartStream<R>>,
    /// The object's first complete publication time, from provider metadata,
    /// and the signing key that authenticated it in its recorded view (§10).
    pub published: crate::Published,
    /// The signed header evidence and each opened part's keyed leaf hasher.
    pub authorship: WriteAuthorship,
}

/// A write's signed header and part roots, and the hashers that check each
/// opened part against its root (D9, D11). The database checks the roots;
/// sync checks the header signature with the rest of the object.
#[derive(Clone)]
pub struct WriteAuthorship {
    /// The cleartext prefix, sealed header, roots and header signature.
    pub evidence: crate::HeaderEvidence,
    /// Each part's keyed leaf hasher, in header order, for the opened parts.
    pub hashers: Vec<Option<coven_crypto::AuthorshipHasher>>,
}

impl WriteAuthorship {
    fn root(&self, part: usize) -> Result<crate::authorship::PartRoot, crate::DbError> {
        let hasher =
            self.hashers
                .get(part)
                .cloned()
                .flatten()
                .ok_or(crate::DbError::Invariant {
                    what: "an opened part has its authorship hasher",
                })?;
        let root =
            *self.evidence.roots.get(part).ok_or_else(|| {
                crate::snapshot_error::invalid("write evidence lacks a part's root")
            })?;
        Ok(crate::authorship::PartRoot::new(hasher, root))
    }
}

fn hash_frame(
    root: &mut crate::authorship::PartRoot,
    frame: &WriteFrame,
) -> Result<(), crate::DbError> {
    match frame {
        WriteFrame::Change(row) => root.record(PartRecord::Change(row)),
        WriteFrame::Execution(execution) => root.record(PartRecord::Execution(execution)),
        WriteFrame::Dismissal(_) => Ok(()),
    }
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
            if self.header.parts.len() != self.parts.len()
                || self.authorship.evidence.roots.len() != self.parts.len()
            {
                return Err(SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            let write = self.header.header.position;
            if crate::download::positions(database)?.covers(self.header.header.position) {
                crate::write_commit::retain_metadata(
                    database,
                    &coven_format::snapshot_rows::AppliedWrite {
                        id: self.header.header.position,
                        timestamp: self.header.header.timestamp,
                        had_read: self.header.header.had_read.clone(),
                    },
                )?;
                for (index, (header, stream)) in
                    self.header.parts.into_iter().zip(self.parts).enumerate()
                {
                    let DownloadedPartStream::Opened(input) = stream else {
                        continue;
                    };
                    let audience = header.audience.clone();
                    let mut root = self.authorship.root(index)?;
                    read_part(input, header, &self.header.header, |frame| {
                        hash_frame(&mut root, &frame)?;
                        let (rows, executions) = match frame {
                            WriteFrame::Change(row) => (vec![row], Vec::new()),
                            WriteFrame::Execution(execution) => (Vec::new(), vec![execution]),
                            WriteFrame::Dismissal(_) => (Vec::new(), Vec::new()),
                        };
                        crate::download::retain_source_rows(
                            database,
                            &DownloadedWrite {
                                header: self.header.header.clone(),
                                parts: vec![DownloadedPart::Opened(WritePart {
                                    audience: audience.clone(),
                                    rows,
                                    executions,
                                    dismissals: Vec::new(),
                                })],
                            },
                        )
                    })?;
                    root.finish(database, write, &audience)?;
                    crate::write_sources::complete(database, &self.header.header, &audience)?;
                }
                authenticate()?;
                crate::authorship::retain_header(database, write, &self.authorship.evidence)?;
                return Ok(crate::ApplyOutcome::AlreadyApplied);
            }
            let opened: Vec<_> = self
                .header
                .parts
                .iter()
                .zip(&self.parts)
                .filter(|(_, stream)| matches!(stream, DownloadedPartStream::Opened(_)))
                .map(|(part, _)| part.audience.clone())
                .collect();
            if let Some(wait) =
                crate::download::prerequisite(database, &self.header.header, &opened)?
            {
                return Ok(crate::ApplyOutcome::Waiting(wait));
            }
            for audience in &opened {
                if crate::entry_effects::reloading(database, audience)? {
                    return Ok(crate::ApplyOutcome::Waiting(crate::WriteWait::Reload(
                        audience.clone(),
                    )));
                }
            }
            crate::pending::clear_log(
                database,
                crate::LogObject::Write(self.header.header.position),
            )?;
            let late = crate::write_landing::land::<P>(
                database,
                schema,
                &self.header.header,
                self.published,
                &opened,
                true,
            )?;
            crate::download_stage::begin(database)?;
            let deleted = crate::store_log_tables::deleted_circles(database)?;
            for (index, (header, stream)) in
                self.header.parts.into_iter().zip(self.parts).enumerate()
            {
                let DownloadedPartStream::Opened(input) = stream else {
                    continue;
                };
                let audience = header.audience.clone();
                let mut root = self.authorship.root(index)?;
                read_part(input, header, &self.header.header, |frame| {
                    hash_frame(&mut root, &frame)?;
                    if late {
                        // Authored evidence and retained parts only: a late
                        // write has no effects or losses (§10).
                        crate::retained_writes::frame(
                            database,
                            &self.header.header,
                            &audience,
                            &frame,
                        )?;
                        let record = match &frame {
                            WriteFrame::Change(row) => Some(PartRecord::Change(row)),
                            WriteFrame::Execution(execution) => {
                                Some(PartRecord::Execution(execution))
                            }
                            WriteFrame::Dismissal(_) => None,
                        };
                        if let Some(record) = record {
                            crate::write_sources::validate(database, &self.header.header, record)?;
                            crate::write_sources::retain(database, &self.header.header, record)?;
                            crate::write_sources::admit(
                                database,
                                &self.header.header,
                                record.row(),
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
                root.finish(database, write, &audience)?;
                crate::write_sources::complete(database, &self.header.header, &audience)?;
            }
            authenticate()?;
            crate::write_headers::retain(database, &original_header)?;
            crate::authorship::retain_header(database, write, &self.authorship.evidence)?;
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

    /// Decode the whole write, checking each opened part against its signed
    /// root and keeping the write's evidence for later snapshots.
    pub(crate) fn read(
        self,
        database: &crate::sqlite::DatabaseConnection,
    ) -> Result<DownloadedWrite, crate::DbError> {
        if self.header.parts.len() != self.parts.len()
            || self.authorship.evidence.roots.len() != self.parts.len()
        {
            return Err(
                SnapshotError::Inconsistent("write stream count differs from its header").into(),
            );
        }
        let write = self.header.header.position;
        let mut parts = Vec::new();
        for (index, (header, stream)) in self.header.parts.into_iter().zip(self.parts).enumerate() {
            let input = match stream {
                DownloadedPartStream::Opened(input) => input,
                DownloadedPartStream::Skipped => {
                    parts.push(DownloadedPart::Skipped(header.audience));
                    continue;
                }
            };
            let audience = header.audience.clone();
            let mut root = self.authorship.root(index)?;
            let mut rows = Vec::new();
            let mut executions = Vec::new();
            let mut dismissals = Vec::new();
            read_part(input, header, &self.header.header, |frame| {
                hash_frame(&mut root, &frame)?;
                match frame {
                    WriteFrame::Change(row) => rows.push(row),
                    WriteFrame::Execution(execution) => executions.push(execution),
                    WriteFrame::Dismissal(dismissal) => dismissals.push(dismissal),
                }
                Ok::<_, crate::DbError>(())
            })
            .map_err(|error| match error {
                // The stream's bytes violate the format, like a snapshot's.
                crate::DbError::WriteFormat(error) => SnapshotError::Format(error).into(),
                error => error,
            })?;
            root.finish(database, write, &audience)?;
            parts.push(DownloadedPart::Opened(WritePart {
                audience,
                rows,
                executions,
                dismissals,
            }));
        }
        crate::authorship::retain_header(database, write, &self.authorship.evidence)?;
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
                WriteFrame::Change(row) => {
                    row.sources.validate_past(write)?;
                    row.execution.sources.validate_past(write)?;
                }
                WriteFrame::Execution(execution) => {
                    execution.execution.sources.validate_past(write)?
                }
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
