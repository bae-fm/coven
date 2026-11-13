//! Incremental write parts supplied by synchronization.

use crate::SnapshotError;
use coven_format::chunks::CHUNK_SIZE;
use coven_format::dismissal::WriteFrame;
use coven_format::write_stream::{PartDecoder, WriteHeaderFrame};
use std::io::Read;

/// A checked write header and its audience streams. The database commits only
/// after sync confirms authentication. Parts follow the header's order and count.
pub struct DownloadedWriteStream<R> {
    /// Authenticated identity, causality and plaintext stream boundaries.
    pub header: WriteHeaderFrame,
    /// Opened plaintext or an explicit unreadable audience, in header order.
    pub parts: Vec<DownloadedPartStream<R>>,
    /// The key that authenticated this original in its authored view.
    pub signer: coven_crypto::SigningKey,
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
            self.hashers.get(part).cloned().flatten().ok_or(crate::DbError::Invariant {
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
    match frame.record() {
        Some(record) => root.record(record),
        None => Ok(()),
    }
}

/// Whether sync could decrypt the corresponding part in the header.
pub enum DownloadedPartStream<R> {
    /// A complete plaintext part stream, starting at its first byte.
    Opened(R),
    /// The part is outside this installation's current audience.
    Skipped,
}

pub(crate) struct PreparedWrite<'a> {
    header: WriteHeaderFrame,
    parts: Vec<Option<PreparedPart<'a>>>,
    signer: coven_crypto::SigningKey,
    evidence: crate::HeaderEvidence,
}
pub(crate) struct PreparedPart<'a> {
    records: crate::input_preparation::PreparedRecords<'a>,
    hashes: Vec<[u8; 32]>,
}
impl<R: Read> DownloadedWriteStream<R> {
    pub(crate) fn prepare<'a>(
        self,
        inputs: &crate::input_preparation::PreparedInputs<'a>,
    ) -> Result<PreparedWrite<'a>, crate::DbError> {
        if self.header.parts.len() != self.parts.len()
            || self.authorship.evidence.roots.len() != self.parts.len()
        {
            return Err(
                SnapshotError::Inconsistent("write stream count differs from its header").into()
            );
        }
        let mut parts = Vec::new();
        for (index, (header, stream)) in self.header.parts.iter().zip(self.parts).enumerate() {
            match stream {
                DownloadedPartStream::Skipped => parts.push(None),
                DownloadedPartStream::Opened(input) => {
                    let mut records = inputs.records();
                    let mut root = self.authorship.root(index)?;
                    read_part(input, header.clone(), |frame| {
                        hash_frame(&mut root, &frame)?;
                        records.push(coven_format::merge_fields::encode_write_frame(&frame)?)
                    })?;
                    parts.push(Some(PreparedPart { records, hashes: root.finish()? }));
                }
            }
        }
        Ok(PreparedWrite {
            header: self.header,
            parts,
            signer: self.signer,
            evidence: self.authorship.evidence,
        })
    }
}
impl PreparedWrite<'_> {
    /// The write's header frame.
    pub(crate) fn header(&self) -> &WriteHeaderFrame {
        &self.header
    }
    /// The key the write was signed with.
    pub(crate) fn signer(&self) -> &coven_crypto::SigningKey {
        &self.signer
    }
    /// The audiences of parts this installation could not open.
    pub(crate) fn unopened_audiences(&self) -> impl Iterator<Item = &coven_merge::Audience> {
        self.header
            .parts
            .iter()
            .zip(&self.parts)
            .filter(|(_, part)| part.is_none())
            .map(|(header, _)| &header.audience)
    }
}

/// Stage one prepared original into the store's connection, inside its
/// containing slot's transaction. The caller materializes the complete batch
/// only after all parts authenticate. Before the write merges, or is consumed
/// refused, `reach` puts back the kept rows it reaches beside the queue
/// (§6.2); a reload passes none, having put back every kept row first.
pub(crate) fn stage_selected(
    database: &crate::sqlite::DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    write: PreparedWrite<'_>,
    slot: coven_merge::Slot,
    refused: bool,
    selected: impl Fn(&coven_merge::Audience) -> bool,
    reach: Option<&mut crate::queued_writes::Reach>,
) -> Result<(), crate::DbError> {
    let opened: Vec<_> = write
        .header
        .parts
        .iter()
        .zip(&write.parts)
        .filter(|(part, stream)| selected(&part.audience) && stream.is_some())
        .map(|(part, _)| part.audience.clone())
        .collect();
    // A write the cut-off consumes is never judged, so what it claims to
    // have read decides nothing (§14.1).
    if !refused {
        crate::write_commit::check_reads(database, &write.header.header)?;
        if let Some(wait) = crate::download::prerequisite(database, &write.header.header)? {
            return Err(crate::DbError::WriteWaiting(wait));
        }
    }
    if !refused {
        crate::merge_store::MergeStore::from_staged(database, &schema.schema).apply(
            &coven_format::write::WriteRecord {
                header: write.header.header.clone(),
                parts: vec![],
            },
        )?;
    }
    crate::forget_entries::superseded(database, write.header.header.position)?;
    crate::write_commit::retain_original(database, &write.header.header, Some(slot))?;
    let original_header = write.header.clone();
    let form = crate::write_sources::KeptForm::new(schema, original_header.header.schema_version);
    let boundaries = crate::write_boundary::WriteBoundary::judging(database)?;
    let mut dismissed = Vec::new();
    read_authenticated(database, write, |header, audience, frame| {
        if !selected(audience) {
            return Ok(());
        }
        if let WriteFrame::Dismissal(dismissal) = &frame {
            dismissed.push(dismissal.row.clone());
        }
        if refused {
            if let Some(record) = frame.record() {
                crate::write_sources::retain(database, &form, header, record)?;
                crate::write_sources::admit(
                    database,
                    header,
                    record.row(),
                    &coven_format::snapshot_rows::ChangeVerdict::Refused,
                )?;
            }
            return Ok(());
        }
        let ignored =
            crate::write_boundary::WriteBoundary::ignore(&boundaries, header, audience, Some(slot));
        crate::download_stage::frame(database, &form, header, ignored, frame)
    })?;
    if let Some(reach) = reach {
        crate::queued_writes::put_back(
            database,
            schema,
            &original_header.header,
            reach,
            dismissed,
        )?;
    }
    if refused {
        crate::forget_records::none_current(
            database,
            original_header.header.position,
            &opened,
            slot,
        )?;
    } else {
        crate::download_stage::stage(
            database,
            schema,
            &original_header.header,
            &opened,
            Some(slot),
            |audience| Ok(selected(audience)),
        )?;
    }
    crate::download_stage::release(database, &original_header.header)
}

/// Retain a prepared write's authenticated source evidence in the store's
/// connection without changing rows or admission.
#[cfg(test)]
pub(crate) fn retain_sources(
    db: &crate::sqlite::DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    write: PreparedWrite<'_>,
) -> Result<(), crate::DbError> {
    crate::download_stage::begin(db)?;
    let header = write.header.header.clone();
    let form = crate::write_sources::KeptForm::new(schema, header.schema_version);
    read_authenticated(db, write, |header, _, frame| {
        if let Some(record) = frame.record() {
            crate::write_sources::retain(db, &form, header, record)?;
        }
        Ok(())
    })?;
    crate::download_stage::release(db, &header)
}

/// Hand each opened part's prepared records to `consume`, then keep the
/// write's plaintext header, and, for another member's write, each part's
/// leaf hashes and the header evidence in the store's connection
/// (`authorship::keeps_evidence`).
fn read_authenticated(
    database: &crate::sqlite::DatabaseConnection,
    write: PreparedWrite<'_>,
    mut consume: impl FnMut(
        &coven_format::write::WriteHeader,
        &coven_merge::Audience,
        WriteFrame,
    ) -> Result<(), crate::DbError>,
) -> Result<(), crate::DbError> {
    let position = write.header.header.position;
    let evidence = crate::authorship::keeps_evidence(database, position)?;
    for (header, part) in write.header.parts.iter().zip(write.parts) {
        let Some(part) = part else {
            continue;
        };
        part.records.each(|bytes| {
            consume(&write.header.header, &header.audience, WriteFrame::decode(&bytes)?)
        })?;
        if evidence {
            crate::authorship::retain_leaves(database, position, &header.audience, &part.hashes)?;
        }
    }
    crate::write_headers::retain(database, &write.header)?;
    if evidence {
        crate::authorship::retain_header(database, position, &write.evidence)?;
    }
    Ok(())
}

/// Decode and validate one part without collecting its row frames. Every
/// consumer checks the same chunk lengths, record counts, ordering and EOF.
pub(crate) fn read_part<R: Read, E: From<SnapshotError> + From<coven_format::Error>>(
    mut input: R,
    header: coven_format::write_stream::PartHeader,
    consume: impl FnMut(WriteFrame) -> Result<(), E>,
) -> Result<(), E> {
    visit_part(
        header,
        |chunk| input.read_exact(chunk).map_err(|error| SnapshotError::Read(error).into()),
        consume,
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
