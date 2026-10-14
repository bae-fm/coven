//! Snapshots stream a header, five ordered sections and an end marker (§15).
//! The caller stages records and supplies the applied write metadata to merge.

use crate::encode_frame_with;
use crate::error::{require, Error, Rule};
use crate::loss::{Loss, LossCause, LossIdentity};
use crate::snapshot_rows::*;
use crate::store_log::SnapshotId;
use crate::value::{name, positive, EntryPositions, WritePositions};
use crate::wire::{decode_frame, Decoder, Encoder, Wire};
use coven_merge::{RowId, WriteId, WriteOracle};

/// Snapshot metadata assembled from its plaintext header and sealed prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Writing device, snapshot number and audience.
    pub id: SnapshotId,
    /// The app schema version.
    pub schema_version: u32,
    /// Consumed device writes from the sealed prefix, including recorded lost writes.
    pub writes: WritePositions,
    /// Consumed store-log positions from the sealed prefix.
    pub store_log: EntryPositions,
    /// Counts: synced rows, applied writes, columns, merged rows, losses and
    /// authored changes.
    pub counts: [u64; SECTIONS],
}
/// The number of record sections (D7).
pub const SECTIONS: usize = 6;
impl Wire for [u64; SECTIONS] {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        for n in self {
            n.put(out)?;
        }
        Ok(())
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let mut counts = [0; SECTIONS];
        for count in &mut counts {
            *count = Wire::get(input)?;
        }
        Ok(counts)
    }
}
impl SnapshotHeader {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.id.number)?;
        self.writes.validate()?;
        self.store_log.validate()
    }
}

/// One bounded record in a snapshot, using the owners' logical identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotRecord {
    /// An app-visible synced row.
    Synced(SyncedRow),
    /// Applied write metadata, before any row needing it is decoded.
    Write(AppliedWrite),
    /// A synced column.
    Column(SyncedColumn),
    /// One row's generations and winning cells; losses travel separately.
    Merge(MergeRow),
    /// A displaced cell, removed row or excluded write's row.
    Loss(Loss),
    /// An original row change and its verdict, kept as source evidence.
    Authored(AuthoredChange),
}
impl SnapshotRecord {
    pub(crate) fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.section().put(out)?;
        match self {
            Self::Synced(v) => v.put(out),
            Self::Write(v) => v.put(out),
            Self::Column(v) => v.put(out),
            Self::Merge(v) => v.put(out),
            Self::Loss(v) => v.put(out),
            Self::Authored(v) => v.put(out),
        }
    }
    pub(crate) fn get(input: &mut Decoder<'_>, oracle: &impl WriteOracle) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Synced(Wire::get(input)?)),
            1 => Ok(Self::Write(Wire::get(input)?)),
            2 => Ok(Self::Column(Wire::get(input)?)),
            3 => Ok(Self::Merge(MergeRow::get(input, oracle)?)),
            4 => Ok(Self::Loss(Wire::get(input)?)),
            5 => Ok(Self::Authored(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "snapshot section",
                tag,
            }),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Synced(v) => v.validate(),
            Self::Write(v) => v.validate(),
            Self::Column(v) => {
                name(&v.table)?;
                name(&v.column)
            }
            Self::Merge(v) => v.validate(),
            Self::Loss(v) => v.validate(),
            Self::Authored(v) => v.validate(),
        }
    }
    fn section(&self) -> u8 {
        match self {
            Self::Synced(_) => 0,
            Self::Write(_) => 1,
            Self::Column(_) => 2,
            Self::Merge(_) => 3,
            Self::Loss(_) => 4,
            Self::Authored(_) => 5,
        }
    }
    fn key(&self) -> RecordKey {
        match self {
            Self::Synced(v) => RecordKey::Row(v.row.clone()),
            Self::Write(v) => RecordKey::Write(v.id),
            Self::Column(v) => RecordKey::Column(v.clone()),
            Self::Merge(v) => RecordKey::Row(v.state.row().clone()),
            Self::Loss(v) => RecordKey::Loss(v.key()),
            Self::Authored(v) => RecordKey::Change(v.id()),
        }
    }
    fn check_header(&self, h: &SnapshotHeader) -> Result<(), Error> {
        let covered = |p: WriteId| require(h.writes.covers(p), "snapshot write", Rule::Coverage);
        let audience = |r: &RowId| {
            require(
                r.audience == h.id.audience,
                "snapshot audience",
                Rule::Audience,
            )
        };
        match self {
            Self::Synced(v) => audience(&v.row),
            Self::Write(v) => {
                covered(v.id)?;
                for p in &v.had_read.0 {
                    covered(*p)?;
                }
                Ok(())
            }
            Self::Column(_) => Ok(()),
            Self::Loss(v) => {
                audience(&v.row)?;
                require(v.covered(&h.writes), "loss writes", Rule::Coverage)?;
                if let LossCause::Excluded { version, .. } = v.cause {
                    require(version <= h.schema_version, "loss cause", Rule::Coverage)?;
                }
                if let LossCause::Rules(rules) = &v.cause {
                    for rule in rules {
                        if let coven_merge::Rule::DeletedCircle(entry) = rule {
                            require(h.store_log.covers(*entry), "loss cause", Rule::Coverage)?;
                        }
                    }
                }
                Ok(())
            }
            Self::Merge(v) => {
                audience(v.state.row())?;
                for p in v.state.generations().values() {
                    covered(*p)?;
                }
                for cell in v.state.cells().values() {
                    covered(cell.write)?;
                }
                Ok(())
            }
            Self::Authored(v) => {
                audience(&v.change.row)?;
                covered(v.header.position)?;
                for source in v.change.sources.changes() {
                    covered(source.write)?;
                }
                match &v.verdict {
                    ChangeVerdict::Excluded(version) => require(
                        *version <= h.schema_version,
                        "change verdict",
                        Rule::Coverage,
                    ),
                    ChangeVerdict::Ignored(entry) => {
                        require(h.store_log.covers(*entry), "change verdict", Rule::Coverage)
                    }
                    ChangeVerdict::Dependent(sources) => {
                        for source in sources {
                            covered(source.write)?;
                        }
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum RecordKey {
    Row(RowId),
    Write(WriteId),
    Column(SyncedColumn),
    Loss((RowId, u64, LossIdentity, bool, u8, Option<WriteId>)),
    Change(crate::write_sources::ChangeId),
}
struct StreamState {
    header: SnapshotHeader,
    remaining: [u64; SECTIONS],
    previous: Option<(u8, RecordKey)>,
    ended: bool,
}
impl StreamState {
    fn new(header: SnapshotHeader) -> Self {
        Self {
            remaining: header.counts,
            header,
            previous: None,
            ended: false,
        }
    }
    fn accept(&mut self, record: &SnapshotRecord) -> Result<(), Error> {
        record.check_header(&self.header)?;
        let section = record.section();
        require(
            !self.ended && self.remaining.iter().position(|n| *n > 0) == Some(section as usize),
            "snapshot section/count",
            Rule::SnapshotSequence,
        )?;
        let key = record.key();
        if let Some((previous_section, previous_key)) = &self.previous {
            require(
                *previous_section != section || *previous_key < key,
                "snapshot record order",
                Rule::Order,
            )?;
        }
        self.remaining[section as usize] -= 1;
        self.previous = Some((section, key));
        Ok(())
    }
    fn end(&mut self) -> Result<(), Error> {
        require(
            !self.ended && self.remaining == [0; SECTIONS],
            "snapshot end",
            Rule::SnapshotSequence,
        )?;
        self.ended = true;
        Ok(())
    }
}

/// Encodes bounded frames, retaining only the header, counts and previous identity.
pub struct SnapshotEncoder(StreamState);
impl SnapshotEncoder {
    /// Metadata used to construct the sealed prefix before emitting frames.
    pub fn header(&self) -> &SnapshotHeader {
        &self.0.header
    }
    /// Start a snapshot and return its header frame.
    pub fn start(header: SnapshotHeader) -> Result<(Self, Vec<u8>), Error> {
        header.validate()?;
        let bytes = encode_frame_with(5, |out| {
            header.id.put(out)?;
            header.schema_version.put(out)?;
            header.counts.put(out)
        })?;
        Ok((Self(StreamState::new(header)), bytes))
    }
    /// Validate and encode a record. Failure leaves the cursor unchanged.
    pub fn record(&mut self, record: SnapshotRecord) -> Result<Vec<u8>, Error> {
        record.validate()?;
        let bytes = encode_frame_with(6, |out| record.put(out))?;
        self.0.accept(&record)?;
        Ok(bytes)
    }
    /// Emit the required end marker after all declared records.
    pub fn finish(&mut self) -> Result<Vec<u8>, Error> {
        let bytes = encode_frame_with(7, |_| Ok(()))?;
        self.0.end()?;
        Ok(bytes)
    }
}

/// Decodes a snapshot without retaining its rows or write history. The caller
/// stages records and commits only after `finish` succeeds. Its oracle must
/// describe the causally closed applied writes recorded by this snapshot.
/// Populate that oracle from section 1. Loss records add no oracle metadata;
/// the consumer validates active losses against merge rows. Only the header, counts and
/// previous record identities are retained between frames. The database consumer
/// checks cross-section completeness, SQL schema rules and app-row visibility;
/// merge's removal computation checks which removal rules actually hold.
pub struct SnapshotDecoder(StreamState);
impl SnapshotDecoder {
    /// Read the initial header frame using positions from the authenticated sealed prefix.
    pub fn start(
        frame: &[u8],
        prefix: &crate::sealed_snapshot::SnapshotObjectPrefix,
    ) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(frame)?;
        require(kind == 5, "snapshot header kind", Rule::Kind)?;
        let header = SnapshotHeader {
            id: Wire::get(&mut input)?,
            schema_version: Wire::get(&mut input)?,
            counts: Wire::get(&mut input)?,
            writes: prefix.writes.clone(),
            store_log: prefix.store_log.clone(),
        };
        require(
            header.id.audience == prefix.audience,
            "snapshot prefix audience",
            Rule::Audience,
        )?;
        input.finish()?;
        header.validate()?;
        Ok(Self(StreamState::new(header)))
    }
    /// Identity, schema and positions of this snapshot.
    pub fn header(&self) -> &SnapshotHeader {
        &self.0.header
    }
    /// Decode a record or end marker. Merge checks row invariants using the
    /// supplied oracle. Every error leaves counts and order unchanged.
    pub fn frame(
        &mut self,
        bytes: &[u8],
        oracle: &impl WriteOracle,
    ) -> Result<Option<SnapshotRecord>, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        if kind == 7 {
            input.finish()?;
            self.0.end()?;
            return Ok(None);
        }
        require(kind == 6, "snapshot record kind", Rule::Kind)?;
        let record = SnapshotRecord::get(&mut input, oracle)?;
        input.finish()?;
        record.validate()?;
        self.0.accept(&record)?;
        Ok(Some(record))
    }
    /// Refuse truncation, including EOF between records without an end marker.
    pub fn finish(&self) -> Result<(), Error> {
        if self.0.ended {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
