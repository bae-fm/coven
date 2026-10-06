//! Snapshots stream a header, five ordered sections and an end marker (§15).
//! The caller stages records and supplies the applied write metadata to merge.

use crate::error::{require, Error, Rule};
use crate::snapshot_rows::*;
use crate::store_log::SnapshotId;
use crate::value::{name, positive, EntryPositions, WritePositions};
use crate::wire::{decode_frame, wire_struct, Decoder, Encoder, Wire};
use crate::{encode_frame, encode_frame_with};
use coven_merge::{RowId, WriteId, WriteOracle};

/// Snapshot identity, covered positions and counts of each ordered section.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Writing device, snapshot number and audience.
    pub id: SnapshotId,
    /// The app schema version.
    pub schema_version: u32,
    /// Consumed device write positions, including recorded lost writes.
    pub writes: WritePositions,
    /// Consumed store-log positions.
    pub store_log: EntryPositions,
    /// Counts: synced rows, applied writes, columns, merged rows, lost writes.
    pub counts: [u64; 5],
}
wire_struct!(
    SnapshotHeader,
    id,
    schema_version,
    writes,
    store_log,
    counts
);
impl Wire for [u64; 5] {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        for n in self {
            n.put(out)?;
        }
        Ok(())
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok([
            Wire::get(input)?,
            Wire::get(input)?,
            Wire::get(input)?,
            Wire::get(input)?,
            Wire::get(input)?,
        ])
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
    /// One row's complete merge state and removal rules.
    Merge(MergeRow),
    /// Header and cause of a write excluded from merge; its row records follow.
    LostWrite(LostWrite),
    /// One row of the immediately preceding excluded write.
    LostWriteRow(LostWriteRow),
}
impl SnapshotRecord {
    pub(crate) fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        let tag = if matches!(self, Self::LostWriteRow(_)) {
            5
        } else {
            self.section()
        };
        tag.put(out)?;
        match self {
            Self::Synced(v) => v.put(out),
            Self::Write(v) => v.put(out),
            Self::Column(v) => v.put(out),
            Self::Merge(v) => v.put(out),
            Self::LostWrite(v) => v.put(out),
            Self::LostWriteRow(v) => v.put(out),
        }
    }
    pub(crate) fn get(input: &mut Decoder<'_>, oracle: &impl WriteOracle) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Synced(Wire::get(input)?)),
            1 => Ok(Self::Write(Wire::get(input)?)),
            2 => Ok(Self::Column(Wire::get(input)?)),
            3 => Ok(Self::Merge(MergeRow::get(input, oracle)?)),
            4 => Ok(Self::LostWrite(Wire::get(input)?)),
            5 => Ok(Self::LostWriteRow(Wire::get(input)?)),
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
            Self::LostWrite(v) => v.validate(),
            Self::LostWriteRow(v) => v.change.validate(),
        }
    }
    fn section(&self) -> u8 {
        match self {
            Self::Synced(_) => 0,
            Self::Write(_) => 1,
            Self::Column(_) => 2,
            Self::Merge(_) => 3,
            Self::LostWrite(_) | Self::LostWriteRow(_) => 4,
        }
    }
    fn key(&self) -> RecordKey {
        match self {
            Self::Synced(v) => RecordKey::Row(v.row.clone()),
            Self::Write(v) => RecordKey::Write(v.id),
            Self::Column(v) => RecordKey::Column(v.clone()),
            Self::Merge(v) => RecordKey::Row(v.state.row().clone()),
            Self::LostWrite(v) => RecordKey::Write(v.header.position),
            Self::LostWriteRow(v) => RecordKey::Row(v.change.row.clone()),
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
            Self::LostWriteRow(v) => audience(&v.change.row),
            Self::Write(v) => {
                covered(v.id)?;
                for p in &v.had_read.0 {
                    covered(*p)?;
                }
                Ok(())
            }
            Self::Column(_) => Ok(()),
            Self::Merge(v) => {
                audience(v.state.row())?;
                for p in v.state.generations().values() {
                    covered(*p)?;
                }
                for cell in v.state.cells().values() {
                    covered(cell.write)?;
                }
                for (key, value) in v.state.lost() {
                    covered(key.write)?;
                    covered(value.replaced_by)?;
                }
                Ok(())
            }
            Self::LostWrite(v) => {
                covered(v.header.position)?;
                require(
                    match v.cause {
                        crate::snapshot_rows::LostWriteCause::SchemaChange(version) => {
                            version <= h.schema_version
                        }
                        crate::snapshot_rows::LostWriteCause::Reset(entry) => {
                            h.store_log.covers(entry)
                        }
                    },
                    "lost write cause",
                    Rule::Coverage,
                )?;
                require(
                    v.audience == h.id.audience,
                    "lost write audience",
                    Rule::Audience,
                )
            }
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum RecordKey {
    Row(RowId),
    Write(WriteId),
    Column(SyncedColumn),
}
struct StreamState {
    header: SnapshotHeader,
    remaining: [u64; 5],
    previous: Option<(u8, RecordKey)>,
    ended: bool,
    lost_rows: Option<LostRows>,
}
struct LostRows {
    remaining: u64,
    previous: Option<RowId>,
}
impl StreamState {
    fn new(header: SnapshotHeader) -> Self {
        Self {
            remaining: header.counts,
            header,
            previous: None,
            ended: false,
            lost_rows: None,
        }
    }
    fn accept(&mut self, record: &SnapshotRecord) -> Result<(), Error> {
        record.check_header(&self.header)?;
        if let SnapshotRecord::LostWriteRow(row) = record {
            let pending = self.lost_rows.as_mut().ok_or(Error::Invalid {
                field: "lost write row without header",
                rule: Rule::SnapshotSequence,
            })?;
            require(
                pending
                    .previous
                    .as_ref()
                    .is_none_or(|previous| *previous < row.change.row),
                "lost write row order",
                Rule::Order,
            )?;
            pending.remaining -= 1;
            pending.previous = Some(row.change.row.clone());
            if pending.remaining == 0 {
                self.lost_rows = None;
            }
            return Ok(());
        }
        require(
            self.lost_rows.is_none(),
            "unfinished lost write rows",
            Rule::SnapshotSequence,
        )?;
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
        if let SnapshotRecord::LostWrite(write) = record {
            self.lost_rows = Some(LostRows {
                remaining: write.row_count,
                previous: None,
            });
        }
        Ok(())
    }
    fn end(&mut self) -> Result<(), Error> {
        require(
            !self.ended && self.remaining == [0; 5] && self.lost_rows.is_none(),
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
    /// Start a snapshot and return its header frame.
    pub fn start(header: SnapshotHeader) -> Result<(Self, Vec<u8>), Error> {
        header.validate()?;
        let bytes = encode_frame(3, &header)?;
        Ok((Self(StreamState::new(header)), bytes))
    }
    /// Validate and encode a record. Failure leaves the cursor unchanged.
    pub fn record(&mut self, record: SnapshotRecord) -> Result<Vec<u8>, Error> {
        record.validate()?;
        let bytes = encode_frame_with(4, |out| record.put(out))?;
        self.0.accept(&record)?;
        Ok(bytes)
    }
    /// Emit the required end marker after all declared records.
    pub fn finish(&mut self) -> Result<Vec<u8>, Error> {
        let bytes = encode_frame_with(5, |_| Ok(()))?;
        self.0.end()?;
        Ok(bytes)
    }
}

/// Decodes a snapshot without retaining its rows or write history. The caller
/// stages records and commits only after `finish` succeeds. Its oracle must
/// describe the causally closed applied writes recorded by this snapshot.
pub struct SnapshotDecoder(StreamState);
impl SnapshotDecoder {
    /// Read the initial header frame.
    pub fn start(frame: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(frame)?;
        require(kind == 3, "snapshot header kind", Rule::Kind)?;
        let header = SnapshotHeader::get(&mut input)?;
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
        if kind == 5 {
            input.finish()?;
            self.0.end()?;
            return Ok(None);
        }
        require(kind == 4, "snapshot record kind", Rule::Kind)?;
        let record = SnapshotRecord::get(&mut input, oracle)?;
        input.finish()?;
        record.validate()?;
        self.0.accept(&record)?;
        Ok(Some(record))
    }
    /// Refuse truncation, including EOF between records without an end marker.
    pub fn finish(&self) -> Result<(), Error> {
        require(self.0.ended, "snapshot end marker", Rule::SnapshotSequence)
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
