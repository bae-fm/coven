//! Snapshots stream a header, eleven ordered sections and an end marker (§16.1).
//! The caller stages records and supplies the applied write metadata to merge.

use crate::admin::SnapshotId;
use crate::encode_frame_with;
use crate::error::{require, Error, Rule};
use crate::kinds;
use crate::loss::{Loss, LossCause, LossIdentity};
use crate::snapshot_rows::*;
use crate::value::{name, positive, Slot};
use crate::wire::{decode_frame, decode_frame_of, Decoder, Encoder, Wire};
use coven_merge::{RowId, WriteId, WriteOracle};

/// Snapshot metadata assembled from its plaintext header and sealed prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Writing device, snapshot number and audience.
    pub id: SnapshotId,
    /// The app schema version.
    pub schema_version: u32,
    /// Last included slot from the signed sealed prefix.
    pub covers: u64,
    /// Counts: applied writes, columns, merged rows, losses, authored
    /// changes, creations, write evidence, three original-slot sections and
    /// per-device last included claims.
    pub counts: [u64; SECTIONS],
}
/// The number of record sections (D7).
pub const SECTIONS: usize = 11;

/// Each record section's place in the stream, and its count's in the
/// header (D7).
pub mod section {
    /// Applied writes.
    pub const WRITE: usize = 0;
    /// Columns.
    pub const COLUMN: usize = 1;
    /// Merged rows.
    pub const MERGE: usize = 2;
    /// Losses.
    pub const LOSS: usize = 3;
    /// Authored changes.
    pub const AUTHORED: usize = 4;
    /// Creations.
    pub const CREATION: usize = 5;
    /// Write evidence.
    pub const EVIDENCE: usize = 6;
    /// Original admin slots, the first of the three sections a history
    /// read decodes.
    pub const ADMIN: usize = 7;
    /// Original key-copy slots.
    pub const KEY_COPY: usize = 8;
    /// Original announcement slots.
    pub const ANNOUNCEMENT: usize = 9;
    /// Each device's last included claim.
    pub const CLAIM: usize = 10;
}
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
    /// The merge rows the snapshot holds.
    pub fn merge_rows(&self) -> u64 {
        self.counts[section::MERGE]
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.id.number)?;
        Ok(())
    }
}

/// The section a snapshot's frame holds a record of; none for its header and
/// end frames.
#[cfg(any(test, feature = "test-utils"))]
pub fn frame_section(frame: &[u8]) -> Option<usize> {
    (frame[0] == kinds::SNAPSHOT_RECORD).then(|| usize::from(frame[crate::FRAME_PREFIX_LEN]))
}

/// One bounded record in a snapshot, using the owners' logical identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotRecord<E = crate::authorship::WriteEvidence> {
    /// Applied write metadata, before any row needing it is decoded.
    Write(AppliedWrite),
    /// A synced column.
    Column(SyncedColumn),
    /// One row's generations and winning cells, which are its values; losses
    /// travel separately.
    Merge(MergeRow),
    /// A displaced cell, removed row or excluded write's row.
    Loss(Loss),
    /// An original row change and its verdict, kept as source evidence.
    Authored(AuthoredChange),
    /// A row's selected original creation (§9.6).
    Creation(crate::authorship::Creation),
    /// Another member's write header, roots and openings in this audience.
    Evidence(E),
    /// Original signed admin slots, including creation; store snapshots only.
    Admin(crate::floor::OriginalSlot),
    /// Original signed key-copy slots; store snapshots only.
    KeyCopy(crate::floor::OriginalSlot),
    /// Original signed announcement slots; store snapshots only.
    Announcement(crate::floor::OriginalSlot),
    /// Last claim included for each known device, including refused claims.
    Claim(LastClaim),
}

/// A device's last included claim and its complete-slot digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastClaim {
    /// The known device.
    pub device: coven_foundation::id_source::DeviceId,
    /// Last included claim across all slot kinds.
    pub number: u64,
    /// SHA-256 of that slot's complete bytes; absent exactly when number is zero.
    pub digest: Option<[u8; 32]>,
}
crate::wire::wire_struct!(LastClaim, device, number, digest);

/// Published-write locations from the snapshot's staged metadata and history.
pub trait WriteSlots: WriteOracle {
    /// Containing slot, or absence when no authenticated metadata establishes it.
    fn slot(&self, write: WriteId) -> Result<Option<Slot>, Self::Error>;
}
impl SnapshotRecord {
    /// Retain decoded records while replacing wire evidence with its checked form.
    pub fn try_map_evidence<E, X>(
        self,
        prepare: impl FnOnce(crate::authorship::WriteEvidence) -> Result<E, X>,
    ) -> Result<SnapshotRecord<E>, X> {
        Ok(match self {
            Self::Write(value) => SnapshotRecord::Write(value),
            Self::Column(value) => SnapshotRecord::Column(value),
            Self::Merge(value) => SnapshotRecord::Merge(value),
            Self::Loss(value) => SnapshotRecord::Loss(value),
            Self::Authored(value) => SnapshotRecord::Authored(value),
            Self::Creation(value) => SnapshotRecord::Creation(value),
            Self::Admin(value) => SnapshotRecord::Admin(value),
            Self::KeyCopy(value) => SnapshotRecord::KeyCopy(value),
            Self::Announcement(value) => SnapshotRecord::Announcement(value),
            Self::Claim(value) => SnapshotRecord::Claim(value),
            Self::Evidence(value) => SnapshotRecord::Evidence(prepare(value)?),
        })
    }

    pub(crate) fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        u8::try_from(self.section()).expect("eleven sections").put(out)?;
        match self {
            Self::Write(v) => v.put(out),
            Self::Column(v) => v.put(out),
            Self::Merge(v) => v.put(out),
            Self::Loss(v) => v.put(out),
            Self::Authored(v) => v.put(out),
            Self::Creation(v) => v.put(out),
            Self::Evidence(v) => v.put(out),
            Self::Admin(v) | Self::KeyCopy(v) | Self::Announcement(v) => v.put(out),
            Self::Claim(v) => v.put(out),
        }
    }
    pub(crate) fn get<O: WriteOracle>(input: &mut Decoder<'_>, oracle: &O) -> Result<Self, O::Error>
    where
        O::Error: From<Error>,
    {
        #[cfg(any(test, feature = "test-utils"))]
        coven_foundation::test_work::snapshot_record_decode();
        Self::read(input, oracle)
    }

    /// Read back a record frame that [`SnapshotDecoder::frame`] already
    /// checked, as a loader staged it. Only the record's own fields are read;
    /// its order, coverage and the stream it came from are not checked again.
    /// A merge row rebuilds its state with `oracle`, as stored rows do.
    pub fn decode_checked<O: WriteOracle>(frame: &[u8], oracle: &O) -> Result<Self, O::Error>
    where
        O::Error: From<Error>,
    {
        let mut input = decode_frame_of(frame, kinds::SNAPSHOT_RECORD, "snapshot record kind")?;
        let record = Self::read(&mut input, oracle)?;
        input.finish()?;
        Ok(record)
    }

    fn read<O: WriteOracle>(input: &mut Decoder<'_>, oracle: &O) -> Result<Self, O::Error>
    where
        O::Error: From<Error>,
    {
        let tag = u8::get(input)?;
        match usize::from(tag) {
            section::WRITE => Ok(Self::Write(Wire::get(input)?)),
            section::COLUMN => Ok(Self::Column(Wire::get(input)?)),
            section::MERGE => Ok(Self::Merge(MergeRow::get(input, oracle)?)),
            section::LOSS => Ok(Self::Loss(Wire::get(input)?)),
            section::AUTHORED => Ok(Self::Authored(Wire::get(input)?)),
            section::CREATION => Ok(Self::Creation(Wire::get(input)?)),
            section::EVIDENCE => Ok(Self::Evidence(Wire::get(input)?)),
            section::ADMIN => Ok(Self::Admin(Wire::get(input)?)),
            section::KEY_COPY => Ok(Self::KeyCopy(Wire::get(input)?)),
            section::ANNOUNCEMENT => Ok(Self::Announcement(Wire::get(input)?)),
            section::CLAIM => Ok(Self::Claim(Wire::get(input)?)),
            _ => Err(Error::UnknownTag { field: "snapshot section", tag }.into()),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Write(v) => v.validate(),
            Self::Column(v) => {
                name(&v.table)?;
                name(&v.column)
            }
            Self::Merge(v) => v.validate(),
            Self::Loss(v) => v.validate(),
            Self::Authored(v) => v.validate(),
            Self::Creation(v) => v.validate(),
            Self::Evidence(v) => v.validate(),
            Self::Admin(v) => v.validate_kind(0),
            Self::KeyCopy(v) => v.validate_kind(2),
            Self::Announcement(v) => v.validate_kind(3),
            Self::Claim(v) => {
                require((v.number == 0) == v.digest.is_none(), "last claim digest", Rule::Required)
            }
        }
    }
    /// The section the record belongs to.
    pub fn section(&self) -> usize {
        match self {
            Self::Write(_) => section::WRITE,
            Self::Column(_) => section::COLUMN,
            Self::Merge(_) => section::MERGE,
            Self::Loss(_) => section::LOSS,
            Self::Authored(_) => section::AUTHORED,
            Self::Creation(_) => section::CREATION,
            Self::Evidence(_) => section::EVIDENCE,
            Self::Admin(_) => section::ADMIN,
            Self::KeyCopy(_) => section::KEY_COPY,
            Self::Announcement(_) => section::ANNOUNCEMENT,
            Self::Claim(_) => section::CLAIM,
        }
    }
    fn key(&self) -> RecordKey {
        match self {
            Self::Write(v) => RecordKey::Write(v.id),
            Self::Column(v) => RecordKey::Column(v.clone()),
            Self::Merge(v) => RecordKey::Row(v.state.row().clone()),
            Self::Loss(v) => RecordKey::Loss(v.key()),
            Self::Authored(v) => RecordKey::Change(v.id()),
            Self::Creation(v) => RecordKey::Row(v.row.clone()),
            Self::Evidence(v) => RecordKey::Write(v.write),
            Self::Admin(v) | Self::KeyCopy(v) | Self::Announcement(v) => RecordKey::Slot(v.slot),
            Self::Claim(v) => RecordKey::Device(v.device),
        }
    }
    fn check_header(&self, h: &SnapshotHeader) -> Result<(), Error> {
        let audience =
            |r: &RowId| require(r.audience == h.id.audience, "snapshot audience", Rule::Audience);
        match self {
            Self::Write(v) => {
                require(v.slot.get() <= h.covers, "snapshot write slot", Rule::Coverage)
            }
            Self::Column(_) | Self::Evidence(_) | Self::Claim(_) => Ok(()),
            Self::Loss(v) => {
                audience(&v.row)?;
                if let LossCause::Excluded { version, .. } = v.cause {
                    require(version <= h.schema_version, "loss cause", Rule::Coverage)?;
                }
                if let LossCause::Rules(rules) = &v.cause {
                    for rule in rules {
                        if let coven_merge::Rule::DeletedCircle(entry) = rule {
                            require(entry.get() <= h.covers, "loss cause", Rule::Coverage)?;
                        }
                    }
                }
                Ok(())
            }
            Self::Merge(v) => audience(v.state.row()),
            Self::Creation(v) => audience(&v.row),
            Self::Authored(v) => {
                audience(v.change.row())?;
                require(
                    v.header.read_through <= h.covers,
                    "authored read-through",
                    Rule::Coverage,
                )?;
                match &v.verdict {
                    ChangeVerdict::Excluded(version) => {
                        require(*version <= h.schema_version, "change verdict", Rule::Coverage)
                    }
                    ChangeVerdict::Ignored(entry) => {
                        require(entry.get() <= h.covers, "change verdict", Rule::Coverage)
                    }
                    _ => Ok(()),
                }
            }
            Self::Admin(v) | Self::KeyCopy(v) | Self::Announcement(v) => require(
                h.id.audience == coven_merge::Audience::Store && v.slot.get() <= h.covers,
                "snapshot historical slot",
                Rule::Coverage,
            ),
        }
    }

    /// Check references against authenticated published-write locations. Callers
    /// populate these from section 1; unknown evidence cannot establish coverage.
    pub fn validate_coverage<S: WriteSlots>(&self, covers: u64, slots: &S) -> Result<(), S::Error>
    where
        S::Error: From<Error>,
    {
        for write in self.named_writes() {
            let covered = slots.slot(write)?.is_some_and(|slot| slot.get() <= covers);
            require(covered, "snapshot write coverage", Rule::Coverage)?;
        }
        Ok(())
    }

    /// The writes this record names, each of which the snapshot must cover.
    fn named_writes(&self) -> Vec<WriteId> {
        match self {
            Self::Column(_)
            | Self::Write(_)
            | Self::Admin(_)
            | Self::KeyCopy(_)
            | Self::Announcement(_)
            | Self::Claim(_) => Vec::new(),
            Self::Loss(v) => v.named_writes(),
            Self::Merge(v) => v
                .state
                .generations()
                .values()
                .copied()
                .chain(v.state.cells().values().map(|cell| cell.write))
                .collect(),
            Self::Creation(v) => vec![v.write, v.creator.write],
            Self::Evidence(v) => vec![v.write],
            Self::Authored(v) => {
                let verdict = match &v.verdict {
                    ChangeVerdict::Dependent(sources) | ChangeVerdict::Derived(sources) => {
                        sources.as_slice()
                    }
                    _ => &[],
                };
                std::iter::once(v.header.position)
                    .chain(v.change.sources().map(|source| source.write))
                    .chain(verdict.iter().map(|source| source.write))
                    .collect()
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
    Slot(Slot),
    Device(coven_foundation::id_source::DeviceId),
}
struct SnapshotStreamState {
    header: SnapshotHeader,
    remaining: [u64; SECTIONS],
    previous: Option<(usize, RecordKey)>,
    ended: bool,
}
impl SnapshotStreamState {
    fn new(header: SnapshotHeader) -> Self {
        Self { remaining: header.counts, header, previous: None, ended: false }
    }
    fn accept(&mut self, record: &SnapshotRecord) -> Result<(), Error> {
        record.check_header(&self.header)?;
        let section = record.section();
        require(
            !self.ended && self.remaining.iter().position(|n| *n > 0) == Some(section),
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
        self.remaining[section] -= 1;
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
pub struct SnapshotEncoder(SnapshotStreamState);
impl SnapshotEncoder {
    /// Metadata used to construct the sealed prefix before emitting frames.
    pub fn header(&self) -> &SnapshotHeader {
        &self.0.header
    }
    /// Start a snapshot and return its header frame.
    pub fn start(header: SnapshotHeader) -> Result<(Self, Vec<u8>), Error> {
        header.validate()?;
        let bytes = encode_frame_with(kinds::SNAPSHOT_HEADER, |out| {
            header.id.put(out)?;
            header.schema_version.put(out)?;
            header.counts.put(out)
        })?;
        Ok((Self(SnapshotStreamState::new(header)), bytes))
    }
    /// Validate and encode a record. Failure leaves the cursor unchanged.
    pub fn record(&mut self, record: SnapshotRecord) -> Result<Vec<u8>, Error> {
        record.validate()?;
        let bytes = encode_frame_with(kinds::SNAPSHOT_RECORD, |out| record.put(out))?;
        self.0.accept(&record)?;
        Ok(bytes)
    }
    /// Emit the required end marker after all declared records.
    pub fn finish(&mut self) -> Result<Vec<u8>, Error> {
        let bytes = encode_frame_with(kinds::SNAPSHOT_END, |_| Ok(()))?;
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
pub struct SnapshotDecoder(SnapshotStreamState);
impl SnapshotDecoder {
    /// Extract original slots and claim settlement evidence before the store's
    /// historical signing keys are known. Other sections are counted and skipped;
    /// this is not row validation. A loader must subsequently decode the entire
    /// authenticated snapshot with `frame` before installing its data.
    pub fn history_frame(&mut self, bytes: &[u8]) -> Result<Option<SnapshotRecord>, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        if kind == kinds::SNAPSHOT_END {
            input.finish()?;
            self.0.end()?;
            return Ok(None);
        }
        require(kind == kinds::SNAPSHOT_RECORD, "snapshot record kind", Rule::Kind)?;
        let tag = u8::get(&mut input)?;
        let at = usize::from(tag);
        if at < section::ADMIN {
            require(
                !self.0.ended && self.0.remaining.iter().position(|n| *n > 0) == Some(at),
                "snapshot section/count",
                Rule::SnapshotSequence,
            )?;
            self.0.remaining[at] -= 1;
            return Ok(None);
        }
        let record = match at {
            section::ADMIN => SnapshotRecord::Admin(Wire::get(&mut input)?),
            section::KEY_COPY => SnapshotRecord::KeyCopy(Wire::get(&mut input)?),
            section::ANNOUNCEMENT => SnapshotRecord::Announcement(Wire::get(&mut input)?),
            section::CLAIM => SnapshotRecord::Claim(Wire::get(&mut input)?),
            _ => return Err(Error::UnknownTag { field: "snapshot section", tag }),
        };
        input.finish()?;
        record.validate()?;
        self.0.accept(&record)?;
        Ok(Some(record))
    }

    /// Read the initial header frame using coverage from the authenticated sealed prefix.
    pub fn start(
        frame: &[u8],
        prefix: &crate::sealed_snapshot::SnapshotObjectPrefix,
    ) -> Result<Self, Error> {
        let mut input = decode_frame_of(frame, kinds::SNAPSHOT_HEADER, "snapshot header kind")?;
        let header = SnapshotHeader {
            id: Wire::get(&mut input)?,
            schema_version: Wire::get(&mut input)?,
            counts: Wire::get(&mut input)?,
            covers: prefix.covers,
        };
        require(header.id.audience == prefix.audience, "snapshot prefix audience", Rule::Audience)?;
        require(
            match prefix.purpose {
                crate::sequence::Purpose::Raise(version) => version == header.schema_version,
                _ => true,
            },
            "snapshot raise version",
            Rule::RaiseVersion,
        )?;
        input.finish()?;
        header.validate()?;
        Ok(Self(SnapshotStreamState::new(header)))
    }
    /// Identity, schema and coverage of this snapshot.
    pub fn header(&self) -> &SnapshotHeader {
        &self.0.header
    }
    /// Decode a record or end marker. Merge checks row invariants using the
    /// supplied oracle. Every error leaves counts and order unchanged.
    pub fn frame<O: WriteSlots>(
        &mut self,
        bytes: &[u8],
        oracle: &O,
    ) -> Result<Option<SnapshotRecord>, O::Error>
    where
        O::Error: From<Error>,
    {
        let (kind, mut input) = decode_frame(bytes)?;
        if kind == kinds::SNAPSHOT_END {
            input.finish()?;
            self.0.end()?;
            return Ok(None);
        }
        require(kind == kinds::SNAPSHOT_RECORD, "snapshot record kind", Rule::Kind)?;
        let record = SnapshotRecord::get(&mut input, oracle)?;
        input.finish()?;
        record.validate()?;
        record.validate_coverage(self.0.header.covers, oracle)?;
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
