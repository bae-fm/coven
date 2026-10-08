//! The decoder's oracle reads only identities carried by this snapshot.

use crate::snapshot_error::{invalid, SnapshotError};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, decoded, encoded};
use crate::DbError;
use coven_format::{merge_fields, snapshot_rows::AppliedWrite, value::WritePositions};
use coven_merge::{MergeError, Timestamp, WriteId, WriteOracle, WritePast};
use rusqlite::params;
use std::cell::RefCell;

/// Section 1 supplies causal metadata for every consumed write, including
/// writes whose rows were excluded. Losses name these writes without repeating
/// their headers or supplying additional oracle entries.
pub(crate) struct SnapshotMetadata<'a> {
    database: &'a DatabaseConnection,
    failure: RefCell<Option<DbError>>,
}

impl<'a> SnapshotMetadata<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection) -> Self {
        Self {
            database,
            failure: RefCell::new(None),
        }
    }

    pub(crate) fn put(&self, write: &AppliedWrite) -> Result<(), DbError> {
        self.database.internal_execute("INSERT INTO temp._coven_snapshot_writes(device,number,timestamp,had_read) VALUES(?1,?2,?3,?4)", params![write.id.device.0.to_be_bytes().as_slice(),write.id.number.to_be_bytes().as_slice(),encoded(merge_fields::encode_timestamp(&write.timestamp))?,encoded(merge_fields::encode_write_positions(&write.had_read))?])?;
        crate::write_commit::retain_metadata(self.database, write)?;
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<(), DbError> {
        match self.failure.borrow_mut().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn read(&self, id: WriteId) -> Result<Option<AppliedWrite>, DbError> {
        let row = self.database.query("SELECT timestamp,had_read FROM temp._coven_snapshot_writes WHERE device=?1 AND number=?2", params![id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()], |r| Ok(AppliedWrite { id, timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_,Vec<u8>>(0)?))?, had_read: decoded(merge_fields::decode_write_positions(&r.get::<_,Vec<u8>>(1)?))? }))?;
        Ok(row.into_iter().next())
    }

    fn observed(&self, id: WriteId) -> Option<AppliedWrite> {
        match self.read(id) {
            Ok(write) => write,
            Err(error) => {
                *self.failure.borrow_mut() = Some(error);
                None
            }
        }
    }

    pub(crate) fn validate(&self, positions: &WritePositions) -> Result<(), DbError> {
        let mut reached = WritePositions(Vec::new());
        self.database.for_each(
            "SELECT device,number FROM temp._coven_snapshot_writes ORDER BY device,number",
            [],
            |r| {
                let id = WriteId {
                    device: coven_foundation::id_source::DeviceId(counter(r.get(0)?)),
                    number: counter(r.get(1)?),
                };
                let previous = reached
                    .0
                    .last()
                    .filter(|p| p.device == id.device)
                    .map_or(0, |p| p.number);
                if previous.checked_add(1) != Some(id.number) {
                    return Err(invalid("applied write identities have a gap"));
                }
                let write = self
                    .read(id)?
                    .ok_or_else(|| invalid("missing applied write"))?;
                let past = write.had_read.causal_past(id);
                for earlier in past.frontier() {
                    let earlier = self
                        .read(*earlier)?
                        .ok_or_else(|| invalid("missing causal write"))?;
                    if earlier.timestamp >= write.timestamp {
                        return Err(SnapshotError::Format(coven_format::Error::Merge(
                            MergeError::CausalTimestamp(id),
                        ))
                        .into());
                    }
                    if earlier
                        .had_read
                        .0
                        .iter()
                        .any(|p| p.device != id.device && !write.had_read.covers(*p))
                    {
                        return Err(invalid("applied writes are not causally closed"));
                    }
                }
                if previous == 0 {
                    reached.0.push(id);
                } else {
                    *reached.0.last_mut().expect("previous device") = id;
                }
                Ok(())
            },
        )?;
        if reached != *positions {
            return Err(invalid("positions disagree with applied write identities"));
        }
        Ok(())
    }
}

impl WriteOracle for SnapshotMetadata<'_> {
    fn timestamp(&self, id: WriteId) -> Option<Timestamp> {
        self.observed(id).map(|w| w.timestamp)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let write = self
            .observed(reader)
            .ok_or(MergeError::MissingWrite(reader))?;
        let past = write.had_read.causal_past(reader);
        Ok(past.contains(&earlier))
    }
}
