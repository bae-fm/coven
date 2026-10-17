//! The decoder's oracle reads only identities carried by this snapshot.

use crate::snapshot_error::{invalid, SnapshotError};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, decoded, encoded};
use crate::DbError;
use coven_format::{merge_fields, snapshot::WriteSlots, snapshot_rows::AppliedWrite};
use coven_merge::{MergeError, Timestamp, WriteId, WriteOracle};
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
        self.database.internal_execute("INSERT INTO temp._coven_snapshot_writes(device,number,timestamp,read_through,slot) VALUES(?1,?2,?3,?4,?5)", params![write.id.device.0.to_be_bytes().as_slice(),write.id.number.to_be_bytes().as_slice(),encoded(merge_fields::encode_timestamp(&write.timestamp))?,write.read_through.to_be_bytes().as_slice(),write.slot.0.to_be_bytes().as_slice()])?;
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
        let row = self.database.query("SELECT timestamp,read_through,slot FROM temp._coven_snapshot_writes WHERE device=?1 AND number=?2", params![id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()], |r| Ok(AppliedWrite { id, timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_,Vec<u8>>(0)?))?, read_through:counter(r.get(1)?),slot:coven_merge::Slot(counter(r.get(2)?)) }))?;
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

    pub(crate) fn validate(&self, covers: u64) -> Result<(), DbError> {
        self.database.for_each("SELECT device,number,slot,read_through,timestamp FROM temp._coven_snapshot_writes",[],|r| {
            let id=WriteId { device:coven_foundation::id_source::DeviceId(counter(r.get(0)?)),number:counter(r.get(1)?) };
            let slot=counter(r.get(2)?);
            let through=counter(r.get(3)?);
            if slot==0 || slot>covers || through>=slot { return Err(invalid("applied write is outside snapshot coverage")); }
            let latest:Option<Vec<u8>>=self.database.query_row("SELECT max(timestamp) FROM temp._coven_snapshot_writes WHERE slot<=?1 OR (device=?2 AND number<?3)",params![through.to_be_bytes().as_slice(),id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()],|r|r.get(0))?;
            let stamp:Vec<u8>=r.get(4)?;
            if latest.is_some_and(|latest|latest>=stamp) {
                return Err(SnapshotError::Format(coven_format::Error::Merge(MergeError::CausalTimestamp(id))).into());
            }
            Ok(())
        })
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
        Ok(coven_merge::had_read(
            reader,
            write.read_through,
            earlier,
            self.slot(earlier),
        ))
    }
}

impl WriteSlots for SnapshotMetadata<'_> {
    fn slot(&self, id: WriteId) -> Option<coven_merge::Slot> {
        self.observed(id).map(|w| w.slot)
    }
}
