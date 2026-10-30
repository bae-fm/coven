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
    /// The snapshot staged under this number on the input connection, or 0
    /// for the one snapshot a loading transaction installs at a time.
    source: i64,
    failure: RefCell<Option<DbError>>,
}

/// The source of the snapshot a loading transaction is installing.
const INSTALLING: i64 = 0;

impl<'a> SnapshotMetadata<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection) -> Self {
        Self::staged(database, INSTALLING)
    }

    /// The applied writes of the snapshot staged as `source` (§16.2).
    pub(crate) fn staged(database: &'a DatabaseConnection, source: i64) -> Self {
        Self {
            database,
            source,
            failure: RefCell::new(None),
        }
    }

    pub(crate) fn create_table(db: &DatabaseConnection) -> Result<(), DbError> {
        db.batch("CREATE TEMP TABLE _coven_snapshot_writes(source INTEGER NOT NULL,device BLOB NOT NULL,number BLOB NOT NULL,timestamp BLOB NOT NULL,read_through BLOB NOT NULL,slot BLOB NOT NULL,PRIMARY KEY(source,device,number)) WITHOUT ROWID")
    }

    /// Forget the applied writes of the snapshot staged as `source`.
    pub(crate) fn remove(db: &DatabaseConnection, source: i64) -> Result<(), DbError> {
        db.internal_execute(
            "DELETE FROM temp._coven_snapshot_writes WHERE source=?1",
            [source],
        )?;
        Ok(())
    }
    pub(crate) fn put(&self, write: &AppliedWrite) -> Result<(), DbError> {
        self.stage(write)?;
        crate::write_commit::retain_metadata(self.database, write)?;
        Ok(())
    }
    pub(crate) fn stage(&self, write: &AppliedWrite) -> Result<(), DbError> {
        self.database.internal_execute("INSERT INTO temp._coven_snapshot_writes(source,device,number,timestamp,read_through,slot) VALUES(?1,?2,?3,?4,?5,?6)", params![self.source,write.id.device.0.to_be_bytes().as_slice(),write.id.number.to_be_bytes().as_slice(),encoded(merge_fields::encode_timestamp(&write.timestamp))?,write.read_through.to_be_bytes().as_slice(),write.slot.0.to_be_bytes().as_slice()])?;
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<(), DbError> {
        match self.failure.borrow_mut().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn read(&self, id: WriteId) -> Result<Option<AppliedWrite>, DbError> {
        let row = self.database.query("SELECT timestamp,read_through,slot FROM temp._coven_snapshot_writes WHERE source=?1 AND device=?2 AND number=?3", params![self.source,id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()], |r| Ok(AppliedWrite { id, timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_,Vec<u8>>(0)?))?, read_through:counter(r.get(1)?),slot:coven_merge::Slot(counter(r.get(2)?)) }))?;
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
        self.database.for_each("SELECT device,number,slot,read_through,timestamp FROM temp._coven_snapshot_writes WHERE source=?1",[self.source],|r| {
            let id=WriteId { device:coven_foundation::id_source::DeviceId(counter(r.get(0)?)),number:counter(r.get(1)?) };
            let slot=counter(r.get(2)?);
            let through=counter(r.get(3)?);
            if slot==0 || slot>covers || through>=slot { return Err(invalid("applied write is outside snapshot coverage")); }
            let latest:Option<Vec<u8>>=self.database.query_row("SELECT max(timestamp) FROM temp._coven_snapshot_writes WHERE source=?1 AND (slot<=?2 OR (device=?3 AND number<?4))",params![self.source,through.to_be_bytes().as_slice(),id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()],|r|r.get(0))?;
            let stamp:Vec<u8>=r.get(4)?;
            if latest.is_some_and(|latest|latest>=stamp) {
                return Err(SnapshotError::Format(coven_format::Error::Merge(MergeError::CausalTimestamp(id))).into());
            }
            Ok(())
        })
    }
}

/// An applied write as `_coven_snapshot_writes` holds it, selected as
/// device, number, timestamp, read-through and slot.
pub(crate) fn applied(r: &rusqlite::Row<'_>) -> rusqlite::Result<AppliedWrite> {
    Ok(AppliedWrite {
        id: WriteId {
            device: coven_foundation::id_source::DeviceId(counter(r.get(0)?)),
            number: counter(r.get(1)?),
        },
        timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(2)?))?,
        read_through: counter(r.get(3)?),
        slot: coven_merge::Slot(counter(r.get(4)?)),
    })
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
