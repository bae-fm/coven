//! Fallible SQL reads behind the merge decoder's immutable metadata oracle.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{
    sqlite::DatabaseConnection,
    write_encoding::{counter, decoded, slot_counter},
    DbError,
};
use coven_merge::{MergeError, Slot, Timestamp, WriteId, WriteOracle};
use rusqlite::params;

/// A record decoded against write metadata failed: reading the metadata
/// failed, or the record failed the format's or the merge's checks. The
/// caller decides what a failed check means for the record it read.
#[derive(Debug)]
pub(crate) enum MetadataFailure {
    Database(DbError),
    Format(coven_format::Error),
}
impl MetadataFailure {
    /// The failure of a record coven stored itself.
    pub(crate) fn stored(self) -> DbError {
        match self {
            Self::Database(error) => error,
            Self::Format(error) => error.into(),
        }
    }
    /// The failure of a record a snapshot carried (§16.2).
    pub(crate) fn snapshot(self) -> DbError {
        match self {
            Self::Database(error) => error,
            Self::Format(error) => crate::SnapshotError::Format(error).into(),
        }
    }
}
impl From<DbError> for MetadataFailure {
    fn from(error: DbError) -> Self {
        Self::Database(error)
    }
}
impl From<coven_format::Error> for MetadataFailure {
    fn from(error: coven_format::Error) -> Self {
        Self::Format(error)
    }
}
impl From<MergeError> for MetadataFailure {
    fn from(error: MergeError) -> Self {
        Self::Format(coven_format::Error::Merge(error))
    }
}

/// A write coven applied, as its `_coven_writes` row holds it.
pub(crate) struct StoredWrite {
    pub(crate) ordinal: i64,
    pub(crate) timestamp: Timestamp,
    pub(crate) read_through: u64,
    pub(crate) slot: Option<Slot>,
}

/// The number of `device`'s newest write in `_coven_writes`, if it has one.
pub(crate) fn newest_number(
    db: &DatabaseConnection,
    device: coven_foundation::id_source::DeviceId,
) -> Result<Option<u64>, DbError> {
    let newest: Option<Vec<u8>> = db.query_row(
        &format!("SELECT max(number) FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1"),
        [device],
        |r| r.get(0),
    )?;
    Ok(newest.map(counter).transpose()?)
}

/// The `read_through` of `device`'s last landed write, if one landed. A
/// device numbers its writes as it commits them and they land in that order,
/// so its landed write with the greatest number landed last; its applied
/// slot never goes back, so that write read furthest (§6.3, §7.1).
pub(crate) fn last_landed_read(
    db: &DatabaseConnection,
    device: coven_foundation::id_source::DeviceId,
) -> Result<Option<u64>, DbError> {
    Ok(db
        .query(
            &format!("SELECT read_through FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND slot IS NOT NULL ORDER BY number DESC LIMIT 1"),
            [device],
            |r| counter(r.get(0)?),
        )?
        .pop())
}

/// `id`'s row in `_coven_writes`, if coven has stored it.
pub(crate) fn stored_write(
    db: &DatabaseConnection,
    id: WriteId,
) -> Result<Option<StoredWrite>, DbError> {
    let mut rows = db.query(
        &format!(
            "SELECT id,timestamp,read_through,slot FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2"
        ),
        params![id.device, id.number.to_be_bytes().as_slice()],
        |r| {
            Ok(StoredWrite {
                ordinal: r.get(0)?,
                timestamp: decoded(coven_format::merge_fields::decode_timestamp(
                    &r.get::<_, Vec<u8>>(1)?,
                ))?,
                read_through: counter(r.get(2)?)?,
                slot: r.get::<_, Option<Vec<u8>>>(3)?.map(slot_counter).transpose()?,
            })
        },
    )?;
    Ok(rows.pop())
}

/// The metadata of the writes coven applied, read from `_coven_writes`.
pub(crate) struct DatabaseMetadata<'a> {
    db: &'a DatabaseConnection,
}
impl<'a> DatabaseMetadata<'a> {
    pub(crate) fn new(db: &'a DatabaseConnection) -> Self {
        Self { db }
    }
}
impl WriteOracle for DatabaseMetadata<'_> {
    type Error = MetadataFailure;
    fn timestamp(&self, id: WriteId) -> Result<Option<Timestamp>, MetadataFailure> {
        Ok(stored_write(self.db, id)?.map(|w| w.timestamp))
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MetadataFailure> {
        let reader_write =
            stored_write(self.db, reader)?.ok_or(MergeError::MissingWrite(reader))?;
        let slot = stored_write(self.db, earlier)?.and_then(|w| w.slot);
        Ok(coven_merge::had_read(reader, reader_write.read_through, earlier, slot))
    }
}

#[cfg(test)]
#[path = "write_metadata_tests.rs"]
mod tests;
