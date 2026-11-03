//! Fallible SQL reads behind the merge decoder's immutable metadata oracle.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{
    sqlite::DatabaseConnection,
    write_encoding::{counter, decoded},
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

/// The metadata of the writes coven applied, read from `_coven_writes`.
pub(crate) struct DatabaseMetadata<'a> {
    db: &'a DatabaseConnection,
}
impl<'a> DatabaseMetadata<'a> {
    pub(crate) fn new(db: &'a DatabaseConnection) -> Self {
        Self { db }
    }
    fn read(&self, id: WriteId) -> Result<Option<(Timestamp, u64, Option<Slot>)>, DbError> {
        let mut rows = self.db.query(
            &format!(
                "SELECT timestamp,read_through,slot FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2"
            ),
            params![
                id.device.0.to_be_bytes().as_slice(),
                id.number.to_be_bytes().as_slice()
            ],
            |r| {
                Ok((
                    decoded(coven_format::merge_fields::decode_timestamp(
                        &r.get::<_, Vec<u8>>(0)?,
                    ))?,
                    counter(r.get(1)?),
                    r.get::<_, Option<Vec<u8>>>(2)?.map(|s| Slot(counter(s))),
                ))
            },
        )?;
        Ok(rows.pop())
    }
}
impl WriteOracle for DatabaseMetadata<'_> {
    type Error = MetadataFailure;
    fn timestamp(&self, id: WriteId) -> Result<Option<Timestamp>, MetadataFailure> {
        Ok(self.read(id)?.map(|w| w.0))
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MetadataFailure> {
        let (_, through, _) = self.read(reader)?.ok_or(MergeError::MissingWrite(reader))?;
        let slot = self.read(earlier)?.and_then(|w| w.2);
        Ok(coven_merge::had_read(reader, through, earlier, slot))
    }
}

#[cfg(test)]
#[path = "write_metadata_tests.rs"]
mod tests;
