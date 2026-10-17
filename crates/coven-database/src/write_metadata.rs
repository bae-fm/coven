//! Fallible SQL reads behind the merge decoder's immutable metadata oracle.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{
    sqlite::DatabaseConnection,
    write_encoding::{counter, decoded},
    DbError,
};
use coven_merge::{MergeError, Slot, Timestamp, WriteId, WriteOracle};
use rusqlite::params;
use std::cell::RefCell;

pub(crate) struct DatabaseMetadata<'a> {
    db: &'a DatabaseConnection,
    error: RefCell<Option<DbError>>,
}
impl<'a> DatabaseMetadata<'a> {
    pub(crate) fn new(db: &'a DatabaseConnection) -> Self {
        Self {
            db,
            error: RefCell::new(None),
        }
    }
    pub(crate) fn check(&self) -> Result<(), DbError> {
        match self.error.borrow_mut().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    fn read(&self, id: WriteId) -> Option<(Timestamp, u64, Option<Slot>)> {
        match self.db.query(
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
        ) {
            Ok(mut rows) => rows.pop(),
            Err(error) => {
                *self.error.borrow_mut() = Some(error);
                None
            }
        }
    }
}
impl WriteOracle for DatabaseMetadata<'_> {
    fn timestamp(&self, id: WriteId) -> Option<Timestamp> {
        self.read(id).map(|w| w.0)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let (_, through, _) = self.read(reader).ok_or(MergeError::MissingWrite(reader))?;
        Ok(coven_merge::had_read(
            reader,
            through,
            earlier,
            self.read(earlier).and_then(|w| w.2),
        ))
    }
}
impl coven_format::snapshot::WriteSlots for DatabaseMetadata<'_> {
    fn slot(&self, id: WriteId) -> Option<Slot> {
        self.read(id).and_then(|w| w.2)
    }
}
