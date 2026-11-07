//! Bounded decoded records prepared independently of the application writer.
use crate::{sqlite::DatabaseConnection, DbError};
use std::cell::Cell;

/// A transaction-local catalog on the input connection, for one input
/// transaction. Its SQLite temporary storage spills to disk; each cursor
/// materializes only one bounded record.
pub(crate) struct PreparedInputs<'a> {
    db: &'a DatabaseConnection,
    next: Cell<i64>,
}
impl<'a> PreparedInputs<'a> {
    /// Create the catalog in the transaction `db` has open. A failed
    /// transaction rolls it back; `finish` drops it once its records are read.
    pub(crate) fn new(db: &'a DatabaseConnection) -> Result<Self, DbError> {
        db.batch("CREATE TEMP TABLE _coven_prepared_records(source INTEGER NOT NULL,ordinal INTEGER NOT NULL,record BLOB NOT NULL,PRIMARY KEY(source,ordinal)) WITHOUT ROWID")?;
        Ok(Self { db, next: Cell::new(0) })
    }
    /// Drop the catalog and every record prepared in it.
    pub(crate) fn finish(self) -> Result<(), DbError> {
        self.db.batch("DROP TABLE temp._coven_prepared_records")
    }
    /// The staged snapshot of the staging `staging`, whose stream carried
    /// `header`, with its write evidence authenticated, read on this
    /// transaction (D7.5). Staging checked every record but this one, and
    /// found `refusal`.
    pub(crate) fn snapshot(
        &self,
        staging: i64,
        header: coven_format::snapshot::SnapshotHeader,
        refusal: Option<(usize, DbError)>,
        authorship: &dyn crate::authorship::Authorship,
    ) -> Result<crate::snapshot_preparation::PreparedSnapshot<'a>, DbError> {
        crate::snapshot_preparation::prepare(self.db, staging, header, refusal, authorship)
    }
    pub(crate) fn records(&self) -> PreparedRecords<'a> {
        let id = self.next.get();
        self.next.set(id + 1);
        PreparedRecords { db: self.db, id, count: 0 }
    }
}

pub(crate) struct PreparedRecords<'a> {
    db: &'a DatabaseConnection,
    id: i64,
    count: i64,
}
impl PreparedRecords<'_> {
    pub(crate) fn push(&mut self, bytes: Vec<u8>) -> Result<(), DbError> {
        self.db.internal_execute(
            "INSERT INTO temp._coven_prepared_records VALUES(?1,?2,?3)",
            (self.id, self.count, bytes),
        )?;
        self.count += 1;
        Ok(())
    }
    pub(crate) fn each(
        &self,
        mut consume: impl FnMut(Vec<u8>) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        self.db.for_each(
            "SELECT record FROM temp._coven_prepared_records WHERE source=?1 ORDER BY ordinal",
            [self.id],
            |row| consume(row.get(0)?),
        )
    }
}
