//! Bounded decoded records prepared independently of the application writer.
use crate::{sqlite::DatabaseConnection, DbError};
use std::cell::Cell;

/// A transaction-local catalog on the input connection. Its SQLite temporary
/// storage spills to disk; each cursor materializes only one bounded record.
pub(crate) struct PreparedInputs<'a> {
    db: &'a DatabaseConnection,
    next: Cell<i64>,
}
impl<'a> PreparedInputs<'a> {
    pub(crate) fn with<T>(
        db: &'a DatabaseConnection,
        consume: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        db.batch("CREATE TEMP TABLE _coven_prepared_records(source INTEGER NOT NULL,ordinal INTEGER NOT NULL,record BLOB NOT NULL,PRIMARY KEY(source,ordinal)) WITHOUT ROWID;
            CREATE TEMP TABLE _coven_prepared_evidence(source INTEGER NOT NULL,write_id BLOB NOT NULL,header BLOB NOT NULL,has_part INTEGER NOT NULL,PRIMARY KEY(source,write_id)) WITHOUT ROWID;
            CREATE TEMP TABLE _coven_prepared_openings(source INTEGER NOT NULL,write_id BLOB NOT NULL,ordinal INTEGER NOT NULL,leaf BLOB NOT NULL,PRIMARY KEY(source,write_id,ordinal)) WITHOUT ROWID")?;
        let result = consume(&Self {
            db,
            next: Cell::new(0),
        })?;
        db.batch("DROP TABLE temp._coven_prepared_records; DROP TABLE temp._coven_prepared_evidence; DROP TABLE temp._coven_prepared_openings")?;
        Ok(result)
    }
    /// Run transaction-local work on the catalog's connection.
    pub(crate) fn run<T>(&self, work: impl FnOnce(&'a DatabaseConnection) -> T) -> T {
        work(self.db)
    }
    pub(crate) fn records(&self) -> PreparedRecords<'a> {
        let id = self.next.get();
        self.next.set(id + 1);
        PreparedRecords {
            db: self.db,
            id,
            count: 0,
        }
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

impl PreparedRecords<'_> {
    pub(crate) fn keep_authorship(
        &self,
        write: coven_merge::WriteId,
        verified: crate::authorship::AuthenticatedEvidence,
    ) -> Result<(), DbError> {
        let id = coven_format::merge_fields::encode_write_id(&write)?;
        self.db.internal_execute(
            "INSERT INTO temp._coven_prepared_evidence VALUES(?1,?2,?3,?4)",
            rusqlite::params![self.id, id, verified.opened.encode()?, verified.part],
        )?;
        for (ordinal, leaf) in verified.leaves.into_iter().enumerate() {
            self.db.internal_execute(
                "INSERT INTO temp._coven_prepared_openings VALUES(?1,?2,?3,?4)",
                rusqlite::params![self.id, id, ordinal as i64, leaf.encode()?],
            )?;
        }
        Ok(())
    }
    pub(crate) fn authorship(
        &self,
        write: coven_merge::WriteId,
    ) -> Result<crate::authorship::AuthenticatedEvidence, DbError> {
        let id = coven_format::merge_fields::encode_write_id(&write)?;
        let (header, part): (Vec<u8>, bool) = self.db.query_row(
            "SELECT header,has_part FROM temp._coven_prepared_evidence WHERE source=?1 AND write_id=?2",
            rusqlite::params![self.id, id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let leaves = self.db.query("SELECT leaf FROM temp._coven_prepared_openings WHERE source=?1 AND write_id=?2 ORDER BY ordinal",
            rusqlite::params![self.id, id], |row| crate::write_encoding::decoded(coven_format::authorship::Leaf::decode(&row.get::<_,Vec<u8>>(0)?)))?;
        Ok(crate::authorship::AuthenticatedEvidence {
            opened: coven_format::write_stream::WriteHeaderFrame::decode(&header)?,
            leaves,
            part,
        })
    }
}
