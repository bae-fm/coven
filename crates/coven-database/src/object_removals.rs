//! Objects this device put in storage for a while, then deletes: an ended
//! invite's marker, which turns away a request sent after the invite ended
//! (§13.2). Nothing references such an object; its row is only the deletion
//! still owed.

use crate::{sqlite::DatabaseConnection, DbError};

/// An object to delete from storage once `after` has passed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRemoval {
    /// The object's path in storage.
    pub path: String,
    /// Seconds since the Unix epoch after which it goes.
    pub after: u64,
}

pub(crate) fn record(db: &DatabaseConnection, removal: &ObjectRemoval) -> Result<(), DbError> {
    let after = i64::try_from(removal.after)
        .map_err(|_overflow| DbError::Invariant { what: "an object's removal time fits i64" })?;
    db.internal_execute(
        "INSERT INTO _coven_object_removals(path,after) VALUES(?1,?2)
         ON CONFLICT(path) DO UPDATE SET after=max(after,excluded.after)",
        (&removal.path, after),
    )?;
    Ok(())
}

/// The object at `path` is gone from storage: no deletion is owed.
pub(crate) fn removed(db: &DatabaseConnection, path: &str) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_object_removals WHERE path=?1", [path])?;
    Ok(())
}

#[cfg(test)]
#[path = "object_removals_tests.rs"]
pub(crate) mod tests;
