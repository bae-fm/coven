//! The new person's name a join's request carried. The saved request is the
//! only other place on the device that holds it, and publishing deletes it,
//! so the store the join published keeps it, on this device only, until the
//! app has recorded the person's profile and acknowledges it (§13.2).

use crate::{sqlite::DatabaseConnection, DbError};

/// The kept name; None once acknowledged, and for a store no join published
/// or one a recovery published, whose request carries no name.
pub(crate) fn read(database: &DatabaseConnection) -> Result<Option<String>, DbError> {
    Ok(database
        .query("SELECT name FROM _coven_joined_invitee WHERE singleton=1", [], |row| row.get(0))?
        .pop())
}

/// Keep `name` with the store a join publishes. A join that publishes again,
/// resumed after it stopped before publishing, keeps the same name its code
/// carries.
pub(crate) fn keep(database: &DatabaseConnection, name: &str) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_joined_invitee(singleton,name) VALUES(1,?1)
         ON CONFLICT(singleton) DO UPDATE SET name=excluded.name",
        [name],
    )?;
    Ok(())
}

/// End the kept name: the app has recorded the person's profile, or its
/// request for one. With none kept, nothing changes.
pub(crate) fn acknowledge(database: &DatabaseConnection) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_joined_invitee", [])?;
    Ok(())
}

#[cfg(test)]
#[path = "joined_invitee_tests.rs"]
pub(crate) mod tests;
