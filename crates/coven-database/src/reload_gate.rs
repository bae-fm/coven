//! Write gates for audiences whose replacement data has not committed.
use crate::{sqlite::DatabaseConnection, DbError, PendingValue};
use coven_merge::Audience;
use rusqlite::params;
use std::collections::BTreeSet;

pub(crate) fn require_reload<P: PendingValue>(
    db: &DatabaseConnection,
    audience: &Audience,
) -> Result<(), DbError> {
    let record = P::reload(audience.clone());
    let subject = crate::pending::SubjectKey::of(record.subject())?;
    db.internal_execute(
        "INSERT INTO _coven_data_reloads(audience,subject) VALUES(?1,?2)
         ON CONFLICT(audience) DO UPDATE SET subject=excluded.subject",
        params![audience, subject],
    )?;
    crate::pending::save(db, &record)
}

/// The audience no longer waits for a reload, with its pending record: its
/// selected data has committed, so writes into it may proceed, or this device
/// no longer reads it.
pub(crate) fn clear_reload(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.pending_change(|| {
        let subject = db
            .query("SELECT subject FROM _coven_data_reloads WHERE audience=?1", [audience], |r| {
                r.get::<_, crate::pending::SubjectKey>(0)
            })?
            .pop();
        if let Some(subject) = subject {
            crate::pending::clear_key(db, &subject)?;
        }
        db.internal_execute("DELETE FROM _coven_data_reloads WHERE audience=?1", [audience])?;
        Ok(())
    })
}

/// Every audience waiting for a reload.
pub(crate) fn reloading_audiences(db: &DatabaseConnection) -> Result<BTreeSet<Audience>, DbError> {
    Ok(db
        .query("SELECT audience FROM _coven_data_reloads", [], |r| {
            crate::write_encoding::audience(r, 0)
        })?
        .into_iter()
        .collect())
}

pub(crate) fn reloading(db: &DatabaseConnection, audience: &Audience) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_data_reloads WHERE audience=?1)",
        [audience],
        |r| r.get(0),
    )
}

#[cfg(test)]
#[path = "reload_gate_tests.rs"]
pub(crate) mod tests;
