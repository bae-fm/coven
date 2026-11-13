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
         ON CONFLICT(audience) DO UPDATE SET subject=excluded.subject,marker=NULL",
        params![audience, subject],
    )?;
    crate::pending::save(db, &record)
}

/// `audience` waits for its rebuild, which runs under the migration write
/// `marker` (§18.3): the write gate and the waiting record are one row, so
/// they are cleared together.
pub(crate) fn require_rebuild<P: PendingValue>(
    db: &DatabaseConnection,
    audience: &Audience,
    marker: coven_merge::WriteId,
) -> Result<(), DbError> {
    require_reload::<P>(db, audience)?;
    db.internal_execute(
        "UPDATE _coven_data_reloads SET marker=?2 WHERE audience=?1",
        params![audience, coven_format::merge_fields::encode_write_id(&marker)?],
    )?;
    Ok(())
}

/// A breaking migration made `marker`: every rebuild waiting runs under it
/// instead, since its run now reaches that migration's version, and the
/// marker has read through every slot applied before it (§18.4).
pub(crate) fn follow(db: &DatabaseConnection, marker: coven_merge::WriteId) -> Result<(), DbError> {
    for audience in rebuilding(db)?.into_keys() {
        db.internal_execute(
            "UPDATE _coven_data_reloads SET marker=?2 WHERE audience=?1",
            params![audience, coven_format::merge_fields::encode_write_id(&marker)?],
        )?;
    }
    Ok(())
}

/// Every audience waiting for its rebuild, with the migration write its
/// rebuild runs under.
pub(crate) fn rebuilding(
    db: &DatabaseConnection,
) -> Result<std::collections::BTreeMap<Audience, coven_merge::WriteId>, DbError> {
    db.query("SELECT audience,marker FROM _coven_data_reloads WHERE marker IS NOT NULL", [], |r| {
        Ok((
            crate::write_encoding::audience(r, 0)?,
            crate::write_encoding::decoded(coven_format::merge_fields::decode_write_id(
                &r.get::<_, Vec<u8>>(1)?,
            ))?,
        ))
    })
    .map(|rows| rows.into_iter().collect())
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
