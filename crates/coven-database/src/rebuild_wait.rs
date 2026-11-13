//! A circle awaiting its rebuild: it became readable below this device's
//! breaking version, so the device never held its rows at the circle's version
//! (§18.3). Its parts pass unread and writes into it are refused until the
//! rebuild commits.
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::{DbError, PendingValue, StoreState};
use coven_merge::Audience;
use std::time::SystemTime;

/// End the wait of every circle `state` no longer lets this device read, or
/// deleted: there is nothing to rebuild.
pub(crate) fn release(db: &DatabaseConnection, state: &StoreState) -> Result<(), DbError> {
    let device = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let member = state.devices.get(&device).map(|device| device.member);
    for audience in crate::reload_gate::rebuilding(db)?.into_keys() {
        let readable = match &audience {
            Audience::Circle(circle) => {
                member.is_some_and(|member| state.reads_circle(*circle, &member))
                    && !state.deleted(&audience)
            }
            Audience::Store => true,
        };
        if !readable {
            crate::reload_gate::clear_reload(db, &audience)?;
        }
    }
    Ok(())
}

/// Start the wait of `circles` in the commit that applies the slot making
/// them readable: queue the migration write their rebuild runs under, an
/// empty marker read through the slot just applied, and gate each circle with
/// a record naming it. Queuing it moves every waiting raise capture's prefix
/// to it (§18.4).
pub(crate) fn begin<P: PendingValue>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    now: SystemTime,
    circles: &[Audience],
) -> Result<(), DbError> {
    let marker = queue_marker(db, schema, now)?;
    for circle in circles {
        crate::reload_gate::require_rebuild::<P>(db, circle, marker.header.position)?;
    }
    Ok(())
}

/// Run every waiting rebuild under a new marker, read through the slot applied
/// now, for rebuilds whose history at their marker's slot is gone (§18.3).
/// The old marker counts as an empty write.
pub(crate) fn restart(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    now: SystemTime,
) -> Result<(), DbError> {
    let marker = queue_marker(db, schema, now)?;
    crate::reload_gate::follow(db, marker.header.position)
}

fn queue_marker(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    now: SystemTime,
) -> Result<coven_format::write::WriteRecord, DbError> {
    let device = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let marker = crate::write_record::rebuild_marker(db, device, now)?;
    crate::write_commit::retain_original(db, &marker.header, None)?;
    crate::write_commit::queue(db, schema, &marker, Default::default())?;
    crate::raise_capture::follow(db, marker.header.position)?;
    Ok(marker)
}

#[cfg(test)]
#[path = "rebuild_wait_tests.rs"]
mod tests;
