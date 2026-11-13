//! A circle awaiting its rebuild: it became readable below this device's
//! breaking version, so the device never held its rows at the circle's version
//! (§18.3). Its parts pass unread and writes into it are refused until the
//! rebuild commits.
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::{DbError, PendingValue, StoreState};
use coven_merge::Audience;
use std::time::SystemTime;

/// The circles `reload` names when every audience in it is a circle this
/// device must rebuild: below `breaking`, the device's newest breaking
/// version, and not deleted. None when `reload` names anything else, such as
/// a store being loaded, which reloads as it always has.
pub(crate) fn rebuilt(
    state: &StoreState,
    reload: &[Audience],
    breaking: u32,
) -> Option<Vec<Audience>> {
    (breaking > 0
        && !reload.is_empty()
        && reload.iter().all(|audience| {
            matches!(audience, Audience::Circle(_)) && state.awaits_raise(audience, breaking)
        }))
    .then(|| reload.to_vec())
}

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
    let device = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let marker = crate::write_record::rebuild_marker(db, device, now)?;
    crate::write_commit::retain_original(db, &marker.header, None)?;
    crate::write_commit::queue(db, schema, &marker, Default::default())?;
    crate::raise_capture::follow(db, marker.header.position)?;
    for circle in circles {
        crate::reload_gate::require_rebuild::<P>(db, circle, marker.header.position)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "rebuild_wait_tests.rs"]
mod tests;
