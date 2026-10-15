//! Install entry-dependent data and reload requirements with the replay (§9).
use crate::{
    sqlite::DatabaseConnection, write_encoding::audience_text, write_schema::WriteSchema, Boundary,
    DbError, EntryId, PendingValue,
};
use coven_foundation::id_source::CircleId;
use coven_merge::Audience;
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// The circles this device reads after a replay (§14.4), from its member's
/// view of the replayed store log.
#[derive(Debug, Default)]
pub(crate) struct CircleReads {
    /// Circles the replay makes readable again
    /// ([`crate::StoreLog::returning_circles`]).
    pub(crate) returning: BTreeSet<CircleId>,
    /// Every circle readable after the replay
    /// ([`crate::StoreLogState::readable_circles`]).
    pub(crate) readable: BTreeSet<CircleId>,
}

impl CircleReads {
    /// Only a readable audience can be reloaded; the store always is while
    /// this device syncs (§10).
    fn reloadable(&self, audience: &Audience) -> bool {
        match audience {
            Audience::Store => true,
            Audience::Circle(circle) => self.readable.contains(circle),
        }
    }
}

fn groups(boundaries: &[Boundary]) -> BTreeMap<Audience, Vec<Boundary>> {
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for boundary in boundaries {
        result
            .entry(boundary.snapshot().audience.clone())
            .or_default()
            .push(boundary.clone());
    }
    result
}

fn set_applied(db: &DatabaseConnection, entry: EntryId, applied: bool) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_boundaries SET applied=?3 WHERE entry_device=?1 AND entry_number=?2",
        params![
            entry.device.0.to_be_bytes().as_slice(),
            entry.number.to_be_bytes().as_slice(),
            applied
        ],
    )?;
    Ok(())
}

/// Rebuild every audience whose selected boundaries changed from its retained
/// inputs, in the replay's transaction. An audience without a complete retained
/// input keeps its committed rows, refuses writes and waits for a reload, as
/// does every returning circle. A circle this device no longer reads waits for
/// no reload: it has nothing to reload from, and its return requires one again.
pub(crate) fn replay<P: PendingValue>(
    db: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    previous: &[Boundary],
    deleted: &BTreeMap<CircleId, EntryId>,
    circles: &CircleReads,
    rejudged: &crate::write_landing::Rejudged,
) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
    // Positions this device passed while it could not read a returning circle
    // never show that it opened the circle's parts, and the inputs retained
    // before the absence cannot reproduce it: it reloads from storage (§14.4).
    let mut returned = BTreeSet::new();
    for circle in &circles.returning {
        let audience = Audience::Circle(*circle);
        crate::entry_inputs::forget(db, &audience, None)?;
        require_reload::<P>(db, &audience, false)?;
        returned.insert(audience);
    }
    let previous = groups(previous);
    let current = groups(&Boundary::load_selected(db)?);
    let changed: BTreeSet<_> = previous
        .keys()
        .chain(current.keys())
        .filter(|a| previous.get(*a) != current.get(*a))
        .cloned()
        .collect();
    let mut inputs = BTreeMap::new();
    let mut resets = Vec::new();
    for audience in changed.difference(&returned) {
        // An origin captured now would already hold a rejudged write's effects.
        if !rejudged.audiences.contains_key(audience) {
            crate::entry_inputs::original(db, schema, audience)?;
        }
        let selected = current.get(audience).map(Vec::as_slice).unwrap_or(&[]);
        let ids: BTreeSet<_> = selected.iter().map(Boundary::entry).collect();
        let applied = crate::entry_inputs::applied(db, audience)?;
        for entry in applied.difference(&ids) {
            set_applied(db, *entry, false)?;
        }
        let mut complete = true;
        for boundary in selected {
            let entry = boundary.entry();
            let known: bool = db.query_row(
                "SELECT included IS NOT NULL FROM _coven_boundaries WHERE entry_device=?1 AND entry_number=?2",
                params![entry.device.0.to_be_bytes().as_slice(), entry.number.to_be_bytes().as_slice()],
                |r| r.get(0),
            )?;
            complete &= known;
        }
        let input = crate::entry_inputs::selected_input(db, audience, selected)?;
        if let Some(input) = input {
            let version: u32 = db.query_row(
                "SELECT version FROM _coven_data_inputs WHERE id=?1",
                [input],
                |r| r.get(0),
            )?;
            complete &= schema.versions.contains(&version);
        }
        // An older app keeps its committed rows and old-version writes until it
        // updates and reloads (§17.1); its writes do not claim the new boundary.
        let update = selected.iter().any(|boundary| {
            matches!(boundary, Boundary::SchemaChange(version) if version.number > *schema.versions.end())
        });
        match (complete && !update, input) {
            (true, Some(input)) => {
                for boundary in selected {
                    let entry = boundary.entry();
                    set_applied(db, entry, true)?;
                    if matches!(boundary, Boundary::Reset { .. }) && !applied.contains(&entry) {
                        resets.push(entry);
                    }
                }
                inputs.insert(audience.clone(), input);
            }
            _ if circles.reloadable(audience) => require_reload::<P>(db, audience, update)?,
            _ => (),
        }
    }
    // A retired write whose window verdict changed is rebuilt from an input
    // retained before it applied (§10). Without one, the audience keeps its
    // rows, refuses writes and waits for a reload.
    for (audience, writes) in &rejudged.audiences {
        if returned.contains(audience)
            || changed.contains(audience)
            || !crate::entry_inputs::reads(db, audience)?
        {
            continue;
        }
        let selected = current.get(audience).map(Vec::as_slice).unwrap_or(&[]);
        let mut input = crate::entry_inputs::selected_input(db, audience, selected)?;
        if let Some(id) = input {
            let (version, positions): (u32, Vec<u8>) = db.query_row(
                "SELECT version,positions FROM _coven_data_inputs WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let positions = crate::write_encoding::decoded(
                coven_format::merge_fields::decode_write_positions(&positions),
            )?;
            input = input.filter(|_| {
                schema.versions.contains(&version)
                    && writes.iter().all(|write| !positions.covers(*write))
            });
        }
        match input {
            Some(input) => {
                inputs.insert(audience.clone(), input);
            }
            None if circles.reloadable(audience) => require_reload::<P>(db, audience, false)?,
            None => (),
        }
    }
    // Replay removes the need for a reload that the device can no longer
    // perform, and its pending record with it (§19.1).
    for audience in reloading_audiences(db)? {
        if !circles.reloadable(&audience) {
            clear_reload(db, &audience)?;
        }
    }
    // The entries selecting either the previous or the new data of a rebuilt
    // audience decide whether the rebuild's removals can still reverse.
    let causes = inputs
        .keys()
        .flat_map(|audience| {
            previous
                .get(audience)
                .into_iter()
                .chain(current.get(audience))
        })
        .flatten()
        .map(Boundary::entry)
        .chain(rejudged.causes.iter().copied())
        .collect();
    let affected = crate::entry_inputs::restore(db, before, schema, &inputs, deleted, causes)?;
    crate::pending::suppress(db, &resets)?;
    for audience in inputs.keys() {
        clear_reload(db, audience)?;
    }
    if !changed.is_empty() {
        crate::pending::refresh(db)?;
    }
    Ok(affected)
}

fn require_reload<P: PendingValue>(
    db: &DatabaseConnection,
    audience: &Audience,
    update: bool,
) -> Result<(), DbError> {
    let record = P::reload(audience.clone(), crate::pending::own_device(db)?);
    let subject = serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)?;
    db.internal_execute(
        "INSERT INTO _coven_data_reloads(audience,subject,update_required) VALUES(?1,?2,?3)
         ON CONFLICT(audience) DO UPDATE SET subject=excluded.subject,update_required=excluded.update_required",
        params![audience_text(audience), subject, update],
    )?;
    crate::pending::save(db, &record)
}

/// The audience no longer waits for a reload, with its pending record: its
/// selected data has committed, so writes into it may proceed, or this device
/// no longer reads it.
pub(crate) fn clear_reload(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    let own = crate::pending::own_device(db)?;
    db.pending_change(|| {
        db.internal_execute("DELETE FROM _coven_pending WHERE reporter=?1 AND subject IN (SELECT subject FROM _coven_data_reloads WHERE audience=?2)", params![own.0.to_be_bytes().as_slice(), audience_text(audience)])?;
        db.internal_execute(
            "DELETE FROM _coven_data_reloads WHERE audience=?1",
            [audience_text(audience)],
        )?;
        Ok(())
    })
}

/// Every audience waiting for a reload.
pub(crate) fn reloading_audiences(db: &DatabaseConnection) -> Result<BTreeSet<Audience>, DbError> {
    Ok(db
        .query("SELECT audience FROM _coven_data_reloads", [], |r| {
            crate::write_encoding::audience(&r.get::<_, String>(0)?)
        })?
        .into_iter()
        .collect())
}

pub(crate) fn reloading(db: &DatabaseConnection, audience: &Audience) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_data_reloads WHERE audience=?1)",
        [audience_text(audience)],
        |r| r.get(0),
    )
}

/// App writes into an audience wait for its reload (§15), except on an app too
/// old for the selected schema, whose old-version writes queue as late writes.
pub(crate) fn refuses_writes(
    db: &DatabaseConnection,
    audience: &Audience,
) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_data_reloads WHERE audience=?1 AND update_required=0)",
        [audience_text(audience)],
        |r| r.get(0),
    )
}

#[cfg(test)]
#[path = "entry_effects_tests.rs"]
pub(crate) mod tests;
