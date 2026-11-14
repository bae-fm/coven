//! Atomic application of the next shared slot and its provisional queue overlay.

use crate::download_stream::PreparedWrite;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, StoreState};
use coven_format::{admin::StoreChange, floor::OriginalSlot, sequence::SlotHeader};
use coven_merge::Slot;
use std::collections::BTreeSet;

/// An authenticated slot whose floor check completed before installation.
pub struct IncomingSlot<R> {
    /// Its exact sequence identity.
    pub slot: Slot,
    /// The authenticated device, claim, author and preceding prefix.
    pub header: SlotHeader,
    /// SHA-256 of every complete sealed slot byte, including its signature.
    pub digest: [u8; 32],
    /// Opened content and the policy decisions made against the preceding prefix.
    pub content: SlotContent<R>,
}

/// One slot kind; readable write parts are applied together.
pub enum SlotContent<R> {
    /// Original writes in increasing author write-number order.
    Writes {
        /// The complete sealed slot's length, signature included.
        size: u64,
        /// The slot's inner writes.
        writes: Vec<R>,
    },
    /// An original admin object and its checked effect, absent for a refusal.
    Admin {
        /// Exact signed evidence retained by store snapshots.
        original: OriginalSlot,
        /// The effect authorized by the preceding prefix.
        effect: Option<AdminEffect>,
    },
    /// An authenticated key-copy object, including refused copies.
    KeyCopy(OriginalSlot),
    /// An authenticated snapshot announcement. Its original is kept only when
    /// it is accepted: no rule reads a refused one (§16.1).
    Announcement {
        /// Exact signed evidence retained by store snapshots.
        original: OriginalSlot,
        /// The announcement as this device accepted it against the preceding
        /// prefix; absent for a refused one, and for one a reload stages again
        /// at or below its store snapshot's coverage, whose originals the
        /// snapshot or this device already holds.
        accepted: Option<coven_format::sequence::SnapshotAnnouncement>,
    },
}

/// Membership and required reload work installed with an authorized admin slot.
pub struct AdminEffect {
    /// The checked original change.
    pub change: StoreChange,
    /// Membership, keys and audience state after the change.
    pub state: StoreState,
    /// The activated snapshot for a raise or reset.
    pub boundary: Option<crate::Boundary>,
    /// The audience requiring replacement data: one change makes at most one.
    /// `apply_slot` records its wait without consuming the slot, or, for a
    /// circle below the device's breaking version, applies the slot and gates
    /// the circle for its rebuild; `load_snapshots` installs the data.
    pub reload: Option<coven_merge::Audience>,
    /// Provider work recorded atomically with this state.
    pub provider_access: Vec<crate::ProviderAccessChange>,
}

pub(crate) fn retain(
    db: &DatabaseConnection,
    kind: &str,
    original: &OriginalSlot,
) -> Result<(), DbError> {
    db.internal_execute("INSERT INTO _coven_slot_records(slot,kind,record) VALUES(?1,?2,?3) ON CONFLICT(slot) DO NOTHING",
        rusqlite::params![original.slot.get().to_be_bytes().as_slice(),kind,original.object])?;
    Ok(())
}

/// Forget the original slots of announcements no rule reads any more
/// (§16.8). Each is a kept announcement's: one missing means the history the
/// caller holds and these records disagree.
pub(crate) fn forget_announcements(
    db: &DatabaseConnection,
    slots: &BTreeSet<Slot>,
) -> Result<(), DbError> {
    for slot in slots {
        let deleted = db.internal_execute(
            "DELETE FROM _coven_slot_records WHERE slot=?1 AND kind='announcement'",
            [slot.get().to_be_bytes()],
        )?;
        if deleted != 1 {
            return Err(DbError::DamagedDatabase);
        }
    }
    Ok(())
}

pub(crate) fn apply<P: crate::PendingValue>(
    db: &mut DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    incoming: IncomingSlot<PreparedWrite<'_>>,
    writers: &crate::Writers,
    files: &crate::file_write::WriteFiles<'_>,
    raises: Option<&crate::migration::MigrationOperation>,
    joining: Option<&crate::joining_write::JoiningHook>,
    now: std::time::SystemTime,
    authenticate: impl FnOnce() -> Result<(), DbError>,
) -> Result<crate::ApplyOutcome, DbError> {
    // The slot that registers this device runs the app's joining write in the
    // same transaction (§13.2).
    let registering = match &incoming.content {
        SlotContent::Admin { effect: Some(effect), .. } => {
            crate::joining_write::owed(db, joining, &effect.state, schema, now, files)?
        }
        _ => None,
    };
    crate::download::source_wait(db.stream_registering(registering, |db| {
        let outcome =
            apply_in::<P>(db, before, schema, incoming, writers, files, raises, now, authenticate)?;
        let applied = matches!(outcome, crate::ApplyOutcome::Applied);
        Ok((outcome, applied))
    }))
}

fn apply_in<P: crate::PendingValue>(
    db: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    incoming: IncomingSlot<PreparedWrite<'_>>,
    writers: &crate::Writers,
    files: &crate::file_write::WriteFiles<'_>,
    raises: Option<&crate::migration::MigrationOperation>,
    now: std::time::SystemTime,
    authenticate: impl FnOnce() -> Result<(), DbError>,
) -> Result<crate::ApplyOutcome, DbError> {
    {
        let files = &crate::file_write::FileWrite::new(db, files);
        // Applying a slot runs no migration and no DDL.
        let _constants = db.keep_constants();
        let applied = crate::sequence::applied(db)?;
        if incoming.slot.get() <= applied {
            return Ok(crate::ApplyOutcome::AlreadyApplied);
        }
        if applied.checked_add(1) != Some(incoming.slot.get()) || incoming.header.after > applied {
            return Err(DbError::SequenceGap { applied, received: incoming.slot });
        }
        // Circles that became readable below this device's breaking version
        // don't reload: the slot applies, and each waits for its rebuild.
        let mut rebuilding = Vec::new();
        if let SlotContent::Admin { effect: Some(effect), .. } = &incoming.content {
            if let Some(audience) = &effect.reload {
                if effect.state.rebuilt_by(audience, crate::raise_capture::breaking_version(db)?) {
                    // The circle doesn't hold the slot back: it applies with
                    // the circle gated (§18.3).
                    rebuilding.push(audience.clone());
                } else {
                    authenticate()?;
                    crate::reload_gate::require_reload::<P>(db, audience)?;
                    return Ok(crate::ApplyOutcome::Waiting(crate::WriteWait::Reload(
                        audience.clone(),
                    )));
                }
            }
        }
        crate::download_stage::begin(db)?;
        // An admin slot with an effect changes membership, cut-offs or
        // boundaries, and with them how every queued write applies: every
        // kept row is put back and the whole queue folds again. Any other
        // slot puts back only the kept rows its writes reach (§6.2).
        let whole = matches!(&incoming.content, SlotContent::Admin { effect: Some(_), .. });
        let mut touched =
            if whole { crate::published::restore(db, schema)? } else { BTreeSet::new() };
        let mut reach = crate::queued_writes::Reach::new();
        crate::pending::clear_slot(db, incoming.slot)?;
        match incoming.content {
            SlotContent::Writes { size, writes } => stage_writes::<P>(
                db,
                schema,
                incoming.slot,
                &incoming.header,
                size,
                writes,
                Staging::Next(&mut reach),
            )?,
            SlotContent::Admin { original, effect } => {
                retain(db, "admin", &original)?;
                if let Some(effect) = effect {
                    let old = crate::store_state_tables::deleted_circles(db)?;
                    let created =
                        !crate::store_state_tables::created(db)? && effect.state.store.is_some();
                    crate::sequence::cutoff(db, incoming.slot, &effect.change)?;
                    crate::store_state_tables::replace(db, &effect.state)?;
                    crate::rebuild_wait::release(db, &effect.state)?;
                    if created {
                        crate::raise_capture::capture_created(db, schema)?;
                    }
                    if let Some(circle) = effect.change.made_circle() {
                        crate::raise_capture::capture_made(db, *circle, &effect.state, raises)?;
                    }
                    crate::provider_access::adopt::<P>(db, &effect.provider_access)?;
                    for circle in effect
                        .state
                        .circle_deletions
                        .keys()
                        .filter(|circle| !old.contains_key(circle))
                    {
                        touched.extend(db.query(
                            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1",
                            [coven_merge::Audience::Circle(*circle)],
                            crate::row_queries::read_identity,
                        )?);
                    }
                    if let Some(boundary) = effect.boundary {
                        crate::write_boundary::record(db, &boundary)?;
                    }
                }
            }
            SlotContent::KeyCopy(original) => retain(db, "key", &original)?,
            SlotContent::Announcement { original, accepted } => {
                if let Some(announcement) = accepted {
                    retain(db, "announcement", &original)?;
                    snapshot_announced(db, &announcement)?;
                }
            }
        }
        authenticate()?;
        crate::sequence::note_claim(db, &incoming.header, incoming.slot, incoming.digest)?;
        crate::sequence::set_applied(db, incoming.slot.get())?;
        if !rebuilding.is_empty() {
            crate::rebuild_wait::begin::<P>(db, schema, now, &rebuilding)?;
        }
        crate::forget_records::forget(db, writers)?;
        let refold = if whole {
            crate::queued_writes::Refold::Whole
        } else {
            touched.extend(reach.restored().iter().cloned());
            crate::queued_writes::Refold::Reached(&reach)
        };
        crate::queued_writes::overlay(db, schema, &mut touched, refold)?;
        let deleted = crate::store_state_tables::deleted_circles(db)?;
        crate::download_stage::materialize(db, before, schema, &deleted, touched, files)?;
        files.before_commit()?;
        crate::operation::end_on_stop(db)?;
        Ok(crate::ApplyOutcome::Applied)
    }
}

/// `announcement` was accepted. An ordinary snapshot's coverage only moves
/// forward and is always usable, so a new device replays for its audience
/// only the slots after it, and those at or below it never count toward that
/// audience's next snapshot again (§16.2): their records of the audience's
/// parts go, and each slot's size once no audience's part records it.
fn snapshot_announced(
    db: &DatabaseConnection,
    announcement: &coven_format::sequence::SnapshotAnnouncement,
) -> Result<(), DbError> {
    if announcement.purpose != coven_format::sequence::Purpose::Ordinary {
        return Ok(());
    }
    let covers = announcement.covers.to_be_bytes();
    db.internal_execute(
        "DELETE FROM _coven_write_slot_audiences WHERE audience=?1 AND slot<=?2",
        rusqlite::params![announcement.snapshot.audience, covers],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_write_slots WHERE slot<=?1
         AND NOT EXISTS(SELECT 1 FROM _coven_write_slot_audiences a WHERE a.slot=_coven_write_slots.slot)",
        [covers],
    )?;
    Ok(())
}

/// How a slot's writes are staged: as the next slot, or replayed by a reload.
pub(crate) enum Staging<'a> {
    /// The next slot, beside the queue: each write puts back the kept rows
    /// it reaches before it merges (§6.2).
    Next(&'a mut crate::queued_writes::Reach),
    /// A reload's, into only the parts newer than their snapshots. The
    /// reload put back every kept row first.
    Reload(&'a crate::snapshot_coverage::SnapshotCoverage),
}

/// Ordinary application and reloads use the same ordering, cut-off and original
/// validation.
pub(crate) fn stage_writes<P: crate::PendingValue>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    slot: Slot,
    header: &SlotHeader,
    size: u64,
    writes: Vec<PreparedWrite<'_>>,
    mut staging: Staging<'_>,
) -> Result<(), DbError> {
    // A slot's object never changes, so a reload that stages it again
    // finds the same size.
    let stored = crate::sqlite::integer("writes slot size", size)?;
    db.internal_execute(
        "INSERT INTO _coven_write_slots(slot,size) VALUES(?1,?2) ON CONFLICT(slot) DO NOTHING",
        rusqlite::params![slot.get().to_be_bytes(), stored],
    )?;
    // The parts of a circle awaiting its rebuild pass unread.
    let gated: BTreeSet<_> = match &staging {
        Staging::Next(_) => crate::reload_gate::rebuilding(db)?.into_keys().collect(),
        Staging::Reload(_) => BTreeSet::new(),
    };
    let mut previous = None;
    for write in writes {
        header.check_write(&write.header().header)?;
        if previous.is_some_and(|number| number >= write.header().header.position.number) {
            return Err(crate::snapshot_error::invalid(
                "slot writes are not in original number order",
            ));
        }
        previous = Some(write.header().header.position.number);
        for part in &write.header().parts {
            db.internal_execute(
                "INSERT INTO _coven_write_slot_audiences(audience,slot) VALUES(?1,?2) ON CONFLICT DO NOTHING",
                rusqlite::params![part.audience, slot.get().to_be_bytes()],
            )?;
        }
        let cutoff =
            crate::sequence::refusal(db, slot, header.device, header.author, write.signer())?;
        // A write above the rebuild's version is ahead of every audience the
        // rebuild holds. The rebuild can't hold its changes at its version, so
        // it keeps them as frozen lost values naming the breaking version the
        // write needs (§18.3).
        let schema_version = write.header().header.schema_version;
        let ahead = match &staging {
            Staging::Reload(coverage)
                if coverage.is_rebuild() && schema_version > db.schema_version()? =>
            {
                Some(coverage.needed(schema_version).ok_or(DbError::Invariant {
                    what: "a rebuild knows the breaking migration a write above it needs",
                })?)
            }
            _ => None,
        };
        if let Some((cutoff, cause)) = cutoff {
            db.internal_execute("INSERT INTO _coven_write_refusals(slot,cutoff) VALUES(?1,?2) ON CONFLICT(slot) DO NOTHING",
                rusqlite::params![slot.get().to_be_bytes(), cutoff.get().to_be_bytes()])?;
            crate::pending::save(db, &P::writes_refused(slot, cutoff, cause))?;
        }
        if let Staging::Reload(coverage) = &staging {
            for audience in write.unopened_audiences() {
                if coverage.needs(audience, slot) {
                    return Err(crate::snapshot_error::invalid(
                        "reload lacks a readable audience part",
                    ));
                }
            }
        }
        match &mut staging {
            Staging::Next(reach) => crate::download_stream::stage_selected(
                db,
                schema,
                write,
                slot,
                cutoff.is_some(),
                None,
                |audience| !gated.contains(audience),
                Some(reach),
            )?,
            Staging::Reload(coverage) => crate::download_stream::stage_selected(
                db,
                schema,
                write,
                slot,
                cutoff.is_some(),
                ahead,
                |audience| coverage.needs(audience, slot),
                None,
            )?,
        }
    }
    Ok(())
}

impl<R: std::io::Read> IncomingSlot<crate::DownloadedWriteStream<R>> {
    pub(crate) fn prepare<'a>(
        self,
        inputs: &crate::input_preparation::PreparedInputs<'a>,
    ) -> Result<IncomingSlot<PreparedWrite<'a>>, DbError> {
        let content = match self.content {
            SlotContent::Writes { size, writes } => SlotContent::Writes {
                size,
                writes: writes
                    .into_iter()
                    .map(|write| write.prepare(inputs))
                    .collect::<Result<_, _>>()?,
            },
            SlotContent::Admin { original, effect } => SlotContent::Admin { original, effect },
            SlotContent::KeyCopy(original) => SlotContent::KeyCopy(original),
            SlotContent::Announcement { original, accepted } => {
                SlotContent::Announcement { original, accepted }
            }
        };
        Ok(IncomingSlot { slot: self.slot, header: self.header, digest: self.digest, content })
    }
}

#[cfg(test)]
#[path = "sequence_apply_tests.rs"]
pub(crate) mod tests;
