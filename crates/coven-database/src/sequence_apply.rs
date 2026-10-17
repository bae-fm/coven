//! Atomic application of the next shared slot and its provisional queue overlay.

use crate::{
    sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, DownloadedWriteStream,
    StoreState,
};
use coven_format::{admin::StoreChange, floor::OriginalSlot, sequence::SlotHeader};
use coven_merge::Slot;
use std::io::Read;

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
    Writes(Vec<DownloadedWriteStream<R>>),
    /// An original admin object and its checked effect, absent for a refusal.
    Admin {
        /// Exact signed evidence retained by store snapshots.
        original: OriginalSlot,
        /// The effect authorized by the preceding prefix.
        effect: Option<AdminEffect>,
    },
    /// An authenticated key-copy object, including refused copies.
    KeyCopy(OriginalSlot),
    /// An authenticated snapshot announcement, including refused announcements.
    Announcement(OriginalSlot),
}

/// Membership and required reload work installed with an authorized admin slot.
pub struct AdminEffect {
    /// The checked original change.
    pub change: StoreChange,
    /// Membership, keys and audience state after the change.
    pub state: StoreState,
    /// The activated snapshot for a raise or reset.
    pub boundary: Option<crate::Boundary>,
    /// Audiences requiring replacement data. `apply_slot` records their wait
    /// without consuming the slot; `load_snapshots` installs it with their data.
    pub reload: Vec<coven_merge::Audience>,
}

pub(crate) fn retain(
    db: &DatabaseConnection,
    kind: &str,
    original: &OriginalSlot,
) -> Result<(), DbError> {
    db.internal_execute("INSERT INTO _coven_slot_records(slot,kind,record) VALUES(?1,?2,?3) ON CONFLICT(slot) DO NOTHING",
        rusqlite::params![original.slot.0.to_be_bytes().as_slice(),kind,original.object])?;
    Ok(())
}

pub(crate) fn apply<P: crate::PendingValue, R: Read>(
    db: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    incoming: IncomingSlot<R>,
    files: &crate::file_write::FileWrite<'_>,
    authenticate: impl FnOnce() -> Result<(), DbError>,
    operation: Option<crate::OperationUpdate>,
) -> Result<crate::ApplyOutcome, DbError> {
    crate::download::source_wait(db.stream_transaction(|db| {
        let applied = crate::sequence::applied(db)?;
        if incoming.slot.0 <= applied {
            return Ok(crate::ApplyOutcome::AlreadyApplied);
        }
        if applied.checked_add(1) != Some(incoming.slot.0) || incoming.header.after > applied {
            return Err(DbError::SequenceGap { applied, received: incoming.slot });
        }
        if let SlotContent::Admin { effect: Some(effect), .. } = &incoming.content {
            if let Some(audience) = effect.reload.first() {
                authenticate()?;
                for required in &effect.reload {
                    crate::reload_gate::require_reload::<P>(db, required)?;
                }
                return Ok(crate::ApplyOutcome::Waiting(crate::WriteWait::Reload(audience.clone())));
            }
        }
        crate::download_stage::begin(db)?;
        let mut touched = crate::published::restore(db, schema)?;
        crate::pending::clear_slot(db, incoming.slot)?;
        match incoming.content {
            SlotContent::Writes(writes) => stage_writes::<P, _>(
                db, schema, incoming.slot, &incoming.header, writes, None,
            )?,
            SlotContent::Admin { original, effect } => {
                retain(db, "admin", &original)?;
                if let Some(effect) = effect {
                    let old = crate::store_state_tables::deleted_circles(db)?;
                    crate::sequence::cutoff(db, incoming.slot, &effect.change)?;
                    crate::store_state_tables::replace(db, &effect.state)?;
                    for circle in effect.state.circle_deletions.keys().filter(|circle| !old.contains_key(circle)) {
                        touched.extend(db.query(
                            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1",
                            [circle.to_string()],
                            crate::row_queries::read_identity,
                        )?);
                    }
                    if let Some(boundary) = effect.boundary {
                        boundary.record(db)?;
                    }
                }
            }
            SlotContent::KeyCopy(original) => retain(db, "key", &original)?,
            SlotContent::Announcement(original) => retain(db, "announcement", &original)?,
        }
        authenticate()?;
        crate::sequence::note_claim(db, &incoming.header, incoming.slot, incoming.digest)?;
        crate::sequence::set_applied(db, incoming.slot.0)?;
        crate::queued_writes::overlay(db, schema)?;
        let deleted = crate::store_state_tables::deleted_circles(db)?;
        crate::download_stage::materialize(db, before, schema, &deleted, touched, files)?;
        files.before_commit()?;
        if let Some(update) = operation {
            crate::operation::advance(db, &update)?;
        }
        Ok(crate::ApplyOutcome::Applied)
    }))
}

/// Ordinary application and reloads use the same ordering, cut-off and original
/// validation. Reload coverage selects only parts newer than their snapshots.
pub(crate) fn stage_writes<P: crate::PendingValue, R: Read>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    slot: Slot,
    header: &SlotHeader,
    writes: Vec<DownloadedWriteStream<R>>,
    coverage: Option<&crate::snapshot_coverage::SnapshotCoverage>,
) -> Result<(), DbError> {
    let mut previous = None;
    for write in writes {
        header.check_write(&write.header.header)?;
        if previous.is_some_and(|number| number >= write.header.header.position.number) {
            return Err(crate::snapshot_error::invalid(
                "slot writes are not in original number order",
            ));
        }
        previous = Some(write.header.header.position.number);
        let cutoff =
            crate::sequence::refusal(db, slot, header.device, header.author, &write.signer)?;
        if let Some(cutoff) = cutoff {
            db.internal_execute("INSERT INTO _coven_write_refusals(slot,cutoff) VALUES(?1,?2) ON CONFLICT(slot) DO NOTHING",
                rusqlite::params![slot.0.to_be_bytes(), cutoff.0.to_be_bytes()])?;
            crate::pending::save(
                db,
                &P::writes_refused(slot, cutoff, crate::pending::own_device(db)?),
            )?;
        }
        if let Some(coverage) = coverage {
            for (part, stream) in write.header.parts.iter().zip(&write.parts) {
                if coverage.needs(&part.audience, slot)
                    && matches!(stream, crate::DownloadedPartStream::Skipped)
                {
                    return Err(crate::snapshot_error::invalid(
                        "reload lacks a readable audience part",
                    ));
                }
            }
        }
        write.stage_selected(db, schema, slot, cutoff.is_some(), |audience| {
            coverage.is_none_or(|coverage| coverage.needs(audience, slot))
        })?;
    }
    Ok(())
}
