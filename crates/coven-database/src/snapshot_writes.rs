//! Fold every retained slot needed by the selected audience prefixes.
use crate::download_stream::PreparedWrite;
use crate::{
    snapshot_coverage::SnapshotCoverage, snapshot_error::invalid, sqlite::DatabaseConnection,
    write_schema::WriteSchema, DbError, IncomingSlot, LoadStep, SlotContent,
};
use coven_foundation::progress::ProgressReport;
pub(crate) fn apply<P: crate::PendingValue>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    slots: Vec<IncomingSlot<PreparedWrite<'_>>>,
    coverage: &SnapshotCoverage,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    let mut applied = coverage.first()?;
    crate::sequence::set_applied(db, applied)?;
    if !slots.is_empty() {
        progress.begin(LoadStep::InstallingSlots, slots.len() as u64);
    }
    for incoming in slots {
        if applied.checked_add(1) != Some(incoming.slot.get())
            || incoming.slot.get() > coverage.target
            || incoming.header.after > applied
        {
            return Err(DbError::SequenceGap { applied, received: incoming.slot });
        }
        match incoming.content {
            SlotContent::Writes { size, writes } => crate::sequence_apply::stage_writes::<P>(
                db,
                schema,
                incoming.slot,
                &incoming.header,
                size,
                writes,
                crate::sequence_apply::Staging::Reload(coverage),
            )?,
            SlotContent::Admin { original, .. } => {
                crate::sequence_apply::retain(db, "admin", &original)?
            }
            SlotContent::KeyCopy(original) => crate::sequence_apply::retain(db, "key", &original)?,
            SlotContent::Announcement { original, accepted: Some(_) } => {
                crate::sequence_apply::retain(db, "announcement", &original)?
            }
            // Refused, or at or below the store snapshot's coverage, where
            // the snapshot's originals or this device's own are the kept ones.
            SlotContent::Announcement { accepted: None, .. } => (),
        }
        crate::sequence::note_claim(db, &incoming.header, incoming.slot, incoming.digest)?;
        applied = incoming.slot.get();
        crate::sequence::set_applied(db, applied)?;
        progress.advance();
    }
    if applied != coverage.target {
        return Err(invalid("reload lacks slots through its fixed target"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_writes_tests.rs"]
mod tests;
