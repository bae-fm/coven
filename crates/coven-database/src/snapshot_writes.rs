//! Fold every retained slot needed by the selected audience prefixes.
use crate::download_stream::PreparedWrite;
use crate::{
    snapshot_coverage::SnapshotCoverage, snapshot_error::invalid, sqlite::DatabaseConnection,
    write_schema::WriteSchema, DbError, IncomingSlot, SlotContent,
};
pub(crate) fn apply<P: crate::PendingValue>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    slots: Vec<IncomingSlot<PreparedWrite<'_>>>,
    coverage: &SnapshotCoverage,
) -> Result<(), DbError> {
    let mut applied = coverage.first()?;
    crate::sequence::set_applied(db, applied)?;
    for incoming in slots {
        if applied.checked_add(1) != Some(incoming.slot.0)
            || incoming.slot.0 > coverage.target
            || incoming.header.after > applied
        {
            return Err(DbError::SequenceGap {
                applied,
                received: incoming.slot,
            });
        }
        match incoming.content {
            SlotContent::Writes(writes) => crate::sequence_apply::stage_writes::<P>(
                db,
                schema,
                incoming.slot,
                &incoming.header,
                writes,
                Some(coverage),
            )?,
            SlotContent::Admin { original, .. } => {
                crate::sequence_apply::retain(db, "admin", &original)?
            }
            SlotContent::KeyCopy(original) => crate::sequence_apply::retain(db, "key", &original)?,
            SlotContent::Announcement(original) => {
                crate::sequence_apply::retain(db, "announcement", &original)?
            }
        }
        crate::sequence::note_claim(db, &incoming.header, incoming.slot, incoming.digest)?;
        applied = incoming.slot.0;
        crate::sequence::set_applied(db, applied)?;
    }
    if applied != coverage.target {
        return Err(invalid("reload lacks slots through its fixed target"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_writes_tests.rs"]
mod tests;
