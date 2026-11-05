//! Whether this installation's own registration or removal is journaled in
//! the store's sequence operations and has not ended.

use crate::sequence_sync::sequence_operations::{Request, SequenceOperation};
use crate::SyncError;
use coven_database::operation_kind;
use coven_format::admin::StoreChange;

/// Whether this installation's registration is journaled and has not ended:
/// its claim may still land, or has landed and not yet been applied. A
/// restore that ends without its store, its device absent from the history
/// it applied, reads this before it removes its installation (§13.1, §11.6).
pub async fn registration_journaled(
    database: &coven_database::Database,
) -> Result<bool, SyncError> {
    change_journaled(database, |change| matches!(change, StoreChange::AddDevice { .. })).await
}

/// Whether this installation's removal of `device`, its own, is journaled
/// and has not ended: its claim may still land, or has landed and not yet
/// been applied. A deletion that leaves the store reads this to learn
/// whether it has passed its point of no return (E1, §11.6).
pub async fn removal_journaled(
    database: &coven_database::Database,
    device: coven_foundation::id_source::DeviceId,
) -> Result<bool, SyncError> {
    change_journaled(
        database,
        |change| matches!(change, StoreChange::RemoveDevice { device: removed } if *removed == device),
    )
    .await
}

/// Whether a journaled sequence operation that hasn't ended makes a change
/// `carries` picks.
async fn change_journaled(
    database: &coven_database::Database,
    carries: impl Fn(&StoreChange) -> bool,
) -> Result<bool, SyncError> {
    for record in database.operations().await? {
        if record.kind == operation_kind::SEQUENCE {
            if let Request::Change(change) =
                serde_json::from_slice::<SequenceOperation>(&record.data)?.request
            {
                if carries(&change) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
