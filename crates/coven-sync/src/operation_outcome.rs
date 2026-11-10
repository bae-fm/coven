//! Reading how an operation ended (§11.4, api.md E6): the kinds
//! `coven_operations` lists, and an outcome as a waiting call's result.

use crate::{OperationKind, OperationOutcome, SyncError};
use coven_database::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use coven_merge::Slot;

/// The landed slot, or the rejection as the error of a call that waits for
/// the operation itself.
pub(crate) fn into_slot(outcome: OperationOutcome) -> Result<Option<Slot>, SyncError> {
    match outcome {
        OperationOutcome::Landed(slot) => Ok(Some(slot)),
        OperationOutcome::Satisfied => Ok(None),
        OperationOutcome::Rejected(reason) => Err(SyncError::Rejected(reason)),
        // Only an operation the app's write journaled ends stopped, and no
        // call waits for one: its outcome is the app's to read.
        OperationOutcome::Stopped(_) => Err(coven_database::DbError::Invariant {
            what: "a call waits only for operations a stop doesn't end",
        }
        .into()),
        // Only storage setup ends cancelled or undone, and it waits for no
        // sequence operation's outcome this way.
        OperationOutcome::Cancelled | OperationOutcome::Undone(_) => {
            Err(coven_database::DbError::Invariant {
                what: "only storage setup is cancelled or undone",
            }
            .into())
        }
        // An invite ends this way, and no call waits for an invite's outcome.
        OperationOutcome::SharingRefused { .. } => Err(coven_database::DbError::Invariant {
            what: "only an invite ends with a sharing refusal",
        }
        .into()),
    }
}

impl OperationKind {
    /// The kind's name, as `coven_operations.kind` lists it.
    pub fn listed(self) -> String {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(name)) => name,
            other => unreachable!("an operation kind is named by its variant: {other:?}"),
        }
    }
}

/// Reads `coven_operations.kind`.
impl FromSql for OperationKind {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        serde_json::from_value(serde_json::Value::String(value.as_str()?.to_owned()))
            .map_err(|error| FromSqlError::Other(Box::new(error)))
    }
}
