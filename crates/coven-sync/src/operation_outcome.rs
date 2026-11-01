//! Reading how an operation ended (§11.4, api.md E6): the kinds
//! `coven_operations` lists, and an outcome as a waiting call's result.

use crate::{OperationKind, OperationOutcome, SyncError};
use coven_database::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};

/// The landed slot, or the rejection as the error of a call that waits for
/// the operation itself.
pub(crate) fn into_slot(outcome: OperationOutcome) -> Result<Option<u64>, SyncError> {
    match outcome {
        OperationOutcome::Landed(slot) => Ok(Some(slot.0)),
        OperationOutcome::Satisfied => Ok(None),
        OperationOutcome::Rejected(reason) => Err(SyncError::Rejected(reason)),
    }
}

impl OperationKind {
    /// The kind's name, as `coven_operations.kind` lists it.
    pub(crate) fn listed(self) -> String {
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
