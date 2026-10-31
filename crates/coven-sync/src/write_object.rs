//! Checks shared by authenticated storage inputs.

use crate::{DamagedObject, Refusal, SyncError};
use coven_storage::ObjectPath;
use std::sync::Arc;

/// Only object checks become permanent; local database and input I/O failures propagate.
pub(crate) fn database_failure(path: &ObjectPath, error: coven_database::DbError) -> SyncError {
    use coven_database::{DbError, SnapshotError};
    use coven_merge::MergeError;
    match error {
        DbError::WriteFormat(error) | DbError::Snapshot(SnapshotError::Format(error))
            if crate::error::newer_format(&error) =>
        {
            SyncError::UpdateRequired(crate::error::required_format(&error).expect("newer format"))
        }
        DbError::InvalidWrite {
            error:
                error @ (MergeError::CausalTimestamp(_)
                | MergeError::CausalClosure(_)
                | MergeError::DuplicateTimestamp(_, _)),
            ..
        } => damaged(path, Refusal::InvalidCausality { cause: Some(Arc::new(error)) }),
        DbError::InvalidWrite { error: error @ MergeError::TimestampDevice(_), .. } => {
            damaged(path, Refusal::WrongIdentity { cause: Some(Arc::new(error)) })
        }
        DbError::WriteFormat(error) | DbError::Snapshot(SnapshotError::Format(error)) => {
            damaged(path, Refusal::from(error))
        }
        error @ (DbError::InvalidWrite { .. }
        | DbError::Schema(_)
        | DbError::Snapshot(
            SnapshotError::Inconsistent(_) | SnapshotError::Schema { .. },
        )) => damaged(path, Refusal::InvalidWrite { cause: Some(Arc::new(error)) }),
        error @ DbError::TooLarge { .. } => {
            damaged(path, Refusal::Parse { cause: Some(Arc::new(error)) })
        }
        error => error.into(),
    }
}

pub(crate) fn checked<T>(
    path: &ObjectPath,
    value: Result<T, coven_format::Error>,
) -> Result<T, SyncError> {
    match value {
        Err(error) if crate::error::newer_format(&error) => Err(SyncError::UpdateRequired(
            crate::error::required_format(&error).expect("newer format"),
        )),
        value => value.map_err(|e| damaged(path, Refusal::from(e))),
    }
}
pub(crate) fn damaged(path: &ObjectPath, failure: Refusal) -> SyncError {
    DamagedObject { path: path.as_str().to_owned(), failure }.into()
}
