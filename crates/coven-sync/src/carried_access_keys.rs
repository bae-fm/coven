//! S3 key deletions a discarded database owed (§10, §13).
//!
//! §10's reset replaces a stale or damaged copy with a new device loaded from
//! storage. A deletion the old copy owed, which the store log may not derive,
//! stays pending in the replacement. A key it made but never recorded is not
//! carried: another live copy of the database may still record it, and §13's
//! reclaim takes it once the old device is retired (KeyListing
//! `reset_spares_recorded_key`, `pending_never_recorded`).

use crate::operation_data::Data;
use crate::SyncError;
use coven_database::{AccessKeyDeletion, OperationRecord};

/// The pending `DeleteAccessKey` work in a discarded operation journal.
pub fn owed_access_key_deletions(
    records: &[OperationRecord],
) -> Result<Vec<AccessKeyDeletion>, SyncError> {
    let mut deletions = Vec::new();
    for record in records {
        if let Data::DeleteAccessKey(key) = Data::read(record)? {
            let operation = Data::DeleteAccessKey(key.clone()).new_operation("coven")?;
            deletions.push(AccessKeyDeletion { key, operation });
        }
    }
    Ok(deletions)
}
