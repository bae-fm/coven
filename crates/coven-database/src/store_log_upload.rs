//! Immutable store-log plaintext, its sealing key and their prerequisite sealed keys (§§6, 9, 18).

use std::time::SystemTime;

use coven_crypto::MemberId;
use coven_format::{
    store_log::{StoreChange, StoreLogEntry},
    Object,
};
use coven_foundation::id_source::{DeviceId, KeyId, StoreId};

use crate::{sqlite::DatabaseConnection, write_encoding::decoded, DbError, EntryId, StoreLog};

/// Sealing choices committed before any storage call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogSealing {
    /// The store key used to re-seal the entry on every attempt.
    pub key: KeyId,
    /// The remaining sealed copies that must be confirmed before this entry.
    pub keys: Vec<crate::KeyUpload>,
}

/// A locally authored entry waiting for publication and atomic replay application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogUpload {
    /// The format recorded in the immutable frame before the first storage call.
    pub format: coven_format::FormatVersion,
    /// The immutable entry whose number the queue reserves.
    pub entry: StoreLogEntry,
    /// Key choices retained across crashes and lost replies.
    pub sealing: StoreLogSealing,
}

/// One committed view of the store-log state and its local publication queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalStoreLog {
    /// The store selected when this database was opened.
    pub store: StoreId,
    /// This install's device id; restored installs use a new id.
    pub device: DeviceId,
    /// Applied entries and their replay.
    pub log: StoreLog,
    /// The fixed entry awaiting publication, with its prerequisite sealed keys.
    pub upload: Option<StoreLogUpload>,
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<Option<StoreLogUpload>, DbError> {
    let mut upload = database
        .query(
            "SELECT record,sealing_key FROM _coven_store_log_uploads",
            [],
            |row| {
                let record = row.get::<_, Vec<u8>>(0)?;
                let format = decoded(coven_format::FormatVersion::decode(&record))?;
                #[cfg(any(test, feature = "test-utils"))]
                database.record_work(|work| work.queued_entry_decodes += 1);
                let Object::StoreLog(entry) = decoded(Object::decode(&record))? else {
                    return Err(rusqlite::Error::InvalidQuery);
                };
                Ok(StoreLogUpload {
                    format,
                    entry,
                    sealing: StoreLogSealing {
                        key: KeyId(uuid::Uuid::from_bytes(row.get(1)?)),
                        keys: Vec::new(),
                    },
                })
            },
        )?
        .into_iter()
        .next();
    if let Some(upload) = &mut upload {
        upload.sealing.keys = crate::key_upload::before_entry(database, upload.entry.position)?;
    }
    Ok(upload)
}

pub(crate) fn prepare<F, E>(
    database: &DatabaseConnection,
    log: &StoreLog,
    store: StoreId,
    name: &str,
    device: DeviceId,
    now: SystemTime,
    author: MemberId,
    change: StoreChange,
    seal: F,
) -> Result<EntryId, E>
where
    F: FnOnce(
        &StoreLog,
        &StoreLogEntry,
        crate::KeyUploadBatch,
    ) -> Result<(StoreLogSealing, Option<crate::OperationUpdate>), E>,
    E: From<DbError>,
{
    // The database owner holds its writer lock across construction and insertion.
    // No callback receives that capability, and a failed seal reserves no number.
    if let Some(upload) = read(database)? {
        return Err(DbError::StoreLogUploadPending(upload.entry.position).into());
    }
    if let StoreChange::CreateStore {
        store: created,
        name: created_name,
        ..
    } = &change
    {
        crate::store_log::check_creation(*created, created_name, store, name)?;
    }
    let mut positions = log.positions();
    // An absent prior entry starts this device at one.
    let previous = positions
        .0
        .iter()
        .find(|entry| entry.device == device)
        .map_or(0, |entry| entry.number);
    positions.0.retain(|entry| entry.device != device);
    let number = previous
        .checked_add(1)
        .ok_or(DbError::StoreLogNumberExhausted)?;
    let entry = StoreLogEntry {
        position: EntryId { device, number },
        timestamp: crate::write_encoding::timestamp(
            crate::write_encoding::latest_timestamp(database)?,
            now,
            device,
        )?,
        author,
        had_read: positions,
        change,
    };
    let record = Object::StoreLog(entry.clone()).encode().map_err(|error| {
        DbError::InvalidStoreLogEntry {
            entry: entry.position,
            error,
        }
    })?;
    let copies = crate::KeyUploadBatch::new(database, store, device, Some(entry.position))?;
    let (sealed, operation) = seal(log, &entry, copies)?;
    let timestamp = crate::write_encoding::encoded(coven_format::merge_fields::encode_timestamp(
        &entry.timestamp,
    ))?;
    database.transaction(|database| {
        database.internal_execute(
            "INSERT INTO _coven_store_log_uploads(device,number,record,sealing_key,timestamp) VALUES(?1,?2,?3,?4,?5)",
            (device.0.to_be_bytes().as_slice(), number.to_be_bytes().as_slice(), &record, sealed.key.0.as_bytes().as_slice(), timestamp),
        )?;
        for key in &sealed.keys {
            if key.writer != device || key.before_entry != Some(entry.position) {
                return Err(DbError::DamagedDatabase);
            }
            crate::key_upload::insert(database, key)?;
        }
        if let Some(update) = &operation { crate::operation::advance(database, update)?; }
        Ok(())
    })?;
    Ok(entry.position)
}

/// Applying a published local entry consumes exactly the bytes its queue reserved.
/// This runs inside the same transaction as the entry and the replay result.
pub(crate) fn retire(
    database: &DatabaseConnection,
    id: EntryId,
    record: &[u8],
) -> Result<(), DbError> {
    for stored in database.query(
        "SELECT record FROM _coven_store_log_uploads WHERE device=?1 AND number=?2",
        (
            id.device.0.to_be_bytes().as_slice(),
            id.number.to_be_bytes().as_slice(),
        ),
        |row| row.get::<_, Vec<u8>>(0),
    )? {
        if stored != record {
            return Err(DbError::StoreLogEntryChanged(id));
        }
    }
    database.internal_execute(
        "DELETE FROM _coven_store_log_uploads WHERE device=?1 AND number=?2",
        (
            id.device.0.to_be_bytes().as_slice(),
            id.number.to_be_bytes().as_slice(),
        ),
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "store_log_upload_tests.rs"]
mod tests;
