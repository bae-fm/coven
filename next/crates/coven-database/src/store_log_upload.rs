//! Fixed store-log bytes and their prerequisite sealed keys (§§6, 9, 18).

use std::collections::BTreeMap;
use std::time::SystemTime;

use coven_crypto::MemberId;
use coven_format::{
    store_log::{StoreChange, StoreLogEntry},
    value::EntryPositions,
    Object,
};
use coven_foundation::id_source::{DeviceId, StoreId};

use crate::{sqlite::DatabaseConnection, write_encoding::decoded, DbError, EntryId, StoreLog};

/// A prerequisite object whose bytes are fixed with its store-log entry.
/// Paths are opaque to the database; sync validates and interprets them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogKeyUpload {
    /// The member's sealed-key storage path.
    pub path: String,
    /// The complete sealed-key envelope, including its ephemeral key and nonce.
    pub bytes: Vec<u8>,
}

/// Sealing output committed before any storage call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedStoreLog {
    /// The encrypted, signed store-log object.
    pub bytes: Vec<u8>,
    /// Every sealed key that must be stored before this entry.
    pub keys: Vec<StoreLogKeyUpload>,
}

/// A locally authored entry waiting for publication and atomic replay application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogUpload {
    /// The immutable entry whose number the queue reserves.
    pub entry: StoreLogEntry,
    /// Bytes retained across crashes and lost replies.
    pub sealed: SealedStoreLog,
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
            "SELECT record,sealed_bytes FROM coven_store_log_uploads",
            [],
            |row| {
                let Object::StoreLog(entry) = decoded(Object::decode(&row.get::<_, Vec<u8>>(0)?))?
                else {
                    return Err(rusqlite::Error::InvalidQuery);
                };
                Ok(StoreLogUpload {
                    entry,
                    sealed: SealedStoreLog {
                        bytes: row.get(1)?,
                        keys: Vec::new(),
                    },
                })
            },
        )?
        .into_iter()
        .next();
    if let Some(upload) = &mut upload {
        upload.sealed.keys = database.query(
            "SELECT path,bytes FROM coven_store_log_key_uploads WHERE device=?1 AND number=?2 ORDER BY path",
            (upload.entry.position.device.0.to_be_bytes().as_slice(), upload.entry.position.number.to_be_bytes().as_slice()),
            |row| Ok(StoreLogKeyUpload { path: row.get(0)?, bytes: row.get(1)? }),
        )?;
    }
    Ok(upload)
}

pub(crate) fn prepare<F, E>(
    database: &DatabaseConnection,
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
    ) -> Result<(SealedStoreLog, Option<crate::OperationUpdate>), E>,
    E: From<DbError>,
{
    // The database owner holds its writer lock across construction and insertion.
    // No callback receives that capability, and a failed seal reserves no number.
    if let Some(upload) = read(database)? {
        return Err(DbError::StoreLogUploadPending(upload.entry.position).into());
    }
    let log = crate::store_log_tables::read(database)?;
    let mut positions = BTreeMap::<DeviceId, u64>::new();
    for applied in &log.entries {
        positions
            .entry(applied.entry.position.device)
            .and_modify(|n| *n = (*n).max(applied.entry.position.number))
            .or_insert(applied.entry.position.number);
    }
    let previous = positions.remove(&device).unwrap_or(0); // An absent prior entry starts this device at one.
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
        had_read: EntryPositions(
            positions
                .into_iter()
                .map(|(device, number)| EntryId { device, number })
                .collect(),
        ),
        change,
    };
    let record = Object::StoreLog(entry.clone()).encode().map_err(|error| {
        DbError::InvalidStoreLogEntry {
            entry: entry.position,
            error,
        }
    })?;
    let (sealed, operation) = seal(&log, &entry)?;
    database.transaction(|database| {
        database.internal_execute(
            "INSERT INTO coven_store_log_uploads(device,number,record,sealed_bytes) VALUES(?1,?2,?3,?4)",
            (device.0.to_be_bytes().as_slice(), number.to_be_bytes().as_slice(), &record, &sealed.bytes),
        )?;
        for key in &sealed.keys {
            database.internal_execute(
                "INSERT INTO coven_store_log_key_uploads(device,number,path,bytes) VALUES(?1,?2,?3,?4)",
                (device.0.to_be_bytes().as_slice(), number.to_be_bytes().as_slice(), &key.path, &key.bytes),
            )?;
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
        "SELECT record FROM coven_store_log_uploads WHERE device=?1 AND number=?2",
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
        "DELETE FROM coven_store_log_uploads WHERE device=?1 AND number=?2",
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
