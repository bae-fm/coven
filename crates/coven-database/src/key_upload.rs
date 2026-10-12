//! Irrevocable sealed-copy reservations and contiguous publication progress.

use crate::{sqlite::DatabaseConnection, DbError, EntryId};
use coven_format::path::ObjectPath;
use coven_foundation::id_source::{DeviceId, StoreId};
use std::num::NonZeroU64;

/// One fixed copy in the writer's publication queue, independent of its operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyUpload {
    /// The device owning this path and numbering sequence.
    pub writer: DeviceId,
    /// The reserved positive number, across audiences and recipients.
    pub number: NonZeroU64,
    /// Complete sealed bytes, including routing, ephemeral key and nonce.
    pub bytes: Vec<u8>,
    /// An entry that cannot publish until this copy is confirmed.
    pub before_entry: Option<EntryId>,
}

impl KeyUpload {
    /// The path authenticated by these fixed bytes.
    pub fn path(&self, store: StoreId) -> ObjectPath {
        ObjectPath::key_copy(store, self.writer, self.number)
    }
}

/// Sealing values allocated under the database writer lock. No number is durable
/// until the enclosing entry transaction commits the returned copies.
pub struct KeyUploadBatch {
    store: StoreId,
    writer: DeviceId,
    previous: u64,
    before_entry: Option<EntryId>,
    copies: Vec<KeyUpload>,
}

impl KeyUploadBatch {
    pub(crate) fn new(
        database: &DatabaseConnection,
        store: StoreId,
        writer: DeviceId,
        before_entry: Option<EntryId>,
    ) -> Result<Self, DbError> {
        let previous = last_reserved(database, writer)?;
        Ok(Self {
            store,
            writer,
            previous,
            before_entry,
            copies: Vec::new(),
        })
    }

    /// Seal at the next path. A failed seal consumes no number.
    pub fn push<F, E>(&mut self, seal: F) -> Result<(), E>
    where
        F: FnOnce(&ObjectPath) -> Result<Vec<u8>, E>,
        E: From<DbError>,
    {
        let number = self
            .previous
            .checked_add(1)
            .ok_or(DbError::KeyCopyNumberExhausted)?;
        let number = NonZeroU64::new(number).expect("incremented copy number");
        let bytes = seal(&ObjectPath::key_copy(self.store, self.writer, number))?;
        self.copies.push(KeyUpload {
            writer: self.writer,
            number,
            bytes,
            before_entry: self.before_entry,
        });
        self.previous = number.get();
        Ok(())
    }

    /// Return the fixed copies to commit alongside their entry's sealing key.
    pub fn into_uploads(self) -> Vec<KeyUpload> {
        self.copies
    }
}

fn last_reserved(database: &DatabaseConnection, writer: DeviceId) -> Result<u64, DbError> {
    Ok(database.query_row(
        "SELECT max(number) FROM (SELECT max(number) AS number FROM _coven_key_uploads WHERE writer=?1 UNION ALL SELECT number FROM _coven_key_publications WHERE writer=?1)",
        [writer.0.to_be_bytes().as_slice()],
        |row| row.get::<_, Option<[u8; 8]>>(0),
    )?.map_or(0, u64::from_be_bytes))
}

pub(crate) fn insert(database: &DatabaseConnection, copy: &KeyUpload) -> Result<(), DbError> {
    if last_reserved(database, copy.writer)?.checked_add(1) != Some(copy.number.get()) {
        return Err(DbError::KeyUploadChanged {
            writer: copy.writer,
            number: copy.number,
        });
    }
    database.internal_execute(
        "INSERT INTO _coven_key_uploads(writer,number,bytes,before_device,before_number) VALUES(?1,?2,?3,?4,?5)",
        (copy.writer.0.to_be_bytes().as_slice(), copy.number.get().to_be_bytes().as_slice(), &copy.bytes,
         copy.before_entry.map(|entry| entry.device.0.to_be_bytes().to_vec()),
         copy.before_entry.map(|entry| entry.number.to_be_bytes().to_vec())),
    )?;
    Ok(())
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<Vec<KeyUpload>, DbError> {
    database.query(
        "SELECT writer,number,bytes,before_device,before_number FROM _coven_key_uploads ORDER BY writer,number", [], decode,
    )
}

pub(crate) fn before_entry(
    database: &DatabaseConnection,
    entry: EntryId,
) -> Result<Vec<KeyUpload>, DbError> {
    database.query(
        "SELECT writer,number,bytes,before_device,before_number FROM _coven_key_uploads WHERE before_device=?1 AND before_number=?2 ORDER BY writer,number",
        (entry.device.0.to_be_bytes().as_slice(), entry.number.to_be_bytes().as_slice()), decode,
    )
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<KeyUpload> {
    let before_entry = match (
        row.get::<_, Option<[u8; 8]>>(3)?,
        row.get::<_, Option<[u8; 8]>>(4)?,
    ) {
        (Some(device), Some(number)) => Some(EntryId {
            device: DeviceId(u64::from_be_bytes(device)),
            number: u64::from_be_bytes(number),
        }),
        (None, None) => None,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(KeyUpload {
        writer: DeviceId(u64::from_be_bytes(row.get(0)?)),
        number: NonZeroU64::new(u64::from_be_bytes(row.get(1)?))
            .ok_or(rusqlite::Error::InvalidQuery)?,
        bytes: row.get(2)?,
        before_entry,
    })
}

pub(crate) fn prepare<F, E>(
    database: &DatabaseConnection,
    store: StoreId,
    writer: DeviceId,
    seal: F,
) -> Result<KeyUpload, E>
where
    F: FnOnce(&ObjectPath) -> Result<Vec<u8>, E>,
    E: From<DbError>,
{
    let mut batch = KeyUploadBatch::new(database, store, writer, None)?;
    batch.push(seal)?;
    let copy = batch.into_uploads().pop().expect("one sealed copy");
    database.transaction(|db| insert(db, &copy))?;
    Ok(copy)
}

pub(crate) fn complete(database: &DatabaseConnection, copy: &KeyUpload) -> Result<(), DbError> {
    database.transaction(|db| {
        // No confirmed copy means the next publication must be number one.
        let previous = db.query(
            "SELECT number FROM _coven_key_publications WHERE writer=?1", [copy.writer.0.to_be_bytes().as_slice()],
            |row| Ok(u64::from_be_bytes(row.get(0)?)),
        )?.into_iter().next().unwrap_or(0);
        if copy.number.get() <= previous {
            return Ok(());
        }
        let fixed = db.query(
            "SELECT writer,number,bytes,before_device,before_number FROM _coven_key_uploads WHERE writer=?1 ORDER BY number LIMIT 1",
            [copy.writer.0.to_be_bytes().as_slice()], decode,
        )?.into_iter().next();
        if previous.checked_add(1) != Some(copy.number.get()) || fixed.as_ref() != Some(copy) {
            return Err(DbError::KeyUploadChanged { writer: copy.writer, number: copy.number });
        }
        let args = (copy.writer.0.to_be_bytes(), copy.number.get().to_be_bytes());
        db.internal_execute("INSERT INTO _coven_key_publications(writer,number) VALUES(?1,?2) ON CONFLICT(writer) DO UPDATE SET number=excluded.number", (&args.0[..], &args.1[..]))?;
        db.internal_execute("DELETE FROM _coven_key_uploads WHERE writer=?1 AND number=?2", (&args.0[..], &args.1[..]))?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "key_upload_tests.rs"]
mod tests;
