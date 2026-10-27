//! An immutable reference to the file observed in one committed row version.

use crate::{
    file_row::{self, FixedReference},
    merge_store::MergeStore,
    sqlite::DatabaseConnection,
    write_rows::{row_id, AppView},
    write_schema::WriteSchema,
    DbError, RowKey,
};
use coven_crypto::{ContentHash, ContentHasher};
use coven_format::value::Value;
use coven_foundation::{
    files::{FileReader, ObservationError},
    id_source::{DeviceId, FileId},
};
use coven_merge::{Audience, RowId};

#[path = "local_file.rs"]
mod local_file;
pub(crate) use local_file::{open as open_local, open_source, queued_source, QueuedSource};
pub use local_file::{LocalFileError, LocalFileStream};

/// The file's facts and the row generation naming it. Each attachment picks a
/// new random reference, so equal facts in one generation are one file: the
/// pending attachment's reference equals the row's once completion writes it
/// (§17.2).
#[derive(Clone, Debug, PartialEq)]
struct FileVersion {
    generation: u64,
    id: Value,
    size: u64,
    hash: ContentHash,
    location: FixedReference,
}

/// A row's file at the time it was read: on the attaching device, the row's
/// pending attachment until its upload completes. Facts and version remain
/// private (§17.2, §17.5).
#[derive(Clone, Debug, PartialEq)]
pub struct FileRef {
    row: RowId,
    key: RowKey,
    column: String,
    namespace: String,
    version: FileVersion,
}

impl FileRef {
    /// The cache namespace declared for this file.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    /// The row's content hash, for checking a complete download.
    pub fn content_hash(&self) -> ContentHash {
        self.version.hash
    }
    /// Check an open file against these captured facts in bounded chunks.
    fn matches_content(&self, reader: &FileReader) -> Result<bool, ObservationError> {
        if reader.size() != self.plaintext_size() {
            return Ok(false);
        }
        let mut hash = ContentHasher::new();
        reader.scan(|bytes| hash.update(bytes))?;
        Ok(hash.finish() == self.content_hash())
    }
    /// The random object id fixed by the attaching write (§17.2).
    pub fn id(&self) -> FileId {
        self.version.location.id
    }
    /// The device that attached the file and is responsible for uploading it.
    pub fn uploader(&self) -> DeviceId {
        self.version.location.uploader
    }

    /// The declared table.
    pub fn table(&self) -> &str {
        &self.row.table
    }
    /// The row's primary key, in declared order.
    pub fn key(&self) -> &RowKey {
        &self.key
    }
    /// The column naming the file.
    pub fn column(&self) -> &str {
        &self.column
    }
    /// The file's size in bytes.
    pub fn plaintext_size(&self) -> u64 {
        self.version.size
    }
    /// The row's audience, whose encrypted writes carry the file's key.
    pub fn audience(&self) -> Audience {
        self.row.audience.clone()
    }

    /// A pending attachment's reference: the row as the attaching write left
    /// it, and the file it attached (§17.2).
    pub(crate) fn pending(
        row: RowId,
        key: RowKey,
        column: String,
        namespace: String,
        generation: u64,
        id: Value,
        size: u64,
        hash: ContentHash,
        location: FixedReference,
    ) -> Self {
        Self {
            row,
            key,
            column,
            namespace,
            version: FileVersion {
                generation,
                id,
                size,
                hash,
                location,
            },
        }
    }

    /// The four file columns' values the row takes when this file is its own.
    /// The row's key as coven stores it (format.md D3).
    pub(crate) fn stored_key(&self) -> &[u8] {
        &self.row.key
    }

    pub(crate) fn columns(&self) -> (&Value, u64, ContentHash, &FixedReference) {
        (
            &self.version.id,
            self.version.size,
            self.version.hash,
            &self.version.location,
        )
    }
}

pub(crate) fn read(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &str,
    key: &RowKey,
) -> Result<FileRef, DbError> {
    current(db, schema, table, key)?.ok_or_else(|| DbError::FileAbsent {
        table: table.into(),
        key: key.clone(),
    })
}

/// The row's current reference while it still names `reference`'s file, in
/// that row version or a newer one; `None` once the row is gone or names
/// another file.
pub(crate) fn naming(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    reference: &FileRef,
) -> Result<Option<FileRef>, DbError> {
    match current(db, schema, reference.table(), reference.key()) {
        Ok(current) => Ok(current.filter(|current| {
            (current.uploader(), current.id()) == (reference.uploader(), reference.id())
        })),
        Err(DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn validate(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    reference: &FileRef,
) -> Result<(), DbError> {
    if current(db, schema, reference.table(), reference.key())?.as_ref() == Some(reference) {
        Ok(())
    } else {
        Err(DbError::FileRefChanged {
            table: reference.table().into(),
            key: reference.key().clone(),
        })
    }
}

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn current(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &str,
    key: &RowKey,
) -> Result<Option<FileRef>, DbError> {
    let (_, file) = file_row::declaration(schema, table)?;
    let (key, values) = match file_row::lookup(db, schema, table, key) {
        Ok(row) => row,
        Err(DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => return Ok(None),
        Err(error) => return Err(error),
    };
    // On the attaching device, the row's current pending attachment is its file.
    if let Some(pending) = crate::pending_attachment::current_reference(db, schema, &key)? {
        return Ok(Some(pending));
    }
    let hash = match (&values[&file.hash], &values[&file.location]) {
        (Value::Null, Value::Null) => return Ok(None),
        (Value::Blob(hash), _) => ContentHash::from_bytes(
            hash.as_slice()
                .try_into()
                .map_err(|_| DbError::DamagedDatabase)?,
        ),
        _ => return Err(DbError::DamagedDatabase),
    };
    let location = FixedReference::decode(&values[&file.location])?;
    if values[&file.id] == Value::Null {
        return Err(DbError::DamagedDatabase);
    }
    let size = file_row::size(&values, file).map_err(|_| DbError::DamagedDatabase)?;
    let visible = AppView::after(db, schema);
    let app = visible.row(&key)?.ok_or(DbError::DamagedDatabase)?;
    let row = row_id(&key, &app);
    let store = MergeStore::new(db, &visible);
    let stored = store.row(&row)?;
    Ok(Some(FileRef {
        key: file_row::key(&key)?,
        row,
        column: file.id.clone(),
        namespace: file.namespace.clone(),
        version: FileVersion {
            generation: stored.state.generation(),
            id: values[&file.id].clone(),
            size,
            hash,
            location,
        },
    }))
}

/// Crate-to-crate operations for coven's owners; not re-exported by the app API.
pub trait FileRefJournal: Sized {
    /// The fixed uploader, object id and key carried inside the row, the same
    /// before and after upload. This never consults key custody.
    fn reference(&self) -> Result<coven_format::file_reference::FileReference, DbError>;
    /// Encode captured facts for a device-local journal. Contains the uploaded
    /// file key; callers must keep these bytes private, like the database itself.
    fn encode(&self) -> Result<Vec<u8>, DbError>;
    /// Read a captured reference from the local journal without consulting the
    /// current row. Validate it before reading or changing that row.
    fn decode(bytes: &[u8]) -> Result<Self, DbError>;
}

impl FileRefJournal for FileRef {
    fn reference(&self) -> Result<coven_format::file_reference::FileReference, DbError> {
        self.version.location.reference()
    }
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        let fields = vec![
            Value::Text(self.row.table.clone()),
            Value::Blob(self.row.key.clone()),
            Value::Text(crate::write_encoding::audience_text(&self.row.audience)),
            Value::Text(self.column.clone()),
            Value::Text(self.namespace.clone()),
            Value::Blob(self.version.generation.to_be_bytes().to_vec()),
            self.version.id.clone(),
            Value::Integer(self.version.size as i64),
            Value::Blob(self.version.hash.as_bytes().to_vec()),
            self.version.location.value(),
        ];
        Ok(coven_format::key::encode_key(&fields)?)
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        let fields = coven_format::key::decode_key(bytes)?;
        let [Value::Text(table), Value::Blob(key), Value::Text(audience), Value::Text(column), Value::Text(namespace), Value::Blob(generation), id, Value::Integer(size), Value::Blob(hash), location] =
            fields.as_slice()
        else {
            return Err(DbError::DamagedDatabase);
        };
        if *size < 0 {
            return Err(DbError::DamagedDatabase);
        }
        Ok(Self {
            row: RowId {
                table: table.clone(),
                key: key.clone(),
                audience: crate::write_encoding::audience(audience)?,
            },
            key: crate::file_row::key(&(table.clone(), key.clone()))?,
            column: column.clone(),
            namespace: namespace.clone(),
            version: FileVersion {
                generation: u64::from_be_bytes(
                    generation
                        .as_slice()
                        .try_into()
                        .map_err(|_| DbError::DamagedDatabase)?,
                ),
                id: id.clone(),
                size: *size as u64,
                hash: ContentHash::from_bytes(
                    hash.as_slice()
                        .try_into()
                        .map_err(|_| DbError::DamagedDatabase)?,
                ),
                location: FixedReference::decode(location)?,
            },
        })
    }
}

#[cfg(test)]
#[path = "file_ref_tests.rs"]
mod tests;
