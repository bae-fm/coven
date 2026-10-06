//! An immutable reference to the file observed in one committed row version.

use crate::{
    file_location::StoredLocation,
    file_row,
    merge_store::MergeStore,
    sqlite::DatabaseConnection,
    write_rows::{row_id, AppView},
    write_schema::WriteSchema,
    DbError, FileLocation, RowKey,
};
use coven_crypto::ContentHash;
use coven_format::value::Value;
use coven_merge::{Audience, RowId, WriteId};

#[path = "local_file.rs"]
mod local_file;
pub(crate) use local_file::open as open_local;
pub use local_file::{LocalFileError, LocalFileStream};

#[derive(Clone, Debug, PartialEq)]
struct FileVersion {
    generation: u64,
    setters: [WriteId; 4],
    id: Value,
    size: u64,
    hash: ContentHash,
    location: StoredLocation,
}

/// A row's file at the time it was read. Facts and version remain private (§16.3).
#[derive(Clone, Debug, PartialEq)]
pub struct FileRef {
    row: RowId,
    key: RowKey,
    column: String,
    version: FileVersion,
}

impl FileRef {
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
    /// Where the file was when this reference was read.
    pub fn location(&self) -> FileLocation {
        self.version.location.public()
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
    let hash = match (&values[&file.hash], &values[&file.location]) {
        (Value::Null, Value::Null) => return Ok(None),
        (Value::Blob(hash), _) => ContentHash::from_bytes(
            hash.as_slice()
                .try_into()
                .map_err(|_| DbError::DamagedDatabase)?,
        ),
        _ => return Err(DbError::DamagedDatabase),
    };
    let location = StoredLocation::decode(&values[&file.location])?;
    if values[&file.id] == Value::Null {
        return Err(DbError::DamagedDatabase);
    }
    let size = file_row::size(&values, file).map_err(|_| DbError::DamagedDatabase)?;
    let visible = AppView::after(db, schema);
    let app = visible.row(&key)?.ok_or(DbError::DamagedDatabase)?;
    let row = row_id(&key, &app);
    let store = MergeStore::new(db, &visible);
    let stored = store.row(&row)?;
    let setters = file.columns().map(|column| {
        stored
            .state
            .cells()
            .get(column)
            .map(|cell| cell.write)
            .ok_or(DbError::DamagedDatabase)
    });
    let [id_setter, size_setter, hash_setter, location_setter] = setters;
    Ok(Some(FileRef {
        key: file_row::key(&key)?,
        row,
        column: file.id.clone(),
        version: FileVersion {
            generation: stored.state.generation(),
            setters: [id_setter?, size_setter?, hash_setter?, location_setter?],
            id: values[&file.id].clone(),
            size,
            hash,
            location,
        },
    }))
}

#[cfg(test)]
#[path = "file_ref_tests.rs"]
mod tests;
