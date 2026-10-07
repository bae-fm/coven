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
use coven_crypto::{ContentHash, ContentHasher};
use coven_format::value::Value;
use coven_foundation::files::{FileReader, ObservationError};
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
    /// This does not validate the current row; callers do that separately when
    /// attaching bytes. Recovery and cleanup can check an obsolete reference.
    pub fn matches_content(&self, reader: &FileReader) -> Result<bool, ObservationError> {
        if reader.size() != self.plaintext_size() {
            return Ok(false);
        }
        let mut hash = ContentHasher::new();
        reader.scan(|bytes| hash.update(bytes))?;
        Ok(hash.finish() == self.content_hash())
    }
    /// The row's file id, for diagnostics.
    pub fn id(&self) -> String {
        match &self.version.id {
            Value::Text(id) => id.clone(),
            value => format!("{:?}", crate::write_encoding::sql_value(value)),
        }
    }
    /// The uploaded object's identity and key carried inside the row.
    /// Returns None for a device-local file; this never consults key custody.
    pub fn uploaded(
        &self,
    ) -> Result<Option<(coven_foundation::id_source::FileId, coven_crypto::FileKey)>, DbError> {
        let StoredLocation::Uploaded(text) = &self.version.location else {
            return Ok(None);
        };
        let (id, key) = text
            .as_str()
            .strip_prefix("uploaded ")
            .and_then(|v| v.split_once(' '))
            .ok_or(DbError::DamagedDatabase)?;
        let id = coven_foundation::id_source::FileId(
            uuid::Uuid::parse_str(id).map_err(|_| DbError::DamagedDatabase)?,
        );
        let mut bytes = [0; 32];
        for (i, pair) in key.as_bytes().chunks_exact(2).enumerate() {
            let digit = |b: u8| {
                if b.is_ascii_digit() {
                    b - b'0'
                } else {
                    b - b'a' + 10
                }
            };
            bytes[i] = digit(pair[0]) * 16 + digit(pair[1]);
        }
        Ok(Some((id, coven_crypto::FileKey::from_bytes(bytes))))
    }
    /// Encode captured facts for a device-local journal. Contains the uploaded
    /// file key; callers must keep these bytes private, like the database itself.
    pub fn encode(&self) -> Result<Vec<u8>, DbError> {
        let mut fields = vec![
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
        for setter in &self.version.setters {
            fields.push(Value::Blob(coven_format::merge_fields::encode_write_id(
                setter,
            )?));
        }
        Ok(coven_format::key::encode_key(&fields)?)
    }
    /// Read a captured reference from the local journal without consulting the
    /// current row. Validate it before reading or changing that row.
    pub fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        let fields = coven_format::key::decode_key(bytes)?;
        let [Value::Text(table), Value::Blob(key), Value::Text(audience), Value::Text(column), Value::Text(namespace), Value::Blob(generation), id, Value::Integer(size), Value::Blob(hash), location, setters @ ..] =
            fields.as_slice()
        else {
            return Err(DbError::DamagedDatabase);
        };
        if setters.len() != 4 || *size < 0 {
            return Err(DbError::DamagedDatabase);
        }
        let setters = setters
            .iter()
            .map(|v| match v {
                Value::Blob(b) => Ok(coven_format::merge_fields::decode_write_id(b)?),
                _ => Err(DbError::DamagedDatabase),
            })
            .collect::<Result<Vec<_>, DbError>>()?;
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
                setters: setters.try_into().map_err(|_| DbError::DamagedDatabase)?,
                id: id.clone(),
                size: *size as u64,
                hash: ContentHash::from_bytes(
                    hash.as_slice()
                        .try_into()
                        .map_err(|_| DbError::DamagedDatabase)?,
                ),
                location: StoredLocation::decode(location)?,
            },
        })
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
        namespace: file.namespace.clone(),
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
