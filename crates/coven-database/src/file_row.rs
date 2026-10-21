//! File columns use the same key and row conversions as ordinary writes.

use crate::{
    schema::TableSchema,
    sqlite::DatabaseConnection,
    write_encoding::{encoded, sql_value, value},
    write_rows::{key_columns, read_values, row_key, AppKey, AppValues},
    write_schema::WriteSchema,
    DbError, FileDecl, RowKey,
};
use coven_crypto::{ContentHash, FileKey, SecretText};
use coven_format::{file_reference::FileReference, value::Value};
use coven_foundation::id_source::{DeviceId, FileId, IdSource};

/// The where-column's fixed uploader, object id and key (D12). Equality compares
/// the whole text, so a changed id or key invalidates an earlier reference.
/// Diagnostics never expose the key.
#[derive(Clone, Debug)]
pub(crate) struct FixedReference {
    pub(crate) uploader: DeviceId,
    pub(crate) id: FileId,
    text: SecretText,
}

impl FixedReference {
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(crate) fn decode(value: &Value) -> Result<Self, DbError> {
        let Value::Text(text) = value else {
            return Err(DbError::DamagedDatabase);
        };
        let reference = FileReference::decode(text).map_err(|_| DbError::DamagedDatabase)?;
        Ok(Self {
            uploader: reference.device,
            id: reference.id,
            text: SecretText::new(text.clone()),
        })
    }

    pub(crate) fn value(&self) -> Value {
        Value::Text(self.text.as_str().to_owned())
    }

    /// The uploader, object id and key, including the key sync encrypts with.
    pub(crate) fn reference(&self) -> Result<FileReference, DbError> {
        Ok(FileReference::decode(self.text.as_str())?)
    }
}

impl PartialEq for FixedReference {
    fn eq(&self, other: &Self) -> bool {
        self.text.as_str() == other.text.as_str()
    }
}

pub(crate) fn declaration<'a>(
    schema: &'a WriteSchema,
    name: &str,
) -> Result<(&'a TableSchema, &'a FileDecl), DbError> {
    let table = schema
        .schema
        .tables
        .get(&name.to_ascii_lowercase())
        .ok_or_else(|| DbError::FileTableNotSynced { table: name.into() })?;
    let declaration = schema
        .declarations
        .iter()
        .find(|d| d.name == table.name)
        .ok_or_else(|| DbError::FileTableNotSynced { table: name.into() })?;
    let file = declaration
        .files
        .as_ref()
        .ok_or_else(|| DbError::FileNotDeclared { table: name.into() })?;
    Ok((table, file))
}

pub(crate) fn lookup(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    table: &str,
    key: &RowKey,
) -> Result<(AppKey, AppValues), DbError> {
    let (table, _) = declaration(schema, table)?;
    if key.0.len() != key_columns(table).len() {
        return Err(DbError::FileKeyArity {
            table: table.name.clone(),
            expected: key_columns(table).len(),
            actual: key.0.len(),
        });
    }
    let values = key
        .0
        .iter()
        .map(|v| value(v.into()))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(index) = values.iter().position(|v| v == &Value::Null) {
        return Err(DbError::FileKeyNull {
            table: table.name.clone(),
            column: key_columns(table)[index].name.clone(),
        });
    }
    let key = encoded(coven_format::key::encode_key(&values))?;
    let values = read_values(database, table, &key)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    Ok(((table.name.clone(), row_key(table, &values)?), values))
}

/// A new attachment's fixed reference: this device as uploader, a random
/// object id and a new key (§17.2). Completion writes it into the row; no
/// later write changes it.
pub(crate) fn new_reference(
    device: DeviceId,
    ids: &dyn IdSource,
) -> Result<FixedReference, DbError> {
    FixedReference::decode(&Value::Text(
        FileReference {
            device,
            id: FileId(ids.new_id()),
            key: FileKey::generate().map_err(DbError::FileKey)?,
        }
        .encode()
        .as_str()
        .to_owned(),
    ))
}

/// Set some of the row's file columns, by the file-filling authority.
fn update(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    values: Vec<(&str, rusqlite::types::Value)>,
) -> Result<(), DbError> {
    let (table, _) = declaration(schema, &key.0)?;
    let columns = values
        .iter()
        .map(|(c, _)| format!("{}=?", crate::sql::identifier(c)))
        .collect::<Vec<_>>()
        .join(",");
    let mut parameters = values.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    parameters.extend(
        crate::write_encoding::decoded(coven_format::key::decode_key(&key.1))?
            .iter()
            .map(sql_value),
    );
    let count = database.file_execute(
        &format!(
            "UPDATE main.{} SET {} WHERE {}",
            crate::sql::identifier(&table.name),
            columns,
            key_columns(table)
                .iter()
                .map(|c| format!("{}=?", crate::sql::identifier(&c.name)))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        rusqlite::params_from_iter(parameters),
    )?;
    if count != 1 {
        return Err(rusqlite::Error::StatementChangedRows(count).into());
    }
    Ok(())
}

/// Clear the row's file: NULL content hash and reference.
pub(crate) fn clear(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<(), DbError> {
    let (_, file) = declaration(schema, &key.0)?;
    update(
        database,
        schema,
        key,
        vec![
            (&file.hash, rusqlite::types::Value::Null),
            (&file.location, rusqlite::types::Value::Null),
        ],
    )
}

/// The write completing an upload: all four file columns, so the row's file
/// comes whole from this one write (§17.2).
#[expect(
    clippy::map_err_ignore,
    reason = "a size past the target integer is TooLarge, which names the field and size"
)]
pub(crate) fn write(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    id: &Value,
    size: u64,
    hash: ContentHash,
    location: &FixedReference,
) -> Result<(), DbError> {
    let (_, file) = declaration(schema, &key.0)?;
    let size = i64::try_from(size).map_err(|_| DbError::TooLarge {
        field: "file size",
        actual: size,
        maximum: i64::MAX as u64,
    })?;
    update(
        database,
        schema,
        key,
        vec![
            (&file.id, sql_value(id)),
            (&file.size, size.into()),
            (
                &file.hash,
                rusqlite::types::Value::Blob(hash.as_bytes().to_vec()),
            ),
            (&file.location, sql_value(&location.value())),
        ],
    )
}

/// An attaching write leaves the row's file as it was (§17.2): a row that
/// existed before the write gets its four file columns back, whatever the
/// app's SQL wrote, so the write changes nothing another device sees of its
/// file; a row the write inserts keeps the app's id and size, with no content
/// hash or reference. The update also lets the authorizer check that this
/// device may write the row, as the completing write will.
pub(crate) fn keep(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    before: Option<&AppValues>,
) -> Result<(), DbError> {
    let (_, file) = declaration(schema, &key.0)?;
    let values = match before {
        Some(row) => [&file.id, &file.size, &file.hash, &file.location]
            .into_iter()
            .map(|column| (column.as_str(), sql_value(&row[column])))
            .collect(),
        None => vec![
            (file.hash.as_str(), rusqlite::types::Value::Null),
            (file.location.as_str(), rusqlite::types::Value::Null),
        ],
    };
    update(database, schema, key, values)
}

pub(crate) fn size(values: &AppValues, file: &FileDecl) -> Result<u64, DbError> {
    match values[&file.size] {
        Value::Integer(n) if n >= 0 => Ok(n as u64),
        _ => Err(DbError::FileSizeInvalid {
            column: file.size.clone(),
            value: sql_value(&values[&file.size]),
        }),
    }
}

pub(crate) fn check_size(values: &AppValues, file: &FileDecl, actual: u64) -> Result<(), DbError> {
    let expected = size(values, file)?;
    if expected == actual {
        Ok(())
    } else {
        Err(DbError::FileSizeMismatch { expected, actual })
    }
}

pub(crate) fn key(key: &AppKey) -> Result<RowKey, DbError> {
    Ok(RowKey(
        crate::write_encoding::decoded(coven_format::key::decode_key(&key.1))?
            .iter()
            .map(sql_value)
            .collect(),
    ))
}

/// The facts binding local bytes to one attachment, independent of its audience.
/// The fixed uploader and object id distinguish identical bytes attached twice.
pub(crate) fn identity(file: &FileDecl, values: &AppValues) -> Result<Option<Vec<u8>>, DbError> {
    if values[&file.hash] == Value::Null {
        return Ok(None);
    }
    if values[&file.id] == Value::Null {
        return Err(DbError::FileIdMissing {
            column: file.id.clone(),
        });
    }
    if values[&file.size] == Value::Null {
        return Err(DbError::FileSizeInvalid {
            column: file.size.clone(),
            value: rusqlite::types::Value::Null,
        });
    }
    encoded(identity_values(
        &values[&file.id],
        &values[&file.size],
        &values[&file.hash],
        &FixedReference::decode(&values[&file.location])?,
    ))
    .map(Some)
}

/// Encode the same local byte identity from a row or a checked file reference.
pub(crate) fn identity_values(
    id: &Value,
    size: &Value,
    hash: &Value,
    fixed: &FixedReference,
) -> Result<Vec<u8>, coven_format::Error> {
    coven_format::key::encode_key(&[
        id.clone(),
        size.clone(),
        hash.clone(),
        Value::Blob(fixed.uploader.0.to_be_bytes().to_vec()),
        Value::Blob(fixed.id.0.as_bytes().to_vec()),
    ])
}

#[cfg(test)]
#[path = "file_row_tests.rs"]
mod tests;
