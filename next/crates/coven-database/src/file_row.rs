//! File columns use the same key and row conversions as ordinary writes.

use crate::{
    schema::TableSchema,
    sqlite::DatabaseConnection,
    write_encoding::{encoded, sql_value, value},
    write_rows::{key_columns, read_values, row_key, AppKey, AppValues},
    write_schema::WriteSchema,
    DbError, FileDecl, RowKey,
};
use coven_crypto::ContentHash;
use coven_format::value::Value;
use coven_foundation::id_source::DeviceId;

pub(crate) fn declaration<'a>(
    schema: &'a WriteSchema,
    name: &str,
) -> Result<(&'a TableSchema, &'a FileDecl), DbError> {
    let table = schema
        .declarations
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| invalid(format!("{name} is not a synced table")))?;
    let file = table
        .files
        .as_ref()
        .ok_or_else(|| invalid(format!("{name} does not declare a file")))?;
    Ok((schema.table(&table.name), file))
}

pub(crate) fn lookup(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    table: &str,
    key: &RowKey,
) -> Result<(AppKey, AppValues), DbError> {
    let (table, _) = declaration(schema, table)?;
    if key.0.len() != key_columns(table).len() {
        return Err(invalid("file row key has the wrong number of columns"));
    }
    let values = key
        .0
        .iter()
        .map(|v| value(v.into()))
        .collect::<Result<Vec<_>, _>>()?;
    let key = encoded(coven_format::key::encode_key(&values))?;
    let values = read_values(database, table, &key)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    Ok(((table.name.clone(), row_key(table, &values)?), values))
}

pub(crate) fn set(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    hash: Option<ContentHash>,
    device: DeviceId,
) -> Result<(), DbError> {
    let (table, file) = declaration(schema, &key.0)?;
    let mut parameters = match hash {
        Some(hash) => vec![
            rusqlite::types::Value::Blob(hash.as_bytes().to_vec()),
            format!("device:{}", device.0).into(),
        ],
        None => vec![rusqlite::types::Value::Null; 2],
    };
    parameters.extend(
        crate::write_encoding::decoded(coven_format::key::decode_key(&key.1))?
            .iter()
            .map(sql_value),
    );
    let count = database.file_execute(
        &format!(
            "UPDATE main.{} SET {}=?,{}=? WHERE {}",
            crate::sql::identifier(&table.name),
            crate::sql::identifier(&file.hash),
            crate::sql::identifier(&file.location),
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

pub(crate) fn size(values: &AppValues, file: &FileDecl) -> Result<u64, DbError> {
    match values[&file.size] {
        Value::Integer(n) if n >= 0 => Ok(n as u64),
        _ => Err(invalid(format!(
            "{} must contain a nonnegative integer byte count",
            file.size
        ))),
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

pub(crate) fn invalid(reason: impl Into<String>) -> DbError {
    DbError::FileAttachment {
        reason: reason.into(),
    }
}

/// The facts binding local bytes to a file, independent of its audience/location.
pub(crate) fn identity(file: &FileDecl, values: &AppValues) -> Result<Vec<u8>, DbError> {
    encoded(coven_format::key::encode_key(&[
        values[&file.id].clone(),
        values[&file.size].clone(),
        values[&file.hash].clone(),
    ]))
}
