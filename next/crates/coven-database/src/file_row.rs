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
        .ok_or_else(|| DbError::FileTableNotSynced { table: name.into() })?;
    let file = table
        .files
        .as_ref()
        .ok_or_else(|| DbError::FileNotDeclared { table: name.into() })?;
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

pub(crate) fn set(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    contents: Option<(ContentHash, u64)>,
    device: DeviceId,
) -> Result<(), DbError> {
    let (table, file) = declaration(schema, &key.0)?;
    let mut columns = vec![&file.hash, &file.location];
    let mut parameters = match contents {
        Some((hash, _)) => vec![
            rusqlite::types::Value::Blob(hash.as_bytes().to_vec()),
            device.0.to_string().into(),
        ],
        None => vec![rusqlite::types::Value::Null; 2],
    };
    if file.provenance == crate::Provenance::AppProvided {
        if let Some((_, size)) = contents {
            let count = i64::try_from(size).map_err(|_| DbError::TooLarge {
                field: "file size",
                actual: size,
                maximum: i64::MAX as u64,
            })?;
            columns.push(&file.size);
            parameters.push(count.into());
        }
    }
    parameters.extend(
        crate::write_encoding::decoded(coven_format::key::decode_key(&key.1))?
            .iter()
            .map(sql_value),
    );
    let count = database.file_execute(
        &format!(
            "UPDATE main.{} SET {} WHERE {}",
            crate::sql::identifier(&table.name),
            columns
                .iter()
                .map(|c| format!("{}=?", crate::sql::identifier(c)))
                .collect::<Vec<_>>()
                .join(","),
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

/// The facts binding local bytes to a file, independent of its audience/location.
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
    encoded(coven_format::key::encode_key(&[
        values[&file.id].clone(),
        values[&file.size].clone(),
        values[&file.hash].clone(),
    ]))
    .map(Some)
}

#[cfg(test)]
#[path = "file_row_tests.rs"]
mod tests;
