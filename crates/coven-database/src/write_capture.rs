//! Net row changes of a local write, from its app phase's row operations.

use std::collections::BTreeMap;

use rusqlite::hooks::Action;

use crate::sqlite::Event;
use crate::write_encoding::{sql_value, value};
use crate::write_rows::{key_columns, row_key, AppKey, AppValues};
use crate::write_schema::WriteSchema;
use crate::{DbError, RowIdentity, RowKey};
use coven_format::value::Value;

/// A row's values before the write; empty if it did not exist.
pub(crate) struct CapturedRow {
    pub(crate) old: AppValues,
}

/// One operation's old and new values by column name. A virtual generated
/// column has no stored value and is left out.
pub(crate) fn values(
    table: &crate::schema::TableSchema,
    values: &[Option<crate::sql_value::SqlValue>],
) -> Result<Option<AppValues>, DbError> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut row = BTreeMap::new();
    for (column, cell) in table.columns.iter().zip(values) {
        if let Some(cell) = cell {
            row.insert(column.name.clone(), value(cell.as_value_ref())?);
        }
    }
    Ok(Some(row))
}

/// Each declared table's rows the app phase changed, keyed as merge keys
/// them, with the values before its first operation. An UPDATE of a primary
/// key ends the old key's row and starts the new one's.
pub(crate) fn capture(
    schema: &WriteSchema,
    events: &[Event],
) -> Result<BTreeMap<AppKey, CapturedRow>, DbError> {
    let mut changes = BTreeMap::<AppKey, Option<AppValues>>::new();
    for event in events {
        let Some(declaration) = schema
            .declarations
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(&event.table))
        else {
            continue;
        };
        let table = schema.table(&declaration.name);
        let old = values(table, &event.old)?;
        let new = values(table, &event.new)?;
        // Check each inserted key, including a key change or a re-add the
        // merge represents as an update.
        if let Some(new) = &new {
            let key_changed = old.as_ref().is_some_and(|old| {
                key_columns(table)
                    .iter()
                    .any(|column| old.get(&column.name) != new.get(&column.name))
            });
            if event.action == Action::SQLITE_INSERT || key_changed {
                validate_key(schema, table, new)?;
            }
        }
        let old_key = old
            .as_ref()
            .map(|old| row_key(table, old))
            .transpose()?
            .map(|key| (table.name.clone(), key));
        let new_key = new
            .as_ref()
            .map(|new| row_key(table, new))
            .transpose()?
            .map(|key| (table.name.clone(), key));
        if let Some(key) = &old_key {
            changes.entry(key.clone()).or_insert_with(|| old.clone());
        }
        if let Some(key) = new_key {
            // A row first seen inserted did not exist before the write.
            changes.entry(key).or_insert(None);
        }
    }
    Ok(changes
        .into_iter()
        .map(|(key, old)| {
            (
                key,
                CapturedRow {
                    old: old.unwrap_or_default(),
                },
            )
        })
        .collect())
}

pub(crate) fn validate_key(
    schema: &WriteSchema,
    table: &crate::schema::TableSchema,
    values: &AppValues,
) -> Result<(), DbError> {
    if schema.declaration(&table.name).identity == RowIdentity::IndependentUuid {
        let columns = key_columns(table);
        let has_uuid = columns.iter().any(|column| match &values[&column.name] {
            Value::Text(text) => uuid::Uuid::parse_str(text).is_ok_and(|id| {
                matches!(id.get_version_num(), 4 | 7)
                    && id.get_variant() == uuid::Variant::RFC4122
                    && id.to_string() == *text
            }),
            _ => false,
        });
        if !has_uuid {
            return Err(DbError::KeyNotUuid {
                table: table.name.clone(),
                key: RowKey(
                    columns
                        .iter()
                        .map(|column| sql_value(&values[&column.name]))
                        .collect(),
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_capture_tests.rs"]
mod tests;
