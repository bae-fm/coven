//! Decode SQLite's session output while its old and new column values exist.

use std::collections::BTreeMap;

use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::Changeset;

use crate::write_encoding::{sql_value, value};
use crate::write_rows::{key_columns, row_key, AppKey, AppValues};
use crate::write_schema::WriteSchema;
use crate::{DbError, RowIdentity, RowKey};
use coven_format::value::Value;

pub(crate) struct CapturedRow {
    pub(crate) old: AppValues,
    pub(crate) new: AppValues,
}

pub(crate) fn capture(
    changeset: &Changeset,
    schema: &WriteSchema,
) -> Result<BTreeMap<AppKey, CapturedRow>, DbError> {
    let mut changes = BTreeMap::<AppKey, CapturedRow>::new();
    let mut items = changeset.iter()?;
    while let Some(item) = items.next()? {
        let operation = item.op()?;
        let table = schema.table(operation.table_name());
        let mut old = BTreeMap::new();
        let mut new = BTreeMap::new();
        for (index, column) in table.columns.iter().filter(|c| !c.generated).enumerate() {
            if operation.code() != Action::SQLITE_INSERT {
                match item.old_value(index) {
                    Ok(v) => {
                        old.insert(column.name.clone(), value(v)?);
                    }
                    Err(rusqlite::Error::InvalidColumnIndex(i)) if i == index => {}
                    Err(error) => return Err(error.into()),
                }
            }
            if operation.code() != Action::SQLITE_DELETE {
                match item.new_value(index) {
                    Ok(v) => {
                        new.insert(column.name.clone(), value(v)?);
                    }
                    Err(rusqlite::Error::InvalidColumnIndex(i)) if i == index => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let values = if operation.code() == Action::SQLITE_INSERT {
            &new
        } else {
            &old
        };
        // Check the captured insert before key collation can normalize its text,
        // including a key change or a re-add that the merge represents as an update.
        if operation.code() == Action::SQLITE_INSERT
            && schema.declaration(&table.name).identity == RowIdentity::IndependentUuid
        {
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
        let key = (table.name.clone(), row_key(table, values)?);
        match changes.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(CapturedRow { old, new });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                // Sessions compare raw keys. SQLite can identify two spellings
                // as one key under a collation; that is one row incarnation.
                let change = entry.get_mut();
                for (target, incoming) in [(&mut change.old, old), (&mut change.new, new)] {
                    for (column, value) in incoming {
                        if let Some(previous) = target.insert(column, value.clone()) {
                            assert_eq!(
                                previous, value,
                                "session disagrees about a canonical row's value"
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(changes)
}

#[cfg(test)]
#[path = "write_capture_tests.rs"]
mod tests;
