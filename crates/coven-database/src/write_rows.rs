//! Indexed app-row access, with session old values supplying the before view.

use crate::declaration::AudienceSource;
use crate::schema::{SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_capture::CapturedRow;
use crate::write_encoding::{encoded, sql_value, value};
use crate::write_schema::WriteSchema;
use crate::{DbError, RowKey};
use coven_format::value::Value;
use coven_merge::{Audience, ForeignKey, RowId};
use rusqlite::params_from_iter;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type AppKey = (String, Vec<u8>);
pub(crate) type AppValues = BTreeMap<String, Value>;

#[derive(Clone)]
pub(crate) struct AppRow {
    pub(crate) values: AppValues,
    pub(crate) audience: Audience,
    pub(crate) parents: BTreeMap<ForeignKey, AppKey>,
}

pub(crate) struct AppView<'a> {
    database: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    overrides: BTreeMap<AppKey, Option<AppValues>>,
    lookup: BTreeMap<(String, Vec<String>, Vec<u8>), Vec<AppKey>>,
    rows: Option<RefCell<BTreeMap<AppKey, Option<AppRow>>>>,
    migration: Option<&'a crate::migration_snapshot::MigrationSnapshot>,
}

impl<'a> AppView<'a> {
    pub(crate) fn after(database: &'a DatabaseConnection, schema: &'a WriteSchema) -> Self {
        Self {
            database,
            schema,
            overrides: BTreeMap::new(),
            lookup: BTreeMap::new(),
            rows: Some(RefCell::new(BTreeMap::new())),
            migration: None,
        }
    }

    pub(crate) fn before(
        database: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        captured: &BTreeMap<AppKey, CapturedRow>,
    ) -> Result<Self, DbError> {
        let mut result = Self::after(database, schema);
        for (key, captured) in captured {
            let table = schema.table(&key.0);
            let old = if captured.old.is_empty() {
                None
            } else {
                let old = match read_values(database, table, &key.1)? {
                    Some(mut values) => {
                        values.extend(captured.old.clone());
                        values
                    }
                    None => captured.old.clone(),
                };
                Some(crate::removal_sql::evaluate_values(database, table, &old)?)
            };
            if let Some(old) = &old {
                result.index_old(key, table, old)?;
            }
            result.overrides.insert(key.clone(), old);
        }
        Ok(result)
    }

    pub(crate) fn without_row_cache(mut self) -> Self {
        self.rows = None;
        self
    }

    fn remember(&self, key: &AppKey, row: Option<AppRow>) -> Option<AppRow> {
        if let Some(rows) = &self.rows {
            rows.borrow_mut().insert(key.clone(), row.clone());
        }
        row
    }

    pub(crate) fn row(&self, key: &AppKey) -> Result<Option<AppRow>, DbError> {
        if let Some(rows) = &self.rows {
            if let Some(row) = rows.borrow().get(key) {
                return Ok(row.clone());
            }
        }
        if let Some(snapshot) = self.migration {
            let row = snapshot.row(self.database, key)?;
            return Ok(self.remember(key, row));
        }
        let table = self.schema.table(&key.0);
        let values = match self.overrides.get(key) {
            Some(values) => values.clone(),
            None => read_values(self.database, table, &key.1)?,
        };
        let Some(values) = values else {
            return Ok(self.remember(key, None));
        };
        let mut parents = BTreeMap::new();
        for foreign_key in &table.foreign_keys {
            let parameters: Vec<_> = foreign_key
                .columns
                .iter()
                .map(|c| values[column_name(table, c)].clone())
                .collect();
            if parameters.iter().any(|v| matches!(v, Value::Null)) {
                continue;
            }
            let target = self.schema.table(&foreign_key.target);
            if !self
                .schema
                .declarations
                .iter()
                .any(|d| d.name == target.name)
            {
                return Err(reference_error(table, &values, &foreign_key.columns[0]));
            }
            let columns = target_columns(target, foreign_key);
            let parent = self
                .find(target, &columns, &parameters)?
                .into_iter()
                .next()
                .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            parents.insert(
                self.schema.foreign_key(table, foreign_key),
                (target.name.clone(), row_key(target, &parent)?),
            );
        }
        let audience = match &self.schema.declaration(&table.name).audience {
            AudienceSource::Store => Audience::Store,
            AudienceSource::Column(column) => column_audience(table, column, &values)?,
            AudienceSource::ForeignKey(column) => {
                let fk = table
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                    .expect("audience foreign key");
                let parent = parents
                    .get(&self.schema.foreign_key(table, fk))
                    .ok_or_else(|| reference_error(table, &values, column))?;
                self.row(parent)?.expect("audience parent exists").audience
            }
            AudienceSource::Both { .. } => unreachable!("validated declaration"),
        };
        let row = AppRow {
            values,
            audience,
            parents,
        };
        Ok(self.remember(key, Some(row)))
    }

    pub(crate) fn migration_before(
        database: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        snapshot: &'a crate::migration_snapshot::MigrationSnapshot,
    ) -> Self {
        let mut view = Self::after(database, schema);
        view.migration = Some(snapshot);
        view
    }

    fn index_old(
        &mut self,
        key: &AppKey,
        table: &TableSchema,
        values: &AppValues,
    ) -> Result<(), DbError> {
        for index in &table.indices {
            if let Some(columns) = index.columns.iter().cloned().collect::<Option<Vec<_>>>() {
                let parts: Vec<_> = columns.iter().map(|c| values[c].clone()).collect();
                if parts.iter().all(|v| !matches!(v, Value::Null)) {
                    self.lookup
                        .entry((table.name.clone(), columns, equality_key(&parts)?))
                        .or_default()
                        .push(key.clone());
                }
            }
        }
        Ok(())
    }

    /// Candidate rows and the cells whose SQL values differ between the views.
    pub(crate) fn changes(
        &self,
        after: &Self,
        keys: impl IntoIterator<Item = AppKey>,
    ) -> Result<BTreeMap<AppKey, BTreeSet<String>>, DbError> {
        let mut changes = BTreeMap::new();
        for key in keys {
            let old = self.row(&key)?;
            let new = after.row(&key)?;
            let columns = new
                .into_iter()
                .flat_map(|r| r.values)
                .filter(|(c, v)| old.as_ref().and_then(|r| r.values.get(c)) != Some(v))
                .map(|(c, _)| c)
                .collect();
            changes.insert(key, columns);
        }
        Ok(changes)
    }

    pub(crate) fn find(
        &self,
        table: &TableSchema,
        columns: &[String],
        values: &[Value],
    ) -> Result<Vec<AppValues>, DbError> {
        if let Some(snapshot) = self.migration {
            return snapshot.find(self.database, table, columns, values);
        }
        let matching = |parameters: &[Value]| {
            self.database.query(
                &format!(
                    "SELECT {} FROM main.{} WHERE {}",
                    column_list(table),
                    identifier(&table.name),
                    columns
                        .iter()
                        .map(|c| format!("{}=?", identifier(c)))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ),
                params_from_iter(parameters.iter().map(sql_value)),
                |r| read_row(table, r),
            )
        };
        if self.overrides.is_empty() {
            return matching(values);
        }
        let values = crate::removal_sql::reference_values(self.database, table, columns, values)?;
        let index = table
            .indices
            .iter()
            .find(|i| {
                i.columns.iter().all(Option::is_some)
                    && i.columns.len() == columns.len()
                    && columns.iter().all(|c| {
                        i.columns
                            .iter()
                            .flatten()
                            .any(|n| n.eq_ignore_ascii_case(c))
                    })
            })
            .expect("foreign key targets a unique index");
        let ordered_columns: Vec<_> = index
            .columns
            .iter()
            .map(|c| c.as_ref().expect("named column").clone())
            .collect();
        let ordered_values: Vec<_> = ordered_columns
            .iter()
            .map(|c| {
                values[columns
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(c))
                    .expect("target column")]
                .clone()
            })
            .collect();
        let wanted = equality_key(&ordered_values)?;
        let mut found = BTreeMap::new();
        for values in matching(&values)? {
            let key = (table.name.clone(), row_key(table, &values)?);
            let old = match self.overrides.get(&key) {
                Some(old) => old.clone(),
                None => Some(values),
            };
            if let Some(old) = old {
                let components: Vec<_> = ordered_columns.iter().map(|c| old[c].clone()).collect();
                if !components.iter().any(|v| matches!(v, Value::Null))
                    && equality_key(&components)? == wanted
                {
                    found.insert(key, old);
                }
            }
        }
        if let Some(keys) = self
            .lookup
            .get(&(table.name.clone(), ordered_columns, wanted))
        {
            for key in keys {
                found.insert(
                    key.clone(),
                    self.overrides[key].clone().expect("indexed old row"),
                );
            }
        }
        Ok(found.into_values().collect())
    }
}

pub(crate) fn read_values(
    database: &DatabaseConnection,
    table: &TableSchema,
    key: &[u8],
) -> Result<Option<AppValues>, DbError> {
    let components = crate::write_encoding::decoded(coven_format::key::decode_key(key))?;
    // A migration can replace the primary key. A key of the old arity cannot
    // identify a row in the new table, even while its deletion is being merged.
    if components.len() != key_columns(table).len() {
        return Ok(None);
    }
    let rows = database.query(
        &format!(
            "SELECT {} FROM main.{} WHERE {}",
            column_list(table),
            identifier(&table.name),
            key_columns(table)
                .iter()
                .map(|c| format!("{}=?", identifier(&c.name)))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        params_from_iter(components.iter().map(sql_value)),
        |r| read_row(table, r),
    )?;
    assert!(rows.len() <= 1, "primary-key lookup returned multiple rows");
    Ok(rows.into_iter().next())
}

pub(crate) fn read_row(
    table: &TableSchema,
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AppValues> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| Ok((c.name.clone(), value(row.get_ref(i)?)?)))
        .collect()
}

pub(crate) fn target_columns(table: &TableSchema, key: &SchemaForeignKey) -> Vec<String> {
    let primary = key_columns(table);
    key.target_columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            column_name(
                table,
                match c {
                    Some(c) => c,
                    None => &primary[i].name,
                },
            )
            .to_owned()
        })
        .collect()
}

/// Equal SQLite key values share these bytes: synced key columns compare text
/// by bytes (§8.5), and the encoding gives equal numbers one form (D3).
pub(crate) fn equality_key(values: &[Value]) -> Result<Vec<u8>, DbError> {
    encoded(coven_format::key::encode_key(values))
}

pub(crate) fn row_id(key: &AppKey, row: &AppRow) -> RowId {
    RowId {
        table: key.0.clone(),
        key: key.1.clone(),
        audience: row.audience.clone(),
    }
}

pub(crate) fn column_name<'a>(table: &'a TableSchema, name: &str) -> &'a str {
    &table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .expect("column exists in validated schema")
        .name
}

pub(crate) fn key_columns(table: &TableSchema) -> Vec<&crate::schema::SchemaColumn> {
    let mut columns: Vec<_> = table.columns.iter().filter(|c| c.primary_key > 0).collect();
    columns.sort_by_key(|c| c.primary_key);
    columns
}

pub(crate) fn column_list(table: &TableSchema) -> String {
    table
        .columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn row_key(table: &TableSchema, values: &AppValues) -> Result<Vec<u8>, DbError> {
    let components: Vec<_> = key_columns(table)
        .iter()
        .map(|c| values[&c.name].clone())
        .collect();
    equality_key(&components)
}

/// A root row's audience: its column's `store` or circle id, in the one
/// spelling every receiver accepts (§14).
pub(crate) fn column_audience(
    table: &TableSchema,
    column: &str,
    values: &AppValues,
) -> Result<Audience, DbError> {
    match &values[column_name(table, column)] {
        Value::Text(text) => crate::write_encoding::canonical_audience(text).ok_or_else(|| {
            DbError::InvalidAudience {
                table: table.name.clone(),
                key: error_key(table, values),
            }
        }),
        v => Err(
            rusqlite::Error::InvalidColumnType(0, column.into(), sql_value(v).data_type()).into(),
        ),
    }
}

pub(crate) fn reference_error(table: &TableSchema, values: &AppValues, column: &str) -> DbError {
    DbError::ReferenceAudience {
        table: table.name.clone(),
        key: error_key(table, values),
        column: column.into(),
    }
}

/// A row's primary key as an error reports it.
fn error_key(table: &TableSchema, values: &AppValues) -> RowKey {
    RowKey(
        key_columns(table)
            .iter()
            .map(|column| sql_value(&values[&column.name]))
            .collect(),
    )
}

#[cfg(test)]
#[path = "write_rows_tests.rs"]
mod tests;
