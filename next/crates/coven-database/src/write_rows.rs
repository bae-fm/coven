//! Indexed app-row access, with session old values supplying the before view.

use crate::declaration::AudienceSource;
use crate::schema::{SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_capture::CapturedRow;
use crate::write_encoding::{audience, encoded, sql_value, value};
use crate::write_schema::WriteSchema;
use crate::{DbError, RowKey};
use coven_format::value::Value;
use coven_merge::{Audience, ForeignKey, RowId};
use rusqlite::params_from_iter;
use std::cell::RefCell;
use std::collections::BTreeMap;

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
    rows: RefCell<BTreeMap<AppKey, Option<AppRow>>>,
}

impl<'a> AppView<'a> {
    pub(crate) fn after(database: &'a DatabaseConnection, schema: &'a WriteSchema) -> Self {
        Self {
            database,
            schema,
            overrides: BTreeMap::new(),
            lookup: BTreeMap::new(),
            rows: RefCell::new(BTreeMap::new()),
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
                for index in &table.indices {
                    if let Some(columns) = index.columns.iter().cloned().collect::<Option<Vec<_>>>()
                    {
                        let values: Vec<_> = columns.iter().map(|c| old[c].clone()).collect();
                        if values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        let encoded = equality_key(&values, &index.collations)?;
                        result
                            .lookup
                            .entry((table.name.clone(), columns, encoded))
                            .or_default()
                            .push(key.clone());
                    }
                }
            }
            result.overrides.insert(key.clone(), old);
        }
        Ok(result)
    }

    pub(crate) fn row(&self, key: &AppKey) -> Result<Option<AppRow>, DbError> {
        if let Some(row) = self.rows.borrow().get(key) {
            return Ok(row.clone());
        }
        let table = self.schema.table(&key.0);
        let values = match self.overrides.get(key) {
            Some(values) => values.clone(),
            None => read_values(self.database, table, &key.1)?,
        };
        let Some(values) = values else {
            self.rows.borrow_mut().insert(key.clone(), None);
            return Ok(None);
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
                .any(|d| d.name.eq_ignore_ascii_case(&target.name))
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
            AudienceSource::Column(column) => match &values[column_name(table, column)] {
                Value::Text(text) => audience(text)?,
                v => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        0,
                        column.clone(),
                        sql_value(v).data_type(),
                    )
                    .into())
                }
            },
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
        self.rows
            .borrow_mut()
            .insert(key.clone(), Some(row.clone()));
        Ok(Some(row))
    }

    pub(crate) fn find(
        &self,
        table: &TableSchema,
        columns: &[String],
        values: &[Value],
    ) -> Result<Vec<AppValues>, DbError> {
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
        let wanted = equality_key(&ordered_values, &index.collations)?;
        let mut found = BTreeMap::new();
        for values in self.database.query(
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
            params_from_iter(values.iter().map(sql_value)),
            |r| read_row(table, r),
        )? {
            let key = (table.name.clone(), row_key(table, &values)?);
            let old = match self.overrides.get(&key) {
                Some(old) => old.clone(),
                None => Some(values),
            };
            if let Some(old) = old {
                let components: Vec<_> = ordered_columns.iter().map(|c| old[c].clone()).collect();
                if !components.iter().any(|v| matches!(v, Value::Null))
                    && equality_key(&components, &index.collations)? == wanted
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

pub(crate) fn equality_key(values: &[Value], collations: &[String]) -> Result<Vec<u8>, DbError> {
    let mut values = values.to_vec();
    for (value, collation) in values.iter_mut().zip(collations) {
        if let Value::Text(text) = value {
            match collation.to_ascii_uppercase().as_str() {
                "BINARY" => {}
                "NOCASE" => {
                    text.make_ascii_lowercase();
                    if let Some(nul) = text.find('\0') {
                        let ignored = text.len() - nul;
                        text.truncate(nul);
                        text.extend(std::iter::repeat_n('\0', ignored));
                    }
                }
                "RTRIM" => text.truncate(text.trim_end_matches(' ').len()),
                other => panic!("unsupported registered collation {other}"),
            }
        }
    }
    encoded(coven_format::key::encode_key(&values))
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
    let key = key_columns(table);
    let primary = table
        .indices
        .iter()
        .find(|index| index.primary)
        .expect("synced key has a primary index");
    let components: Vec<_> = key.iter().map(|c| values[&c.name].clone()).collect();
    equality_key(&components, &primary.collations)
}

pub(crate) fn reference_error(table: &TableSchema, values: &AppValues, column: &str) -> DbError {
    DbError::ReferenceAudience {
        table: table.name.clone(),
        key: RowKey(
            key_columns(table)
                .iter()
                .map(|column| sql_value(&values[&column.name]))
                .collect(),
        ),
        column: column.into(),
    }
}

#[cfg(test)]
#[path = "write_rows_tests.rs"]
mod tests;
