//! Schema-derived lookups and one-row SQLite evaluators, prepared when opening.

use crate::removal_schema::TableRules;
use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::{DbError, SyncedTable};
use coven_merge::ForeignKey;
use std::collections::BTreeMap;

pub(crate) struct WriteSchema {
    pub(crate) schema: Schema,
    pub(crate) declarations: Vec<SyncedTable>,
    pub(crate) rules: BTreeMap<String, TableRules>,
}

impl WriteSchema {
    pub(crate) fn read(
        db: &DatabaseConnection,
        mut declarations: Vec<SyncedTable>,
    ) -> Result<Self, DbError> {
        let schema = Schema::read(db)?;
        for declaration in &mut declarations {
            let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
            declaration.name = table.name.clone();
            if let Some(file) = &mut declaration.files {
                for column in [
                    &mut file.id,
                    &mut file.size,
                    &mut file.hash,
                    &mut file.location,
                ] {
                    *column = crate::write_rows::column_name(table, column).to_owned();
                }
            }
        }
        let mut rules = BTreeMap::new();
        for declaration in &declarations {
            let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
            rules.insert(table.name.clone(), TableRules::read(db, table)?);
        }
        let result = Self {
            schema,
            declarations,
            rules,
        };
        result.prepare(db)?;
        Ok(result)
    }

    pub(crate) fn table(&self, name: &str) -> &TableSchema {
        &self.schema.tables[&name.to_ascii_lowercase()]
    }
    /// Validate an incoming row's table, key shape and declared audience.
    pub(crate) fn row_table(&self, row: &coven_merge::RowId) -> Result<&TableSchema, DbError> {
        use crate::snapshot_error::invalid;
        if !self
            .declarations
            .iter()
            .any(|declaration| declaration.name == row.table)
        {
            return Err(invalid("row names a table that does not sync"));
        }
        let table = self.table(&row.table);
        let key = coven_format::key::decode_key(&row.key)?;
        if key.len() != crate::write_rows::key_columns(table).len() {
            return Err(invalid("row key has the wrong number of columns"));
        }
        let values = crate::write_rows::key_columns(table)
            .into_iter()
            .map(|column| column.name.clone())
            .zip(key)
            .collect();
        crate::write_capture::validate_key(self, table, &values)
            .map_err(|_| invalid("row key is not a declared independent UUID"))?;
        if matches!(
            self.declaration(&row.table).audience,
            crate::declaration::AudienceSource::Store
        ) && row.audience != coven_merge::Audience::Store
        {
            return Err(invalid("store table is in a circle audience"));
        }
        Ok(table)
    }

    /// Validate incoming cells, including historical snapshot losses, against
    /// the same column and reference declarations used for downloaded writes.
    pub(crate) fn row_column(
        &self,
        table: &TableSchema,
        name: &str,
        value: &coven_merge::ColumnValue<coven_format::value::Value>,
    ) -> Result<(), DbError> {
        use crate::snapshot_error::invalid;
        if !table.columns.iter().any(|column| column.name == name) {
            return Err(invalid("row names an unknown column"));
        }
        for (key, parent) in &value.parents {
            if !key.columns.0.iter().any(|column| column == name)
                || parent.row.table != key.parent
                || !table
                    .foreign_keys
                    .iter()
                    .any(|foreign| self.foreign_key(table, foreign) == *key)
            {
                return Err(invalid(
                    "reference does not match the application foreign key",
                ));
            }
            self.row_table(&parent.row)?;
        }
        Ok(())
    }

    /// Inserts and present snapshot rows supply their key and reference cells;
    /// updates can omit unchanged cells. Both obey identical identity checks.
    pub(crate) fn row_columns<'a>(
        &self,
        row: &coven_merge::RowId,
        complete: bool,
        columns: impl IntoIterator<
            Item = (
                &'a String,
                &'a coven_merge::ColumnValue<coven_format::value::Value>,
            ),
        >,
    ) -> Result<&TableSchema, DbError> {
        use crate::{
            declaration::AudienceSource,
            snapshot_error::invalid,
            write_rows::{column_name, key_columns, row_key, AppValues},
        };
        use coven_format::value::Value;
        let table = self.row_table(row)?;
        let columns: BTreeMap<_, _> = columns
            .into_iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect();
        for (name, value) in &columns {
            self.row_column(table, name, value)?;
        }
        if complete
            && key_columns(table)
                .iter()
                .map(|column| column.name.as_str())
                .chain(
                    table
                        .foreign_keys
                        .iter()
                        .flat_map(|key| &key.columns)
                        .map(|name| column_name(table, name)),
                )
                .any(|name| !columns.contains_key(name))
        {
            return Err(invalid("present row is missing a key or reference cell"));
        }
        let mut keys: AppValues = key_columns(table)
            .into_iter()
            .map(|column| column.name.clone())
            .zip(coven_format::key::decode_key(&row.key)?)
            .collect();
        for (name, value) in &mut keys {
            if let Some(incoming) = columns.get(name.as_str()) {
                *value = incoming.value.clone();
            }
        }
        if row_key(table, &keys)? != row.key {
            return Err(invalid("row cells disagree with their key"));
        }
        let audience = &self.declaration(&row.table).audience;
        let name = match audience {
            AudienceSource::Store => return Ok(table),
            AudienceSource::Column(name) | AudienceSource::ForeignKey(name) => {
                column_name(table, name)
            }
            AudienceSource::Both { .. } => unreachable!("validated declaration"),
        };
        let Some(value) = columns.get(name) else {
            return if complete {
                Err(invalid("present row is missing its audience cell"))
            } else {
                Ok(table)
            };
        };
        match audience {
            AudienceSource::Column(_) => {
                if value.value != Value::Text(crate::write_encoding::audience_text(&row.audience)) {
                    return Err(invalid("row cells disagree with their audience"));
                }
            }
            AudienceSource::ForeignKey(_) => {
                let foreign = table
                    .foreign_keys
                    .iter()
                    .find(|key| {
                        key.columns
                            .iter()
                            .any(|column| column_name(table, column) == name)
                    })
                    .expect("declared audience reference");
                let parent = value
                    .parents
                    .get(&self.foreign_key(table, foreign))
                    .ok_or_else(|| invalid("audience reference has no parent"))?;
                if parent.row.audience != row.audience {
                    return Err(invalid("inherited audience differs from its parent"));
                }
            }
            _ => unreachable!("audience cell declaration"),
        }
        Ok(table)
    }

    pub(crate) fn declaration(&self, name: &str) -> &SyncedTable {
        self.declarations
            .iter()
            .find(|d| d.name == name)
            .expect("synced table declaration")
    }

    pub(crate) fn foreign_key(&self, table: &TableSchema, key: &SchemaForeignKey) -> ForeignKey {
        self.schema.foreign_key(table, key)
    }

    pub(crate) fn prepare(&self, db: &DatabaseConnection) -> Result<(), DbError> {
        for declaration in &self.declarations {
            let table = self.table(&declaration.name);
            let rules = &self.rules[&table.name];
            db.batch(&format!(
                "CREATE TEMP TABLE {} ({} INTEGER PRIMARY KEY, {})",
                identifier(&evaluation_name(table)),
                identifier(&ordinal(table)),
                rules.columns.join(",")
            ))?;
            db.batch(&format!(
                "CREATE TEMP TABLE {} ({} INTEGER PRIMARY KEY, {})",
                identifier(&affinity_name(table)),
                identifier(&ordinal(table)),
                table
                    .columns
                    .iter()
                    .map(|c| format!("{} {}", identifier(&c.name), identifier(&c.kind)))
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;
        }
        Ok(())
    }
}

pub(crate) fn evaluation_name(table: &TableSchema) -> String {
    format!("_coven_eval_{}", table.name)
}
pub(crate) fn affinity_name(table: &TableSchema) -> String {
    format!("_coven_affinity_{}", table.name)
}
pub(crate) fn ordinal(table: &TableSchema) -> String {
    let mut name = "coven_ordinal".to_owned();
    while table
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&name))
    {
        name.push('_');
    }
    name
}
#[cfg(test)]
#[path = "write_schema_tests.rs"]
mod tests;
