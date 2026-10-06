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
    pub(crate) fn declaration(&self, name: &str) -> &SyncedTable {
        self.declarations
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(name))
            .expect("synced table declaration")
    }

    pub(crate) fn foreign_key(&self, table: &TableSchema, key: &SchemaForeignKey) -> ForeignKey {
        self.schema.foreign_key(table, key)
    }

    fn prepare(&self, db: &DatabaseConnection) -> Result<(), DbError> {
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
    format!("coven_eval_{}", table.name)
}
pub(crate) fn affinity_name(table: &TableSchema) -> String {
    format!("coven_affinity_{}", table.name)
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
