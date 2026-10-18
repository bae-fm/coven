//! SQLite's UPDATE OF tells explicit NULL assignments from unchanged cells.

use super::DatabaseConnection;
use crate::sql::identifier;
use crate::write_schema::WriteSchema;
use crate::DbError;

pub(crate) const TABLE: &str = "_coven_null_assignments";
pub(crate) const TRIGGER_PREFIX: &str = "_coven_null_assignment_";

/// Temporary capture objects live only inside the local write transaction.
/// Success removes them explicitly; failure rolls their creation back.
pub(super) struct ReferenceAssignments {
    triggers: Vec<String>,
}

impl ReferenceAssignments {
    pub(super) fn begin(
        database: &DatabaseConnection,
        schema: &WriteSchema,
    ) -> Result<Self, DbError> {
        let _scope = database.authorization.internal();
        let mut triggers = Vec::new();
        for declaration in &schema.declarations {
            let table = schema.table(&declaration.name);
            for column in table.columns.iter().filter(|column| {
                !column.generated
                    && table.foreign_keys.iter().any(|key| {
                        key.on_delete == "SET NULL"
                            && key
                                .columns
                                .iter()
                                .any(|name| name.eq_ignore_ascii_case(&column.name))
                    })
            }) {
                if triggers.is_empty() {
                    database.connection.execute_batch(&format!(
                        "CREATE TEMP TABLE {TABLE}(table_name TEXT, column_name TEXT)"
                    ))?;
                }
                let trigger = format!("{TRIGGER_PREFIX}{}", triggers.len());
                let name = identifier(&column.name);
                database.connection.execute_batch(&format!(
                    "CREATE TEMP TRIGGER {} AFTER UPDATE OF {name} ON main.{}
                     WHEN old.{name} IS NULL AND new.{name} IS NULL
                     BEGIN INSERT INTO {TABLE} VALUES('{}','{}'); END",
                    identifier(&trigger),
                    identifier(&table.name),
                    table.name.replace('\'', "''"),
                    column.name.replace('\'', "''"),
                ))?;
                triggers.push(trigger);
            }
        }
        Ok(Self { triggers })
    }

    pub(super) fn finish(self, database: &DatabaseConnection) -> Result<Vec<i64>, DbError> {
        let _scope = database.authorization.internal();
        let surviving = if self.triggers.is_empty() {
            Vec::new()
        } else {
            database.query(&format!("SELECT rowid FROM temp.{TABLE}"), [], |row| {
                row.get(0)
            })?
        };
        for trigger in &self.triggers {
            database
                .connection
                .execute_batch(&format!("DROP TRIGGER temp.{}", identifier(trigger)))?;
        }
        if !self.triggers.is_empty() {
            database
                .connection
                .execute_batch(&format!("DROP TABLE temp.{TABLE}"))?;
        }
        Ok(surviving)
    }
}
