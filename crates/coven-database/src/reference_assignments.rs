//! SQLite's UPDATE OF tells explicit NULL assignments from unchanged cells.

use super::DatabaseConnection;
use crate::sql::identifier;
use crate::write_schema::WriteSchema;
use crate::DbError;

pub(crate) const TABLE: &str = "_coven_null_assignments";
pub(crate) const TRIGGER_PREFIX: &str = "_coven_null_assignment_";

/// The columns whose explicit NULL assignments a write records: those a
/// foreign key sets to NULL when its parent goes.
fn watched(schema: &WriteSchema) -> Vec<(&str, &str)> {
    let mut watched = Vec::new();
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
            watched.push((table.name.as_str(), column.name.as_str()));
        }
    }
    watched
}

/// Install the writer's capture objects with the schema it writes, once:
/// creating or dropping them is a schema change, which would expire every
/// statement prepared on the connection if each write did it (§4). They live
/// as long as the connection, and so does the schema.
pub(crate) fn install(database: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    let _scope = database.authorization.internal();
    let watched = watched(schema);
    if watched.is_empty() {
        return Ok(());
    }
    database.connection.execute_batch(&format!(
        "CREATE TEMP TABLE {TABLE}(table_name TEXT, column_name TEXT)"
    ))?;
    for (index, (table, column)) in watched.into_iter().enumerate() {
        let name = identifier(column);
        database.connection.execute_batch(&format!(
            "CREATE TEMP TRIGGER {} AFTER UPDATE OF {name} ON main.{}
             WHEN old.{name} IS NULL AND new.{name} IS NULL
             BEGIN INSERT INTO {TABLE} VALUES('{}','{}'); END",
            identifier(&format!("{TRIGGER_PREFIX}{index}")),
            identifier(table),
            table.replace('\'', "''"),
            column.replace('\'', "''"),
        ))?;
    }
    Ok(())
}

/// One write's use of the capture objects: it clears what an earlier
/// transaction recorded, and reads what its own app SQL recorded.
pub(super) struct ReferenceAssignments {
    capturing: bool,
}

impl ReferenceAssignments {
    pub(super) fn begin(
        database: &DatabaseConnection,
        schema: &WriteSchema,
    ) -> Result<Self, DbError> {
        let _scope = database.authorization.internal();
        let capturing = !watched(schema).is_empty();
        if capturing {
            database
                .connection
                .execute_batch(&format!("DELETE FROM temp.{TABLE}"))?;
        }
        Ok(Self { capturing })
    }

    /// The rows the write's explicit NULL assignments recorded.
    pub(super) fn finish(self, database: &DatabaseConnection) -> Result<Vec<i64>, DbError> {
        let _scope = database.authorization.internal();
        if !self.capturing {
            return Ok(Vec::new());
        }
        database.query(&format!("SELECT rowid FROM temp.{TABLE}"), [], |row| {
            row.get(0)
        })
    }
}
