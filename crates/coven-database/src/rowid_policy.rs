//! Hidden row addresses remain stable even when a declared column shadows ROWID.

use super::DatabaseConnection;
use crate::authorization::ChangedTables;
use crate::sql::identifier;
use std::collections::BTreeSet;

impl DatabaseConnection {
    /// Read every table's hidden rowid again, as the connection opens and
    /// after a migration.
    pub(super) fn refresh_hidden_rowids(&self) -> rusqlite::Result<()> {
        self.protect_rowids(None)
    }

    /// Read again the hidden rowids of the tables app SQL created or
    /// dropped since they were last read, or every table's after it altered
    /// one. Each table is read once as it appears, not again after every
    /// later statement: a schema of hundreds of tables would read them all
    /// after each one.
    pub(super) fn refresh_changed_rowids(&self) -> rusqlite::Result<()> {
        match self.authorization.take_changed_tables() {
            ChangedTables::None => Ok(()),
            ChangedTables::Named(tables) => self.protect_rowids(Some(&tables)),
            ChangedTables::All => self.protect_rowids(None),
        }
    }

    /// Guard the hidden rowids of the tables `only` names, by schema and
    /// lowercase name, or of every table when `None`.
    fn protect_rowids(&self, only: Option<&BTreeSet<(String, String)>>) -> rusqlite::Result<()> {
        let _scope = self.authorization.internal();
        let guards = match only {
            Some(tables) => tables.iter().map(|(schema, table)| guard(schema, table)).collect(),
            None => self
                .connection
                .prepare("SELECT name FROM temp.sqlite_schema WHERE type='trigger' AND name GLOB '_coven_rowid_guard_*'")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };
        for guard in guards {
            self.connection
                .execute_batch(&format!("DROP TRIGGER IF EXISTS temp.{}", identifier(&guard)))?;
        }
        // A separate primary-key index distinguishes an ordinary rowid from
        // an INTEGER PRIMARY KEY alias, including INTEGER PRIMARY KEY DESC.
        let mut statement = self.connection.prepare(
            "SELECT lower(schema),lower(name),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='rowid'),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='_rowid_'),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='oid')
             FROM pragma_table_list AS t
             WHERE schema IN ('main','temp') AND type IN ('table','shadow') AND wr=0
             AND name NOT GLOB 'sqlite_*'
             AND (NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE pk>0)
                  OR EXISTS (SELECT 1 FROM pragma_index_list(t.name,t.schema) WHERE origin='pk'))
             AND (?1 IS NULL OR (lower(schema)=?1 AND lower(name)=?2))",
        )?;
        let read = |r: &rusqlite::Row<'_>| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, bool>(3)?,
                r.get::<_, bool>(4)?,
            ))
        };
        let tables = match only {
            None => statement
                .query_map([None::<&str>, None], read)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            Some(tables) => {
                let mut found = Vec::new();
                for (schema, table) in tables {
                    for row in statement.query_map([Some(schema), Some(table)], read)? {
                        found.push(row?);
                    }
                }
                found
            }
        };
        drop(statement);
        let mut unambiguous = BTreeSet::new();
        for (schema, table, rowid, underscore, oid) in tables {
            if !rowid {
                unambiguous.insert((schema, table));
                continue;
            }
            let alias = if !underscore {
                "_rowid_"
            } else if !oid {
                "oid"
            } else {
                // All names are declared columns: the hidden address cannot be named.
                continue;
            };
            // SQLITE_UPDATE reports "ROWID" for every hidden alias, and also
            // for a declared column of that name. Let SQLite compare the actual
            // addresses instead of rejecting legitimate declared-column writes.
            // TEMP keeps this connection's guard out of the app's durable schema.
            self.connection.execute_batch(&format!(
                "CREATE TEMP TRIGGER {} BEFORE UPDATE ON {}.{}
                 WHEN old.{alias} IS NOT new.{alias}
                 BEGIN SELECT _coven_rowid_changed(); END",
                identifier(&guard(&schema, &table)),
                identifier(&schema),
                identifier(&table),
            ))?;
        }
        self.authorization.set_hidden_rowids(only, unambiguous);
        Ok(())
    }
}

/// The guard trigger of a table whose declared column shadows `rowid`.
fn guard(schema: &str, table: &str) -> String {
    format!("_coven_rowid_guard_{schema}_{table}")
}

#[cfg(test)]
#[path = "rowid_policy_tests.rs"]
mod tests;
