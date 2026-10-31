//! Hidden row addresses remain stable even when a declared column shadows ROWID.

use super::DatabaseConnection;
use crate::sql::identifier;
use std::collections::BTreeSet;

impl DatabaseConnection {
    pub(super) fn refresh_hidden_rowids(&self) -> rusqlite::Result<()> {
        let _scope = self.authorization.internal();
        let guards = self
            .connection
            .prepare("SELECT name FROM temp.sqlite_schema WHERE type='trigger' AND name GLOB '_coven_rowid_guard_*'")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for guard in guards {
            self.connection.execute_batch(&format!("DROP TRIGGER temp.{}", identifier(&guard)))?;
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
                  OR EXISTS (SELECT 1 FROM pragma_index_list(t.name,t.schema) WHERE origin='pk'))",
        )?;
        let tables = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, bool>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let mut unambiguous = BTreeSet::new();
        for (index, (schema, table, rowid, underscore, oid)) in tables.into_iter().enumerate() {
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
                identifier(&format!("_coven_rowid_guard_{index}")),
                identifier(&schema),
                identifier(&table),
            ))?;
        }
        self.authorization.set_hidden_rowids(unambiguous);
        Ok(())
    }
}
