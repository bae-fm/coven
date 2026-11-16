//! Hidden row addresses remain stable even when a declared column shadows ROWID.

use super::StoreSql;
use crate::store_failure::StoreFailure;
use crate::DbError;
use sqlive::sql::identifier;
use std::collections::BTreeSet;

/// Guard the hidden rowids of the tables `only` names, by schema and
/// lowercase name, or of every table when `None`. Of those tables, the ones
/// whose hidden rowid is unambiguous: app SQL can't change it. A failure is
/// SQLite's error, as the policy's refresh reports it.
pub(crate) fn protect_rowids(
    sql: &StoreSql,
    only: Option<&BTreeSet<(String, String)>>,
) -> sqlive::rusqlite::Result<BTreeSet<(String, String)>> {
    guard_rowids(sql, only).map_err(|error| match error {
        sqlive::Error::Sqlite(error) => error,
        other => sqlive::rusqlite::Error::UserFunctionError(Box::new(other)),
    })
}

fn guard_rowids(
    sql: &StoreSql,
    only: Option<&BTreeSet<(String, String)>>,
) -> Result<BTreeSet<(String, String)>, sqlive::Error<StoreFailure, DbError>> {
    let guards = match only {
        Some(tables) => tables.iter().map(|(schema, table)| guard(schema, table)).collect(),
        None => sql.query(
            "SELECT name FROM temp.sqlite_schema WHERE type='trigger' AND name GLOB '_coven_rowid_guard_*'",
            [],
            |r| r.get::<_, String>(0),
        )?,
    };
    for guard in guards {
        sql.batch(&format!("DROP TRIGGER IF EXISTS temp.{}", identifier(&guard)))?;
    }
    // A separate primary-key index distinguishes an ordinary rowid from
    // an INTEGER PRIMARY KEY alias, including INTEGER PRIMARY KEY DESC.
    let tables_sql = "SELECT lower(schema),lower(name),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='rowid'),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='_rowid_'),
                    EXISTS(SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE lower(name)='oid')
             FROM pragma_table_list AS t
             WHERE schema IN ('main','temp') AND type IN ('table','shadow') AND wr=0
             AND name NOT GLOB 'sqlite_*'
             AND (NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE pk>0)
                  OR EXISTS (SELECT 1 FROM pragma_index_list(t.name,t.schema) WHERE origin='pk'))
             AND (?1 IS NULL OR (lower(schema)=?1 AND lower(name)=?2))";
    let read = |r: &sqlive::rusqlite::Row<'_>| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, bool>(2)?,
            r.get::<_, bool>(3)?,
            r.get::<_, bool>(4)?,
        ))
    };
    let tables = match only {
        None => sql.query(tables_sql, [None::<&str>, None], read)?,
        Some(tables) => {
            let mut found = Vec::new();
            for (schema, table) in tables {
                found.extend(sql.query(tables_sql, [Some(schema), Some(table)], read)?);
            }
            found
        }
    };
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
        sql.batch(&format!(
            "CREATE TEMP TRIGGER {} BEFORE UPDATE ON {}.{}
                 WHEN old.{alias} IS NOT new.{alias}
                 BEGIN SELECT _coven_rowid_changed(); END",
            identifier(&guard(&schema, &table)),
            identifier(&schema),
            identifier(&table),
        ))?;
    }
    Ok(unambiguous)
}

/// The guard trigger of a table whose declared column shadows `rowid`.
fn guard(schema: &str, table: &str) -> String {
    format!("{GUARD}{schema}_{table}")
}

const GUARD: &str = "_coven_rowid_guard_";

/// Whether `trigger` is one of the guards on a hidden rowid.
pub(crate) fn rowid_guard(trigger: &str) -> bool {
    trigger.to_ascii_lowercase().starts_with(GUARD)
}

#[cfg(test)]
#[path = "rowid_policy_tests.rs"]
mod tests;
