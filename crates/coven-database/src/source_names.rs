//! Schema identities used to validate original sources across explicit renames.

use crate::{migration_names::MigrationMatch, schema::Schema, sqlite::DatabaseConnection, DbError};
use rusqlite::params;

pub(crate) fn record(
    db: &DatabaseConnection,
    previous: u32,
    version: u32,
    after: &Schema,
    names: &MigrationMatch,
) -> Result<(), DbError> {
    for table in after.tables.values() {
        // Coven's files table is one table in every version, which no
        // migration renames: its origin is fixed.
        if crate::file_table::is_files(&table.name) {
            db.internal_execute(
                "INSERT INTO _coven_table_origins VALUES(?1,?2,0,?2) ON CONFLICT(version,name) DO NOTHING",
                params![version, table.name],
            )?;
            for column in &table.columns {
                db.internal_execute(
                    "INSERT INTO _coven_column_origins VALUES(?1,?2,?3,0,?2,?3) ON CONFLICT(version,table_name,name) DO NOTHING",
                    params![version, table.name, column.name],
                )?;
            }
            continue;
        }
        let prior =
            names.tables.iter().find(|(_, target)| **target == table.name).map(|(old, _)| old);
        let origin = match prior {
            Some(old) => table_origin(db, previous, old)?,
            None => (version, table.name.clone()),
        };
        // A batch can introduce an object before its final version. Its per-step
        // origin remains authoritative when it has no predecessor at batch start.
        db.internal_execute(
            "INSERT INTO _coven_table_origins VALUES(?1,?2,?3,?4) ON CONFLICT(version,name) DO UPDATE SET origin_version=excluded.origin_version,origin_name=excluded.origin_name WHERE ?5",
            params![version,table.name,origin.0,origin.1,prior.is_some()],
        )?;
        for column in &table.columns {
            let prior = prior.and_then(|table| {
                names.columns.iter().find(|((t, _), target)| t == table && **target == column.name)
            });
            let origin = match prior {
                Some(((table, column), _)) => column_origin(db, previous, table, column)?,
                None => (version, table.name.clone(), column.name.clone()),
            };
            db.internal_execute(
                "INSERT INTO _coven_column_origins VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(version,table_name,name) DO UPDATE SET origin_version=excluded.origin_version,origin_table=excluded.origin_table,origin_name=excluded.origin_name WHERE ?7",
                params![version,table.name,column.name,origin.0,origin.1,origin.2,prior.is_some()],
            )?;
        }
    }
    Ok(())
}

fn table_origin(
    db: &DatabaseConnection,
    version: u32,
    table: &str,
) -> Result<(u32, String), DbError> {
    db.query_row(
        "SELECT origin_version,origin_name FROM _coven_table_origins WHERE version=?1 AND name=?2",
        params![version, table],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

fn column_origin(
    db: &DatabaseConnection,
    version: u32,
    table: &str,
    column: &str,
) -> Result<(u32, String, String), DbError> {
    db.query_row("SELECT origin_version,origin_table,origin_name FROM _coven_column_origins WHERE version=?1 AND table_name=?2 AND name=?3",params![version,table,column],|r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))
}

/// Resolve one known schema identity into another version. Absence means the
/// target schema has no such object; a reused spelling is a different origin.
pub(crate) fn table(
    db: &DatabaseConnection,
    from: u32,
    name: &str,
    to: u32,
) -> Result<Option<String>, DbError> {
    if from == to {
        return Ok(Some(name.to_owned()));
    }
    db.query_row("SELECT (SELECT target.name FROM _coven_table_origins source JOIN _coven_table_origins target ON target.origin_version=source.origin_version AND target.origin_name=source.origin_name WHERE source.version=?1 AND source.name=?2 AND target.version=?3)",params![from,name,to],|r| r.get(0))
}

pub(crate) fn column(
    db: &DatabaseConnection,
    from: u32,
    table: &str,
    name: &str,
    to: u32,
    target: &str,
) -> Result<Option<String>, DbError> {
    if from == to {
        return Ok((table == target).then(|| name.to_owned()));
    }
    db.query_row("SELECT (SELECT target.name FROM _coven_column_origins source JOIN _coven_column_origins target ON target.origin_version=source.origin_version AND target.origin_table=source.origin_table AND target.origin_name=source.origin_name WHERE source.version=?1 AND source.table_name=?2 AND source.name=?3 AND target.version=?4 AND target.table_name=?5)",params![from,table,name,to,target],|r| r.get(0))
}
