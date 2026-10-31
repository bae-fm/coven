//! Show the installed rows the removal rules don't reach (§8.4, §16.5).
//!
//! Each such row is shown as SQLite stores its merged values. A new device
//! inserts them a table at a time, parents first. A device that already has
//! rows changes in place each one its app's table shows differently, then
//! deletes, children first, each one the snapshot no longer has. The rules
//! ran first over the rows they reach, so a key or unique value one of those
//! gives up is free before an installed row takes it, and the app's triggers
//! see every change once, a parent's before its children's.

use crate::removal::Deferral;
use crate::snapshot_layout::{SnapshotLayout, TableLayout};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, sql_value, value};
use crate::write_rows::{key_columns, key_parameters, key_predicate, AppValues};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_merge::RowId;
use rusqlite::{params, params_from_iter};
use std::collections::BTreeSet;

pub(crate) fn materialize(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    region: &BTreeSet<RowId>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    for row in region {
        db.internal_execute(
            "INSERT INTO temp._coven_install_region(table_name,key,audience) VALUES(?1,?2,?3)",
            params![row.table, row.key, audience_text(&row.audience)],
        )?;
    }
    let layout = SnapshotLayout::new(schema);
    let order = crate::snapshot_conflicts::parents_first(schema);
    db.materialize(|db| {
        let mut deferred = Deferral::default();
        for name in &order {
            insert(db, schema, layout.table(name), &mut deferred)?;
        }
        for name in &order {
            update(db, schema, layout.table(name), &mut deferred)?;
        }
        for name in order.iter().rev() {
            delete(db, schema, layout.table(name), &mut deferred)?;
        }
        Ok(())
    })?;
    for table in &layout.tables {
        written(db, table)?;
    }
    crate::file_write::link_installed(db)?;
    crate::snapshot_files::protect(db, schema, &layout, files)
}

/// The installed row in `i` is one the rules didn't reach.
fn clean(table: &TableLayout) -> String {
    format!(
        "i.{present}=1 AND NOT EXISTS(SELECT 1 FROM temp._coven_install_region g WHERE g.table_name=?1 AND g.key=i.{key} AND g.audience=i.{audience})",
        present = table.column("present"),
        key = table.column("key"),
        audience = table.column("audience"),
    )
}

/// The app columns coven writes: every one SQLite doesn't generate.
fn written_columns(table: &TableLayout) -> Vec<String> {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(j, _)| !table.generated[*j])
        .map(|(_, name)| identifier(name))
        .collect()
}

/// `a`'s key columns equal `b`'s.
fn same_key(schema: &WriteSchema, table: &TableLayout, a: &str, b: &str) -> String {
    key_columns(schema.table(&table.table))
        .iter()
        .map(|column| format!("{a}.{name}={b}.{name}", name = identifier(&column.name)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Insert the rows the app's table doesn't have yet, in key order.
fn insert(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &TableLayout,
    deferred: &mut Deferral,
) -> Result<(), DbError> {
    let schema_table = format!("main.{}", identifier(&table.table));
    let new = format!(
        "FROM {} AS i WHERE {} AND NOT EXISTS(SELECT 1 FROM {schema_table} AS a WHERE {})",
        table.installed(),
        clean(table),
        same_key(schema, table, "a", "i")
    );
    let count: i64 = db.query_row(&format!("SELECT count(*) {new}"), [&table.table], |r| {
        r.get(0)
    })?;
    if count == 0 {
        return Ok(());
    }
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_changed(table_name,key,kind) SELECT ?1,i.{},'insert' {new}",
            table.column("key")
        ),
        [&table.table],
    )?;
    deferred.before_change(db)?;
    let columns = written_columns(table);
    let inserted = db.internal_execute(
        &format!(
            "INSERT INTO {schema_table}({}) SELECT {} {new} ORDER BY i.{}",
            columns.join(","),
            columns
                .iter()
                .map(|column| format!("i.{column}"))
                .collect::<Vec<_>>()
                .join(","),
            table.column("key")
        ),
        [&table.table],
    )?;
    if inserted as i64 != count {
        return Err(rusqlite::Error::StatementChangedRows(inserted).into());
    }
    Ok(())
}

/// Change in place each row the app's table shows differently, setting
/// only the columns that differ.
fn update(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &TableLayout,
    deferred: &mut Deferral,
) -> Result<(), DbError> {
    let schema_table = schema.table(&table.table);
    let app = format!("main.{}", identifier(&table.table));
    let columns = written_columns(table);
    let differs: Vec<_> = columns
        .iter()
        .map(|column| format!("NOT (typeof(i.{column})=typeof(a.{column}) AND i.{column} IS a.{column} COLLATE BINARY)"))
        .collect();
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_changed(table_name,key,kind)
             SELECT ?1,i.{},'update' FROM {} AS i JOIN {app} AS a ON {} WHERE {} AND ({})",
            table.column("key"),
            table.installed(),
            same_key(schema, table, "a", "i"),
            clean(table),
            differs.join(" OR ")
        ),
        [&table.table],
    )?;
    let names: Vec<_> = schema_table
        .columns
        .iter()
        .filter(|column| !column.generated)
        .map(|column| column.name.clone())
        .collect();
    let select = |alias: &str| {
        names
            .iter()
            .map(|name| format!("{alias}.{}", identifier(name)))
            .collect::<Vec<_>>()
            .join(",")
    };
    db.for_each(
        &format!(
            "SELECT {},{} FROM temp._coven_install_changed c
             JOIN {} AS i ON i.{}=c.key AND {}
             JOIN {app} AS a ON {} WHERE c.table_name=?1 AND c.kind='update' ORDER BY c.key",
            select("i"),
            select("a"),
            table.installed(),
            table.column("key"),
            clean(table),
            same_key(schema, table, "a", "i"),
        ),
        [&table.table],
        |r| {
            let read = |offset: usize| -> rusqlite::Result<AppValues> {
                names
                    .iter()
                    .enumerate()
                    .map(|(n, name)| Ok((name.clone(), value(r.get_ref(offset + n)?)?)))
                    .collect()
            };
            let values = read(0)?;
            let before = read(names.len())?;
            let changed: Vec<_> = names
                .iter()
                .filter(|name| before[*name] != values[*name])
                .collect();
            deferred.before_change(db)?;
            let parameters = changed
                .iter()
                .map(|name| sql_value(&values[*name]))
                .chain(
                    key_columns(schema_table)
                        .iter()
                        .map(|column| sql_value(&before[&column.name])),
                )
                .collect::<Vec<_>>();
            let changed_rows = db.internal_execute(
                &format!(
                    "UPDATE {app} SET {} WHERE {}",
                    changed
                        .iter()
                        .map(|name| format!("{}=?", identifier(name)))
                        .collect::<Vec<_>>()
                        .join(","),
                    key_predicate(schema_table)
                ),
                params_from_iter(parameters),
            )?;
            crate::removal::changed_once(changed_rows)
        },
    )
}

/// Delete each row the app showed for the audience that its snapshot no
/// longer has.
fn delete(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &TableLayout,
    deferred: &mut Deferral,
) -> Result<(), DbError> {
    let schema_table = schema.table(&table.table);
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_changed(table_name,key,kind)
             SELECT b.table_name,b.key,'delete' FROM temp._coven_install_before b WHERE b.table_name=?1 AND b.shown=1
             AND NOT EXISTS(SELECT 1 FROM {} AS i WHERE i.{}=b.key AND i.{}=b.audience AND i.{}=1)
             AND NOT EXISTS(SELECT 1 FROM temp._coven_install_region g WHERE g.table_name=b.table_name AND g.key=b.key AND g.audience=b.audience)",
            table.installed(),
            table.column("key"),
            table.column("audience"),
            table.column("present"),
        ),
        [&table.table],
    )?;
    db.for_each(
        "SELECT key FROM temp._coven_install_changed WHERE table_name=?1 AND kind='delete' ORDER BY key",
        [&table.table],
        |r| {
            let key: Vec<u8> = r.get(0)?;
            if crate::write_rows::read_values(db, schema_table, &key)?.is_none() {
                return Ok(());
            }
            deferred.before_change(db)?;
            let changed = db.internal_execute(
                &format!(
                    "DELETE FROM main.{} WHERE {}",
                    identifier(&table.table),
                    key_predicate(schema_table)
                ),
                params_from_iter(key_parameters(&key)?),
            )?;
            crate::removal::changed_once(changed)
        },
    )
}

/// Each generated value an installed row's write computed differently
/// from SQLite's reading of its merged inputs (Appendix B.1).
fn written(db: &DatabaseConnection, table: &TableLayout) -> Result<(), DbError> {
    for (j, name) in table.columns.iter().enumerate() {
        if !table.generated[j] {
            continue;
        }
        db.internal_execute(
            &format!(
                "INSERT INTO _coven_written_values(column_id,row_id,value)
                 SELECT ?2,r.id,i.{value} FROM {installed} AS i JOIN _coven_rows r ON r.table_name=?1 AND r.key=i.{key} AND r.audience=i.{audience_column} AND r.generation=i.{generation}
                 WHERE {clean} AND i.{setter_device} IS NOT NULL AND NOT (typeof(i.{value})=typeof(i.{column}) AND i.{value} IS i.{column} COLLATE BINARY)",
                value = table.value(j),
                installed = table.installed(),
                key = table.column("key"),
                audience_column = table.column("audience"),
                generation = table.column("generation"),
                clean = clean(table),
                setter_device = table.setter_device(j),
                column = identifier(name),
            ),
            params![table.table, db.column_id(&table.table, name)?],
        )?;
    }
    Ok(())
}
