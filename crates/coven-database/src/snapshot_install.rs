//! Install a staged snapshot's rows on the writer: copy each staged row from
//! the input connection once, check the copies with SQL, and write the merge
//! records from them one table at a time (§16.2, §21.2).

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::snapshot_error::invalid;
use crate::snapshot_layout::{SnapshotLayout, TableLayout};
use crate::snapshot_preparation::PreparedSnapshot;
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{sql_value, value};
use crate::write_rows::AppValues;
use crate::write_schema::WriteSchema;
use crate::{DbError, LoadStep};
use coven_foundation::progress::ProgressReport;
use coven_merge::{Audience, RowId};
use rusqlite::types::ToSqlOutput;
use rusqlite::{params, params_from_iter};

/// The passes installing makes over each snapshot's tables, one table at a
/// time: `copy_rows`, `persist_rows` and `finish`. Each table counts once in
/// each pass toward the `Installing` step (§13.4).
pub(crate) const TABLE_PASSES: u64 = 3;

/// The writer's tables for the snapshots one reload installs: each synced
/// table's rows, their earlier generations, and their source records; and
/// what installing works out from them: the rows shown before, those the
/// removal rules start from and reach, the app rows changed, the keys whose
/// lost values the install replaced, and what those rows protect.
pub(crate) fn create_tables(db: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    for table in &SnapshotLayout::new(schema).tables {
        db.batch(&table.create_installed(schema))?;
    }
    db.batch(
        "CREATE TEMP TABLE _coven_install_generations(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,generation BLOB NOT NULL,device BLOB NOT NULL,number BLOB NOT NULL);
         CREATE TEMP TABLE _coven_install_parts(write_id BLOB NOT NULL,audience TEXT NOT NULL,header BLOB NOT NULL,PRIMARY KEY(write_id,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_authored(write_id BLOB NOT NULL,audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,record BLOB NOT NULL,admission BLOB,stop BLOB,PRIMARY KEY(write_id,audience,table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_before(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,shown INTEGER NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_conflicts(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_region(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_changed(table_name TEXT NOT NULL,key BLOB NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(table_name,key,kind)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_lost(table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_protected(table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_references(file TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(file,table_name,key,column_name)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_stored(device BLOB NOT NULL,object BLOB NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(device,object,table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_released(file TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_records(file TEXT PRIMARY KEY NOT NULL,namespace TEXT,object_path TEXT) WITHOUT ROWID;",
    )
}

/// Drop the install's tables whole.
pub(crate) fn drop_tables(db: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    for table in &SnapshotLayout::new(schema).tables {
        db.batch(&format!("DROP TABLE {}", table.installed()))?;
    }
    db.batch(
        "DROP TABLE temp._coven_install_generations; DROP TABLE temp._coven_install_parts; DROP TABLE temp._coven_install_authored;
         DROP TABLE temp._coven_install_before; DROP TABLE temp._coven_install_conflicts; DROP TABLE temp._coven_install_region;
         DROP TABLE temp._coven_install_changed; DROP TABLE temp._coven_install_lost; DROP TABLE temp._coven_install_protected; DROP TABLE temp._coven_install_references;
         DROP TABLE temp._coven_install_stored; DROP TABLE temp._coven_install_released; DROP TABLE temp._coven_install_records",
    )
}

/// Copy `select`'s rows from the staged tables into the writer with
/// `insert`, which takes each row's columns as its parameters, in order.
fn copy(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
    select: &str,
    insert: &str,
) -> Result<(), DbError> {
    prepared.each(select, [], |row| {
        let count = row.as_ref().column_count();
        let values = (0..count)
            .map(|i| row.get_ref(i).map(ToSqlOutput::Borrowed))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        db.internal_execute(insert, params_from_iter(values))?;
        Ok(())
    })
}

/// Copy the snapshot's rows and earlier generations. A row's app columns are
/// given as SQLite stores them: a column a cell sets takes the cell's value,
/// any other its default.
pub(crate) fn copy_rows(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
    layout: &SnapshotLayout,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    for table in &layout.tables {
        let names = table.names();
        let mut columns = names.clone();
        let mut values: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
        for (j, name) in table.columns.iter().enumerate() {
            columns.push(identifier(name));
            values.push(format!(
                "CASE WHEN ?{} IS NOT NULL THEN ?{} ELSE {} END",
                table.setter_at(j) + 1,
                table.value_at(j) + 1,
                table.defaults[j].as_deref().unwrap_or("NULL")
            ));
        }
        copy(
            db,
            prepared,
            &format!(
                "SELECT {} FROM {} ORDER BY 1,2",
                names.join(","),
                table.staged(prepared.staging())
            ),
            &format!(
                "INSERT INTO {}({}) VALUES({})",
                table.installed(),
                columns.join(","),
                values.join(",")
            ),
        )?;
        progress.advance();
    }
    copy(
        db,
        prepared,
        &format!(
            "SELECT table_name,key,audience,generation,device,number FROM {} ORDER BY rowid",
            prepared.table("generations")
        ),
        "INSERT INTO temp._coven_install_generations(table_name,key,audience,generation,device,number) VALUES(?1,?2,?3,?4,?5,?6)",
    )
}

fn exists<P: rusqlite::Params>(
    db: &DatabaseConnection,
    sql: &str,
    params: P,
) -> Result<bool, DbError> {
    db.query_row(sql, params, |r| r.get(0))
}

/// Write the merge records of the audience's rows, one statement per table
/// for its rows and its earlier generations, and one per column for its
/// cells and per reference for their parents. The audience's records were
/// deleted first, so nothing conflicts.
pub(crate) fn persist_rows(
    db: &DatabaseConnection,
    layout: &SnapshotLayout,
    audience: &Audience,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    db.internal_execute(
        &format!(
            "INSERT INTO _coven_rows(table_name,key,audience,generation,write_id)
             SELECT g.table_name,g.key,g.audience,g.generation,
               (SELECT id FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=g.device AND number=g.number)
             FROM temp._coven_install_generations AS g WHERE g.audience=?1"
        ),
        params![audience],
    )?;
    for table in &layout.tables {
        let installed = table.installed();
        let key = table.column("key");
        let audience_column = table.column("audience");
        let generation = table.column("generation");
        db.internal_execute(
            &format!(
                "INSERT INTO _coven_rows(table_name,key,audience,generation,write_id)
                 SELECT ?2,i.{key},i.{audience_column},i.{generation},
                   (SELECT id FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=i.{device} AND number=i.{number})
                 FROM {installed} AS i WHERE i.{audience_column}=?1 ORDER BY i.{key}",
                device = table.column("device"),
                number = table.column("number"),
            ),
            params![audience, table.table],
        )?;
        for (j, name) in table.columns.iter().enumerate() {
            db.internal_execute(
                &format!(
                    "INSERT INTO _coven_cells(column_id,row_id,write_id)
                     SELECT ?3,r.id,(SELECT id FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=i.{setter_device} AND number=i.{setter_number})
                     FROM {installed} AS i JOIN _coven_rows r ON r.table_name=?2 AND r.key=i.{key} AND r.audience=i.{audience_column} AND r.generation=i.{generation}
                     WHERE i.{audience_column}=?1 AND i.{setter_device} IS NOT NULL",
                    setter_device = table.setter_device(j),
                    setter_number = table.setter_number(j),
                ),
                params![audience, table.table, db.column_id(&table.table, name)?],
            )?;
        }
        for (q, (j, foreign)) in table.pairs.iter().enumerate() {
            db.internal_execute(
                &format!(
                    "INSERT INTO _coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation)
                     SELECT r.id,?3,?4,?5,i.{parent_key},i.{parent_audience},i.{parent_generation}
                     FROM {installed} AS i JOIN _coven_rows r ON r.table_name=?2 AND r.key=i.{key} AND r.audience=i.{audience_column} AND r.generation=i.{generation}
                     WHERE i.{audience_column}=?1 AND i.{parent_key} IS NOT NULL",
                    parent_key = table.parent_key(q),
                    parent_audience = table.parent_audience(q),
                    parent_generation = table.parent_generation(q),
                ),
                params![
                    audience,
                    table.table,
                    db.column_id(&table.table, &table.columns[*j])?,
                    crate::row_queries::foreign_key(db, &table.table, foreign)?,
                    foreign.parent
                ],
            )?;
        }
        progress.advance();
    }
    Ok(())
}

/// Each cell of a row the snapshot's author shows holds what SQLite stores for
/// its column. A row its author took out, with a live rules loss, keeps its
/// cells as written, whatever the columns became since.
pub(crate) fn finish(
    db: &DatabaseConnection,
    layout: &SnapshotLayout,
    audience: &Audience,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    for table in &layout.tables {
        let stored: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .map(|(j, name)| {
                format!(
                    "(i.{} IS NOT NULL AND NOT {})",
                    table.setter_device(j),
                    crate::sql::same_value(
                        &format!("i.{}", table.value(j)),
                        &format!("i.{}", identifier(name))
                    )
                )
            })
            .collect();
        let refused = exists(
            db,
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {installed} AS i WHERE i.{audience_column}=?1 AND i.{present}=1 AND ({stored})
                   AND NOT EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=?2 AND l.key=i.{key} AND l.audience=i.{audience_column} AND l.frozen=0 AND l.replacement_kind='rules'))",
                installed = table.installed(),
                audience_column = table.column("audience"),
                present = table.column("present"),
                key = table.column("key"),
                stored = stored.join(" OR "),
            ),
            params![audience, table.table],
        )?;
        if refused {
            return Err(invalid("cell values disagree with their schema"));
        }
        progress.advance();
    }
    Ok(())
}

/// Install the snapshot's source records (D7): each part's header must be
/// the one the device holds, and an original the device holds must be the
/// same bytes. The snapshot's verdicts and stop slots replace the device's.
pub(crate) fn sources(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
    audience: &Audience,
) -> Result<(), DbError> {
    copy(
        db,
        prepared,
        &format!(
            "SELECT write_id,audience,header FROM {} ORDER BY write_id,audience",
            prepared.table("parts")
        ),
        "INSERT INTO temp._coven_install_parts(write_id,audience,header) VALUES(?1,?2,?3)",
    )?;
    copy(
        db,
        prepared,
        &format!(
            "SELECT write_id,audience,table_name,key,record,admission,stop FROM {} ORDER BY rowid",
            prepared.table("authored")
        ),
        "INSERT INTO temp._coven_install_authored(write_id,audience,table_name,key,record,admission,stop) VALUES(?1,?2,?3,?4,?5,?6,?7)",
    )?;
    if exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM temp._coven_install_parts s WHERE s.audience=?1
         AND EXISTS(SELECT 1 FROM _coven_authored_parts p WHERE p.write_id=s.write_id AND p.audience=s.audience AND p.header<>s.header))",
        params![audience],
    )? {
        return Err(invalid("original source header changed"));
    }
    if exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM temp._coven_install_authored s JOIN _coven_authored_changes c ON c.write_id=s.write_id AND c.audience=s.audience AND c.table_name=s.table_name AND c.key=s.key WHERE s.audience=?1 AND c.record<>s.record)",
        params![audience],
    )? {
        return Err(invalid("original source change changed"));
    }
    crate::write_sources::install(db, audience)
}

/// Whether the snapshot carries the source record of this change.
pub(crate) fn carried(
    db: &DatabaseConnection,
    write: &[u8],
    audience: &Audience,
    table: &str,
    key: &[u8],
) -> Result<bool, DbError> {
    exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM temp._coven_install_authored WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4)",
        params![write, audience, table, key],
    )
}

/// The audience's creations, after `ownership::clear_audience` removed its
/// earlier ones.
pub(crate) fn creations(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
) -> Result<(), DbError> {
    copy(
        db,
        prepared,
        &format!(
            "SELECT table_name,key,audience,write_id,creator FROM {} ORDER BY rowid",
            prepared.table("creations")
        ),
        crate::ownership::LOADED,
    )
}

/// The merged values of an installed row: each value its cells give.
pub(crate) fn values(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    row: &RowId,
) -> Result<AppValues, DbError> {
    let table = &schema.tables[&row.table.to_ascii_lowercase()];
    let layout = TableLayout::new(schema, table);
    let mut select = Vec::new();
    for j in 0..layout.columns.len() {
        select.push(layout.value(j));
        select.push(layout.setter_device(j));
    }
    db.query_row(
        &format!(
            "SELECT {} FROM {} WHERE {}=?1 AND {}=?2",
            select.join(","),
            layout.installed(),
            layout.column("key"),
            layout.column("audience")
        ),
        params![row.key, &row.audience],
        |r| {
            let mut values = AppValues::new();
            for (j, name) in layout.columns.iter().enumerate() {
                if r.get_ref(1 + 2 * j)? != rusqlite::types::ValueRef::Null {
                    values.insert(name.clone(), value(r.get_ref(2 * j)?)?);
                }
            }
            Ok(values)
        },
    )
}

/// The installed rows of `audience` holding `value` for the unique
/// constraint `unique`, as SQLite reads their merged values.
pub(crate) fn claim_holders(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    table: &crate::schema::TableSchema,
    unique: &crate::removal_schema::UniqueSql,
    audience: &Audience,
    value: &[coven_format::value::Value],
) -> Result<Vec<RowId>, DbError> {
    let layout = TableLayout::new(schema, table);
    let condition = unique
        .expressions
        .iter()
        .enumerate()
        .map(|(n, expression)| format!("({expression})=?{}", n + 2))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut parameters = vec![rusqlite::types::Value::from(audience)];
    parameters.extend(value.iter().map(sql_value));
    db.query(
        &format!(
            "SELECT {} FROM {} AS {} WHERE {}=?1 AND {}=1 AND ({}) AND {condition}",
            layout.column("key"),
            layout.installed(),
            identifier(&table.name),
            layout.column("audience"),
            layout.column("present"),
            unique.predicate
        ),
        params_from_iter(parameters),
        |r| Ok(RowId { table: table.name.clone(), key: r.get(0)?, audience: audience.clone() }),
    )
}

/// Each coordinates value of the audience's file records, as the snapshot
/// gives it.
pub(crate) fn file_locations(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    mut visit: impl FnMut(&coven_format::value::Value) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let Some(table) = SnapshotLayout::new(schema)
        .tables
        .into_iter()
        .find(|table| crate::file_table::is_files(&table.table))
    else {
        return Ok(());
    };
    let Some(j) = table
        .columns
        .iter()
        .position(|column| Some(column.as_str()) == crate::file_protection::location(&table.table))
    else {
        return Ok(());
    };
    db.for_each(
        &format!(
            "SELECT {} FROM {} WHERE {}=?1 AND {}=1",
            table.value(j),
            table.installed(),
            table.column("audience"),
            table.column("present"),
        ),
        params![audience],
        |r| visit(&value(r.get_ref(0)?)?),
    )
}

#[cfg(test)]
#[path = "snapshot_install_tests.rs"]
pub(crate) mod tests;
