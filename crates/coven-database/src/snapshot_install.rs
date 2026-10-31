//! Install a staged snapshot's rows on the writer: copy each staged row from
//! the input connection once, check the copies with SQL, and write the merge
//! records from them one table at a time (§16.2, §21.2).

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::snapshot_error::invalid;
use crate::snapshot_layout::{SnapshotLayout, Staged, TableLayout};
use crate::snapshot_preparation::PreparedSnapshot;
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, sql_value, value};
use crate::write_rows::AppValues;
use crate::write_schema::WriteSchema;
use crate::{DbError, LoadStep};
use coven_foundation::progress::ProgressReport;
use coven_merge::{Audience, RowId};
use rusqlite::types::ToSqlOutput;
use rusqlite::{params, params_from_iter};

/// The passes installing makes over each snapshot's tables, one table at a
/// time: `copy_rows`, `check_synced`, `compare_rows`, `check_merged`,
/// `persist_rows` and `finish`. Each table counts once in each pass toward
/// the `Installing` step (§13.4).
pub(crate) const TABLE_PASSES: u64 = 6;

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
         CREATE TEMP TABLE _coven_install_parts(write_id BLOB NOT NULL,audience TEXT NOT NULL,header BLOB NOT NULL,complete INTEGER NOT NULL,PRIMARY KEY(write_id,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_authored(write_id BLOB NOT NULL,audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,record BLOB NOT NULL,admission BLOB,PRIMARY KEY(write_id,audience,table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_before(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,shown INTEGER NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_conflicts(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_region(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_changed(table_name TEXT NOT NULL,key BLOB NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(table_name,key,kind)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_lost(table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_protected(table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_references(file TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(file,table_name,key,column_name)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_stored(device BLOB NOT NULL,file BLOB NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,PRIMARY KEY(device,file,table_name,key)) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_released(file TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID;
         CREATE TEMP TABLE _coven_install_records(file TEXT PRIMARY KEY NOT NULL,namespace TEXT,cache_id TEXT) WITHOUT ROWID;",
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

/// A typed comparison of two SQL values: the same storage class and the same
/// bytes, as `Value` compares them.
fn same(a: &str, b: &str) -> String {
    format!("(typeof({a})=typeof({b}) AND {a} IS {b} COLLATE BINARY)")
}

/// Whether the row's synced record, in the table `alias` names, has
/// column `j`.
fn synced_has(layout: &TableLayout, alias: &str, j: usize, has: bool) -> String {
    format!("substr({alias}{},{},1)='{}'", layout.column("synced"), j + 1, if has { 1 } else { 0 })
}

/// Copy the snapshot's rows and earlier generations: each row's synced
/// record and merge record, staged apart, become one row. Its app columns
/// are given as SQLite stores them: a column its synced record has, or with
/// no synced record a column its merge record sets, takes the staged value;
/// any other takes the column's default.
pub(crate) fn copy_rows(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
    layout: &SnapshotLayout,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    for table in &layout.tables {
        let names = table.names();
        let mut joined: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(at, name)| match at {
                TableLayout::KEY | TableLayout::AUDIENCE => format!("coalesce(m.{name},s.{name})"),
                TableLayout::SYNCED => format!("s.{name}"),
                TableLayout::MERGED | TableLayout::PRESENT => format!("coalesce(m.{name},0)"),
                TableLayout::KEYED => format!("coalesce(s.{name},1)"),
                _ => format!("m.{name}"),
            })
            .collect();
        for j in 0..table.columns.len() {
            joined[table.value_at(j)] = format!(
                "CASE WHEN {} THEN s.{value} ELSE m.{value} END",
                synced_has(table, "s.", j, true),
                value = table.value(j)
            );
        }
        let synced = TableLayout::SYNCED + 1;
        let mut columns = names.clone();
        let mut values: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
        for (j, name) in table.columns.iter().enumerate() {
            columns.push(identifier(name));
            values.push(format!(
                "CASE WHEN substr(?{synced},{},1)='1' OR (?{synced} IS NULL AND ?{} IS NOT NULL) THEN ?{} ELSE {} END",
                j + 1,
                table.setter_at(j) + 1,
                table.value_at(j) + 1,
                table.defaults[j].as_deref().unwrap_or("NULL")
            ));
        }
        copy(
            db,
            prepared,
            &format!(
                "SELECT {} FROM {} AS m FULL JOIN {} AS s ON s.{key}=m.{key} AND s.{audience}=m.{audience} ORDER BY 1,2",
                joined.join(","),
                table.staged(prepared.staging, Staged::Merged),
                table.staged(prepared.staging, Staged::Synced),
                key = table.column("key"),
                audience = table.column("audience"),
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

/// A merge row against the synced row staged for it, on the input
/// connection: a deleted row has none, and each cell the synced row has is
/// its value and parents. A cell the synced row lacks has no parents; its
/// value is checked against the column's default once SQLite gives it.
pub(crate) fn compare_rows(
    prepared: &PreparedSnapshot<'_>,
    layout: &SnapshotLayout,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    let exists = |sql: String| {
        let mut found = false;
        prepared.each(&sql, [], |r| {
            found = r.get(0)?;
            Ok(())
        })?;
        Ok::<_, DbError>(found)
    };
    for table in &layout.tables {
        let joined = format!(
            "{} AS m JOIN {} AS s ON s.{key}=m.{key} AND s.{audience}=m.{audience}",
            table.staged(prepared.staging, Staged::Merged),
            table.staged(prepared.staging, Staged::Synced),
            key = table.column("key"),
            audience = table.column("audience"),
        );
        if exists(format!(
            "SELECT EXISTS(SELECT 1 FROM {joined} WHERE m.{}=0)",
            table.column("present")
        ))? {
            return Err(invalid("deleted merge row also has a synced row"));
        }
        let mut disagree = Vec::new();
        for j in 0..table.columns.len() {
            let pairs: Vec<_> = table
                .pairs
                .iter()
                .enumerate()
                .filter(|(_, (column, _))| *column == j)
                .map(|(q, _)| q)
                .collect();
            let mut differs = vec![format!(
                "NOT {}",
                same(&format!("s.{}", table.value(j)), &format!("m.{}", table.value(j)))
            )];
            differs.extend(pairs.iter().map(|q| {
                let parent = |column: String| format!("s.{column} IS m.{column}");
                format!(
                    "NOT ({} AND {} AND {})",
                    parent(table.parent_key(*q)),
                    parent(table.parent_audience(*q)),
                    parent(table.parent_generation(*q))
                )
            }));
            let mut parented: Vec<_> =
                pairs.iter().map(|q| format!("m.{} IS NOT NULL", table.parent_key(*q))).collect();
            parented.push("0".into());
            disagree.push(format!(
                "(m.{} IS NOT NULL AND (({} AND ({})) OR ({} AND ({}))))",
                table.setter_device(j),
                synced_has(table, "s.", j, true),
                differs.join(" OR "),
                synced_has(table, "s.", j, false),
                parented.join(" OR ")
            ));
        }
        if exists(format!("SELECT EXISTS(SELECT 1 FROM {joined} WHERE {})", disagree.join(" OR ")))?
        {
            return Err(invalid("synced and merge values disagree"));
        }
        progress.advance();
    }
    Ok(())
}

/// The synced rows' checks that need SQLite's own reading of the values:
/// each value is already what SQLite stores, and gives the row's key.
pub(crate) fn check_synced(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    layout: &SnapshotLayout,
    audience: &Audience,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    let audience = audience_text(audience);
    for table in &layout.tables {
        let installed = table.installed();
        let schema_table = schema.table(&table.table);
        let stored: Vec<_> = (0..table.columns.len())
            .map(|j| {
                format!(
                    "({} AND NOT {})",
                    synced_has(table, "", j, true),
                    same(&table.value(j), &identifier(&table.columns[j]))
                )
            })
            .collect();
        if exists(
            db,
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {installed} WHERE {}=?1 AND {} IS NOT NULL AND ({}))",
                table.column("audience"),
                table.column("synced"),
                stored.join(" OR ")
            ),
            params![audience],
        )? {
            return Err(invalid("synced values disagree with their key or schema"));
        }
        let keys = crate::write_rows::key_columns(schema_table);
        for (key, values) in db.query(
            &format!(
                "SELECT {},{} FROM {installed} WHERE {}=?1 AND {} IS NOT NULL AND {}=0",
                table.column("key"),
                keys.iter().map(|c| identifier(&c.name)).collect::<Vec<_>>().join(","),
                table.column("audience"),
                table.column("synced"),
                table.column("keyed")
            ),
            params![audience],
            |r| {
                let values: AppValues = keys
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Ok((c.name.clone(), value(r.get_ref(i + 1)?)?)))
                    .collect::<rusqlite::Result<_>>()?;
                Ok((r.get::<_, Vec<u8>>(0)?, values))
            },
        )? {
            if crate::write_rows::row_key(schema_table, &values)? != key {
                return Err(invalid("synced values disagree with their key or schema"));
            }
        }
        progress.advance();
    }
    Ok(())
}

/// The merge rows' checks against their synced rows that need SQLite's
/// own reading of the values: a cell on a column its synced row lacks holds
/// that column's default, and so does a column the synced row has but no
/// cell sets.
pub(crate) fn check_merged(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    layout: &SnapshotLayout,
    audience: &Audience,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    let audience = audience_text(audience);
    for table in &layout.tables {
        let installed = table.installed();
        let schema_table = schema.table(&table.table);
        let defaulted: Vec<_> = (0..table.columns.len())
            .map(|j| {
                format!(
                    "({} AND {} IS NOT NULL AND NOT {})",
                    synced_has(table, "", j, false),
                    table.setter_device(j),
                    same(&table.value(j), &identifier(&table.columns[j]))
                )
            })
            .collect();
        if exists(
            db,
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {installed} WHERE {}=?1 AND {} IS NOT NULL AND {}=1 AND ({}))",
                table.column("audience"),
                table.column("synced"),
                table.column("merged"),
                defaulted.join(" OR ")
            ),
            params![audience],
        )? {
            return Err(invalid("synced and merge values disagree"));
        }
        let defaults = crate::removal_sql::evaluate_values(db, schema_table, &AppValues::new())?;
        let mut parameters = vec![rusqlite::types::Value::from(audience.clone())];
        let mut unset = Vec::new();
        for (j, name) in table.columns.iter().enumerate() {
            parameters.push(sql_value(&defaults[name]));
            unset.push(format!(
                "({} AND {} IS NULL AND NOT {})",
                synced_has(table, "", j, true),
                table.setter_device(j),
                same(&identifier(name), &format!("?{}", parameters.len()))
            ));
        }
        if exists(
            db,
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {installed} WHERE {}=?1 AND {} IS NOT NULL AND {}=1 AND ({}))",
                table.column("audience"),
                table.column("synced"),
                table.column("merged"),
                unset.join(" OR ")
            ),
            params_from_iter(parameters),
        )? {
            return Err(invalid("synced values disagree with merge defaults"));
        }
        progress.advance();
    }
    Ok(())
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
    let audience = audience_text(audience);
    db.internal_execute(
        "INSERT INTO _coven_rows(table_name,key,audience,generation,write_id)
         SELECT g.table_name,g.key,g.audience,g.generation,
           (SELECT id FROM _coven_writes WHERE substr(timestamp,9,8)=g.device AND number=g.number)
         FROM temp._coven_install_generations AS g WHERE g.audience=?1",
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
                 FROM {installed} AS i WHERE i.{audience_column}=?1 AND i.{merged}=1 ORDER BY i.{key}",
                device = table.column("device"),
                number = table.column("number"),
                merged = table.column("merged"),
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

/// Every synced row has a merge record, and a merge row present without a
/// synced row is one the snapshot's author took out, with a live rules loss
/// that a synced row doesn't have.
pub(crate) fn finish(
    db: &DatabaseConnection,
    layout: &SnapshotLayout,
    audience: &Audience,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<(), DbError> {
    let audience = audience_text(audience);
    for table in &layout.tables {
        let unmatched = exists(
            db,
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {installed} AS i WHERE i.{audience_column}=?1 AND (
                   (i.{synced} IS NOT NULL AND i.{merged}=0)
                   OR ((i.{synced} IS NOT NULL OR i.{present}=1)
                       AND (i.{synced} IS NULL) != EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=?2 AND l.key=i.{key} AND l.audience=i.{audience_column} AND l.frozen=0 AND l.replacement_kind='rules'))))",
                installed = table.installed(),
                audience_column = table.column("audience"),
                synced = table.column("synced"),
                merged = table.column("merged"),
                present = table.column("present"),
                key = table.column("key"),
            ),
            params![audience, table.table],
        )?;
        if unmatched {
            return Err(invalid("synced row has no merge record"));
        }
        progress.advance();
    }
    Ok(())
}

/// Install the snapshot's source records (D7): each part's header must be
/// the one the device holds, an original the device holds must be the same
/// bytes, and a part the device holds whole gains no change. The snapshot's
/// verdicts replace the device's.
pub(crate) fn sources(
    db: &DatabaseConnection,
    prepared: &PreparedSnapshot<'_>,
    audience: &Audience,
) -> Result<(), DbError> {
    copy(
        db,
        prepared,
        &format!(
            "SELECT write_id,audience,header,complete FROM {} ORDER BY write_id,audience",
            prepared.table("parts")
        ),
        "INSERT INTO temp._coven_install_parts(write_id,audience,header,complete) VALUES(?1,?2,?3,?4)",
    )?;
    copy(
        db,
        prepared,
        &format!(
            "SELECT write_id,audience,table_name,key,record,admission FROM {} ORDER BY rowid",
            prepared.table("authored")
        ),
        "INSERT INTO temp._coven_install_authored(write_id,audience,table_name,key,record,admission) VALUES(?1,?2,?3,?4,?5,?6)",
    )?;
    let audience = audience_text(audience);
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
        "SELECT EXISTS(SELECT 1 FROM temp._coven_install_authored s WHERE s.audience=?1
         AND EXISTS(SELECT 1 FROM _coven_authored_parts p WHERE p.write_id=s.write_id AND p.audience=s.audience AND p.complete=1)
         AND NOT EXISTS(SELECT 1 FROM _coven_authored_changes c WHERE c.write_id=s.write_id AND c.audience=s.audience AND c.table_name=s.table_name AND c.key=s.key))",
        params![audience],
    )? {
        return Err(invalid("original source part changed"));
    }
    if exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM temp._coven_install_authored s JOIN _coven_authored_changes c ON c.write_id=s.write_id AND c.audience=s.audience AND c.table_name=s.table_name AND c.key=s.key WHERE s.audience=?1 AND c.record<>s.record)",
        params![audience],
    )? {
        return Err(invalid("original source change changed"));
    }
    db.internal_execute(
        "INSERT INTO _coven_authored_parts(write_id,audience,header,complete)
         SELECT write_id,audience,header,0 FROM temp._coven_install_parts s WHERE s.audience=?1
         AND NOT EXISTS(SELECT 1 FROM _coven_authored_parts p WHERE p.write_id=s.write_id AND p.audience=s.audience)",
        params![audience],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_authored_changes(write_id,audience,table_name,key,record,admission)
         SELECT write_id,audience,table_name,key,record,admission FROM temp._coven_install_authored WHERE audience=?1
         ON CONFLICT(write_id,audience,table_name,key) DO UPDATE SET admission=excluded.admission",
        params![audience],
    )?;
    Ok(())
}

/// Mark the parts the snapshot holds whole, once all their changes are in.
pub(crate) fn complete_parts(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_authored_parts SET complete=1 WHERE complete=0 AND (write_id,audience) IN (
           SELECT write_id,audience FROM temp._coven_install_parts WHERE audience=?1 AND complete=1)",
        params![audience_text(audience)],
    )?;
    Ok(())
}

/// Whether the snapshot carries the source record of this change.
pub(crate) fn carried(
    db: &DatabaseConnection,
    write: &[u8],
    audience: &str,
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
        "INSERT INTO _coven_creations(table_name,key,audience,write_id,creator) VALUES(?1,?2,?3,?4,?5)",
    )
}

/// The merged values of an installed row: each value its synced record or
/// its merge cells give.
pub(crate) fn values(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    row: &RowId,
) -> Result<AppValues, DbError> {
    let table = &schema.tables[&row.table.to_ascii_lowercase()];
    let layout = TableLayout::new(schema, table);
    let mut select = vec![layout.column("synced")];
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
        params![row.key, audience_text(&row.audience)],
        |r| {
            let synced: Option<String> = r.get(0)?;
            let mut values = AppValues::new();
            for (j, name) in layout.columns.iter().enumerate() {
                let has = match &synced {
                    Some(synced) => synced.as_bytes()[j] == b'1',
                    None => false,
                } || r.get_ref(2 + 2 * j)? != rusqlite::types::ValueRef::Null;
                if has {
                    values.insert(name.clone(), value(r.get_ref(1 + 2 * j)?)?);
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
    let mut parameters = vec![rusqlite::types::Value::from(audience_text(audience))];
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
            "SELECT CASE WHEN {synced} IS NOT NULL AND {} THEN {} ELSE {} END FROM {} WHERE {}=?1 AND ({synced} IS NOT NULL OR {}=1)",
            synced_has(&table, "", j, false),
            identifier(&table.columns[j]),
            table.value(j),
            table.installed(),
            table.column("audience"),
            table.column("present"),
            synced = table.column("synced"),
        ),
        params![audience_text(audience)],
        |r| visit(&value(r.get_ref(0)?)?),
    )
}
