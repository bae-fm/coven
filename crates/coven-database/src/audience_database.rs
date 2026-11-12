//! A database of its own that holds some audiences' rows at an earlier
//! version, and the check that runs a breaking migration over it (§18.3).
//!
//! The database holds the app's schema as it was, the rows of the store and
//! of each circle the device reads, and none of the device's local tables or
//! coven state, so a migration run there can read only what an audience
//! holds. Running the migrations there once per audience and comparing the
//! result with the real run's finds every read across audiences.

use crate::authorization::SqlAuthorization;
use crate::migration_snapshot::{key_sql, MigrationCapture};
use crate::schema::{Schema, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, value};
use crate::write_rows::{column_list, key_columns, AppValues, AppView};
use crate::write_schema::WriteSchema;
use crate::{DbError, Migration, MigrationContext, SyncedTable};
use coven_foundation::files::{StoreDir, StoreDirAccess};
use coven_merge::Audience;
use rusqlite::params_from_iter;
use rusqlite::types::Value as SqlValue;
use std::collections::{BTreeMap, BTreeSet};

/// Stages the database for a run from the live connection, whose schema and
/// rows it copies.
pub(crate) type Stage<'a> = dyn Fn(&DatabaseConnection) -> Result<AudienceDatabase, DbError> + 'a;

/// The app's rows before a run, split by audience, in a database of their
/// own: the store's in its tables, each circle's in staging tables until its
/// run.
pub(crate) struct AudienceDatabase {
    /// None only while dropping, which closes the connection before the
    /// files go.
    database: Option<DatabaseConnection>,
    directory: StoreDir,
    circles: BTreeSet<Audience>,
    /// The staged tables: each one's name, its staging table and its columns.
    staged: Vec<Staged>,
    /// The app's triggers by name and creating SQL: none fires on a copied
    /// row, so a run takes them out while it inserts its circle's rows.
    triggers: Vec<(String, String)>,
}

struct Staged {
    table: String,
    staging: String,
    columns: Vec<String>,
}

impl AudienceDatabase {
    /// Copy the schema `before` and its synced rows out of `live`: tables
    /// first, then the rows, then the indexes, views and triggers, so no
    /// trigger fires on a copied row.
    pub(crate) fn stage(
        directory: &StoreDir,
        sqlite: &crate::SqliteSettings,
        functions: &crate::SqlFunctions,
        observer: &crate::observation::CommitObserver,
        device: coven_foundation::id_source::DeviceId,
        tables: &[SyncedTable],
        live: &DatabaseConnection,
    ) -> Result<Self, DbError> {
        directory.remove_audience_database()?;
        let before = Schema::read(live, &crate::file_table::registered_columns(live)?)?;
        let database = DatabaseConnection::open(
            &directory.audience_database_path(),
            false,
            SqlAuthorization::new(tables),
            sqlite,
            functions,
        )?;
        // From here every way out removes the files.
        let mut result = Self {
            database: Some(database),
            directory: directory.clone(),
            circles: BTreeSet::new(),
            staged: Vec::new(),
            triggers: before.triggers(),
        };
        let transfers = crate::transfers_in_flight::TransfersInFlight::new(observer.clone());
        let network = crate::network_facts::NetworkFacts::new(observer.clone());
        let database = result.database.as_mut().expect("open");
        database.serve_views(device, &transfers, &network)?;
        database.batch(&crate::internal_schema::initial_schema())?;
        let synced = MigrationCapture::new(live, tables)?.synced().clone();
        let before_run = synced_before(live)?;
        let (mut circles, mut staged) = (BTreeSet::new(), Vec::new());
        database.transaction(|scratch| {
            scratch.batch("PRAGMA defer_foreign_keys=ON")?;
            for sql in before.create_sql("table") {
                scratch.batch(sql)?;
            }
            for table in before.tables.values() {
                let name = table.name.to_ascii_lowercase();
                // Coven's files table is synced, and a migration may read it
                // (a join to a file's size), so its rows are copied like any.
                if !(crate::file_table::is_files(&table.name) || synced.contains(&name))
                    || key_columns(table).is_empty()
                {
                    continue;
                }
                let synced_before_run =
                    crate::file_table::is_files(&table.name) || before_run.contains(&name);
                copy_rows(live, scratch, table, synced_before_run, &mut circles, &mut staged)?;
            }
            for kind in ["index", "view", "trigger"] {
                for sql in before.create_sql(kind) {
                    scratch.batch(sql)?;
                }
            }
            Ok(())
        })?;
        result.circles = circles;
        result.staged = staged;
        Ok(result)
    }
}

impl Drop for AudienceDatabase {
    fn drop(&mut self) {
        // An open file cannot be removed on every platform.
        drop(self.database.take());
        // A file left behind is space no longer used: the next check replaces it.
        self.directory.remove_audience_database().ok();
    }
}

/// Run `migrations` over the store's rows alone, then over each circle's rows
/// with the store's, and compare each audience's rows, and the store's, with
/// `live`'s after the real run. The first table whose rows differ is
/// `MigrationReadsAcross`.
pub(crate) fn verify(
    audiences: &mut AudienceDatabase,
    live: &DatabaseConnection,
    migrations: &[&Migration],
    tables: &[SyncedTable],
) -> Result<(), DbError> {
    let schema = WriteSchema::read(live, tables.to_vec())?;
    let view = AppView::after(live, &schema);
    // Coven's files table is the one synced table no migration changes
    // (§18.7), so there is nothing in it to compare; its rows are copied so a
    // migration can read them.
    let compared: Vec<&TableSchema> = schema
        .declarations
        .iter()
        .map(|declaration| schema.table(&declaration.name))
        .filter(|table| {
            table.name != crate::internal_schema::FILES && !key_columns(table).is_empty()
        })
        .collect();
    let runs: Vec<Audience> =
        std::iter::once(Audience::Store).chain(audiences.circles.iter().cloned()).collect();
    let staged = &audiences.staged;
    let triggers = &audiences.triggers;
    let difference = audiences.database.as_mut().expect("open").transaction(|scratch| {
        for run in &runs {
            scratch.batch("SAVEPOINT reads_across")?;
            if *run != Audience::Store {
                // The store's rows were copied before the app's triggers
                // existed; the circle's go in the same way, so none fires on
                // a copied row. The savepoint's rollback restores them.
                for (name, _) in triggers {
                    scratch.batch(&format!("DROP TRIGGER main.{}", identifier(name)))?;
                }
                for stage in staged {
                    let columns = stage.columns.iter().map(|c| identifier(c)).collect::<Vec<_>>().join(",");
                    scratch.internal_execute(
                        &format!(
                            "INSERT INTO main.{t}({columns}) SELECT {columns} FROM temp.{s} WHERE coven_audience=?1",
                            t = identifier(&stage.table),
                            s = identifier(&stage.staging),
                        ),
                        [run.to_string()],
                    )?;
                }
                for (_, sql) in triggers {
                    scratch.batch(sql)?;
                }
            }
            run_migrations(scratch, migrations, tables)?;
            let mut difference = None;
            // One table's rows at a time, so a large library holds no more
            // than its largest table.
            for table in &compared {
                let live_rows = read_live(live, &view, table)?;
                let scratch_rows = read_table(scratch, table)?;
                if let Some(audience) = differing(run, &live_rows, &scratch_rows) {
                    difference = Some((table.name.clone(), audience));
                    break;
                }
            }
            scratch.batch("ROLLBACK TO reads_across; RELEASE reads_across")?;
            if difference.is_some() {
                return Ok(difference);
            }
        }
        Ok(None)
    })?;
    match difference {
        Some((table, audience)) => Err(DbError::MigrationReadsAcross { table, audience }),
        None => Ok(()),
    }
}

/// The audience whose rows differ between the real run's rows and a run over
/// `run`'s rows and the store's, if any: a row either run holds that the other
/// does not, or holds with other values.
fn differing(
    run: &Audience,
    live: &BTreeMap<Vec<u8>, (AppValues, Audience)>,
    scratch: &BTreeMap<Vec<u8>, AppValues>,
) -> Option<Audience> {
    for (key, values) in scratch {
        match live.get(key) {
            Some((live_values, _)) if live_values == values => {}
            Some((_, audience)) => return Some(audience.clone()),
            None => return Some(run.clone()),
        }
    }
    for (key, (values, audience)) in live {
        if (*audience == Audience::Store || audience == run) && scratch.get(key) != Some(values) {
            return Some(audience.clone());
        }
    }
    None
}

fn run_migrations(
    scratch: &DatabaseConnection,
    migrations: &[&Migration],
    tables: &[SyncedTable],
) -> Result<(), DbError> {
    let mut before = Schema::read(scratch, &[])?;
    let mut capture = MigrationCapture::new(scratch, tables)?;
    for migration in migrations {
        let context = MigrationContext::new(scratch, before, capture);
        scratch.migrating(|| migration.apply(&context))?;
        let after = Schema::read(scratch, &[])?;
        let (_, _, next) = context.finish(&after);
        capture = next;
        before = after;
    }
    capture.discard()
}

/// A synced table's rows in `live` by key, each with its audience.
fn read_live(
    live: &DatabaseConnection,
    view: &AppView<'_>,
    table: &TableSchema,
) -> Result<BTreeMap<Vec<u8>, (AppValues, Audience)>, DbError> {
    let mut rows = BTreeMap::new();
    for (key, values) in read_table(live, table)? {
        let audience = view.audience_of(table, &values)?;
        rows.insert(key, (values, audience));
    }
    Ok(rows)
}

/// A synced table's rows by key.
fn read_table(
    database: &DatabaseConnection,
    table: &TableSchema,
) -> Result<BTreeMap<Vec<u8>, AppValues>, DbError> {
    let sql = format!(
        "SELECT {},{} FROM main.{}",
        key_sql(table, ""),
        column_list(table),
        identifier(&table.name)
    );
    let mut rows = BTreeMap::new();
    database.for_each::<_, DbError>(&sql, [], |r| {
        let key: Vec<u8> = r.get(0)?;
        let values = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| Ok((c.name.clone(), value(r.get_ref(i + 1)?)?)))
            .collect::<rusqlite::Result<AppValues>>()?;
        rows.insert(key, values);
        Ok(())
    })?;
    Ok(rows)
}

/// Copy `table`'s rows from `live`: the store's into `scratch`'s table, a
/// circle's into a staging table with its audience.
///
/// Every write that makes a synced row records it (a write, a received
/// change, an installed snapshot, a migration's own changes), so a row of a
/// table that synced before this run and has no merge record is damage:
/// `UnrecordedRow`, never a guessed audience. The one table whose rows can
/// have none is one that was local before and is synced from this run on
/// (`synced_before_run` is false): its rows belong to this device alone and
/// every run holds them, as it holds the store's.
fn copy_rows(
    live: &DatabaseConnection,
    scratch: &DatabaseConnection,
    table: &TableSchema,
    synced_before_run: bool,
    circles: &mut BTreeSet<Audience>,
    staged: &mut Vec<Staged>,
) -> Result<(), DbError> {
    let audiences = row_audiences(live, &table.name)?;
    let columns: Vec<String> = table.synced_columns().map(|c| c.name.clone()).collect();
    let list = columns.iter().map(|c| identifier(c)).collect::<Vec<_>>().join(",");
    let staging = format!("_coven_audience_stage_{}", staged.len());
    let mut stage = None;
    let insert_store = format!(
        "INSERT INTO main.{}({list}) VALUES({})",
        identifier(&table.name),
        vec!["?"; columns.len()].join(",")
    );
    let sql = format!("SELECT {},{list} FROM main.{}", key_sql(table, ""), identifier(&table.name));
    live.for_each::<_, DbError>(&sql, [], |r| {
        let key: Vec<u8> = r.get(0)?;
        let values = (0..columns.len())
            .map(|i| r.get::<_, SqlValue>(i + 1))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let audience = match audiences.get(&key) {
            Some(audience) => audience.clone(),
            None if !synced_before_run => Audience::Store,
            None => return Err(DbError::UnrecordedRow { table: table.name.clone() }),
        };
        match audience {
            Audience::Store => {
                scratch.internal_execute(&insert_store, params_from_iter(values))?;
            }
            circle => {
                if stage.is_none() {
                    scratch.batch(&format!(
                        "CREATE TEMP TABLE {}(coven_audience TEXT NOT NULL,{})",
                        identifier(&staging),
                        columns.iter().map(|c| identifier(c)).collect::<Vec<_>>().join(",")
                    ))?;
                    stage = Some(format!(
                        "INSERT INTO temp.{}(coven_audience,{list}) VALUES(?,{})",
                        identifier(&staging),
                        vec!["?"; columns.len()].join(",")
                    ));
                }
                let mut parameters = vec![SqlValue::Text(circle.to_string())];
                parameters.extend(values);
                scratch.internal_execute(
                    stage.as_ref().expect("staging table made"),
                    params_from_iter(parameters),
                )?;
                circles.insert(circle);
            }
        }
        Ok(())
    })?;
    if stage.is_some() {
        staged.push(Staged { table: table.name.clone(), staging, columns });
    }
    Ok(())
}

/// The tables whose columns the library recorded as synced before this run, lowercase.
fn synced_before(live: &DatabaseConnection) -> Result<BTreeSet<String>, DbError> {
    Ok(live
        .query("SELECT DISTINCT lower(table_name) FROM _coven_columns WHERE synced=1", [], |r| {
            r.get(0)
        })?
        .into_iter()
        .collect())
}

/// The audience of each row of `table` that is present, by key.
fn row_audiences(
    live: &DatabaseConnection,
    table: &str,
) -> Result<BTreeMap<Vec<u8>, Audience>, DbError> {
    let rows = live.query(
        &format!(
            "SELECT r.key,r.audience,r.generation FROM _coven_rows r WHERE r.table_name=?1 AND NOT EXISTS(SELECT 1 FROM _coven_rows newer WHERE newer.table_name=r.table_name AND newer.key=r.key AND newer.audience=r.audience AND newer.generation>r.generation) AND NOT {}",
            crate::internal_schema::RULES_REMOVED_SQL
        ),
        [table],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                audience(r, 1)?,
                crate::write_encoding::counter(r.get(2)?)?,
            ))
        },
    )?;
    Ok(rows
        .into_iter()
        .filter(|(_, _, generation)| coven_merge::generation_present(*generation))
        .map(|(key, audience, _)| (key, audience))
        .collect())
}

#[cfg(test)]
#[path = "audience_database_tests.rs"]
mod tests;
