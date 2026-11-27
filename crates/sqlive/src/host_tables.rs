//! Tables whose rows the host keeps in memory (SPEC.md section 3): facts that
//! last only as long as the process, which SQL reads as it reads a table.
//! Each is declared by a CREATE TABLE with a primary key. The host changes
//! rows by their keys, and sqlive judges each change for the waiting live
//! queries as it judges a write's change to a row.

#[path = "host_table_module.rs"]
mod module;

use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};

use panic_as_error::locks::lock_unpoisoned;
use sqlive_reads::{MainSchema, RowImage};

use crate::net_changes::{ColumnChange, NetRows, RowOperation};
use crate::snapshot_values::SnapshotValue;
use crate::{compare, RowChange, SqlValue};

/// One row of a host table, its values in the declared columns' order.
type Row = Arc<[SqlValue]>;

/// Every host table's rows, by the table's place in the declarations, each in
/// its key's order. A change makes new lists for the tables it changes, so a
/// transaction keeps the rows it took.
type Rows = Vec<Arc<Vec<Row>>>;

/// One declared table.
struct Declared {
    /// The name as the declaration spells it.
    name: String,
    /// The columns' names, lowercase, in order.
    columns: Arc<[String]>,
    /// The columns' names and declared types, as SQLite's declaration of the
    /// virtual table repeats them.
    declared: Vec<(String, String)>,
    /// The places of the primary key's columns among `columns`.
    key: Vec<usize>,
}

impl Declared {
    /// The values of `row`'s key columns.
    fn key_of(&self, row: &[SqlValue]) -> Vec<SqlValue> {
        self.key.iter().map(|&place| row[place].clone()).collect()
    }

    /// Where the row with `key` is among `rows`, or where it would go.
    fn find(&self, rows: &[Row], key: &[SqlValue]) -> Result<usize, usize> {
        rows.binary_search_by(|row| {
            self.key
                .iter()
                .zip(key)
                .map(|(&place, value)| {
                    compare(&row[place], value).expect("a host table's key holds no NULL")
                })
                .find(|order| order.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }
}

/// A change for the judge of the database the tables are attached to
/// (`host_judge.rs`): it judges `changes`, which the rows already hold,
/// tells the live queries of them, and then answers on `judged`. No write
/// judges or commits meanwhile.
pub(crate) struct JudgeRequest {
    pub(crate) changes: Vec<RowChange>,
    pub(crate) judged: mpsc::SyncSender<()>,
}

/// The way to the attached database's judge, or none while no database is
/// attached.
type JudgeChannel = Option<mpsc::Sender<JudgeRequest>>;

struct Shared {
    /// The tables as the scope planner reads them.
    schema: MainSchema,
    tables: Vec<Declared>,
    /// Shared apart from the rest, for each connection reading them to keep
    /// as long as it does.
    rows: Arc<Mutex<Rows>>,
    /// Orders changes to the rows against writes: a change holds it while
    /// it changes the rows and has the change judged and reported, and a
    /// write while it judges its own changes, commits and reports them.
    order: Mutex<()>,
    /// Where a change is judged. Attaching and detaching hold `order` too.
    judge: Mutex<JudgeChannel>,
}

/// The tables whose rows a host keeps in memory beside one database. Clones
/// share them.
#[derive(Clone)]
pub struct HostTables(Arc<Shared>);

impl HostTables {
    /// The tables `declarations` declare, each one `CREATE TABLE` with a
    /// primary key, all of them empty. Their key columns hold no NULL and
    /// compare as BINARY.
    pub fn new(declarations: &[&str]) -> crate::SqliteResult<Self> {
        let schema = MainSchema::of_host_tables(declarations)?;
        let mut names: Vec<String> = schema.host_table_names().map(str::to_owned).collect();
        names.sort();
        let tables: Vec<Declared> = names
            .into_iter()
            .map(|name| {
                let columns = schema.host_table_columns(&name).expect("the table was just listed");
                let key: Vec<usize> = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(place, (_, _, key))| key.then_some(place))
                    .collect();
                assert!(!key.is_empty(), "the host table {name} declares a primary key");
                Declared {
                    columns: columns
                        .iter()
                        .map(|(column, _, _)| column.to_ascii_lowercase())
                        .collect(),
                    declared: columns
                        .iter()
                        .map(|(column, declared, _)| ((*column).to_owned(), (*declared).to_owned()))
                        .collect(),
                    key,
                    name,
                }
            })
            .collect();
        let rows = tables.iter().map(|_| Arc::new(Vec::new())).collect();
        Ok(Self(Arc::new(Shared {
            schema,
            tables,
            rows: Arc::new(Mutex::new(rows)),
            order: Mutex::new(()),
            judge: Mutex::new(None),
        })))
    }

    /// The tables as the scope planner reads them.
    pub(crate) fn schema(&self) -> &MainSchema {
        &self.0.schema
    }

    /// Change rows with `edit`, all at once: a transaction reads every one of
    /// `edit`'s changes or none. sqlive judges the rows that end other than
    /// they began, and each live query they could change runs again. Changing
    /// rows writes nothing to the database. It waits while a write judges its
    /// changes and commits, and never while a write runs its statements. Don't
    /// call it from a thread that is judging, such as from a SQL function a
    /// lookup calls.
    pub fn change<R>(&self, edit: impl FnOnce(&mut HostRows<'_>) -> R) -> R {
        let _ordered = lock_unpoisoned(&self.0.order);
        let (result, changes) = {
            let mut rows = lock_unpoisoned(&self.0.rows);
            let mut edited =
                HostRows { tables: &self.0.tables, rows: rows.clone(), net: NetRows::default() };
            let result = edit(&mut edited);
            *rows = edited.rows;
            let mut changes = Vec::new();
            edited.net.finish(&mut changes);
            (result, changes)
        };
        if let (false, Some(judge)) = (changes.is_empty(), lock_unpoisoned(&self.0.judge).as_ref())
        {
            let (judged, answer) = mpsc::sync_channel(1);
            judge
                .send(JudgeRequest { changes, judged })
                .expect("the judge serves while the tables are attached");
            answer.recv().expect("the judge answers each change");
        }
        result
    }

    /// Keep the rows from changing while the returned guard lives, for a
    /// write to judge its changes against them, commit and report them.
    pub(crate) fn hold(&self) -> HeldHostRows<'_> {
        HeldHostRows { _order: lock_unpoisoned(&self.0.order) }
    }

    /// Send changes to `judge` from now on.
    pub(crate) fn attach(&self, judge: mpsc::Sender<JudgeRequest>) {
        let _ordered = lock_unpoisoned(&self.0.order);
        let previous = lock_unpoisoned(&self.0.judge).replace(judge);
        assert!(previous.is_none(), "host tables serve one database");
    }

    /// Stop judging changes, closing the channel to the judge, once no
    /// change or write holds it.
    pub(crate) fn detach(&self) {
        let _ordered = lock_unpoisoned(&self.0.order);
        drop(lock_unpoisoned(&self.0.judge).take());
    }

    /// The rows as they are now, for a connection's transactions to take.
    pub(crate) fn now(&self) -> impl Fn() -> Rows + Send + Sync + 'static {
        let rows = Arc::clone(&self.0.rows);
        move || lock_unpoisoned(&rows).clone()
    }
}

/// Register each of `tables` on `connection` as a virtual table of its name,
/// which reads the rows `taken` gives.
pub(crate) fn register(
    tables: &HostTables,
    connection: &rusqlite::Connection,
    taken: &SnapshotValue<Rows>,
) -> rusqlite::Result<()> {
    for (place, table) in tables.0.tables.iter().enumerate() {
        module::register(connection, &table.name, &table.declared, place, taken.clone())?;
    }
    Ok(())
}

/// The rows stay as they are while this lives (`HostTables::hold`).
pub(crate) struct HeldHostRows<'a> {
    _order: MutexGuard<'a, ()>,
}

/// The rows of the host tables, as one change edits them.
pub struct HostRows<'a> {
    tables: &'a [Declared],
    rows: Rows,
    net: NetRows,
}

impl HostRows<'_> {
    /// Add `row`, the values of `table`'s columns in order, in place of the
    /// row with its key if there is one, and say whether there was one.
    ///
    /// Panics when no host table is named `table`, when `row` holds another
    /// number of values than the table has columns, or when its key holds a
    /// NULL.
    pub fn put(&mut self, table: &str, row: Vec<SqlValue>) -> bool {
        let place = self.place(table);
        let declared = &self.tables[place];
        assert_eq!(row.len(), declared.columns.len(), "one value per column of {table}");
        let key = declared.key_of(&row);
        assert!(!key.contains(&SqlValue::Null), "a key of {table} holds no NULL");
        let row: Row = row.into();
        let rows = Arc::make_mut(&mut self.rows[place]);
        let before = match declared.find(rows, &key) {
            Ok(found) => Some(std::mem::replace(&mut rows[found], Arc::clone(&row))),
            Err(free) => {
                rows.insert(free, Arc::clone(&row));
                None
            }
        };
        let replaced = before.is_some();
        self.record(place, key, before, Some(row));
        replaced
    }

    /// Remove the row of `table` with `key`, the values of its primary key's
    /// columns in the order the table declares those columns, if there is
    /// one.
    ///
    /// Panics when no host table is named `table`.
    pub fn delete(&mut self, table: &str, key: &[SqlValue]) {
        let place = self.place(table);
        let declared = &self.tables[place];
        let Ok(found) = declared.find(&self.rows[place], key) else { return };
        let before = Arc::make_mut(&mut self.rows[place]).remove(found);
        self.record(place, key.to_vec(), Some(before), None);
    }

    fn place(&self, table: &str) -> usize {
        self.tables
            .iter()
            .position(|declared| declared.name.eq_ignore_ascii_case(table))
            .unwrap_or_else(|| panic!("no host table is named {table}"))
    }

    /// Record the operation that left the row with `key` of the table at
    /// `place` as `after`, from `before`.
    fn record(
        &mut self,
        place: usize,
        key: Vec<SqlValue>,
        before: Option<Row>,
        after: Option<Row>,
    ) {
        let declared = &self.tables[place];
        let image = |row: &Option<Row>| {
            row.as_ref().map(|row| {
                RowImage::new(
                    Arc::clone(&declared.columns),
                    row.iter().map(|value| Some(value.clone())).collect(),
                )
            })
        };
        let value = |row: &Option<Row>, column: usize| row.as_ref().map(|row| row[column].clone());
        let columns = declared
            .columns
            .iter()
            .enumerate()
            .filter(|(column, _)| !declared.key.contains(column))
            .map(|(column, name)| {
                ColumnChange::new(name.clone(), true, value(&before, column), value(&after, column))
            })
            .collect();
        self.net.record(RowOperation {
            table: declared.name.to_ascii_lowercase(),
            key,
            before: image(&before),
            after: image(&after),
            columns,
            generated: Arc::new([]),
        });
    }
}

#[cfg(test)]
#[path = "host_tables_tests.rs"]
mod tests;
