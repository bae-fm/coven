//! The virtual table module through which SQLite reads one host table's
//! rows: read-only, and eponymous, so SQL names it as it names a table and
//! `sqlite_schema` lists nothing.

use std::borrow::Cow;
use std::ffi::{c_int, CStr, CString};
use std::marker::PhantomData;
use std::sync::Arc;

use rusqlite::vtab::{
    Context, Filters, IndexInfo, Module, VTab, VTabConfig, VTabConnection, VTabCursor,
};

use super::{Row, Rows};
use crate::snapshot_values::SnapshotValue;

/// What one table's module reads.
pub(super) struct Source {
    /// The CREATE TABLE that tells SQLite the table's columns.
    declaration: CString,
    /// The table's place among the host tables.
    place: usize,
    /// Every host table's rows, as the connection's transaction took them.
    taken: SnapshotValue<Rows>,
}

/// Register the module of the table `name`, whose columns are `columns`, each
/// with its declared type, on `connection`.
pub(super) fn register(
    connection: &rusqlite::Connection,
    name: &str,
    columns: &[(String, String)],
    place: usize,
    taken: SnapshotValue<Rows>,
) -> rusqlite::Result<()> {
    const MODULE: Module<HostTable> = Module::eponymous_only_module();
    let columns: Vec<String> = columns
        .iter()
        .map(|(column, declared)| format!("{} {declared}", sqlive_reads::sql::identifier(column)))
        .collect();
    let declaration = CString::new(format!("CREATE TABLE x({})", columns.join(",")))?;
    connection.create_module(name, &MODULE, Some(Source { declaration, place, taken }))
}

/// One connection's table, as SQLite connects to it.
#[repr(C)]
struct HostTable {
    /// SQLite's own part, which must come first.
    base: rusqlite::vtab::sqlite3_vtab,
    place: usize,
    taken: SnapshotValue<Rows>,
}

// SAFETY: `HostTable` is `repr(C)` and its first field is `sqlite3_vtab`, as
// `VTab` requires.
unsafe impl<'vtab> VTab<'vtab> for HostTable {
    type Aux = Source;
    type Cursor = Cursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Source>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> rusqlite::Result<(Cow<'static, CStr>, Self)> {
        let source = aux.expect("every host table's module is registered with its source");
        // A view and a trigger may read it, as they read a table: reading it
        // has no effect.
        db.config(VTabConfig::Innocuous)?;
        let table = Self {
            base: rusqlite::vtab::sqlite3_vtab::default(),
            place: source.place,
            taken: source.taken.clone(),
        };
        Ok((Cow::Owned(source.declaration.clone()), table))
    }

    /// Every search reads every row. A host table holds what a process has
    /// under way, such as its transfers.
    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<bool> {
        info.set_estimated_rows(25);
        info.set_estimated_cost(25.0);
        Ok(true)
    }

    fn open(&'vtab mut self) -> rusqlite::Result<Cursor<'vtab>> {
        Ok(Cursor {
            base: rusqlite::vtab::sqlite3_vtab_cursor::default(),
            table: self,
            rows: Arc::new(Vec::new()),
            at: 0,
            borrowed: PhantomData,
        })
    }
}

/// One search of a table's rows.
#[repr(C)]
struct Cursor<'vtab> {
    /// SQLite's own part, which must come first.
    base: rusqlite::vtab::sqlite3_vtab_cursor,
    table: *const HostTable,
    rows: Arc<Vec<Row>>,
    at: usize,
    borrowed: PhantomData<&'vtab HostTable>,
}

// SAFETY: `Cursor` is `repr(C)` and its first field is `sqlite3_vtab_cursor`,
// as `VTabCursor` requires.
unsafe impl VTabCursor for Cursor<'_> {
    fn filter(
        &mut self,
        _index: c_int,
        _index_text: Option<&str>,
        _arguments: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        // SAFETY: SQLite closes every cursor before it disconnects the table
        // that opened it.
        let table = unsafe { &*self.table };
        self.rows = Arc::clone(&table.taken.get()[table.place]);
        self.at = 0;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.at += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.at >= self.rows.len()
    }

    fn column(&self, context: &mut Context, column: c_int) -> rusqlite::Result<()> {
        let column = usize::try_from(column).expect("SQLite asks for a declared column");
        context.set_result(&self.rows[self.at][column])
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(i64::try_from(self.at).expect("a table's rows fit in memory"))
    }
}
