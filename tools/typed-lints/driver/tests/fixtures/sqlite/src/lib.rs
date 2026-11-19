//! Where `clippy::sqlite_outside_its_homes` finds code that reaches SQLite
//! outside the SQLite capability's homes, which the policy of this fixture's
//! workspace (`sqlite/`) sets to `src/store.rs`. A line that ends in a
//! `finding` comment holds one finding, and no other line holds any.

mod store;

pub use store::Store;

use rusqlite as sqlite; // finding
use rusqlite::Connection as Db; // finding
use rusqlite::Row; // finding
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef};

/// SQL value conversions, and the errors and results they report, reach no
/// database.
pub struct Key(pub String);

impl ToSql for Key {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Text(self.0.clone())))
    }
}

impl FromSql for Key {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().map(|text| Key(text.to_string())).map_err(|_| FromSqlError::InvalidType)
    }
}

pub fn refuse(index: usize) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnType(index, String::new(), rusqlite::types::Type::Null)
}

pub struct Leaky {
    pub connection: rusqlite::Connection, // finding
}

/// A name an import brought in names its item, not the crate: the import
/// holds the finding.
pub fn read_row(row: &Row<'_>) -> rusqlite::Result<String> {
    row.get(0)
}

/// What a home hands on, named through the home.
pub fn through_the_home(row: &store::Row<'_>) -> rusqlite::Result<String> {
    row.get(0)
}

/// The crate under another name is still the crate.
pub fn aliased(row: &sqlite::Row<'_>) -> rusqlite::Result<String> /* finding */ {
    row.get(0)
}

/// A handle whose type the compiler infers: the closure's parameter.
pub fn count(store: &Store) -> usize {
    store.with_connection(|connection| connection.changes()) // finding
}

/// Several handles in one body make one finding, where the first is.
pub fn transfer(store: &Store) {
    let first = store.open(); // finding
    let second = store.open();
    drop((first, second));
}

/// A handle inside another type the code is made of.
pub fn several(store: &Store) -> usize {
    let held = Some(vec![store.open()]); // finding
    held.map_or(0, |handles| handles.len())
}

/// A body that names SQLite holds its findings where it names it.
pub fn named(store: &Store) -> usize {
    let connection: Db = store.open(); // finding
    connection.changes()
}

/// A row is no handle: only naming it is a finding.
pub fn rows(store: &Store) -> Vec<String> {
    store.with_rows(|row| row.get(0).unwrap_or_default())
}

/// An owner that holds a handle in a field of its own holds it for its
/// holders.
pub fn owner() -> Store {
    Store::new()
}

pub fn initialize() -> i32 {
    rusqlite::ffi::sqlite3_initialize() // finding
}

#[expect(clippy::sqlite_outside_its_homes, reason = "the test of a mark")]
pub fn marked(store: &Store) -> usize {
    store.open().changes()
}

#[cfg(feature = "test-utils")]
pub fn test_connection() -> rusqlite::Connection {
    rusqlite::Connection::open_in_memory()
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    pub(crate) fn open() -> Connection {
        Connection::open_in_memory()
    }
}
