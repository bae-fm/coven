//! The SQLite capability's home in the fixture: its owner of the database
//! holds the connection and runs the work others ask for.

use rusqlite::Connection;
pub use rusqlite::Row;

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn new() -> Store {
        Store { connection: Connection::open_in_memory() }
    }

    pub fn open(&self) -> Connection {
        Connection::open_in_memory()
    }

    pub fn with_connection<T>(&self, work: impl FnOnce(&Connection) -> T) -> T {
        work(&self.connection)
    }

    pub fn with_rows<T>(&self, mut work: impl FnMut(&Row<'_>) -> T) -> Vec<T> {
        vec![work(&Row(&self.connection))]
    }
}
