//! A test of the fixture's package, under its `tests/` folder: test code
//! opens its own connection, so `clippy::sqlite_outside_its_homes` finds
//! nothing here.

use rusqlite::Connection;

pub fn open() -> Connection {
    Connection::open_in_memory()
}
