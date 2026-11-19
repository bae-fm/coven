// The database's home: no callable returns a raw handle, and no public one
// takes one unless the policy lists it. Each line that holds a finding of
// `clippy::owner_hands_out_dependency` holds a `finding` comment.

use rusqlite::Connection;

pub struct Store {
    connection: Connection,
}

pub fn leak(/* finding */) -> Connection {
    Connection::open_in_memory()
}

pub fn take(/* finding */ connection: &Connection) {
    connection.changes();
}

fn leaf(connection: &Connection) {
    connection.changes();
}

impl Store {
    pub fn open(connection: &Connection) -> Store {
        leaf(connection);
        Store { connection: Connection::open_in_memory() }
    }

    pub fn open_returning(/* finding */ connection: &Connection) -> Connection {
        leaf(connection);
        Connection::open_in_memory()
    }

    pub fn take(/* finding */ connection: &Connection) {
        leaf(connection);
    }

    fn absorb(/* finding */ &mut self, connection: &Connection) {
        leaf(connection);
        leaf(&self.connection);
    }

    fn leaf(&self, connection: &Connection) {
        leaf(connection);
        leaf(&self.connection);
    }

    fn session(/* finding */ &self) -> rusqlite::session::Session<'_> {
        self.leaf(&self.connection);
        rusqlite::session::Session(&self.connection)
    }
}

pub trait Opener {
    fn leak(&self) -> Connection; // finding

    fn take(connection: &Connection); // finding
}

// A closed session the policy names, declared away from the crate's root.
pub struct Batch {
    connection: crate::Database,
}

pub fn use_all(store: &mut Store, batch: &Batch) {
    store.absorb(&Connection::open_in_memory());
    let _ = store.session();
    let _ = &batch.connection;
}
