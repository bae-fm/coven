// A crate of the workspace that the checked crate depends on: a type whose
// method returns the raw handle it holds, which the checked crate's methods
// hand out too when they return the type. Each line that holds a finding
// holds a `finding` comment.

pub struct Records {
    connection: rusqlite::Connection,
}

impl Records {
    pub fn open() -> Records {
        Records { connection: rusqlite::Connection::open_in_memory() }
    }

    pub fn connection(/* finding */ &self) -> &rusqlite::Connection {
        &self.connection
    }
}
