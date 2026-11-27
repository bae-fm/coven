//! The connection on which a database judges changes to its host tables
//! (SPEC.md section 7). A change judges as a write does, with lookups that
//! read the rows it leaves: here, the latest commit of the database beside
//! the host tables as the change left them.

use std::sync::{Arc, Mutex};

use panic_as_error::locks::{into_unpoisoned, lock_unpoisoned};

use crate::authorization::{Policy, PolicyError};
use crate::connection::HostSql;
use crate::host_tables::HostJudge;
use crate::{HostTables, RowChange};

/// A database's judge of its host tables' changes, attached to them until
/// this drops.
pub(crate) struct HostAttachment<P: Policy> {
    tables: HostTables,
    /// The judge's connection, which the tables hold too while attached.
    connection: Option<Arc<Mutex<HostSql<P>>>>,
}

impl<P: Policy> HostAttachment<P> {
    /// Judge `tables`' changes on `connection`, which serves the database's
    /// handle.
    pub(crate) fn attach(tables: HostTables, connection: HostSql<P>) -> Self {
        let connection = Arc::new(Mutex::new(connection));
        tables.attach(Box::new(Judge(Arc::clone(&connection))));
        Self { tables, connection: Some(connection) }
    }

    /// Stop judging and close the connection.
    pub(crate) fn close(mut self) -> Result<(), PolicyError<P>> {
        self.tables.detach();
        let connection = self.connection.take().expect("only close takes the connection");
        match Arc::try_unwrap(connection) {
            Ok(connection) => into_unpoisoned(connection).close(),
            Err(_held) => Err(crate::Error::Invariant {
                what: "detached host tables let go of their judge's connection",
            }),
        }
    }
}

impl<P: Policy> Drop for HostAttachment<P> {
    fn drop(&mut self) {
        if self.connection.is_some() {
            self.tables.detach();
        }
    }
}

struct Judge<P: Policy>(Arc<Mutex<HostSql<P>>>);

impl<P: Policy> HostJudge for Judge<P> {
    fn report(&mut self, changes: Vec<RowChange>) {
        let connection = lock_unpoisoned(&self.0);
        connection.report_host_changes(changes);
    }
}
