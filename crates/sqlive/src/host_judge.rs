//! The connection on which a database judges changes to its host tables
//! (SPEC.md section 7), on a thread of the database's own. A change judges as
//! a write does, with lookups that read the rows it leaves: here, the latest
//! commit of the database beside the host tables as the change left them.
//! The tables reach the thread by a channel, so what changes their rows holds
//! no connection.

use std::sync::mpsc;
use std::thread::JoinHandle;

use crate::authorization::{Policy, PolicyError};
use crate::connection::HostSql;
use crate::host_tables::JudgeRequest;
use crate::HostTables;

/// The thread's name.
const THREAD: &str = "sqlive-host-judge";

/// A database's judge of its host tables' changes, attached to them until
/// this drops.
pub(crate) struct HostAttachment<P: Policy> {
    tables: HostTables,
    /// `close` takes this as the thread stops, or else the drop does. The
    /// thread hands its connection back.
    thread: Option<JoinHandle<HostSql<P>>>,
}

impl<P: Policy> HostAttachment<P> {
    /// Judge `tables`' changes on `connection`, which serves the database's
    /// handle, on a thread of its own.
    pub(crate) fn attach(
        tables: HostTables,
        connection: HostSql<P>,
    ) -> Result<Self, PolicyError<P>> {
        let (requests, asked) = mpsc::channel::<JudgeRequest>();
        let thread = std::thread::Builder::new()
            .name(THREAD.into())
            .spawn(move || {
                // The channel closes as the tables detach.
                for JudgeRequest { changes, judged } in asked {
                    connection.report_host_changes(changes);
                    judged.send(()).expect("a change waits for its judgment");
                }
                connection
            })
            .map_err(crate::Error::Thread)?;
        tables.attach(requests);
        Ok(Self { tables, thread: Some(thread) })
    }

    /// Stop judging and close the connection.
    pub(crate) fn close(mut self) -> Result<(), PolicyError<P>> {
        let thread = self.thread.take().expect("only close and drop stop the judge");
        // Detaching closes the channel, which ends the thread.
        self.tables.detach();
        // `report_host_changes` catches a panic, so the thread returns.
        thread.join().expect("the judge's thread returns").close()
    }
}

/// A database whose open failed after its judge started stops it too.
impl<P: Policy> Drop for HostAttachment<P> {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.tables.detach();
            drop(thread.join().expect("the judge's thread returns"));
        }
    }
}
