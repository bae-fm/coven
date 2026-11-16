//! The statements a connection ran while a profile was open, with what
//! SQLite counted of each, for tests and measurements of the plans SQLite
//! chose.

use super::{HostSql, Policy};
use std::sync::Arc;

/// One statement execution the profile saw.
#[derive(Clone, Debug)]
pub struct ProfiledStatement {
    /// The statement's text, as prepared.
    pub sql: String,
    /// Full-scan steps of this execution alone.
    pub fullscan_steps: i32,
    /// Executions of this prepared statement so far, this one included.
    pub runs: i32,
    /// Virtual machine steps of this prepared statement so far, this
    /// execution's included.
    pub vm_steps: i32,
    /// Times SQLite prepared this statement again so far, this execution's
    /// included.
    pub reprepares: i32,
}

impl<P: Policy> HostSql<P> {
    /// Trace this connection's statements until the matching
    /// [`Self::end_profile`]. A profile inside another, as a migration's
    /// inside the open's, leaves the outer one tracing.
    pub(super) fn start_profile(&mut self) {
        let open = self.profiles.get();
        self.profiles.set(open + 1);
        if open > 0 {
            return;
        }
        self.scans.lock().unwrap().clear();
        let scans = Arc::clone(&self.scans);
        self.connection
            .trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE,
                Some(move |event: rusqlite::trace::TraceEvent<'_>| {
                    if let rusqlite::trace::TraceEvent::Profile(statement, _) = event {
                        scans.lock().unwrap().push(ProfiledStatement {
                            sql: statement.sql().into_owned(),
                            fullscan_steps: statement
                                .get_status(rusqlite::StatementStatus::FullscanStep),
                            runs: statement.get_status(rusqlite::StatementStatus::Run),
                            vm_steps: statement.get_status(rusqlite::StatementStatus::VmStep),
                            reprepares: statement.get_status(rusqlite::StatementStatus::RePrepare),
                        });
                    }
                }),
            )
            .unwrap();
    }

    /// End a profile; only the outermost stops tracing.
    pub(super) fn end_profile(&mut self) {
        let open = self.profiles.get() - 1;
        self.profiles.set(open);
        if open == 0 {
            self.connection
                .trace_v2(
                    rusqlite::trace::TraceEventCodes::empty(),
                    None::<fn(rusqlite::trace::TraceEvent<'_>)>,
                )
                .unwrap();
        }
    }

    /// The statements the last profile saw, in the order they ran.
    pub fn profiled_statements(&self) -> Vec<ProfiledStatement> {
        self.scans.lock().unwrap().clone()
    }
}
