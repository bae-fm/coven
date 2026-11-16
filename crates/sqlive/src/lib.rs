//! SQLite connections whose commits rerun the live queries they change.
//!
//! A [`Database`] holds one database's connections: a writer, readers beside
//! it, and upkeep threads that checkpoint its WAL and keep its planner's
//! statistics. Each connection is a [`HostSql`], judged by the host's
//! [`Policy`] as SQLite prepares each statement; [`NoPolicy`] adds nothing to
//! sqlive's own rules. A write transaction captures what it changed, and a
//! live read records the tables, columns and rows its statements read: sqlive
//! parses each statement's SQL into the conditions that narrow each table it
//! reads. sqlive then judges a commit's row changes against each live query's
//! reads to tell whether the query's result could have changed. A [`Handle`]
//! holds the subscriptions the judged commits wake, and the failure that ends
//! every later call.

mod authorization;
mod change_capture;
mod commit_scope;
mod connection;
mod connection_setup;
mod database;
mod database_changes;
mod derived;
mod error;
mod from_item;
mod handle;
mod live_query;
#[cfg(any(test, feature = "test-utils"))]
mod live_query_runs;
mod no_policy;
mod planner_statistics;
#[cfg(any(test, feature = "test-utils"))]
pub mod random_scopes;
mod read_capture;
mod read_set;
mod row_scope;
mod scope_lookup;
mod scope_order;
mod scope_plan;
mod scope_row;
mod scope_subquery;
mod scope_term;
mod select_syntax;
pub mod sql;
mod sql_value;
mod sqlite_path;
mod sqlite_settings;
mod sqlite_temporary;
#[cfg(any(test, feature = "test-utils"))]
mod test_records;
mod token_scan;
mod upkeep;
mod wal_checkpoints;

pub use authorization::{
    FunctionCall, HostError, Owner, Policy, PolicyAction, PolicyError, Refresh, StatementReads,
    StatementScope, WriteSql,
};
pub use caught;
pub use commit_scope::{committing, enter, CommitScope};
#[cfg(any(test, feature = "profiles"))]
pub use connection::ProfiledStatement;
pub use connection::{CookieScope, HostSql, Operation, Recorded};
pub use connection_setup::Setup;
pub use database::{Database, Options, Reader};
pub use database_changes::DatabaseChanges;
pub use error::{EngineFailure, Error, RollbackJoin};
pub use handle::{
    AttemptCommits, CommitState, CommitSubscription, Handle, Judged, UnrecordedTables,
};
pub use live_query::{
    LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent, Run,
};
#[cfg(any(test, feature = "test-utils"))]
pub use live_query_runs::LiveQueryRuns;
pub use no_policy::{HandleFailure, NoPolicy};
pub use read_capture::{ReadCapture, ReadFrame, WholeViews};
pub use read_set::{affected, Answers, ColumnSet, ReadSet, RowChange, TableRead, ROWS_PER_TABLE};
pub use row_scope::{RowImage, RowScope, Scopes};
pub use rusqlite;
pub use scope_lookup::{Judging, Lookup, Lookups, LOOKUP_STEPS};
pub use scope_order::LastRow;
pub(crate) use scope_plan::ScopePlan;
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub(crate) use sql_value::parameters;
pub use sql_value::SqlValue;
pub use sqlite_settings::{
    AnalysisLimit, AutoVacuum, CacheSize, CacheSpill, CheckpointMode, IncrementalVacuum,
    JournalSizeLimit, PageSize, SecureDelete, SqliteSettings, SqliteSettingsInEffect, TempStore,
    WalAutocheckpoint, WalCheckpoint, CHECKPOINT_PAGES,
};
pub use sqlite_temporary::{set_temporary_directory, TemporaryDirectoryError};
#[cfg(any(test, feature = "test-utils"))]
pub use test_records::{
    report_cost, report_time, CommitPoint, ConnectionWork, ConnectionWorkCounter, StatementLog,
};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
