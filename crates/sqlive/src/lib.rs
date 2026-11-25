//! SQLite connections whose commits rerun the live queries they change.
//!
//! A [`Database`] holds one database's connections: a writer, readers beside
//! it, and upkeep threads that checkpoint its WAL and keep its planner's
//! statistics. Each connection is a [`HostSql`], and the host's [`Policy`]
//! judges each statement as SQLite prepares it; [`NoPolicy`] adds nothing to
//! sqlive's own rules. A write transaction captures what it changed, and a
//! live read records the tables, columns and rows its statements read: sqlive
//! parses each statement's SQL into the conditions that narrow each table it
//! reads. sqlive then judges a commit's row changes against the reads of each
//! live query whose scopes the changed rows could pass, to tell whether the
//! query's result could have changed. A [`Handle`] holds the subscriptions the
//! judged commits wake, indexed by the values their scopes' equalities hold,
//! and the failure that ends every later call.
//!
//! A [`Database`] also tells its host the main database file's size after
//! each checkpoint, its checkpointer's or the host's, through a
//! [`CheckpointedSize`] ([`Database::checkpointed_size`]), so the host can show the file's size
//! without reading the file itself.

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod authorization;
mod change_capture;
mod checkpointed_size;
mod commit_scope;
mod connection;
mod connection_setup;
mod database;
mod database_changes;
mod derived;
mod error;
mod from_item;
mod handle;
mod kept_counts;
mod live_query;
#[cfg(any(test, feature = "test-utils"))]
mod live_query_runs;
mod net_changes;
mod no_policy;
mod planner_statistics;
#[cfg(any(test, feature = "test-utils"))]
pub mod random_scopes;
mod read_capture;
mod read_index;
mod read_report;
mod read_set;
mod row_scope;
mod schema_objects;
mod scope_edge;
mod scope_lookup;
mod scope_one_of;
mod scope_or;
mod scope_order;
mod scope_plan;
mod scope_row;
mod scope_subquery;
mod scope_term;
mod scope_window;
mod select_syntax;
mod snapshot_values;
mod source_order;
pub mod sql;
mod sql_value;
mod sql_walk;
mod sqlite_path;
mod sqlite_settings;
mod sqlite_temporary;
#[cfg(any(test, feature = "test-utils"))]
mod test_records;
mod token_scan;
#[cfg(any(test, feature = "test-utils"))]
pub mod triggered_inserts;
mod unreported_columns;
mod upkeep;
mod wal_checkpoints;
mod whole_view_names;

pub use authorization::{
    FunctionCall, HostError, LocalSql, Owner, Policy, PolicyAction, PolicyError, Refresh,
    StatementReads, StatementScope, WriteSql,
};
pub use checkpointed_size::CheckpointedSize;
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
pub use panic_as_error;
pub use read_capture::{ReadCapture, ReadFrame, WholeViews};
#[cfg(any(test, feature = "test-utils"))]
pub use read_report::read_report;
pub use read_report::{ReadEntry, ReadReport, ReadRows};
pub use read_set::{affected, Answers, ColumnSet, ReadSet, RowChange, TableRead, ROWS_PER_TABLE};
pub use row_scope::{RowImage, RowScope, Scopes};
pub use rusqlite;
pub use scope_lookup::{Judging, Lookup, Lookups, NoAnswer, LOOKUP_STEPS};
pub use scope_order::LastRow;
pub(crate) use scope_plan::ScopePlan;
pub use scope_plan::ScopePlanSize;
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub use snapshot_values::SnapshotValue;
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
