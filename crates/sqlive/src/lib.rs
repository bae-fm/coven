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
mod error;
mod handle;
mod host_judge;
mod host_tables;
mod kept_counts;
mod live_query;
#[cfg(any(test, feature = "test-utils"))]
mod live_query_runs;
mod net_changes;
mod no_policy;
mod planner_statistics;
#[cfg(any(test, feature = "test-utils"))]
pub mod random_scopes;
mod read_index;
mod snapshot_values;
mod sqlite_path;
mod sqlite_settings;
mod sqlite_temporary;
#[cfg(any(test, feature = "test-utils"))]
mod test_records;
#[cfg(any(test, feature = "test-utils"))]
pub mod triggered_inserts;
mod upkeep;
mod wal_checkpoints;

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
pub use host_tables::{HostRows, HostTables};
pub use live_query::{
    LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent, Run,
};
#[cfg(any(test, feature = "test-utils"))]
pub use live_query_runs::LiveQueryRuns;
pub use no_policy::{HandleFailure, NoPolicy};
pub use panic_as_error;
pub use rusqlite;
pub use rusqlite::{Error as SqliteError, Result as SqliteResult, Row, ToSql};
pub use sqlite_settings::{
    AnalysisLimit, AutoVacuum, CacheSize, CacheSpill, CheckpointMode, IncrementalVacuum,
    JournalSizeLimit, PageSize, SecureDelete, SqliteSettings, SqliteSettingsInEffect, TempStore,
    WalAutocheckpoint, WalCheckpoint, CHECKPOINT_PAGES,
};
pub use sqlite_temporary::{set_temporary_directory, TemporaryDirectoryError};
#[cfg(any(test, feature = "test-utils"))]
pub use sqlive_reads::read_report;
pub use sqlive_reads::sql;
pub use sqlive_reads::SchemaDiffers;
pub use sqlive_reads::{
    affected, Affinity, Answers, ColumnNulls, ColumnOrigin, ColumnSet, CompiledStatement, Judging,
    LastRow, Lookup, Lookups, NoAnswer, ProbedColumn, ProbedStatement, ReadCapture, ReadEntry,
    ReadFrame, ReadPlace, ReadReport, ReadRows, ReadSet, RowChange, RowCondition, RowImage,
    RowScope, SchemaProbe, ScopePlanSize, Scopes, SelectNull, SqlPosition, SqlValue,
    StandInFunction, TableColumn, TableRead, Unanalyzable, UndecodablePlan, LOOKUP_STEPS,
    ROWS_PER_TABLE,
};
pub(crate) use sqlive_reads::{
    affected_by, compare, parameters, past_row_limit, CompiledPlan, MainSchema, ScopePlan,
    ValueKey, FTS5_CONTENT, MAIN_TABLES,
};
#[cfg(any(test, feature = "test-utils"))]
pub use test_records::{
    report_cost, report_time, CommitPoint, ConnectionWork, ConnectionWorkCounter, StatementLog,
};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
