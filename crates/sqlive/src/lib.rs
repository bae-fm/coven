//! What a live query's statements read, the rows each read depends on, and
//! the live queries that run again when a commit changes them.
//!
//! sqlive parses a statement's SQL into the conditions that narrow each table
//! it reads, and a read set records the tables, columns and rows a run read.
//! sqlive then judges a commit's row changes against a read set to tell whether
//! the run's result could have changed. A [`Handle`] holds the subscriptions
//! the judged commits wake, and the failure that ends every later call.

mod commit_scope;
mod database_changes;
mod derived;
mod error;
mod from_item;
mod handle;
mod live_query;
#[cfg(any(test, feature = "test-utils"))]
mod live_query_runs;
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
mod token_scan;

pub use caught;
pub use commit_scope::{committing, enter, CommitScope};
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
pub use read_capture::{ReadCapture, ReadFrame, WholeViews};
pub use read_set::{affected, Answers, ColumnSet, ReadSet, RowChange, TableRead, ROWS_PER_TABLE};
pub use row_scope::{RowImage, RowScope, Scopes};
pub use scope_lookup::{Judging, Lookup, Lookups, LOOKUP_STEPS};
pub use scope_order::LastRow;
pub use scope_plan::ScopePlan;
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub use sql_value::{parameters, SqlValue};
