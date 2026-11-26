//! What sqlive knows of a SELECT without running it: the conditions that
//! narrow each table it reads (its scope plan), what it reads of each table
//! and column (its read report), and the origin of each column it returns
//! (its schema probe). The crate holds no async runtime: an app's build script
//! takes the compiler of `sqlive-compose`, which reads these, and the live
//! queries that run on them stay in `sqlive`.

mod derived;
mod from_item;
mod read_capture;
mod read_report;
mod read_set;
mod row_scope;
mod schema_objects;
mod schema_probe;
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
mod source_order;
pub mod sql;
mod sql_value;
mod sql_walk;
mod token_scan;
mod unreported_columns;
mod whole_view_names;

pub use read_capture::{ReadCapture, ReadFrame, WholeViews};
#[cfg(any(test, feature = "test-utils"))]
pub use read_report::read_report;
pub use read_report::{ReadEntry, ReadReport, ReadRows};
pub use read_set::{
    affected, affected_by, past_row_limit, Answers, ColumnSet, ReadSet, RowChange, TableRead,
    ValueKey, ROWS_PER_TABLE,
};
pub use row_scope::{compare, RowImage, RowScope, Scopes};
pub use rusqlite;
pub use rusqlite::{Error as SqliteError, Result as SqliteResult, Row, ToSql};
pub use schema_objects::{FTS5_CONTENT, MAIN_TABLES};
pub use schema_probe::{ColumnOrigin, ProbedColumn, ProbedStatement, SchemaProbe, StandInFunction};
pub use scope_lookup::{Judging, Lookup, Lookups, NoAnswer, LOOKUP_STEPS};
pub use scope_order::LastRow;
pub use scope_plan::{ScopePlan, ScopePlanSize};
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub use sql_value::{parameters, SqlValue};
