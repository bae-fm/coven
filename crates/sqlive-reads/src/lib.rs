//! What sqlive knows of a SELECT without running it: the conditions that
//! narrow each table it reads (its scope plan), what it reads of each table
//! and column (its read report), and the origin of each column it returns
//! (its schema probe). The crate holds no async runtime: an app's build script
//! takes the compiler of `sqlive-compose`, which reads these, and the live
//! queries that run on them stay in `sqlive`.

mod bare_names;
mod column_order;
mod comparison;
mod compiled_plan;
mod derived;
mod from_item;
mod from_star;
mod handles;
pub use handles::{ReadOnly, Writer};
mod never_null;
mod read_capture;
mod read_report;
mod read_set;
mod reference_columns;
mod result_nulls;
mod row_order;
mod row_scope;
mod schema_objects;
mod schema_probe;
mod scope_condition;
mod scope_edge;
mod scope_lookup;
mod scope_narrowing;
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
mod table_keys;
mod table_places;
mod token_scan;
mod told_apart;
mod unread_columns;
mod unreported_columns;
mod view_trees;
mod whole_view_names;

pub use bare_names::bare_column_beside_table_function;
pub use compiled_plan::{
    CompiledPlan, CompiledStatement, SchemaDiffers, Unanalyzable, UndecodablePlan,
};
pub use read_capture::{ReadCapture, ReadFrame};
#[cfg(any(test, feature = "test-utils"))]
pub use read_report::read_report;
pub use read_report::{ReadEntry, ReadPlace, ReadReport, ReadRows};
pub use read_set::{
    affected, affected_by, past_row_limit, Answers, ColumnSet, ReadSet, RowChange, TableRead,
    ValueKey, ROWS_PER_TABLE,
};
pub use result_nulls::{ColumnNulls, SelectNull};
pub use row_scope::{compare, RowImage, RowScope, Scopes};
pub use rusqlite;
pub use rusqlite::{Error as SqliteError, Result as SqliteResult, Row, ToSql};
pub use schema_objects::{MainSchema, FTS5_CONTENT, MAIN_TABLES};
pub use schema_probe::{
    ColumnOrigin, ProbedColumn, ProbedStatement, SchemaProbe, StandInFunction, TableColumn,
};
pub use scope_lookup::{Judging, Lookup, Lookups, NoAnswer, LOOKUP_STEPS};
pub use scope_order::LastRow;
pub use scope_plan::{ScopePlan, ScopePlanSize};
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub use sql_value::{SqlValue, Values};
pub use table_places::SqlPosition;
