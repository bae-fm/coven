//! What a live query's statements read, and the rows each read depends on.
//!
//! sqlive parses a statement's SQL into the conditions that narrow each table
//! it reads, and a read set records the tables, columns and rows a run read.
//! sqlive then judges a commit's row changes against a read set to tell whether
//! the run's result could have changed.

mod derived;
mod from_item;
mod read_capture;
mod read_set;
mod row_scope;
mod schema_source;
mod scope_lookup;
mod scope_plan;
mod scope_row;
mod scope_subquery;
mod scope_term;
mod select_syntax;
pub mod sql;
mod sql_value;

pub use read_capture::{ReadCapture, ReadFrame, WholeViews};
pub use read_set::{affected, Answers, ColumnSet, ReadSet, RowChange, TableRead, ROWS_PER_TABLE};
pub use row_scope::{RowImage, RowScope, Scopes};
pub use schema_source::SchemaSource;
pub use scope_lookup::{Judging, Lookup, Lookups, LOOKUP_STEPS};
pub use scope_plan::ScopePlan;
pub use scope_row::RowCondition;
pub use scope_term::Affinity;
pub use sql_value::{parameters, SqlValue};
