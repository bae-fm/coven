//! The merge of §8 and §14, as functions with no I/O.
//!
//! [`apply`] implements Appendix B's incremental step; [`from_writes`] gives
//! its order-free definition. [`removals`] runs the three removal steps and
//! [`recompute`] restricts that work to a closed region (Appendix B, B9).
//! Values are opaque to the merge. The database supplies CHECK results and
//! unique claims after resolving references, using its own SQL semantics.

mod error;
mod history;
mod input;
mod removal;
mod state;
mod timestamp;

pub use error::MergeError;
pub use history::{from_writes, History};
pub use input::{
    Audience, Change, ColumnValue, Operation, Parent, RowId, Write, WriteId, WriteOracle,
};
pub use removal::{
    recompute, removals, resolve_reference, Constraints, Group, OnDelete, Reference,
    ReferenceValue, RemovalResult, RemovalRow, RemovalView, Rule, UniqueClaim,
};
pub use state::{apply, Cell, LostChange, LostKey, LostValue, RowState, RowUpdate};
pub use timestamp::Timestamp;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
