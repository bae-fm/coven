//! The merge of §8 and §14, as functions with no I/O.
//!
//! [`apply`] implements Appendix B's incremental step. Its order-free definition
//! is checked in tests. [`removals`] runs the three removal steps and
//! [`recompute`] restricts that work to a closed region (Appendix B, B9).
//! Values are opaque to the merge. The database supplies CHECK results and
//! unique claims after resolving references, using its own SQL semantics.

mod error;
#[cfg(any(test, feature = "test-utils"))]
mod history;
mod input;
mod removal;
mod state;
mod timestamp;

pub use error::MergeError;
#[cfg(test)]
use history::from_writes;
#[cfg(any(test, feature = "test-utils"))]
pub use history::History;
pub use input::{
    Audience, Change, ColumnValue, ConstraintColumns, ForeignKey, Operation, Parent, RowId,
    UniqueConstraint, Write, WriteId, WriteOracle, WritePast,
};
pub use removal::{
    recompute, recompute_fingerprint, removals, resolve_reference, Constraints, Group, OnDelete,
    Reference, ReferenceValue, RemovalResult, RemovalRow, RemovalView, Rule, UniqueClaim,
};
pub use state::{apply, Cell, LostChange, LostKey, LostValue, RowState, RowUpdate};
pub use timestamp::Timestamp;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
