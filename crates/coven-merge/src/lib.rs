//! The merge of §8, §9 and §15, as functions with no I/O.
//!
//! [`apply`] implements the merge proof's incremental step. Its order-free definition
//! is checked in tests. [`recompute`] runs the three removal steps over a
//! closed region (proofs/merge.md B9).
//! Values are opaque to the merge. The database supplies CHECK results and
//! unique claims after resolving references, using its own SQL semantics.

mod audience_sql;
mod dependency;
mod error;
#[cfg(any(test, feature = "test-utils"))]
mod history;
mod input;
mod sequence;
pub use sequence::{had_read, ClaimOrder, SlotPast};
mod removal;
mod state;
mod timestamp;

pub use dependency::Provenance;
pub use error::MergeError;
#[cfg(test)]
use history::from_writes;
#[cfg(any(test, feature = "test-utils"))]
pub use history::History;
pub use input::{
    Audience, Change, ColumnValue, ConstraintColumns, ForeignKey, Operation, Parent, RowId, Slot,
    UniqueConstraint, Write, WriteId, WriteOracle, WritePast,
};
pub use removal::{
    recompute, resolve_reference, Constraints, Group, OnDelete, Reference, ReferenceValue,
    RemovalResult, RemovalRow, RemovalView, Rule, UniqueClaim,
};
#[cfg(any(test, feature = "test-utils"))]
pub use removal::{removals, FullRemovalView};
pub use state::{apply, Cell, LostChange, LostKey, LostValue, RowState, RowUpdate};
pub use timestamp::Timestamp;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
