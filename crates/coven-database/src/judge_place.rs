//! Where the write's steps leave the row a reference's values key
//! (§9.6, §15.4, D5).
//!
//! An app row's key is in one audience at a time, and every operation of
//! the write's SQL on it is a step on the merge row of the audience it is in
//! then: a move is the delete of the old audience's row and the insert of
//! the new one's, at one position. So the last earlier step on any row with
//! that key says where the key is: in that row, at the incarnation the step
//! made, or nowhere after a delete. Only a move that carried the row along
//! with its ancestor, or a migration difference, changes a row with no step;
//! once the write holds such a change for the key, its steps don't say.
//!
//! A row change names its parents as the whole write leaves them, at the
//! generation the write's own record of each parent leaves it at, which an
//! action's step need not: a parent deleted and added again keeps its
//! incarnation in its row change, and a deferred reference's parent can be
//! added after the step (Lean `Place.net_sound`).

use super::{Indexed, IndexedRow, StepIndex};
use coven_format::snapshot_rows::OriginalRow;
use coven_format::write::Half;
use coven_merge::{Audience, Parent, RowId};

/// A key's row as the write's steps before one step leave it.
pub(crate) enum Place<'a> {
    /// No earlier step touched a row with the key: it is as the authoring
    /// view holds it, which no step carries.
    Untouched,
    /// No step says where the key is: the write changed a row with it at no
    /// step of its own, or steps on two of its rows share a position without
    /// being a move's halves.
    Unplaced,
    /// The last earlier step on a row with the key, and that row.
    Last(&'a RowId, &'a Indexed),
}

/// The parent a row change's reference names for a key (Lean `namedNet`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NetParent {
    /// No step touched the key: the parent its setter recorded, which an
    /// action's step through the reference names too.
    Setter,
    /// The row the write's steps leave the key in at its end, at the
    /// generation the write's record of that row leaves it at, when it is
    /// present there in an audience the reference may name; none otherwise.
    Named(Option<Parent>),
}

impl StepIndex {
    /// Where `key` of `table` is before `sequence`, as its rows' last step
    /// there says: the step itself shows where the key was when it ran, even
    /// when a change of no step of its own moved the key afterwards.
    pub(crate) fn last_on_key(&self, table: &str, key: &[u8], sequence: u32) -> Place<'_> {
        self.last(table, key, Some(sequence))
    }

    /// Where `key` of `table` is before `sequence`: as [`Self::last_on_key`]
    /// says, unless the write changed a row with the key at no step of its
    /// own, which may have moved it since.
    pub(crate) fn place(&self, table: &str, key: &[u8], sequence: u32) -> Place<'_> {
        self.placed(table, key, Some(sequence))
    }

    /// The parent a row change's reference names for `key` of `table`
    /// (Lean `namedNet`), given which audiences it may name a parent in;
    /// none when no step says where the key is.
    pub(crate) fn net_parent(
        &self,
        table: &str,
        key: &[u8],
        readable: impl Fn(&Audience) -> bool,
    ) -> Option<NetParent> {
        match self.placed(table, key, None) {
            Place::Untouched => Some(NetParent::Setter),
            Place::Unplaced => None,
            Place::Last(row, step) => Some(NetParent::Named(
                (!step.delete && readable(&row.audience))
                    .then(|| Parent { row: row.clone(), generation: self.rows[row].ends }),
            )),
        }
    }

    /// Where `key` of `table` is before `before`, or once the write's SQL
    /// has run when `before` is none, unless a row with the key changed at
    /// no step of its own.
    fn placed(&self, table: &str, key: &[u8], before: Option<u32>) -> Place<'_> {
        if self.key_rows(table, key).any(|(row, _)| self.unstepped.contains(row)) {
            return Place::Unplaced;
        }
        self.last(table, key, before)
    }

    /// The last step on a row with `key` of `table` before `before`, or of
    /// all when `before` is none.
    fn last(&self, table: &str, key: &[u8], before: Option<u32>) -> Place<'_> {
        let mut last: Option<(&RowId, &Indexed)> = None;
        for (row, indexed) in self.key_rows(table, key) {
            let earlier = |step: &&Indexed| before.is_none_or(|before| step.sequence < before);
            let Some(step) = indexed.steps.iter().rev().find(earlier) else {
                continue;
            };
            last = match last {
                Some((_, held)) if held.sequence > step.sequence => last,
                Some((_, held)) if held.sequence < step.sequence => Some((row, step)),
                // Two rows with one key share a position only as a move's
                // halves; the key is where the insert half put it.
                Some((_, held)) if held.half == Half::Move && step.half == Half::Move => {
                    match (held.delete, step.delete) {
                        (true, false) => Some((row, step)),
                        (false, true) => last,
                        _ => return Place::Unplaced,
                    }
                }
                Some(_) => return Place::Unplaced,
                None => Some((row, step)),
            };
        }
        last.map_or(Place::Untouched, |(row, step)| Place::Last(row, step))
    }

    /// The write's rows with `key` of `table`, in every audience.
    fn key_rows(
        &self,
        table: &str,
        key: &[u8],
    ) -> impl Iterator<Item = (&RowId, &IndexedRow)> + use<'_> {
        let first = RowId { table: table.to_owned(), key: key.to_vec(), audience: Audience::Store };
        let (table, key) = (first.table.clone(), first.key.clone());
        self.rows.range(first..).take_while(move |(row, _)| row.table == table && row.key == key)
    }
}

/// Whether the write changed this row beyond what its steps did: a net
/// change with no steps, or steps that don't begin at the generation the
/// change begins at or leave the row present where the change leaves it
/// absent, or the other way, as when a move carried the row in or out. An
/// execution record has no net change, so its steps leave the row as
/// present as they found it.
pub(super) fn unstepped(record: &OriginalRow) -> bool {
    let steps = &record.execution().steps;
    let (Some(first), Some(last)) = (steps.first(), steps.last()) else {
        return matches!(record, OriginalRow::Change(_));
    };
    let Ok(stepped) = last.change.after() else {
        return true;
    };
    // The generation the record says the row ends at.
    let ends = match record {
        OriginalRow::Change(change) => {
            let Ok(after) = change.change.after() else {
                return true;
            };
            if first.change.generation != change.change.generation {
                return true;
            }
            after
        }
        OriginalRow::Execution(_) => first.change.generation,
    };
    coven_merge::generation_present(ends) != coven_merge::generation_present(stepped)
}

#[cfg(test)]
#[path = "judge_place_tests.rs"]
mod tests;
