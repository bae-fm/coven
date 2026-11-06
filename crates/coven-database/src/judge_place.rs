//! Where the write's steps leave the row a reference's values key
//! (§9.6, §15.4, D5).
//!
//! An app row's key is in one audience at a time, and every operation of
//! the write's SQL on it is a step on the merge row of the audience it is in
//! then: a move is the delete of the old audience's row and the insert of
//! the new one's, at one position, whether the row's own update made it or
//! the move of an ancestor that carried the row along. So the last earlier
//! step on any row with that key says where the key is: in that row, at the
//! incarnation the step made, or nowhere after a delete.
//!
//! The write can also change a row at no step of its own: a migration
//! difference, or a removed row a move carries, which SQLite's tables never
//! held and so is never the row the key is in. Such a change leaves the
//! steps standing unless it may have moved the key where the reference
//! looks (Lean `Place.resolve_sound`).
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
    /// Steps on two of its rows share a position without being a move's
    /// halves, which no SQL makes.
    Unplaced,
    /// The last earlier step on a row with the key, and that row.
    Last(&'a RowId, &'a Indexed),
}

impl StepIndex {
    /// Where `key` of `table` is before `sequence`, as its rows' last step
    /// there says (Lean `Place.place`).
    pub(crate) fn place(&self, table: &str, key: &[u8], sequence: u32) -> Place<'_> {
        self.last(table, key, Some(sequence))
    }

    /// The parent a reference of a row in `child` records for `key` of
    /// `table`, found at `place` (Lean `Place.resolve`), given the parent
    /// its setter recorded and the generation a step's row names; none when
    /// a row with the key the write changed at no step of its own may have
    /// moved it where the reference looks. A step's row stands unless that
    /// change was on it. After a delete, or in an audience the reference
    /// can't name, there is no parent, unless such a change was on a row it
    /// can name. With no step, the setter's parent stands unless that change
    /// was on it, and no parent stands unless it was on a row the reference
    /// can name.
    pub(crate) fn resolve(
        &self,
        table: &str,
        key: &[u8],
        place: Place<'_>,
        child: &Audience,
        setter: Option<&Parent>,
        generation: impl Fn(&RowId, &Indexed) -> u64,
    ) -> Option<Option<Parent>> {
        let seen = |audience: &Audience| *audience == Audience::Store || audience == child;
        let marked = |row: &RowId| self.unstepped.contains(row);
        let reached = self.key_rows(table, key).any(|(row, _)| marked(row) && seen(&row.audience));
        match place {
            Place::Unplaced => None,
            Place::Last(row, step) if step.delete || marked(row) => {
                let named = !step.delete && seen(&row.audience);
                (!named && !reached).then_some(None)
            }
            Place::Last(row, step) => Some(
                seen(&row.audience)
                    .then(|| Parent { row: row.clone(), generation: generation(row, step) }),
            ),
            Place::Untouched => match setter {
                Some(parent) => (!marked(&parent.row)).then(|| Some(parent.clone())),
                None => (!reached).then_some(None),
            },
        }
    }

    /// The parent a row change's reference of a row in `child` names for
    /// `key` of `table` (Lean `namedNet`): [`Self::resolve`] where the
    /// whole write's steps leave the key, at the generation the write's
    /// record of that row leaves it at.
    pub(crate) fn net_parent(
        &self,
        table: &str,
        key: &[u8],
        child: &Audience,
        setter: Option<&Parent>,
    ) -> Option<Option<Parent>> {
        let place = self.last(table, key, None);
        self.resolve(table, key, place, child, setter, |row, _| self.rows[row].ends)
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
/// absent, or the other way. An execution record has no net change, so its
/// steps leave the row as present as they found it.
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
