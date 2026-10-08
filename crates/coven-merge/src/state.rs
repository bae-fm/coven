use crate::{ColumnValue, MergeError, Operation, RowId, Timestamp, Write, WriteId, WriteOracle};
use std::collections::BTreeMap;

/// The winning write and its value in one column (`_coven_cells`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cell<V> {
    /// The write whose timestamp won.
    pub write: WriteId,
    /// The value and parent generations that write set.
    pub value: ColumnValue<V>,
}

/// A value is identified by the column and write that set it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LostKey {
    /// The synced column's name.
    pub column: String,
    /// The write that set the lost value.
    pub write: WriteId,
}

/// A value replaced only by writes that had not read it (`_coven_lost`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostValue<V> {
    /// The incarnation in which the value was set.
    pub incarnation: u64,
    /// The value, with its original reference metadata.
    pub value: ColumnValue<V>,
    /// The earliest delete, or otherwise the current winning setter.
    pub replaced_by: WriteId,
}

/// One table, key and audience's merged state. Removed rows retain this state;
/// a removal never advances their generation (§8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowState<V> {
    pub(crate) row: RowId,
    pub(crate) generation: u64,
    pub(crate) generations: BTreeMap<u64, WriteId>,
    pub(crate) cells: BTreeMap<String, Cell<V>>,
    pub(crate) lost: BTreeMap<LostKey, LostValue<V>>,
}

impl<V> RowState<V> {
    /// A row that has never been inserted, at generation zero.
    pub fn new(row: RowId) -> Self {
        Self {
            row,
            generation: 0,
            generations: BTreeMap::new(),
            cells: BTreeMap::new(),
            lost: BTreeMap::new(),
        }
    }
    /// Load the compact records kept by the database or a snapshot. Validate
    /// their generation sequence, cell presence, reference metadata and lost
    /// replacers without replaying write bodies. As with `apply`, the oracle
    /// is the causally closed applied set; it supplies timestamps and past.
    pub fn from_parts(
        row: RowId,
        generations: BTreeMap<u64, WriteId>,
        cells: BTreeMap<String, Cell<V>>,
        lost: BTreeMap<LostKey, LostValue<V>>,
        oracle: &impl WriteOracle,
    ) -> Result<Self, MergeError> {
        let mut generation: u64 = 0;
        let mut previous_stamp = None;
        for (g, write) in &generations {
            let expected = generation
                .checked_add(1)
                .ok_or(MergeError::GenerationExhausted)?;
            if *g != expected {
                return Err(MergeError::GenerationGap(expected));
            }
            let stamp = timestamp(oracle, *write)?;
            if previous_stamp.is_some_and(|previous| previous >= stamp) {
                return Err(MergeError::GenerationTimestamp(*g));
            }
            previous_stamp = Some(stamp);
            generation = *g;
        }
        if generation.is_multiple_of(2) && !cells.is_empty() {
            return Err(MergeError::DeletedRowHasCells);
        }
        for (column, cell) in &cells {
            let stamp = timestamp(oracle, cell.write)?;
            if previous_stamp.is_some_and(|start| stamp < start) {
                return Err(MergeError::CellBeforeIncarnation(column.clone()));
            }
            for parent in cell.value.parents.values() {
                parent.validate_written(&row)?;
            }
        }
        let state = Self {
            row,
            generation,
            generations,
            cells,
            lost,
        };
        for (key, value) in &state.lost {
            state.validate_loss(key, value, oracle)?;
        }
        Ok(state)
    }

    /// Validate an independently decoded cell loss against this row's validated
    /// generations and winning cells. The oracle is the causally closed applied
    /// set, as for `from_parts`; existing losses do not affect this check.
    pub fn validate_loss(
        &self,
        key: &LostKey,
        value: &LostValue<V>,
        oracle: &impl WriteOracle,
    ) -> Result<(), MergeError> {
        let invalid = || MergeError::InvalidLostValue(key.clone());
        let inc = value.incarnation;
        if inc.is_multiple_of(2) || inc > self.generation {
            return Err(invalid());
        }
        let start = self.generations.get(&inc).ok_or_else(invalid)?;
        let setter_stamp = timestamp(oracle, key.write)?;
        if setter_stamp < timestamp(oracle, *start)? {
            return Err(invalid());
        }
        let canonical = if inc < self.generation {
            let delete = inc.checked_add(1).ok_or(MergeError::GenerationExhausted)?;
            *self.generations.get(&delete).ok_or_else(invalid)?
        } else {
            let cell = self.cells.get(&key.column).ok_or_else(invalid)?;
            if timestamp(oracle, cell.write)? <= setter_stamp {
                return Err(invalid());
            }
            cell.write
        };
        if value.replaced_by != canonical || oracle.had_read(canonical, key.write)? {
            return Err(invalid());
        }
        for parent in value.value.parents.values() {
            parent.validate_written(&self.row)?;
        }
        Ok(())
    }

    /// The table, key and audience this state belongs to.
    pub fn row(&self) -> &RowId {
        &self.row
    }
    /// How many times the row has been created, deleted or re-added.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Whether the row is present in merged state, before removal rules.
    pub fn present(&self) -> bool {
        !self.generation.is_multiple_of(2)
    }
    /// The smallest-stamped write that moved the row to each generation.
    pub fn generations(&self) -> &BTreeMap<u64, WriteId> {
        &self.generations
    }
    /// The winning write and value of every set column.
    pub fn cells(&self) -> &BTreeMap<String, Cell<V>> {
        &self.cells
    }
    /// Every lost value, its incarnation and what replaced it.
    pub fn lost(&self) -> &BTreeMap<LostKey, LostValue<V>> {
        &self.lost
    }

    /// §8.5: a unique claim dates from the latest winning setter of any of
    /// its columns. Null reference substitutions keep that setter.
    pub fn claim_timestamp(
        &self,
        columns: &[String],
        oracle: &impl WriteOracle,
    ) -> Result<Timestamp, MergeError> {
        let mut latest = None;
        for column in columns {
            let cell = self
                .cells
                .get(column)
                .ok_or_else(|| MergeError::MissingClaimColumn(column.clone()))?;
            let stamp = timestamp(oracle, cell.write)?;
            latest = Some(match latest {
                Some(old) => std::cmp::max(old, stamp),
                None => stamp,
            });
        }
        latest.ok_or(MergeError::EmptyClaim)
    }
}

/// A change to the row's lost-value records, to commit with its merged state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LostChange<V> {
    /// Create or replace a lost-value record.
    Put(LostKey, LostValue<V>),
    /// A replacing write had read the value, so it is no longer lost.
    Remove(LostKey),
}

/// The complete new row state and the changes to its lost-value records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowUpdate<V> {
    /// Commit this state with the write's other row changes.
    pub state: RowState<V>,
    /// Changes to `_coven_lost` for this row only.
    pub lost_changes: Vec<LostChange<V>>,
}

pub(crate) fn timestamp(oracle: &impl WriteOracle, id: WriteId) -> Result<Timestamp, MergeError> {
    oracle.timestamp(id).ok_or(MergeError::MissingWrite(id))
}

/// Apply one write to one row (Appendix B, B5; Lean `step`). It touches only
/// the supplied row. All past writes must be applied and the arriving write
/// must be absent from the oracle. Call for each changed row against the same
/// pre-write oracle, then commit all results in the database's transaction.
/// Parity, future generations, missing metadata and overflow are checked here.
/// A nonzero generation needs a read at least as recent as its earliest
/// recorded changer. That is the locally checkable part of assumption 3:
/// the canonical record cannot tell whether another read write also reached
/// the generation. The authoring database establishes the full witness when
/// producing a change; test histories validate it explicitly. The input
/// state is unchanged on every error path.
pub fn apply<V: Clone + Eq, P: crate::WritePast>(
    state: &RowState<V>,
    write: &Write<V, P>,
    oracle: &impl WriteOracle,
) -> Result<RowUpdate<V>, MergeError> {
    write.validate_metadata(oracle)?;
    let change = write
        .changes
        .get(&state.row)
        .ok_or_else(|| MergeError::MissingChange(state.row.clone()))?;
    change.validate(&state.row)?;
    if change.generation > state.generation {
        return Err(MergeError::GenerationAhead {
            row: state.row.clone(),
        });
    }
    if change.generation != 0 {
        let first = state.generations.get(&change.generation).ok_or_else(|| {
            MergeError::GenerationNotSeen {
                row: state.row.clone(),
                generation: change.generation,
            }
        })?;
        let earliest = timestamp(oracle, *first)?;
        let mut possible_witness = false;
        for past in write.had_read.frontier() {
            possible_witness |= timestamp(oracle, *past)? >= earliest;
        }
        if !possible_witness {
            return Err(MergeError::GenerationNotSeen {
                row: state.row.clone(),
                generation: change.generation,
            });
        }
    }
    let incarnation = change.incarnation()?;
    let columns = change.operation.columns();
    let deletes = matches!(change.operation, Operation::Delete);
    let mut next = state.clone();
    // Existing lost values are updated from the OLD row, exactly as lostStep.
    for (key, old) in &state.lost {
        let sets = columns.is_some_and(|c| c.contains_key(&key.column));
        let replaces = (deletes && change.generation == old.incarnation)
            || (sets
                && incarnation == old.incarnation
                && timestamp(oracle, key.write)? < write.timestamp);
        if replaces {
            if write.had_read.contains(&key.write) {
                next.lost.remove(key);
            } else {
                let replacement = if deletes {
                    if old.incarnation < state.generation
                        && timestamp(oracle, old.replaced_by)? < write.timestamp
                    {
                        old.replaced_by
                    } else {
                        write.id
                    }
                } else if old.incarnation < state.generation
                    || timestamp(oracle, old.replaced_by)? > write.timestamp
                {
                    old.replaced_by
                } else {
                    write.id
                };
                next.lost.insert(
                    key.clone(),
                    LostValue {
                        replaced_by: replacement,
                        ..old.clone()
                    },
                );
            }
        }
    }
    // Current values become lost only when the arriving replacer hadn't read them.
    for (column, current) in &state.cells {
        let replaces = (deletes && change.generation == state.generation)
            || (incarnation == state.generation
                && columns.is_some_and(|c| c.contains_key(column))
                && timestamp(oracle, current.write)? < write.timestamp);
        if replaces && !write.had_read.contains(&current.write) {
            next.lost.insert(
                LostKey {
                    column: column.clone(),
                    write: current.write,
                },
                LostValue {
                    incarnation: state.generation,
                    value: current.value.clone(),
                    replaced_by: write.id,
                },
            );
        }
    }
    // An arriving value loses to an already-applied delete or later setter.
    if let Some(columns) = columns {
        for (column, value) in columns {
            let replacement = if incarnation < state.generation {
                let deleted_generation = incarnation
                    .checked_add(1)
                    .ok_or(MergeError::GenerationExhausted)?;
                Some(*state.generations.get(&deleted_generation).ok_or_else(|| {
                    MergeError::GenerationNotSeen {
                        row: state.row.clone(),
                        generation: deleted_generation,
                    }
                })?)
            } else if incarnation == state.generation {
                match state.cells.get(column) {
                    Some(current) if timestamp(oracle, current.write)? > write.timestamp => {
                        Some(current.write)
                    }
                    _ => None,
                }
            } else {
                None
            };
            if let Some(replaced_by) = replacement {
                next.lost.insert(
                    LostKey {
                        column: column.clone(),
                        write: write.id,
                    },
                    LostValue {
                        incarnation,
                        value: value.clone(),
                        replaced_by,
                    },
                );
            }
        }
    }
    if change.operation.advances() {
        let generation = change
            .generation
            .checked_add(1)
            .ok_or(MergeError::GenerationExhausted)?;
        let winner = match state.generations.get(&generation) {
            Some(old) if timestamp(oracle, *old)? < write.timestamp => *old,
            _ => write.id,
        };
        next.generations.insert(generation, winner);
        if state.generation < generation {
            next.generation = generation;
            next.cells.clear();
            if let Operation::Insert(columns) = &change.operation {
                for (column, value) in columns {
                    next.cells.insert(
                        column.clone(),
                        Cell {
                            write: write.id,
                            value: value.clone(),
                        },
                    );
                }
            }
        }
    }
    if incarnation == state.generation {
        if let Some(columns) = columns {
            for (column, value) in columns {
                let wins = match state.cells.get(column) {
                    Some(old) => timestamp(oracle, old.write)? < write.timestamp,
                    None => true,
                };
                if wins {
                    next.cells.insert(
                        column.clone(),
                        Cell {
                            write: write.id,
                            value: value.clone(),
                        },
                    );
                }
            }
        }
    }
    let mut lost_changes = Vec::new();
    for key in state.lost.keys() {
        if !next.lost.contains_key(key) {
            lost_changes.push(LostChange::Remove(key.clone()));
        }
    }
    for (key, value) in &next.lost {
        if state.lost.get(key) != Some(value) {
            lost_changes.push(LostChange::Put(key.clone(), value.clone()));
        }
    }
    Ok(RowUpdate {
        state: next,
        lost_changes,
    })
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
