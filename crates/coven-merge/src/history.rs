use crate::{MergeError, RowId, Timestamp, Write, WriteId, WriteOracle};
use std::collections::BTreeMap;
#[cfg(test)]
use {
    crate::{Cell, LostKey, LostValue, Operation, RowState},
    std::collections::BTreeSet,
};

/// A finite, causally closed set of writes satisfying Appendix B's four
/// assumptions. It is also an in-memory oracle for an applied set.
/// Test support: requires `test-utils` outside this crate's unit tests.
#[derive(Clone, Debug)]
pub struct History<V> {
    writes: BTreeMap<WriteId, Write<V>>,
}

impl<V> History<V> {
    /// Validate a set without imposing an arrival order. Check timestamp
    /// uniqueness, timestamp causality, generation witnesses and parity.
    pub fn new(writes: impl IntoIterator<Item = Write<V>>) -> Result<Self, MergeError> {
        let mut by_id = BTreeMap::new();
        let mut stamps = BTreeMap::new();
        for write in writes {
            if write.timestamp.device() != write.id.device {
                return Err(MergeError::TimestampDevice(write.id));
            }
            if by_id.contains_key(&write.id) {
                return Err(MergeError::DuplicateWrite(write.id));
            }
            if let Some(other) = stamps.insert(write.timestamp, write.id) {
                return Err(MergeError::DuplicateTimestamp(other, write.id));
            }
            for (row, change) in &write.changes {
                change.validate(row)?;
            }
            by_id.insert(write.id, write);
        }
        let history = Self { writes: by_id };
        for write in history.writes.values() {
            for past in &write.had_read {
                let stamp = history
                    .timestamp(*past)
                    .ok_or(MergeError::MissingWrite(*past))?;
                if stamp >= write.timestamp {
                    return Err(MergeError::CausalTimestamp(write.id));
                }
            }
            for (row, change) in &write.changes {
                if change.generation != 0 {
                    let mut seen = false;
                    for past in &write.had_read {
                        seen |= history.moved_to(*past, row)? == Some(change.generation);
                    }
                    if !seen {
                        return Err(MergeError::GenerationNotSeen {
                            row: row.clone(),
                            generation: change.generation,
                        });
                    }
                }
            }
        }
        Ok(history)
    }

    fn moved_to(&self, write: WriteId, row: &RowId) -> Result<Option<u64>, MergeError> {
        let write = self
            .writes
            .get(&write)
            .ok_or(MergeError::MissingWrite(write))?;
        match write.changes.get(row) {
            Some(change) if change.operation.advances() => Ok(Some(
                change
                    .generation
                    .checked_add(1)
                    .ok_or(MergeError::GenerationExhausted)?,
            )),
            _ => Ok(None),
        }
    }

    /// Writes by their device and number; this is a set, not an arrival order.
    pub fn writes(&self) -> &BTreeMap<WriteId, Write<V>> {
        &self.writes
    }
}

impl<V> WriteOracle for History<V> {
    fn timestamp(&self, write: WriteId) -> Option<Timestamp> {
        self.writes.get(&write).map(|w| w.timestamp)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let write = self
            .writes
            .get(&reader)
            .ok_or(MergeError::MissingWrite(reader))?;
        if !self.writes.contains_key(&earlier) {
            return Err(MergeError::MissingWrite(earlier));
        }
        Ok(write.had_read.contains(&earlier))
    }
}

/// The merged state as a function of a set alone (Appendix B, B4; Lean
/// `IsSpec`). This does not call `apply` or sort writes into a causal order.
/// Each component is selected directly from all setters and deletes.
#[cfg(test)]
pub(crate) fn from_writes<V: Clone>(
    history: &History<V>,
) -> Result<BTreeMap<RowId, RowState<V>>, MergeError> {
    let rows: BTreeSet<_> = history
        .writes
        .values()
        .flat_map(|w| w.changes.keys().cloned())
        .collect();
    let mut result = BTreeMap::new();
    for row in rows {
        let changes: Vec<_> = history
            .writes
            .values()
            .filter_map(|w| w.changes.get(&row).map(|c| (w, c)))
            .collect();
        let mut state = RowState::new(row.clone());
        for (write, change) in &changes {
            if change.operation.advances() {
                let generation = change
                    .generation
                    .checked_add(1)
                    .ok_or(MergeError::GenerationExhausted)?;
                state.generation = state.generation.max(generation);
                let earlier = match state.generations.get(&generation) {
                    Some(old) => write.timestamp < crate::state::timestamp(history, *old)?,
                    None => true,
                };
                if earlier {
                    state.generations.insert(generation, write.id);
                }
            }
        }
        for (write, change) in &changes {
            if let Some(columns) = change.operation.columns() {
                let incarnation = change.incarnation()?;
                for (column, value) in columns {
                    if incarnation == state.generation {
                        let wins = match state.cells.get(column) {
                            Some(old) => {
                                write.timestamp > crate::state::timestamp(history, old.write)?
                            }
                            None => true,
                        };
                        if wins {
                            state.cells.insert(
                                column.clone(),
                                Cell {
                                    write: write.id,
                                    value: value.clone(),
                                },
                            );
                        }
                    }
                    let mut replacers = Vec::new();
                    let mut deletes = Vec::new();
                    for (other, ch) in &changes {
                        if matches!(ch.operation, Operation::Delete) && ch.generation == incarnation
                        {
                            deletes.push(*other);
                            replacers.push(*other);
                        } else if ch.incarnation()? == incarnation
                            && ch
                                .operation
                                .columns()
                                .is_some_and(|c| c.contains_key(column))
                            && write.timestamp < other.timestamp
                        {
                            replacers.push(*other);
                        }
                    }
                    if !replacers.is_empty()
                        && replacers.iter().all(|w| !w.had_read.contains(&write.id))
                    {
                        let replacement = deletes
                            .iter()
                            .min_by_key(|w| w.timestamp)
                            .or_else(|| replacers.iter().max_by_key(|w| w.timestamp));
                        if let Some(replacement) = replacement {
                            state.lost.insert(
                                LostKey {
                                    column: column.clone(),
                                    write: write.id,
                                },
                                LostValue {
                                    incarnation,
                                    value: value.clone(),
                                    replaced_by: replacement.id,
                                },
                            );
                        }
                    }
                }
            }
        }
        result.insert(row, state);
    }
    Ok(result)
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
