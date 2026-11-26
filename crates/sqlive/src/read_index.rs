//! Which waiting live queries a write's row changes could affect
//! (SPEC.md section 7). A handle files each read of each subscription waiting
//! on known reads under the read's table and, when the read's scope has a key
//! (`RowScope::key`), under the values the key names in its column. A change to
//! a row is judged only against the subscriptions filed under its table without
//! a key, and those filed under a value its row holds in the key's column
//! before or after the change: a row that holds none of a key's values passes
//! no scope with that key. So judging a write reads only the live queries whose
//! scopes its rows could pass, however many wait.

use crate::SqlValue as Value;
use crate::{compare, RowImage};
use crate::{ReadSet, RowChange};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A subscription's number among its handle's.
pub(crate) type SubscriptionId = u64;

/// The reads of the subscriptions that wait on known reads, by table.
#[derive(Default)]
pub(crate) struct ReadIndex {
    tables: HashMap<String, TableReads>,
}

/// The reads of one table.
#[derive(Default)]
struct TableReads {
    /// The subscriptions with a read of the table whose scope has no key,
    /// which every change to the table reaches.
    unkeyed: BTreeSet<SubscriptionId>,
    /// The subscriptions with a read of the table whose scope has a key, by
    /// the key's column.
    keyed: HashMap<String, KeyedReads>,
}

/// The reads of one table whose scopes key on one column.
#[derive(Default)]
struct KeyedReads {
    /// Every subscription with such a read, which a row reaches whose value
    /// in the column the write didn't record: such a row passes every scope
    /// that tests the column.
    all: BTreeSet<SubscriptionId>,
    /// The subscriptions with such a read, by each value its key names.
    values: BTreeMap<KeyValue, BTreeSet<SubscriptionId>>,
}

impl ReadIndex {
    /// File `reads`, subscription `id`'s, under their tables and keys.
    pub(crate) fn insert(&mut self, id: SubscriptionId, reads: &ReadSet) {
        for read in reads {
            let table = self.tables.entry(read.table.clone()).or_default();
            let Some((column, values)) = read.scope.key() else {
                table.unkeyed.insert(id);
                continue;
            };
            let keyed = table.keyed.entry(column.to_owned()).or_default();
            keyed.all.insert(id);
            for value in values.into_iter().filter_map(KeyValue::of) {
                keyed.values.entry(value).or_default().insert(id);
            }
        }
    }

    /// Take out `reads`, which `insert` filed for subscription `id`.
    pub(crate) fn remove(&mut self, id: SubscriptionId, reads: &ReadSet) {
        for read in reads {
            let Some(table) = self.tables.get_mut(&read.table) else {
                continue;
            };
            match read.scope.key() {
                None => {
                    table.unkeyed.remove(&id);
                }
                Some((column, values)) => {
                    if let Some(keyed) = table.keyed.get_mut(column) {
                        keyed.all.remove(&id);
                        for value in values.into_iter().filter_map(KeyValue::of) {
                            if let Some(filed) = keyed.values.get_mut(&value) {
                                filed.remove(&id);
                                if filed.is_empty() {
                                    keyed.values.remove(&value);
                                }
                            }
                        }
                        if keyed.all.is_empty() {
                            table.keyed.remove(column);
                        }
                    }
                }
            }
            if table.unkeyed.is_empty() && table.keyed.is_empty() {
                self.tables.remove(&read.table);
            }
        }
    }

    /// Each subscription that some of `changes` could affect, with the
    /// positions of those changes among `changes`, in order. A change no
    /// row names, as a view's column that changed with no write, reaches
    /// every subscription that reads its table.
    pub(crate) fn candidates(&self, changes: &[RowChange]) -> BTreeMap<SubscriptionId, Vec<usize>> {
        let mut found: BTreeMap<SubscriptionId, Vec<usize>> = BTreeMap::new();
        for (position, change) in changes.iter().enumerate() {
            let Some(table) = self.tables.get(&change.table) else {
                continue;
            };
            let mut reach = |id: &SubscriptionId| {
                let positions = found.entry(*id).or_default();
                if positions.last() != Some(&position) {
                    positions.push(position);
                }
            };
            table.unkeyed.iter().for_each(&mut reach);
            for (column, keyed) in &table.keyed {
                match &change.rows {
                    None => keyed.all.iter().for_each(&mut reach),
                    Some((old, new)) => {
                        for row in [old, new].into_iter().flatten() {
                            keyed.reached(column, row).into_iter().flatten().for_each(&mut reach);
                        }
                    }
                }
            }
        }
        found
    }
}

impl KeyedReads {
    /// The subscriptions whose key, on `column`, `row` could pass: those
    /// filed under the value it holds there, every one when the write
    /// didn't record its value, and none when it holds NULL, which equals
    /// nothing.
    fn reached(&self, column: &str, row: &RowImage) -> Option<&BTreeSet<SubscriptionId>> {
        match row.get(column) {
            None => Some(&self.all),
            Some(value) => KeyValue::of(value.clone()).and_then(|key| self.values.get(&key)),
        }
    }
}

/// A value other than NULL, ordered as SQLite compares values with BINARY
/// collation and no conversion (`compare`), as a scope's equality compares
/// them: an integer and a real that compare equal are one key.
struct KeyValue(Value);

impl KeyValue {
    /// `value` as a key, unless it is NULL, which no scope's equality
    /// passes.
    fn of(value: Value) -> Option<Self> {
        (!matches!(value, Value::Null)).then_some(Self(value))
    }
}

impl Ord for KeyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0).expect("a key is never NULL")
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for KeyValue {}

#[cfg(test)]
#[path = "read_index_tests.rs"]
mod tests;
