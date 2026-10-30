//! The rows a merge has read and may read again: every one, or the most
//! recently used up to a budget of their values' bytes, so applying a
//! streamed write reads a row once for each run of uses while never holding
//! the write's rows whole (§21.2).

use coven_format::value::Value;
use std::cell::RefCell;
use std::collections::BTreeMap;

/// The bytes of rows each of a streamed write's merge caches keeps, beyond
/// which the least recently used go: 256 KiB, a few hundred rows of a
/// library's size, or four of 64 KiB.
pub(crate) const RECENT: usize = 256 << 10;

/// What a cached row holds, counted toward a budget.
pub(crate) trait Weight {
    fn weight(&self) -> usize;
}

/// The bytes a value holds.
pub(crate) fn value_weight(value: &Value) -> usize {
    match value {
        Value::Text(text) => text.len(),
        Value::Blob(bytes) => bytes.len(),
        _ => 8,
    }
}

/// What each cached row costs beyond its values: its key and bookkeeping.
const ENTRY: usize = 128;

pub(crate) enum RowCache<K, V> {
    /// Nothing is kept: each read goes to the database.
    Off,
    /// Every row read is kept until the cache drops.
    All(RefCell<BTreeMap<K, V>>),
    /// The most recently used rows, up to `budget` bytes.
    Recent(RefCell<Recent<K, V>>),
}

pub(crate) struct Recent<K, V> {
    budget: usize,
    bytes: usize,
    tick: u64,
    rows: BTreeMap<K, (V, u64, usize)>,
    order: BTreeMap<u64, K>,
}

impl<K: Ord + Clone, V: Clone + Weight> RowCache<K, V> {
    pub(crate) fn all() -> Self {
        Self::All(RefCell::new(BTreeMap::new()))
    }

    pub(crate) fn recent(budget: usize) -> Self {
        Self::Recent(RefCell::new(Recent {
            budget,
            bytes: 0,
            tick: 0,
            rows: BTreeMap::new(),
            order: BTreeMap::new(),
        }))
    }

    /// An empty cache keeping what this one keeps.
    pub(crate) fn like<W: Clone + Weight>(&self) -> RowCache<K, W> {
        match self {
            Self::Off => RowCache::Off,
            Self::All(_) => RowCache::all(),
            Self::Recent(recent) => RowCache::recent(recent.borrow().budget),
        }
    }

    pub(crate) fn keeps(&self) -> bool {
        !matches!(self, Self::Off)
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        match self {
            Self::Off => None,
            Self::All(rows) => rows.borrow().get(key).cloned(),
            Self::Recent(recent) => recent.borrow_mut().get(key),
        }
    }

    /// Look at a kept row without counting it as a use.
    pub(crate) fn peek<T>(&self, key: &K, look: impl FnOnce(&V) -> T) -> Option<T> {
        match self {
            Self::Off => None,
            Self::All(rows) => rows.borrow().get(key).map(look),
            Self::Recent(recent) => recent.borrow().rows.get(key).map(|(row, _, _)| look(row)),
        }
    }

    pub(crate) fn insert(&self, key: K, row: V) {
        match self {
            Self::Off => {}
            Self::All(rows) => {
                rows.borrow_mut().insert(key, row);
            }
            Self::Recent(recent) => recent.borrow_mut().insert(key, row),
        }
    }
}

impl<K: Ord + Clone, V: Clone + Weight> Recent<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        let tick = self.tick;
        let (row, used, _) = self.rows.get_mut(key)?;
        let row = row.clone();
        let previous = std::mem::replace(used, tick);
        self.tick += 1;
        let key = self.order.remove(&previous).expect("a kept row is ordered");
        self.order.insert(tick, key);
        Some(row)
    }

    fn insert(&mut self, key: K, row: V) {
        let weight = row.weight() + ENTRY;
        if let Some((_, used, old)) = self.rows.remove(&key) {
            self.order.remove(&used);
            self.bytes -= old;
        }
        // A row larger than the whole budget is read again on each use.
        if weight > self.budget {
            return;
        }
        while self.bytes + weight > self.budget {
            let (_, oldest) = self.order.pop_first().expect("a kept row to let go");
            let (_, _, freed) = self.rows.remove(&oldest).expect("an ordered row is kept");
            self.bytes -= freed;
        }
        self.order.insert(self.tick, key.clone());
        self.rows.insert(key, (row, self.tick, weight));
        self.tick += 1;
        self.bytes += weight;
    }
}

#[cfg(test)]
#[path = "row_cache_tests.rs"]
mod tests;
