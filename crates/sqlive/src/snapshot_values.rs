//! Values the host keeps in memory beside a database, which its SQL
//! functions read as each transaction's snapshot saw them (§4).
//!
//! A statement reads the tables from its transaction's snapshot, which fixes
//! the commits it sees. A function that read the host's memory as each call
//! ran could join those commits to memory that a later commit's consequences
//! had already changed, a moment that never existed. So each transaction
//! takes every value once, as its snapshot is fixed, and every call in the
//! transaction reads what it took. A read fixes its snapshot and takes the
//! values while no commit of the database is underway ([`CommitOrder`]), so
//! the values are those of a moment between its snapshot's last commit and
//! the next. A write takes them as it begins: it holds the database's write
//! lock, so no other commit can come between its snapshot and its values.

use panic_as_error::locks::{lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// One value the host keeps in memory, as one connection's transactions
/// read it: the host's SQL functions hold it and read it with [`Self::get`].
#[derive(Clone)]
pub struct SnapshotValue<T> {
    slot: Arc<SnapshotSlot<T>>,
}

struct SnapshotSlot<T> {
    /// The value as it is now.
    now: Box<dyn Fn() -> T + Send + Sync>,
    /// The value as the open transaction's snapshot saw it, if a
    /// transaction took it.
    taken: Mutex<Option<T>>,
}

impl<T: Clone> SnapshotValue<T> {
    /// The value as the open transaction's snapshot saw it. A statement
    /// outside a transaction reads it as it is now.
    pub fn get(&self) -> T {
        let taken = lock_unpoisoned(&self.slot.taken).clone();
        taken.unwrap_or_else(|| (self.slot.now)())
    }
}

/// What a connection's transaction does with one of its values.
trait TakeAtSnapshot: Send + Sync {
    /// Keep the value as it is now, for the transaction to read.
    fn take(&self);
    /// The transaction ended: the next statement reads the value afresh.
    fn forget(&self);
}

impl<T: Send> TakeAtSnapshot for SnapshotSlot<T> {
    fn take(&self) {
        let now = (self.now)();
        *lock_unpoisoned(&self.taken) = Some(now);
    }

    fn forget(&self) {
        *lock_unpoisoned(&self.taken) = None;
    }
}

/// The values one connection's setup registered, which each of its
/// transactions takes as its snapshot is fixed and forgets as it ends.
pub(crate) struct SnapshotValues(Vec<Arc<dyn TakeAtSnapshot>>);

impl SnapshotValues {
    /// None registered yet.
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    /// A value, which `now` reads as it is at the moment, that each
    /// transaction then takes and forgets.
    pub(crate) fn value<T: Clone + Send + 'static>(
        &mut self,
        now: impl Fn() -> T + Send + Sync + 'static,
    ) -> SnapshotValue<T> {
        let slot = Arc::new(SnapshotSlot { now: Box::new(now), taken: Mutex::new(None) });
        self.0.push(Arc::clone(&slot) as Arc<dyn TakeAtSnapshot>);
        SnapshotValue { slot }
    }

    /// The transaction's snapshot is fixed: keep each value as it is now.
    pub(crate) fn take(&self) {
        for value in &self.0 {
            value.take();
        }
    }

    /// The transaction ended.
    pub(crate) fn forget(&self) {
        for value in &self.0 {
            value.forget();
        }
    }
}

/// Orders one database's commits against its reads' snapshots: a commit
/// runs while no read fixes its snapshot, and a read fixes its snapshot and
/// takes its values while no commit runs. The database's connections share
/// it; a connection opened alone has its own.
pub(crate) struct CommitOrder(RwLock<()>);

impl CommitOrder {
    /// No commit running and no snapshot being fixed.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(()))
    }

    /// Fix a read's snapshot and take its values while this is held.
    pub(crate) fn reading(&self) -> RwLockReadGuard<'_, ()> {
        read_unpoisoned(&self.0)
    }

    /// Commit while this is held.
    pub(crate) fn committing(&self) -> RwLockWriteGuard<'_, ()> {
        write_unpoisoned(&self.0)
    }
}

#[cfg(test)]
#[path = "snapshot_values_tests.rs"]
mod tests;
