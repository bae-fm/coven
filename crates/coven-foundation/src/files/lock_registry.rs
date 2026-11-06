//! Process-local lock lifetimes. Only closing instances cause an async wait.

use super::file_lock::FileLock;
use super::lock::{lock_bootstrap, lock_existing, StoreLockError};
use crate::id_source::StoreId;
use crate::task::{blocking_completing, BugSinkRef};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum LockKind {
    Writer,
    Bootstrap,
    /// A shared readers' lock: any number of instances hold it at once, and
    /// no writer or bootstrap waits on one closing.
    Reader,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockPhase {
    Live,
    Closing,
    Released,
}

type StoreKey = (PathBuf, StoreId);

#[derive(Debug)]
struct Registry {
    stores: BTreeMap<StoreKey, BTreeMap<(LockKind, LockInstance), Arc<LockEntry>>>,
    /// How many locks were reserved: each reservation is the next instance.
    reserved: u64,
}

// Independently constructed layouts must see the same closing instance. The
// registry mutex serializes classification and reservation. A reservation stays
// registered until its acquisition worker finishes and its OS handle closes.
static STORES: LazyLock<Mutex<Registry>> =
    LazyLock::new(|| Mutex::new(Registry { stores: BTreeMap::new(), reserved: 0 }));

/// What a lock of `STORES` names. The registry is shared by every store
/// this process opens, and a change to it checks the registry's rules as it
/// goes, so a panic can leave it half-changed, and no one store's handle
/// covers it: the process ends (api.md E15).
const REGISTRY: &str = "the process's store lock registry";

/// One reservation of a lock: no other reservation in the process, of any
/// store, has the same instance.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct LockInstance(u64);

#[derive(Debug)]
struct LockEntry {
    store: StoreKey,
    kind: LockKind,
    instance: LockInstance,
    phase: watch::Sender<LockPhase>,
}

/// Marks an owning scope as closing on drop, without retaining its lock.
/// Share an `Arc` of this guard across clones of that owner. A distinct owner
/// scope can use `closing_guard`; ending either scope marks the instance.
#[derive(Debug)]
pub struct StoreClosingGuard(Option<Arc<LockEntry>>);

impl StoreClosingGuard {
    /// A separate owning scope for the same lock instance.
    pub fn closing_guard(&self) -> Self {
        Self(self.0.clone())
    }

    /// Begin closing before awaiting shutdown of workers and connections.
    /// A guard from a released instance never changes a replacement instance.
    pub fn begin_close(&self) {
        let Some(entry) = &self.0 else { return };
        let registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
        if *entry.phase.borrow() == LockPhase::Released {
            return;
        }
        let Some(locks) = registry.stores.get(&entry.store) else {
            crate::locks::abort_broken(REGISTRY, "a held lock is registered")
        };
        // A dropped bootstrap owns the installation, including a writer that
        // may still be opening or running blocking work on its behalf. Any
        // other lock closes only its own instance: one reader closing leaves
        // the others live.
        for held in locks.values() {
            if entry.kind == LockKind::Bootstrap || Arc::ptr_eq(held, entry) {
                held.phase.send_replace(LockPhase::Closing);
            }
        }
    }

    /// Transfer lifetime responsibility to the successfully constructed owner.
    pub fn disarm(mut self) {
        self.0.take();
    }
}

impl Drop for StoreClosingGuard {
    fn drop(&mut self) {
        self.begin_close();
    }
}

#[derive(Debug)]
pub(super) struct RegisteredLock {
    // Field order makes notification follow both unlock and file close.
    _file: FileLock,
    registration: LockRegistration,
}

#[derive(Debug)]
struct LockRegistration {
    entry: Arc<LockEntry>,
}

impl RegisteredLock {
    pub(super) fn closing_guard(&self) -> StoreClosingGuard {
        self.registration.closing_guard()
    }

    pub(super) fn instance(&self) -> LockInstance {
        self.registration.entry.instance
    }

    #[cfg(test)]
    pub(super) fn inherited_file(&self) -> std::fs::File {
        self._file.inherited_file()
    }
}

impl LockRegistration {
    fn closing_guard(&self) -> StoreClosingGuard {
        StoreClosingGuard(Some(self.entry.clone()))
    }

    fn acquire_file(self) -> Result<RegisteredLock, StoreLockError> {
        let (directory, id) = &self.entry.store;
        let file = match self.entry.kind {
            LockKind::Writer => lock_existing(directory, *id, false)?.1,
            LockKind::Reader => lock_existing(directory, *id, true)?.1,
            LockKind::Bootstrap => lock_bootstrap(directory, *id)?,
        };
        Ok(RegisteredLock { _file: file, registration: self })
    }

    /// Take the lock file on the blocking pool. A caller that stops waiting
    /// leaves the work to finish, and its panic goes to `bugs` (E15).
    async fn open_on_worker(self, bugs: &BugSinkRef) -> Result<RegisteredLock, StoreLockError> {
        let closing = self.closing_guard();
        let result =
            blocking_completing("lock the store", bugs.clone(), move || self.acquire_file())
                .await
                .map_err(|failed| StoreLockError::from(crate::files::FileError::from(failed)))
                .and_then(|result| result);
        closing.disarm();
        result
    }
}

impl Drop for LockRegistration {
    fn drop(&mut self) {
        let mut registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
        let Some(locks) = registry.stores.get_mut(&self.entry.store) else {
            crate::locks::abort_broken(REGISTRY, "a held lock is registered")
        };
        match locks.remove(&(self.entry.kind, self.entry.instance)) {
            Some(removed) if Arc::ptr_eq(&removed, &self.entry) => {}
            _ => crate::locks::abort_broken(REGISTRY, "a held lock is registered as itself"),
        }
        if locks.is_empty() {
            registry.stores.remove(&self.entry.store);
        }
        self.entry.phase.send_replace(LockPhase::Released);
    }
}

/// The registered instances of `kind` among one store's locks.
fn held(
    locks: &BTreeMap<(LockKind, LockInstance), Arc<LockEntry>>,
    kind: LockKind,
) -> impl Iterator<Item = &Arc<LockEntry>> {
    locks.iter().filter(move |((held, _), _)| *held == kind).map(|(_, entry)| entry)
}

/// Take a shared readers' lock now, registered so that a deletion in this
/// process waits for it once its holder begins closing.
pub(super) fn acquire_reader(
    directory: &Path,
    id: StoreId,
) -> Result<RegisteredLock, StoreLockError> {
    let registration = {
        let mut registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
        reserve(&mut registry, (directory.to_owned(), id), LockKind::Reader)
    };
    registration.acquire_file()
}

fn reserve(registry: &mut Registry, store: StoreKey, kind: LockKind) -> LockRegistration {
    registry.reserved += 1;
    let entry = Arc::new(LockEntry {
        store: store.clone(),
        kind,
        instance: LockInstance(registry.reserved),
        phase: watch::channel(LockPhase::Live).0,
    });
    let key = (kind, entry.instance);
    assert!(registry.stores.entry(store).or_default().insert(key, entry.clone()).is_none());
    LockRegistration { entry }
}

#[cfg(any(test, feature = "test-utils"))]
pub(super) fn try_acquire(
    directory: &Path,
    id: StoreId,
    kind: LockKind,
) -> Result<RegisteredLock, StoreLockError> {
    let registration = {
        let mut registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
        let store = (directory.to_owned(), id);
        if registry.stores.get(&store).is_some_and(|locks| held(locks, kind).next().is_some()) {
            return Err(StoreLockError::AlreadyOpen(id));
        }
        reserve(&mut registry, store, kind)
    };
    registration.acquire_file()
}

enum Acquisition {
    Waiting(watch::Receiver<LockPhase>),
    Reserved(LockRegistration),
}

pub(super) async fn acquire_registered(
    directory: &Path,
    id: StoreId,
    kind: LockKind,
    bugs: &BugSinkRef,
) -> Result<RegisteredLock, StoreLockError> {
    let store = (directory.to_owned(), id);
    loop {
        let acquisition = {
            let mut registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
            let locks = registry.stores.get(&store);
            // A live holder of the requested lock always refuses immediately,
            // even if a different lock on this directory is closing.
            if locks.is_some_and(|locks| {
                held(locks, kind).any(|entry| *entry.phase.borrow() == LockPhase::Live)
            }) {
                return Err(StoreLockError::AlreadyOpen(id));
            }
            // Readers share the store with a writer and a bootstrap, so
            // neither waits on one closing.
            match locks.and_then(|locks| {
                locks.values().find(|entry| {
                    entry.kind != LockKind::Reader && *entry.phase.borrow() == LockPhase::Closing
                })
            }) {
                Some(entry) => Acquisition::Waiting(entry.phase.subscribe()),
                None => Acquisition::Reserved(reserve(&mut registry, store.clone(), kind)),
            }
        };
        let mut released = match acquisition {
            Acquisition::Reserved(registration) => return registration.open_on_worker(bugs).await,
            Acquisition::Waiting(released) => released,
        };
        released
            .wait_for(|phase| *phase == LockPhase::Released)
            .await
            .expect("registered lock publishes release before dropping its sender");
    }
}

/// Wait until no instance of the store at `directory` in this process is
/// closing, a read-only one included. A live instance is not waited for:
/// whatever lock the caller takes next refuses it.
pub(super) async fn closed(directory: &Path, id: StoreId) {
    let store = (directory.to_owned(), id);
    loop {
        let mut released = {
            let registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
            match registry.stores.get(&store).and_then(|locks| {
                locks.values().find(|entry| *entry.phase.borrow() == LockPhase::Closing)
            }) {
                Some(entry) => entry.phase.subscribe(),
                None => return,
            }
        };
        released
            .wait_for(|phase| *phase == LockPhase::Released)
            .await
            .expect("registered lock publishes release before dropping its sender");
    }
}

/// Whether `instance` of the store's bootstrap lock is held, and its holder
/// not closing: the bootstrap that took it is running.
pub(super) fn bootstrap_running(directory: &Path, id: StoreId, instance: LockInstance) -> bool {
    let registry = crate::locks::lock_or_abort(&STORES, REGISTRY);
    registry
        .stores
        .get(&(directory.to_owned(), id))
        .and_then(|locks| locks.get(&(LockKind::Bootstrap, instance)))
        .is_some_and(|entry| *entry.phase.borrow() == LockPhase::Live)
}

#[cfg(test)]
#[path = "lock_registry_tests.rs"]
mod tests;
