//! Process-local lock lifetimes. Only closing instances cause an async wait.

use super::file_lock::FileLock;
use super::lock::{lock_bootstrap, lock_existing, StoreLockError};
use crate::id_source::StoreId;
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockPhase {
    Live,
    Closing,
    Released,
}

type StoreKey = (PathBuf, StoreId);
type Registry = BTreeMap<StoreKey, BTreeMap<LockKind, Arc<LockEntry>>>;

// Independently constructed layouts must see the same closing instance. The
// registry mutex serializes classification and reservation. A reservation stays
// registered until its acquisition worker finishes and its OS handle closes.
static STORES: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Debug)]
struct LockEntry {
    store: StoreKey,
    kind: LockKind,
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
        let stores = STORES.lock().expect("store registry lock poisoned");
        if *entry.phase.borrow() == LockPhase::Released {
            return;
        }
        let locks = stores.get(&entry.store).expect("held lock is registered");
        // A dropped bootstrap owns the installation, including a writer that
        // may still be opening or running blocking work on its behalf.
        for (kind, held) in locks {
            if entry.kind == LockKind::Bootstrap || *kind == entry.kind {
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
            LockKind::Bootstrap => lock_bootstrap(directory, *id)?,
        };
        Ok(RegisteredLock { _file: file, registration: self })
    }

    async fn open_on_worker(self) -> Result<RegisteredLock, StoreLockError> {
        let closing = self.closing_guard();
        let result = match tokio::task::spawn_blocking(move || self.acquire_file()).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("store lock acquisition task was cancelled: {error}"),
        };
        closing.disarm();
        result
    }
}

impl Drop for LockRegistration {
    fn drop(&mut self) {
        let mut stores = STORES.lock().expect("store registry lock poisoned");
        let locks = stores.get_mut(&self.entry.store).expect("held lock is registered");
        let removed = locks.remove(&self.entry.kind).expect("held lock kind");
        assert!(Arc::ptr_eq(&removed, &self.entry));
        if locks.is_empty() {
            stores.remove(&self.entry.store);
        }
        self.entry.phase.send_replace(LockPhase::Released);
    }
}

fn reserve(stores: &mut Registry, store: StoreKey, kind: LockKind) -> LockRegistration {
    let entry = Arc::new(LockEntry {
        store: store.clone(),
        kind,
        phase: watch::channel(LockPhase::Live).0,
    });
    assert!(stores.entry(store).or_default().insert(kind, entry.clone()).is_none());
    LockRegistration { entry }
}

#[cfg(any(test, feature = "test-utils"))]
pub(super) fn try_acquire(
    directory: &Path,
    id: StoreId,
    kind: LockKind,
) -> Result<RegisteredLock, StoreLockError> {
    let registration = {
        let mut stores = STORES.lock().expect("store registry lock poisoned");
        let store = (directory.to_owned(), id);
        if stores.get(&store).is_some_and(|locks| locks.contains_key(&kind)) {
            return Err(StoreLockError::AlreadyOpen(id));
        }
        reserve(&mut stores, store, kind)
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
) -> Result<RegisteredLock, StoreLockError> {
    let store = (directory.to_owned(), id);
    loop {
        let acquisition = {
            let mut stores = STORES.lock().expect("store registry lock poisoned");
            let locks = stores.get(&store);
            // A live holder of the requested lock always refuses immediately,
            // even if a different lock on this directory is closing.
            if locks
                .and_then(|locks| locks.get(&kind))
                .is_some_and(|entry| *entry.phase.borrow() == LockPhase::Live)
            {
                return Err(StoreLockError::AlreadyOpen(id));
            }
            match locks.and_then(|locks| {
                locks.values().find(|entry| *entry.phase.borrow() == LockPhase::Closing)
            }) {
                Some(entry) => Acquisition::Waiting(entry.phase.subscribe()),
                None => Acquisition::Reserved(reserve(&mut stores, store.clone(), kind)),
            }
        };
        let mut released = match acquisition {
            Acquisition::Reserved(registration) => return registration.open_on_worker().await,
            Acquisition::Waiting(released) => released,
        };
        released
            .wait_for(|phase| *phase == LockPhase::Released)
            .await
            .expect("registered lock publishes release before dropping its sender");
    }
}

#[cfg(test)]
#[path = "lock_registry_tests.rs"]
mod tests;
