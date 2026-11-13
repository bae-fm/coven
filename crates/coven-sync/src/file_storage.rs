//! Whether the file layer has storage, and if not why (§17.7): the four
//! states every reader follows one rule for.

use crate::{
    files::{Files, FilesInner, FilesMode},
    FileReadError,
};
use coven_database::DbError;
use coven_storage::Storage;
use std::sync::Arc;

/// What the file layer's storage is. A reader goes ahead when it is
/// connected, waits while a connection is coming, and fails at once when
/// none is or the store has closed.
#[derive(Clone)]
pub(crate) enum StorageState {
    /// Storage is connected.
    Connected(Arc<dyn Storage>),
    /// A connection attempt is running or backing off, so storage will be
    /// connected, or the attempt ends and says otherwise.
    Connecting,
    /// Sync is stopped or no storage is set up: nothing connects until the
    /// app starts sync or sets storage up.
    Released,
    /// The store has closed.
    Closed,
}

/// The facts the four states follow from. Each is written by one owner:
/// the connection's through `Files::set_storage`, the attempt's through
/// `ConnectionComing`, and `Files::close`.
pub(super) struct StorageSlot {
    storage: Option<Arc<dyn Storage>>,
    /// A connection attempt is running or backing off.
    attempting: bool,
    closed: bool,
    /// Which connection `storage` is, counting every change of it.
    connection: u64,
}

impl StorageSlot {
    /// The slot of a store as it opens, with the storage its owner loaded, if
    /// any.
    pub(super) fn opened(storage: Option<Arc<dyn Storage>>) -> Self {
        Self { storage, attempting: false, closed: false, connection: 0 }
    }

    pub(super) fn state(&self) -> StorageState {
        if self.closed {
            StorageState::Closed
        } else if let Some(storage) = &self.storage {
            StorageState::Connected(storage.clone())
        } else if self.attempting {
            StorageState::Connecting
        } else {
            StorageState::Released
        }
    }

    pub(super) fn connection(&self) -> u64 {
        self.connection
    }

    /// Replace the storage, as a new connection or its release.
    pub(super) fn connect(&mut self, storage: Option<Arc<dyn Storage>>) {
        self.storage = storage;
        self.connection += 1;
    }

    pub(super) fn close(&mut self) {
        self.storage = None;
        self.closed = true;
    }

    /// The store is closing: reads fail as closed, though the storage stays
    /// until the close releases it.
    pub(super) fn closing(&mut self) {
        self.closed = true;
    }
}

impl<M: FilesMode> FilesInner<M> {
    /// The storage's state now.
    pub(crate) fn storage_state(&self) -> StorageState {
        self.storage.borrow().state()
    }

    /// The first state that is not connecting, waiting while a connection
    /// is coming. A call that stops waiting leaves nothing behind. Callers
    /// hold none of the locks a connection change takes
    /// (`Files::set_storage`'s), which the change that ends the wait needs.
    pub(crate) async fn settled(&self) -> Result<StorageState, FileReadError> {
        let mut changes = self.storage.subscribe();
        loop {
            // Registered before the state is read: a close between them
            // still wakes this wait.
            let closing = self.hold_wake.notified();
            tokio::pin!(closing);
            closing.as_mut().enable();
            self.check_open()?;
            let state = changes.borrow_and_update().state();
            if !matches!(state, StorageState::Connecting) {
                return Ok(state);
            }
            tokio::select! {
                () = closing => {}
                changed = changes.changed() => {
                    if changed.is_err() {
                        return Err(DbError::StoreClosed.into());
                    }
                }
            }
        }
    }
}

/// The storage `state` holds, or why a read or request has none: no storage
/// is connected and none is coming, or the store has closed.
pub(super) fn connected(state: StorageState) -> Result<Arc<dyn Storage>, FileReadError> {
    match state {
        StorageState::Connected(storage) => Ok(storage),
        StorageState::Connecting | StorageState::Released => Err(FileReadError::NoStorage),
        StorageState::Closed => Err(DbError::StoreClosed.into()),
    }
}

impl Files {
    /// Note that a connection attempt is running: until the returned guard
    /// drops, reads that need storage wait for it rather than fail. The
    /// guard drops when the attempt ends, connected or not, and a store
    /// with no storage then reads as released.
    pub fn expect_connection(&self) -> ConnectionComing {
        self.inner.storage.send_modify(|slot| slot.attempting = true);
        ConnectionComing { inner: self.inner.clone() }
    }
}

impl Files {
    /// Note that the store is closing, before its connection is released: a
    /// read waiting for that connection, or asked after this, fails as the
    /// store closing, not as having no storage.
    pub fn closing(&self) {
        self.inner.storage.send_modify(|slot| slot.closing());
    }
}

/// A connection attempt's mark on the file layer's storage: while it lives,
/// a connection is coming (§17.7).
#[must_use = "the connection is expected only while this lives"]
pub struct ConnectionComing {
    inner: Arc<FilesInner>,
}

impl Drop for ConnectionComing {
    fn drop(&mut self) {
        self.inner.storage.send_if_modified(|slot| std::mem::take(&mut slot.attempting));
    }
}
