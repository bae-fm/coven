//! The change signal for one store directory's on-device blob copies.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// Raised whenever a blob copy in a store directory's `storage/cache` or
/// `storage/pinned` folder is written or removed.
///
/// Those folders are the cache's truth — whether a blob is kept offline is
/// which folder its file is in — so no database commit announces a change to
/// them. Every [`crate::store_dir::StoreDir`] naming one directory shares one
/// signal, because the directory is the shared resource: the store handle,
/// its database, and its sync loop each hold their own `StoreDir` value for
/// the same directory. The value counts changes; receivers only compare it.
#[derive(Clone, Debug)]
pub(crate) struct BlobCopySignal {
    sender: Arc<tokio::sync::watch::Sender<u64>>,
}

impl BlobCopySignal {
    /// The signal of the directory at `path`, shared with every other
    /// `StoreDir` naming it while any of them is alive.
    pub(crate) fn for_directory(path: &Path) -> Self {
        static SIGNALS: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::watch::Sender<u64>>>>> =
            OnceLock::new();
        let key = path.components().collect::<PathBuf>();
        let mut signals = SIGNALS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("blob copy signal registry poisoned");
        if let Some(sender) = signals.get(&key).and_then(Weak::upgrade) {
            return Self { sender };
        }
        signals.retain(|_, sender| sender.strong_count() > 0);
        let sender = Arc::new(tokio::sync::watch::channel(0).0);
        signals.insert(key, Arc::downgrade(&sender));
        Self { sender }
    }

    pub(crate) fn raise(&self) {
        self.sender
            .send_modify(|changes| *changes = changes.wrapping_add(1));
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.sender.subscribe()
    }
}
