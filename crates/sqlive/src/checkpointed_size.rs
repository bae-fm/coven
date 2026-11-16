//! The main database file's size after each checkpoint, for the host.

use tokio::sync::watch;

/// A host's view of the main database file's size in bytes after the latest
/// checkpoint (`Database::checkpointed_size`). Each view keeps its own mark
/// of the sizes it has returned.
pub struct CheckpointedSize {
    sizes: watch::Receiver<u64>,
}

impl CheckpointedSize {
    pub(crate) fn new(sizes: watch::Receiver<u64>) -> Self {
        Self { sizes }
    }

    /// The size now. A later `changed` waits for a size after this one.
    pub fn bytes(&mut self) -> u64 {
        *self.sizes.borrow_and_update()
    }

    /// Wait until a checkpoint changes the size from the one this view last
    /// returned, and return the new size. Returns `None` once the database
    /// has closed, since no checkpoint sends a size after that.
    pub async fn changed(&mut self) -> Option<u64> {
        self.sizes.changed().await.ok()?;
        Some(self.bytes())
    }
}
