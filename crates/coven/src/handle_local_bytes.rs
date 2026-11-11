//! What this device keeps for the store, measured on request and as it
//! changes (§17.8, §19.3, E8).

use crate::*;

impl CovenHandle {
    /// One measurement of what the store keeps on this device. The cache's
    /// counters, the partial downloads' sizes and the budgets come from one
    /// read of the database; the store's directory is then walked for its own
    /// files and the archives resets kept. The cache's bytes are counted by
    /// its entries, so the walk leaves its folder out.
    pub async fn local_bytes(&self) -> CovenResult<LocalBytes> {
        Ok(self.database.local_bytes().await?)
    }

    /// The first measurement at once, then a new one after each write that
    /// changes the cache's committed entries or a budget, and after each
    /// `delete_discarded_devices`. Partial downloads are measured with each
    /// but start none.
    pub fn subscribe_local_bytes(&self) -> LocalBytesWatch {
        self.database.subscribe_local_bytes()
    }
}

#[cfg(test)]
#[path = "handle_local_bytes_tests.rs"]
mod tests;
