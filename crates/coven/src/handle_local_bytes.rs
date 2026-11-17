//! What this device keeps for the store, read from coven's tables, once and
//! as it changes (§17.8, §19.3, E8).

use crate::*;

impl CovenHandle {
    /// What the store keeps on this device, from one snapshot of coven's
    /// tables: each declared namespace's budget and counters, the partial
    /// downloads that pins left, the store's own files and the archives
    /// that resets kept.
    pub async fn local_bytes(&self) -> CovenResult<LocalBytes> {
        Ok(self.database.local_bytes().await?)
    }

    /// The same read as a live query: it runs again after each write that
    /// changes what it reads, such as the cache's committed entries, a
    /// budget, a database file's size after a checkpoint, or an archive
    /// deleted. A read that only updates a cache entry's last-read time does
    /// not run the query again.
    pub fn subscribe_local_bytes(&self) -> LiveQuery<LocalBytes, CovenError> {
        self.database.subscribe_local_bytes()
    }
}

#[cfg(test)]
#[path = "handle_local_bytes_tests.rs"]
mod tests;
