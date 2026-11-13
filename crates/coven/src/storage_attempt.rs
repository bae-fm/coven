//! A connection attempt's task and its mark on the file layer: reads that
//! need storage wait while the attempt lives (§17.7).

use super::*;
use coven_foundation::locks::lock_unpoisoned;

impl StorageConnections {
    /// Start and record the connection attempt for `epoch` as its own task. Reads that
    /// need storage wait while it lives, and the file layer reads as
    /// released once it ends without a connection.
    pub(super) fn start_attempt(self: &Arc<Self>, epoch: u64) {
        let owner = self.clone();
        let ending = self.clone();
        let parks = coven_sync::Parks::new();
        #[cfg(test)]
        let attempt_parks = parks.clone();
        let coming = self.files.expect_connection();
        let task = crate::task::spawn_owned(
            "connect to storage",
            self.database.bugs().clone(),
            async move {
                let _coming = coming;
                owner.connect_started(epoch, parks).await
            },
            |bug| async move { ending.attempt_panicked(bug).await },
        );
        *lock_unpoisoned(&self.attempts) = Some(ConnectionAttempt {
            epoch,
            task,
            #[cfg(test)]
            parks: attempt_parks,
        });
    }
}
