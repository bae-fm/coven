//! Deleting the installations resets kept, at the app's request (§19.3, E5).

use crate::*;

impl CovenHandle {
    /// Delete the installations `reset_device` kept in this store's directory,
    /// each moved aside when its reset swapped it out (§19.3). Nothing reads
    /// them once the reset's notice is in the database, and the store's own
    /// files stay. Each archive leaves the directory and its row in
    /// `coven_archive_bytes` in one step, then their files go. Returns once
    /// they are gone and the directory is synced. A failure midway returns
    /// its error and leaves the rest for the next call, which removes what
    /// remains; with none left it succeeds.
    pub async fn delete_discarded_devices(&self) -> CovenResult<()> {
        self.database.delete_discarded_devices().await.map_err(|error| match error {
            DbError::Disk(file) => CovenError::File(file),
            error => error.into(),
        })
    }
}

#[cfg(test)]
#[path = "handle_discarded_devices_tests.rs"]
mod tests;
