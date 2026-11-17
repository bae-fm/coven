//! Deleting the installations resets kept, at the app's request (§19.3, E5).

use crate::*;

impl CovenHandle {
    /// Delete the installations `reset_device` kept in this store's directory,
    /// each moved aside when its reset swapped it out (§19.3). Nothing reads
    /// them once the reset's notice is in the database, and the store's own
    /// files stay. In one step, coven moves each archive out of the
    /// directory into a removal folder and deletes its row in
    /// `coven_archive_bytes`, then it deletes that folder. Returns once it
    /// has deleted them and synced the directory. A failure midway returns
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
