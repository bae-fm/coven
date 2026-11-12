//! Deleting the installations resets kept, at the app's request (§19.3, E5).

use crate::*;

impl CovenHandle {
    /// Delete the installations `reset_device` kept in this store's directory,
    /// each moved aside when its reset swapped it out (§19.3). Nothing reads
    /// them once the reset's notice is in the database, and the store's own
    /// files stay. Returns once they are gone and the directory is synced. A
    /// failure midway returns its error and leaves the rest, part of one
    /// perhaps, for the next call, which removes what remains; with none left
    /// it succeeds.
    pub async fn delete_discarded_devices(&self) -> CovenResult<()> {
        let directory = self.directory.clone();
        let bugs = self.database.bugs().clone();
        let result =
            crate::task::blocking_completing("delete the discarded devices", bugs, move || {
                directory.delete_discarded_devices()
            })
            .await;
        // Whatever it removed before a failure changes what the store keeps.
        self.database.local_bytes_changed();
        result??;
        Ok(())
    }
}

#[cfg(test)]
#[path = "handle_discarded_devices_tests.rs"]
mod tests;
