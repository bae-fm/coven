//! How far each file in flight has got, by file (E7, E8): progress this
//! process holds in memory, beside the views that say which files are in
//! flight.
use crate::*;

impl CovenHandle {
    /// How far each file this process is sending has got, by file: every
    /// one being sent first, then each change as its bytes go, until the
    /// store closes (E7). The files are the ones `coven_sending` lists.
    pub fn subscribe_upload_transfers(&self) -> TransferWatch {
        self.database.uploads_in_flight().subscribe()
    }
}

#[cfg(test)]
#[path = "handle_transfers_tests.rs"]
mod tests;
