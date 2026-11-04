//! The upload queue as the app controls it (§17.9, E7): what it holds, an
//! early retry, and the pause, which holds trailing uploads only (§17.4).
use crate::*;

impl CovenHandle {
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected, with the
    /// files this device is sending now (E7).
    pub async fn uploads(&self) -> CovenResult<UploadQueue> {
        Ok(self.database.upload_queue().await?.into())
    }
    /// Optionally retry recoverable uploads early; provider cooldowns still
    /// hold them and individual failures stay in pending() (§17.9). While
    /// uploads are paused, or the network is metered and the app moves files
    /// only on unmetered networks, it tries only bound ones, and while sync
    /// is stopped none, saying so (§17.4, §17.9).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.files.retry_uploads_now().await
    }
    /// Pause trailing uploads, or resume them, in a write of its own
    /// (`CovenWrite::set_uploads_paused`).
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()> {
        self.write(move |write| Ok(write.set_uploads_paused(paused)?)).await
    }
    /// Move files only on unmetered networks, or on any, in a write of its
    /// own (`CovenWrite::set_unmetered_only`).
    pub async fn set_unmetered_only(&self, unmetered_only: bool) -> CovenResult<()> {
        self.write(move |write| Ok(write.set_unmetered_only(unmetered_only)?)).await
    }
}

#[cfg(test)]
#[path = "handle_uploads_tests.rs"]
mod tests;
