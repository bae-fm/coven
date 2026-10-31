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
    /// uploads are paused it tries only bound ones (§17.4).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.files.retry_uploads_now().await
    }
    /// Pause trailing uploads, or resume them. A paused upload keeps its
    /// place, including a provider upload session in progress; the setting
    /// persists. Bound uploads run on, since writes wait for them (§17.4).
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()> {
        Ok(self.files.set_uploads_paused(paused).await?)
    }
}

#[cfg(test)]
#[path = "handle_uploads_tests.rs"]
mod tests;
