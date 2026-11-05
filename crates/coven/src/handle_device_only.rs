//! Making cloud files device-only on this device at the app's request
//! (§17.3, E8): downloading them, then the write that switches them.

use crate::*;

impl CovenHandle {
    /// Downloads cloud files to the paths named under `folder`, for a write
    /// to switch them all to device-only on this device with
    /// `CovenWrite::make_device_only`, each copy becoming the original this
    /// device records (§17.3). Each name is written as the one rule for
    /// every platform makes it. The member must be in each file's audience
    /// and may change every row referencing it. A path through a symbolic
    /// link below the folder, where something is already, or naming what
    /// another path names once written, case and Unicode form aside, is
    /// refused. `on_progress` is called on this task with each file's
    /// download and how many of its bytes, and of all the call's, are down,
    /// at most a few times a second. Until it returns, `cancel` stops the
    /// call; a failure, a panic in a download among them, or a cancel removes
    /// every copy and folder it made.
    pub async fn prepare_device_only(
        &self,
        folder: &std::path::Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &tokio::sync::watch::Receiver<bool>,
        on_progress: impl Fn(Progress<DeviceOnlyStep>),
    ) -> Result<PreparedDeviceOnly, DeviceOnlyError> {
        let (progress, watched) = tokio::sync::watch::channel(None);
        let preparing = self.files.prepare_device_only(folder, files, cancel, progress);
        coven_foundation::progress::forward(watched, preparing, on_progress).await
    }

    /// `prepare_device_only`, then `CovenWrite::make_device_only` in a write
    /// of its own. A write that fails removes the copies.
    pub async fn make_device_only(
        &self,
        folder: &std::path::Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &tokio::sync::watch::Receiver<bool>,
        on_progress: impl Fn(Progress<DeviceOnlyStep>),
    ) -> Result<(), DeviceOnlyError> {
        let prepared = self.prepare_device_only(folder, files, cancel, on_progress).await?;
        self.write(move |write| Ok(write.make_device_only(prepared)?))
            .await
            .map_err(DeviceOnlyError::from)
    }
}

#[cfg(test)]
#[path = "handle_device_only_tests.rs"]
mod tests;
