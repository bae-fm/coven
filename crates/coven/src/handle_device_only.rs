//! Making cloud files device-only on this device at the app's request
//! (§17.3, E8).

use crate::*;

impl CovenHandle {
    /// Downloads cloud files to the paths named under `folder`, then
    /// switches them all to device-only on this device in one write, each
    /// copy becoming the original this device records (§17.3). Each name is
    /// written as the one rule for every platform makes it. The member must
    /// be in each file's audience and may change every row referencing it. A
    /// path through a symbolic link below the folder, where something is
    /// already, or naming what another path names once written, case and
    /// Unicode form aside, is refused. Until the write commits, `cancel` stops the call;
    /// a failure or a cancel removes every copy and folder it made.
    pub async fn make_device_only(
        &self,
        folder: &std::path::Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), DeviceOnlyError> {
        self.files.make_device_only(folder, files, cancel).await
    }
}

#[cfg(test)]
#[path = "handle_device_only_tests.rs"]
mod tests;
