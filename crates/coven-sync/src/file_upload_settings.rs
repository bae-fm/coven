//! The writer's copies of the upload settings, which only its upload runs and
//! the app's changes of a setting use (§12.3, §17.4, §17.9).

use super::{FilesInner, TrailingHold};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The writer's copies of the upload settings.
pub struct UploadSettings {
    /// The persisted pause setting as of the last upload run or app change;
    /// parts check it between sends.
    paused: AtomicBool,
    /// Whether a self-replacement pauses this device's writes, as of the last
    /// read of the upload queue or a completion it refused (§12.3).
    writes_paused: AtomicBool,
    /// How many app retries had been asked when the write pause was last
    /// read: only those can be answered that it holds writes.
    writes_paused_read_after: AtomicU64,
    /// How many app retries have been asked.
    retries_asked: AtomicU64,
    /// Held by an app's change of either setting, and by an upload run from
    /// its read of the persisted settings until it has acted on them, so
    /// neither the copies here nor a retry's answer comes from a setting
    /// already replaced.
    hold_change: tokio::sync::Mutex<()>,
    /// Holds the next upload run once it has read the persisted settings,
    /// telling the test, until the test resumes it.
    #[cfg(test)]
    settings_read_gate: std::sync::Mutex<
        Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>,
    >,
    /// How many times an app's change of a setting, or a run, has found
    /// `hold_change` held and waited.
    #[cfg(test)]
    change_waits: tokio::sync::watch::Sender<usize>,
}

impl UploadSettings {
    pub(super) fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            writes_paused: AtomicBool::new(false),
            writes_paused_read_after: AtomicU64::new(0),
            retries_asked: AtomicU64::new(0),
            hold_change: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            settings_read_gate: std::sync::Mutex::new(None),
            #[cfg(test)]
            change_waits: tokio::sync::watch::channel(0).0,
        }
    }

    /// The app's pause as of now.
    pub(super) fn paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }
    pub(super) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }
    pub(super) fn writes_paused(&self) -> bool {
        self.writes_paused.load(Ordering::Acquire)
    }
    pub(super) fn set_writes_paused(&self, paused: bool) {
        self.writes_paused.store(paused, Ordering::Release);
    }
    pub(super) fn writes_paused_read_after(&self) -> u64 {
        self.writes_paused_read_after.load(Ordering::Acquire)
    }
    pub(super) fn retries_asked(&self) -> u64 {
        self.retries_asked.load(Ordering::Acquire)
    }
    /// The write pause as read, after `asked` app retries.
    pub(super) fn read_writes_paused(&self, paused: bool, asked: u64) {
        self.writes_paused.store(paused, Ordering::Release);
        self.writes_paused_read_after.store(asked, Ordering::Release);
    }
    #[cfg(test)]
    pub(super) fn hold_after_settings_read(
        &self,
        gate: (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>),
    ) {
        *self.settings_read_gate.lock().expect("the gate is not poisoned") = Some(gate);
    }
    #[cfg(test)]
    pub(super) fn take_settings_read_gate(
        &self,
    ) -> Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)> {
        self.settings_read_gate.lock().expect("the gate is not poisoned").take()
    }
    /// Count an app retry asked, returning how many were asked before it.
    pub(super) fn retry_asked(&self) -> u64 {
        self.retries_asked.fetch_add(1, Ordering::AcqRel)
    }

    /// `hold_change`, for a run; a test hears each wait for it.
    pub(super) async fn lock_hold_change(&self) -> tokio::sync::MutexGuard<'_, ()> {
        #[cfg(test)]
        match self.hold_change.try_lock() {
            Ok(change) => return change,
            Err(_) => self.change_waits.send_modify(|waits| *waits += 1),
        }
        self.hold_change.lock().await
    }

    /// `hold_change`, for a setting's change on its write's blocking
    /// thread; a test hears each wait for it.
    pub(super) fn hold_change_blocking(&self) -> tokio::sync::MutexGuard<'_, ()> {
        #[cfg(test)]
        match self.hold_change.try_lock() {
            Ok(change) => return change,
            Err(_) => self.change_waits.send_modify(|waits| *waits += 1),
        }
        self.hold_change.blocking_lock()
    }

    /// Wait until a setting's change or a run finds `hold_change` held and
    /// waits, after this call.
    #[cfg(test)]
    pub(super) fn change_waits_from_now(&self) -> impl std::future::Future<Output = ()> {
        let mut waits = self.change_waits.subscribe();
        let now = *waits.borrow_and_update();
        async move {
            coven_foundation::clock::within(waits.wait_for(|waits| *waits > now))
                .await
                .expect("a change waits for the run")
                .expect("the waits are kept");
        }
    }
}

impl FilesInner {
    /// What holds trailing uploads now (§12.3, §17.4, §17.9): the app's
    /// pause, a self-replacement's pause of this device's writes, or a
    /// metered network while the app moves files only on unmetered ones.
    pub(crate) fn trailing_hold(&self) -> Option<TrailingHold> {
        let uploads = &self.uploads;
        if uploads.paused() {
            Some(TrailingHold::Paused)
        } else if uploads.writes_paused() {
            Some(TrailingHold::MemberKeysChanging)
        } else if self.metered_holds(false) {
            Some(TrailingHold::Metered)
        } else {
            None
        }
    }
    /// How many app retries had been asked when the write pause was last read.
    pub(crate) fn writes_paused_read_after(&self) -> u64 {
        self.uploads.writes_paused_read_after()
    }
    /// A trailing upload's completion was refused: the pause holds the lane
    /// until a read of the queue finds it ended.
    pub(crate) fn writes_were_paused(&self) {
        self.uploads.set_writes_paused(true);
    }
    /// Whether anything holds trailing uploads now.
    pub(crate) fn trailing_held(&self) -> bool {
        self.trailing_hold().is_some()
    }
    /// Take the app's pause as the owner's copy.
    pub(crate) fn set_paused(&self, paused: bool) {
        self.uploads.set_paused(paused);
    }
    /// Count an app retry asked, returning how many were asked before it.
    pub(crate) fn retry_asked(&self) -> u64 {
        self.uploads.retry_asked()
    }
    /// `hold_change`, for a run; a test hears each wait for it.
    pub(crate) async fn lock_hold_change(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.uploads.lock_hold_change().await
    }
    /// `hold_change`, for a setting's change on its write's blocking thread.
    pub(super) fn hold_change_blocking(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.uploads.hold_change_blocking()
    }
    /// Wait until a setting's change or a run finds `hold_change` held and
    /// waits, after this call.
    #[cfg(test)]
    pub(crate) fn change_waits_from_now(&self) -> impl std::future::Future<Output = ()> {
        self.uploads.change_waits_from_now()
    }
    /// Hold the next upload run once it has read the persisted settings,
    /// until the test resumes it.
    #[cfg(test)]
    pub(crate) fn hold_after_settings_read(
        &self,
        gate: (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>),
    ) {
        self.uploads.hold_after_settings_read(gate);
    }
    /// The gate a test set, if any, for the run that has read the settings.
    #[cfg(test)]
    pub(crate) fn take_settings_read_gate(
        &self,
    ) -> Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)> {
        self.uploads.take_settings_read_gate()
    }
    /// Read whether a self-replacement pauses this device's writes.
    pub(crate) async fn read_writes_paused(&self) -> Result<(), coven_database::DbError> {
        let uploads = &self.uploads;
        let asked = uploads.retries_asked();
        let paused = self.database.writes_paused().await?;
        uploads.read_writes_paused(paused, asked);
        Ok(())
    }
}
