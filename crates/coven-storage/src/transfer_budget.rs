//! The handle's transfer limits and shared leases for response bodies.

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

/// Limits shared by the open store's transfer consumers (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferLimits {
    /// Maximum file uploads running together.
    pub uploads: NonZeroUsize,
    /// Maximum concurrent downloads, including sequence and file reads.
    pub downloads: NonZeroUsize,
}
impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            uploads: NonZeroUsize::MIN,
            downloads: NonZeroUsize::MIN,
        }
    }
}

/// Compose once at the handle's root and pass to every connection and file
/// owner. Existing response bodies retain their leases when a limit changes.
pub struct TransferBudget {
    state: Mutex<State>,
    changed: Notify,
}
struct State {
    limits: TransferLimits,
    downloads: usize,
}
impl TransferBudget {
    /// An unused budget with the app's configured limits.
    pub fn new(limits: TransferLimits) -> Self {
        Self {
            state: Mutex::new(State {
                limits,
                downloads: 0,
            }),
            changed: Notify::new(),
        }
    }
    /// The current limits, also used by the next upload drain.
    pub fn limits(&self) -> TransferLimits {
        self.state.lock().expect("transfer budget poisoned").limits
    }
    /// Change admission for subsequent downloads without cancelling an active
    /// response. Reducing the limit waits for enough existing bodies to end.
    pub fn set_limits(&self, limits: TransferLimits) {
        self.state.lock().expect("transfer budget poisoned").limits = limits;
        self.changed.notify_waiters();
    }
    pub(crate) async fn download(self: &Arc<Self>) -> DownloadLease {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().expect("transfer budget poisoned");
                if state.downloads < state.limits.downloads.get() {
                    state.downloads += 1;
                    return DownloadLease(self.clone());
                }
            }
            changed.await;
        }
    }
}

pub(crate) struct DownloadLease(Arc<TransferBudget>);
impl Drop for DownloadLease {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("transfer budget poisoned")
            .downloads -= 1;
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "transfer_budget_tests.rs"]
mod tests;
