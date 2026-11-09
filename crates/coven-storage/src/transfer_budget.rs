//! The handle's transfer limits and shared leases for response bodies.

use coven_foundation::locks::lock_unpoisoned;
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
    /// Maximum requests for file bytes in flight together, each holding a
    /// lease while its body is read; one file's pin takes as many as it can.
    /// The sequence's own reads don't count against it.
    pub downloads: NonZeroUsize,
}
impl Default for TransferLimits {
    fn default() -> Self {
        Self { uploads: NonZeroUsize::MIN, downloads: NonZeroUsize::MIN }
    }
}

/// Compose once at the handle's root and pass to every connection and file
/// owner. Existing response bodies retain their leases when a limit changes.
pub struct TransferBudget {
    state: Mutex<BudgetState>,
    changed: Notify,
    /// How many times a download has found the limit full and waited, for
    /// a test.
    #[cfg(any(test, feature = "test-utils"))]
    waited: tokio::sync::watch::Sender<usize>,
}
struct BudgetState {
    limits: TransferLimits,
    downloads: usize,
}
impl TransferBudget {
    /// An unused budget with the app's configured limits.
    pub fn new(limits: TransferLimits) -> Self {
        Self {
            state: Mutex::new(BudgetState { limits, downloads: 0 }),
            changed: Notify::new(),
            #[cfg(any(test, feature = "test-utils"))]
            waited: tokio::sync::watch::channel(0).0,
        }
    }
    /// The current limits; the next upload to start checks the upload limit.
    pub fn limits(&self) -> TransferLimits {
        lock_unpoisoned(&self.state).limits
    }
    /// Change admission for subsequent downloads without cancelling an active
    /// response. Reducing the limit waits for enough existing bodies to end.
    pub fn set_limits(&self, limits: TransferLimits) {
        lock_unpoisoned(&self.state).limits = limits;
        self.changed.notify_waiters();
    }
    pub(crate) async fn download(self: &Arc<Self>) -> DownloadLease {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = lock_unpoisoned(&self.state);
                if state.downloads < state.limits.downloads.get() {
                    state.downloads += 1;
                    return DownloadLease(self.clone());
                }
            }
            #[cfg(any(test, feature = "test-utils"))]
            self.waited.send_modify(|waited| *waited += 1);
            changed.await;
        }
    }

    /// Wait until downloads have found the limit full and waited for a body
    /// to end `count` more times than before this call.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn downloads_waiting_from_now(
        &self,
        count: usize,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut waited = self.waited.subscribe();
        let now = *waited.borrow_and_update();
        async move {
            coven_foundation::clock::within(waited.wait_for(|waited| *waited >= now + count))
                .await
                .expect("the downloads wait")
                .expect("the budget is kept");
        }
    }
}

pub(crate) struct DownloadLease(Arc<TransferBudget>);
impl Drop for DownloadLease {
    fn drop(&mut self) {
        lock_unpoisoned(&self.0.state).downloads -= 1;
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "transfer_budget_tests.rs"]
mod tests;
