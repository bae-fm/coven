//! What a test reads of a file budget's use. A build without test support
//! keeps nothing.

#[cfg(any(test, feature = "test-utils"))]
use super::BudgetShared;
#[cfg(any(test, feature = "test-utils"))]
use crate::locks::lock_unpoisoned;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::{Arc, Mutex};

#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
struct Counts {
    leased: usize,
    most_leased: usize,
    holding: usize,
    most_holding: usize,
}

/// The use of one budget, counted when a test can read it.
pub(super) struct Tally {
    #[cfg(any(test, feature = "test-utils"))]
    counts: Mutex<Counts>,
    #[cfg(any(test, feature = "test-utils"))]
    waits: tokio::sync::watch::Sender<usize>,
}

impl Tally {
    pub(super) fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test-utils"))]
            counts: Mutex::new(Counts::default()),
            #[cfg(any(test, feature = "test-utils"))]
            waits: tokio::sync::watch::channel(0).0,
        }
    }

    /// A lease found the budget full and waited.
    pub(super) fn waited(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        self.waits.send_modify(|waits| *waits += 1);
    }

    /// A lease of `count` places was granted.
    pub(super) fn leased(&self, count: usize) {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let mut counts = lock_unpoisoned(&self.counts);
            counts.leased += count;
            counts.most_leased = counts.most_leased.max(counts.leased);
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = count;
    }

    /// A lease of `count` places went back to the budget.
    pub(super) fn returned(&self, count: usize) {
        #[cfg(any(test, feature = "test-utils"))]
        {
            lock_unpoisoned(&self.counts).leased -= count;
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = count;
    }

    /// A place was taken from a lease.
    pub(super) fn holding(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let mut counts = lock_unpoisoned(&self.counts);
            counts.holding += 1;
            counts.most_holding = counts.most_holding.max(counts.holding);
        }
    }

    /// A place went back to its lease.
    pub(super) fn released(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        {
            lock_unpoisoned(&self.counts).holding -= 1;
        }
    }
}

/// What a test reads of a budget: the places its leases hold, the places
/// holding an open file or stream, and the waits for the budget. Cloning
/// shares the reading.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct FileBudgetWatch {
    shared: Arc<BudgetShared>,
}

#[cfg(any(test, feature = "test-utils"))]
impl FileBudgetWatch {
    pub(super) fn of(shared: Arc<BudgetShared>) -> Self {
        Self { shared }
    }
    /// The places leased now.
    pub fn leased(&self) -> usize {
        lock_unpoisoned(&self.shared.tally.counts).leased
    }
    /// The most places leased at once.
    pub fn most_leased(&self) -> usize {
        lock_unpoisoned(&self.shared.tally.counts).most_leased
    }
    /// The places holding an open file or stream now: taken from a lease and
    /// not yet given back.
    pub fn holding(&self) -> usize {
        lock_unpoisoned(&self.shared.tally.counts).holding
    }
    /// The most places holding an open file or stream at once.
    pub fn most_holding(&self) -> usize {
        lock_unpoisoned(&self.shared.tally.counts).most_holding
    }
    /// Wait until leases have found the budget full and waited `count` more
    /// times than before this call.
    pub fn lease_waits_from_now(
        &self,
        count: usize,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut waits = self.shared.tally.waits.subscribe();
        let now = *waits.borrow_and_update();
        async move {
            crate::clock::within(waits.wait_for(|waits| *waits >= now + count))
                .await
                .expect("the lease wait")
                .expect("the budget is kept");
        }
    }
}
