//! What a test reads of a file budget's use. A build without test support
//! keeps nothing.

#[cfg(any(test, feature = "test-utils"))]
use super::BudgetShared;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
struct Counts {
    leased: usize,
    most_leased: usize,
    holding: usize,
    most_holding: usize,
    waits: usize,
}

/// The use of one budget, counted when a test can read it. Each change
/// wakes the waits a test started on it.
pub(super) struct Tally {
    #[cfg(any(test, feature = "test-utils"))]
    counts: tokio::sync::watch::Sender<Counts>,
}

impl Tally {
    pub(super) fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test-utils"))]
            counts: tokio::sync::watch::channel(Counts::default()).0,
        }
    }

    /// A lease found the budget full and waited.
    pub(super) fn waited(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        self.counts.send_modify(|counts| counts.waits += 1);
    }

    /// A lease of `count` places was granted.
    pub(super) fn leased(&self, count: usize) {
        #[cfg(any(test, feature = "test-utils"))]
        self.counts.send_modify(|counts| {
            counts.leased += count;
            counts.most_leased = counts.most_leased.max(counts.leased);
        });
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = count;
    }

    /// A lease of `count` places went back to the budget.
    pub(super) fn returned(&self, count: usize) {
        #[cfg(any(test, feature = "test-utils"))]
        self.counts.send_modify(|counts| counts.leased -= count);
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = count;
    }

    /// A place was taken from a lease.
    pub(super) fn holding(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        self.counts.send_modify(|counts| {
            counts.holding += 1;
            counts.most_holding = counts.most_holding.max(counts.holding);
        });
    }

    /// A place went back to its lease.
    pub(super) fn released(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        self.counts.send_modify(|counts| counts.holding -= 1);
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
        self.shared.tally.counts.borrow().leased
    }
    /// The most places leased at once.
    pub fn most_leased(&self) -> usize {
        self.shared.tally.counts.borrow().most_leased
    }
    /// The places holding an open file or stream now: taken from a lease and
    /// not yet given back.
    pub fn holding(&self) -> usize {
        self.shared.tally.counts.borrow().holding
    }
    /// The most places holding an open file or stream at once.
    pub fn most_holding(&self) -> usize {
        self.shared.tally.counts.borrow().most_holding
    }
    /// Wait until leases have found the budget full and waited `count` more
    /// times than before this call.
    pub fn lease_waits_from_now(
        &self,
        count: usize,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut counts = self.shared.tally.counts.subscribe();
        let now = counts.borrow_and_update().waits;
        async move {
            crate::clock::within(counts.wait_for(|counts| counts.waits >= now + count))
                .await
                .expect("the lease wait")
                .expect("the budget is kept");
        }
    }
    /// Wait until the budget is empty: every lease has given its places
    /// back, so no place holds an open file or stream either. A budget empty
    /// at the call is empty at once.
    pub fn empty(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut counts = self.shared.tally.counts.subscribe();
        async move {
            crate::clock::within(counts.wait_for(|counts| counts.leased == 0))
                .await
                .expect("the budget empties")
                .expect("the budget is kept");
        }
    }
}
