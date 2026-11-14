//! The app's budget of open files: places that every open of a user's file
//! takes, sized under the process's descriptor limit, so the app cannot run
//! out of descriptors through those opens.
//!
//! A piece of work waits on the budget in one place, `FileBudget::lease`,
//! and takes every place it will hold in that one wait. A lease hands its
//! places out to the work's own opens, which wait only for a place one of the
//! work's own files gives back. The budget's line is first come, first
//! served, so a waiting lease of several places is never passed by later
//! leases of one.

use std::{io, num::NonZeroUsize, path::Path, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[path = "file_budget_tally.rs"]
mod tally;
#[cfg(any(test, feature = "test-utils"))]
pub use tally::FileBudgetWatch;
use tally::Tally;

/// The descriptor limit a platform with no limit counts as: macOS's
/// `OPEN_MAX`.
const UNLIMITED_DESCRIPTORS: u64 = 10240;

/// The installation's places for open files, shared by every store its app
/// opens. Cloning shares them. The app makes one when it starts, and each
/// piece of work takes a lease of places from it (E1).
#[derive(Clone)]
pub struct FileBudget {
    shared: Arc<BudgetShared>,
}

struct BudgetShared {
    places: Arc<Semaphore>,
    size: NonZeroUsize,
    tally: Tally,
}

/// A lease asked for more places than the budget has, so no wait could
/// grant it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a lease of {requested} places asks for more than the budget's {size}")]
pub struct LeaseTooLarge {
    /// The places the lease asked for.
    pub requested: usize,
    /// The places the budget has.
    pub size: usize,
}

/// Places taken from a budget in one wait, for one piece of work. The work's
/// openers share it by cloning it. The places go back to the budget once the
/// lease, every place taken from it and every file opened in one are gone.
#[derive(Clone)]
pub struct FileLease {
    shared: Arc<LeaseShared>,
}

struct LeaseShared {
    places: Arc<Semaphore>,
    count: usize,
    budget: Arc<BudgetShared>,
    // Returns the places to the budget after the lease's own accounting.
    _held: OwnedSemaphorePermit,
}

/// One place of a lease, taken for one open: a file of a user, or a stream
/// whose bytes come from no such file. It goes back to its lease when it
/// drops.
pub struct LeasePlace {
    // Drops first, before the lease it returns to.
    _permit: OwnedSemaphorePermit,
    lease: Arc<LeaseShared>,
}

/// An open file holding its place. The file closes before the place goes
/// back.
pub struct LeasedFile<F> {
    // Declared first: fields drop in order, so the file closes before its
    // place goes back.
    file: F,
    _place: LeasePlace,
}

impl FileBudget {
    /// A budget of `size` places, at most `u32::MAX`. The app makes one
    /// with `under_descriptor_limit`; a test that needs a size makes it here.
    pub fn new(size: NonZeroUsize) -> Self {
        let size = size.min(NonZeroUsize::new(u32::MAX as usize).expect("u32::MAX is not zero"));
        Self {
            shared: Arc::new(BudgetShared {
                places: Arc::new(Semaphore::new(size.get())),
                size,
                tally: Tally::new(),
            }),
        }
    }

    /// A budget of an eighth of the process's descriptor limit, at least 1:
    /// 32 places under a macOS app's limit of 256, 128 under 1024. A
    /// process with no limit counts as 10240, macOS's `OPEN_MAX`, and so does
    /// Windows, where the handles `std::fs` and SQLite open have no limit.
    ///
    /// One place stands for at most 4 descriptors: the file it opens, or the
    /// cache files a stream's reads in flight open (at most 3), and one file
    /// the work writes beside it. The places therefore take at most half the
    /// limit, and the other half holds what the budget doesn't count: each open
    /// store's database files, locks, sockets and the platform's own
    /// descriptors. The limit is read once, here.
    pub fn under_descriptor_limit() -> Self {
        Self::new(size_for_limit(soft_descriptor_limit()))
    }

    /// How many places the budget has.
    pub fn size(&self) -> usize {
        self.shared.size.get()
    }

    /// Take `count` places in one wait, behind every lease that began
    /// waiting before. Call it only while holding no place of this budget:
    /// a holder that waits here can wait forever. A `count` above the
    /// budget's size fails at once.
    pub async fn lease(&self, count: usize) -> Result<FileLease, LeaseTooLarge> {
        let size = self.size();
        let wanted = u32::try_from(count)
            .ok()
            .filter(|_| count <= size)
            .ok_or(LeaseTooLarge { requested: count, size })?;
        // The first poll of the acquire puts this lease in the line, so the
        // wait is recorded only after it: a test that sees the wait knows
        // every lease it counts is already behind the ones before it.
        let mut acquire = Box::pin(self.shared.places.clone().acquire_many_owned(wanted));
        let held = match futures_lite::future::poll_once(&mut acquire).await {
            Some(held) => held,
            None => {
                self.shared.tally.waited();
                acquire.await
            }
        }
        .expect("the budget's semaphore is never closed");
        Ok(self.granted(count, held))
    }

    /// [`Self::lease`], blocking this thread in the same line as the async
    /// leases. Call it only off the async workers: on the blocking pool or a
    /// thread of the caller's own.
    pub fn lease_blocking(&self, count: usize) -> Result<FileLease, LeaseTooLarge> {
        futures_lite::future::block_on(self.lease(count))
    }

    fn granted(&self, count: usize, held: OwnedSemaphorePermit) -> FileLease {
        self.shared.tally.leased(count);
        FileLease {
            shared: Arc::new(LeaseShared {
                places: Arc::new(Semaphore::new(count)),
                count,
                budget: self.shared.clone(),
                _held: held,
            }),
        }
    }

    /// What a test reads of this budget's use.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn watch(&self) -> FileBudgetWatch {
        FileBudgetWatch::of(self.shared.clone())
    }
}

/// The places of a budget a test's installation has.
#[cfg(any(test, feature = "test-utils"))]
const TEST_PLACES: usize = 64;

#[cfg(any(test, feature = "test-utils"))]
impl FileBudget {
    /// A budget of 64 places, as an installation made for a test has, with
    /// no other budget's work competing for them.
    pub fn for_test() -> Self {
        Self::new(NonZeroUsize::new(TEST_PLACES).expect("64 is not zero"))
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl FileLease {
    /// All 64 places of a budget of its own, for a test that opens files
    /// without a budget to watch.
    pub async fn for_test() -> Self {
        FileBudget::for_test().lease(TEST_PLACES).await.expect("a test budget holds its own size")
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl LeasePlace {
    /// One place of a budget of its own, for a test that opens a file without
    /// a budget to watch.
    pub async fn for_test() -> Self {
        FileLease::for_test().await.place().await
    }
}

impl std::fmt::Debug for FileBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileBudget").field("size", &self.shared.size).finish_non_exhaustive()
    }
}

impl FileLease {
    /// One place of this lease, for one open. It waits only for a place the
    /// work's own files give back, never for the budget.
    pub async fn place(&self) -> LeasePlace {
        let permit = self
            .shared
            .places
            .clone()
            .acquire_owned()
            .await
            .expect("a lease's semaphore is never closed");
        LeasePlace::taken(permit, self.shared.clone())
    }

    /// [`Self::place`], blocking this thread. Call it only off the async
    /// workers: on the blocking pool or a thread of the caller's own.
    pub fn place_blocking(&self) -> LeasePlace {
        futures_lite::future::block_on(self.place())
    }

    /// How many places the lease took.
    pub fn count(&self) -> usize {
        self.shared.count
    }
}

impl std::fmt::Debug for FileLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLease").field("count", &self.shared.count).finish_non_exhaustive()
    }
}

impl Drop for LeaseShared {
    fn drop(&mut self) {
        self.budget.tally.returned(self.count);
    }
}

impl LeasePlace {
    fn taken(permit: OwnedSemaphorePermit, lease: Arc<LeaseShared>) -> Self {
        lease.budget.tally.holding();
        Self { _permit: permit, lease }
    }

    /// Open the file at `path` in this place. A failed open gives the place
    /// back.
    pub fn open(self, path: &Path) -> io::Result<LeasedFile<std::fs::File>> {
        Ok(LeasedFile { file: std::fs::File::open(path)?, _place: self })
    }

    /// Open the file at `path` in this place, as [`Self::open`].
    pub async fn open_async(self, path: &Path) -> io::Result<LeasedFile<tokio::fs::File>> {
        Ok(LeasedFile { file: tokio::fs::File::open(path).await?, _place: self })
    }
}

impl Drop for LeasePlace {
    fn drop(&mut self) {
        self.lease.budget.tally.released();
    }
}

impl<F> std::ops::Deref for LeasedFile<F> {
    type Target = F;
    fn deref(&self) -> &F {
        &self.file
    }
}

impl<F> std::ops::DerefMut for LeasedFile<F> {
    fn deref_mut(&mut self) -> &mut F {
        &mut self.file
    }
}

/// The soft limit on the process's open descriptors; `None` for no limit,
/// and on Windows, which has none for the handles coven opens.
fn soft_descriptor_limit() -> Option<u64> {
    #[cfg(unix)]
    {
        rustix::process::getrlimit(rustix::process::Resource::Nofile).current
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// The size of a budget under a descriptor limit: an eighth of it, at least
/// 1. No limit counts as 10240.
fn size_for_limit(limit: Option<u64>) -> NonZeroUsize {
    let places = limit.unwrap_or(UNLIMITED_DESCRIPTORS) / 8;
    let places = usize::try_from(places).unwrap_or(usize::MAX);
    NonZeroUsize::new(places).unwrap_or(NonZeroUsize::MIN)
}

#[cfg(test)]
#[path = "file_budget_tests.rs"]
mod tests;
