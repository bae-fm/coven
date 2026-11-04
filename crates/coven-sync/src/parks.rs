//! Where a loop parks: each poll that finds nothing the loop waits on
//! ready, told to a test.
//!
//! A loop's `select!` takes `polling` as its first branch and `parked` as
//! its last, biased: `parked` is polled only after every branch before it
//! was polled in the same poll and none was ready. Without test-utils both
//! are never ready and count nothing. The loop holds `running` while it
//! runs, so a test waiting for a park ends when the loop does.

/// The parks of one loop.
#[derive(Clone)]
pub struct Parks {
    #[cfg(any(test, feature = "test-utils"))]
    inner: std::sync::Arc<ParkCounts>,
}

#[cfg(any(test, feature = "test-utils"))]
struct ParkCounts {
    /// Polls of the loop's `select!` begun so far.
    polls: std::sync::atomic::AtomicU64,
    /// The latest poll that parked, or `u64::MAX` once the loop has ended.
    parked: tokio::sync::watch::Sender<u64>,
    /// What polls the loop again.
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

/// Held by the loop while it runs: dropped, it ends every wait for a park.
pub struct LoopRunning<'a> {
    #[cfg(any(test, feature = "test-utils"))]
    parks: &'a Parks,
    #[cfg(not(any(test, feature = "test-utils")))]
    parks: std::marker::PhantomData<&'a Parks>,
}

impl Drop for LoopRunning<'_> {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-utils"))]
        self.parks.inner.parked.send_modify(|parked| *parked = u64::MAX);
    }
}

impl Parks {
    /// One loop's parks, none counted yet.
    pub fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test-utils"))]
            inner: std::sync::Arc::new(ParkCounts {
                polls: std::sync::atomic::AtomicU64::new(0),
                parked: tokio::sync::watch::channel(0).0,
                waker: std::sync::Mutex::new(None),
            }),
        }
    }

    /// What the loop holds while it runs.
    pub fn running(&self) -> LoopRunning<'_> {
        #[cfg(any(test, feature = "test-utils"))]
        return LoopRunning { parks: self };
        #[cfg(not(any(test, feature = "test-utils")))]
        LoopRunning { parks: std::marker::PhantomData }
    }

    /// The first branch of the loop's `select!`: a poll begins. Never ready.
    pub fn polling(&self) -> impl std::future::Future<Output = std::convert::Infallible> + '_ {
        #[cfg(any(test, feature = "test-utils"))]
        return std::future::poll_fn(|context| {
            self.inner.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut waker = coven_foundation::locks::lock_unpoisoned(&self.inner.waker);
            match &mut *waker {
                Some(waker) => waker.clone_from(context.waker()),
                None => *waker = Some(context.waker().clone()),
            }
            std::task::Poll::Pending
        });
        #[cfg(not(any(test, feature = "test-utils")))]
        std::future::pending()
    }

    /// The last branch of the loop's `select!`: no branch before it was
    /// ready in this poll. Never ready.
    pub fn parked(&self) -> impl std::future::Future<Output = std::convert::Infallible> + '_ {
        #[cfg(any(test, feature = "test-utils"))]
        return std::future::poll_fn(|_| {
            let poll = self.inner.polls.load(std::sync::atomic::Ordering::SeqCst);
            self.inner.parked.send_modify(|parked| *parked = (*parked).max(poll));
            std::task::Poll::Pending
        });
        #[cfg(not(any(test, feature = "test-utils")))]
        std::future::pending()
    }

    /// Wait until the loop parks in a poll begun after this call, every
    /// branch it waits on polled after whatever the caller did before and
    /// none ready, or until it ends. The loop is polled once more for it,
    /// as a spurious wake would, so a loop nothing has woken parks again.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn parked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut parked = self.inner.parked.subscribe();
        let now = self.inner.polls.load(std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = &*coven_foundation::locks::lock_unpoisoned(&self.inner.waker) {
            waker.wake_by_ref();
        }
        async move {
            coven_foundation::clock::within(parked.wait_for(|parked| *parked > now))
                .await
                .expect("the loop parks")
                .expect("the loop's parks are kept");
        }
    }
}
