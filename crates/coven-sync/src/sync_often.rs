//! Apps asking for idle passes more often while a guard lives (E5).

use std::{collections::BTreeMap, time::Duration};
use tokio::sync::watch;

/// How long an idle loop waits between passes while no app asks for less.
pub(crate) const IDLE_INTERVAL: Duration = Duration::from_secs(30);

/// The intervals apps hold, each with how many guards hold it.
pub(crate) struct SyncOften(watch::Sender<BTreeMap<Duration, usize>>);

impl SyncOften {
    pub(crate) fn new() -> Self {
        Self(watch::channel(BTreeMap::new()).0)
    }
    /// Hold idle passes at most `interval` apart until the guard drops.
    pub(crate) fn hold(&self, interval: Duration) -> SyncOftenGuard {
        self.0.send_modify(|held| *held.entry(interval).or_default() += 1);
        SyncOftenGuard { held: self.0.clone(), interval }
    }
    /// The idle wait the loop uses now, and its changes.
    pub(crate) fn subscribe(&self) -> IdleInterval {
        IdleInterval(self.0.subscribe())
    }
}

/// The wait between idle passes: the shortest interval held, never longer
/// than 30 seconds.
pub(crate) struct IdleInterval(watch::Receiver<BTreeMap<Duration, usize>>);

impl IdleInterval {
    pub(crate) fn current(&self) -> Duration {
        self.0
            .borrow()
            .keys()
            .next()
            .map_or(IDLE_INTERVAL, |shortest| (*shortest).min(IDLE_INTERVAL))
    }
    /// Resolves when a guard is taken or dropped. Without the sync state no
    /// guard can be taken, and every guard keeps it, so the wait then never
    /// ends.
    pub(crate) async fn changed(&mut self) {
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// An app's request for idle passes at most its interval apart. The
/// request lasts until the guard drops; the shortest interval held wins.
#[derive(Debug)]
#[must_use = "dropping the guard ends the request at once"]
pub struct SyncOftenGuard {
    held: watch::Sender<BTreeMap<Duration, usize>>,
    interval: Duration,
}

impl Drop for SyncOftenGuard {
    fn drop(&mut self) {
        self.held.send_modify(|held| {
            let count = held
                .get_mut(&self.interval)
                .expect("a guard's interval stays held until the guard drops");
            *count -= 1;
            if *count == 0 {
                held.remove(&self.interval);
            }
        });
    }
}
