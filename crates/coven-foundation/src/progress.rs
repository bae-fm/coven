//! How far a long operation has come, one step at a time, so a host can draw
//! a bar for it instead of a bare spinner (api.md E1).

use crate::clock::ClockRef;
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;

/// How far an operation has come: the step it is on, and how many of that
/// step's units are done out of its total. Each step names its unit. Steps
/// arrive in the order the operation runs them, and within a step `done`
/// only grows and never passes `total`, which never shrinks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress<S> {
    /// The step the operation is on.
    pub step: S,
    /// The step's units done so far.
    pub done: u64,
    /// The step's units in all, known as the step begins. It grows only
    /// when work the step had no way to count at its start turns up, such
    /// as slots that land while it reads them.
    pub total: u64,
}

/// The least time between two reports within one step: a host redraws a
/// few times a second, never once per row.
pub const REPORT_INTERVAL: Duration = Duration::from_millis(250);

/// Reports one operation's progress into a watch. A step's beginning and its
/// last unit are always reported; between them, a report waits until
/// [`REPORT_INTERVAL`] has passed since the one before. Reporting stores the
/// newest value and writes nothing to a database.
pub struct ProgressReport<S> {
    sender: watch::Sender<Option<Progress<S>>>,
    clock: ClockRef,
    current: Option<Progress<S>>,
    /// When the newest report was sent, by the clock's elapsed time.
    sent: Duration,
}

impl<S: Clone + PartialEq> ProgressReport<S> {
    /// Report into `sender`, pacing reports by `clock`.
    pub fn new(sender: watch::Sender<Option<Progress<S>>>, clock: ClockRef) -> Self {
        let sent = clock.elapsed();
        Self { sender, clock, current: None, sent }
    }

    /// Report one run of the operation `channel` carries, pacing reports
    /// by `clock`.
    pub fn of(channel: &ProgressChannel<S>, clock: ClockRef) -> Self {
        Self::new(channel.0.clone(), clock)
    }

    /// Report into a watch nobody holds, for work that runs the same steps
    /// where no host draws them.
    pub fn unwatched(clock: ClockRef) -> Self {
        Self::new(watch::channel(None).0, clock)
    }

    /// Begin `step`, with `total` units to do; reported at once. A step
    /// begins once, after the steps before it, unless the operation starts
    /// over.
    pub fn begin(&mut self, step: S, total: u64) {
        debug_assert!(self.current.as_ref().is_none_or(|current| current.step != step));
        self.send(Progress { step, done: 0, total });
    }

    /// The operation starts over from its first step, as when what it read
    /// went stale before it could use it. Its steps begin again.
    pub fn restart(&mut self) {
        self.current = None;
    }

    /// `done` of the current step's units are done.
    pub fn done(&mut self, done: u64) {
        let Some(current) = self.current.clone() else {
            debug_assert!(false, "progress is reported within a step");
            return;
        };
        debug_assert!(current.done <= done && done <= current.total);
        let finished = done == current.total;
        self.update(Progress { done, ..current }, finished);
    }

    /// One more of the current step's units is done.
    pub fn advance(&mut self) {
        let done = self.current.as_ref().map_or(0, |current| current.done + 1);
        self.done(done);
    }

    /// The current step has `total` units in all: more than it counted as
    /// it began, since work it had no way to count turned up.
    pub fn grow(&mut self, total: u64) {
        let Some(current) = self.current.clone() else {
            debug_assert!(false, "progress is reported within a step");
            return;
        };
        debug_assert!(current.total <= total);
        self.update(Progress { total, ..current }, false);
    }

    /// Report `progress`, which another operation's report gave and the
    /// caller put in this operation's steps, as the part of this operation
    /// that other one runs: a step it begins is reported at once, and within
    /// a step its units are paced as [`Self::done`]'s are.
    pub fn relay(&mut self, progress: Progress<S>) {
        match &self.current {
            Some(current) if current.step == progress.step => {
                let finished = progress.done == progress.total;
                self.update(progress, finished);
            }
            _ => self.send(progress),
        }
    }

    /// Report `next` if the step just finished or a report is due; else
    /// keep it for the next report.
    fn update(&mut self, next: Progress<S>, finished: bool) {
        if finished || self.clock.elapsed() >= self.sent + REPORT_INTERVAL {
            self.send(next);
        } else {
            self.current = Some(next);
        }
    }

    fn send(&mut self, progress: Progress<S>) {
        self.current = Some(progress.clone());
        self.sent = self.clock.elapsed();
        // Nobody may be watching: the newest value is kept either way.
        self.sender.send_replace(Some(progress));
    }
}

/// Where an owner reports each run of one kind of operation, such as a
/// store's open, for the host watching it. Each run reports into it afresh.
#[derive(Clone)]
pub struct ProgressChannel<S>(watch::Sender<Option<Progress<S>>>);

impl<S: Clone + PartialEq> ProgressChannel<S> {
    /// A channel no operation has reported into.
    pub fn new() -> Self {
        Self(watch::channel(None).0)
    }

    /// Hear the runs reported from now on.
    pub fn watch(&self) -> ProgressWatch<S> {
        ProgressWatch(self.0.subscribe())
    }
}

/// Hears the progress an owner's operation reports into its
/// [`ProgressChannel`], for a host that draws it.
pub struct ProgressWatch<S>(watch::Receiver<Option<Progress<S>>>);

impl<S: Clone + PartialEq> ProgressWatch<S> {
    /// Run `work` to its end, calling `report` on this task with each
    /// progress it reports, as [`forward`] does. The future is `forward`'s
    /// own, so an opening's future grows by nothing.
    pub fn forward<T>(
        self,
        work: impl Future<Output = T>,
        report: impl Fn(Progress<S>),
    ) -> impl Future<Output = T> {
        forward(self.0, work, report)
    }
}

/// Run `work` to its end, calling `report` on this task with the progress
/// it reports into `progress`. A host that redraws more slowly than `work`
/// reports sees the newest value, never an older one after it; a report
/// made just before `work` ends is still delivered, and none twice. What
/// the watch held before `progress` subscribed belongs to an earlier
/// operation and is never delivered.
pub async fn forward<S: Clone + PartialEq, T>(
    mut progress: watch::Receiver<Option<Progress<S>>>,
    work: impl Future<Output = T>,
    report: impl Fn(Progress<S>),
) -> T {
    let mut reported = None;
    let mut deliver = |newest: Option<Progress<S>>| {
        if newest != reported {
            if let Some(value) = newest.clone() {
                reported = newest;
                report(value);
            }
        }
    };
    let mut work = std::pin::pin!(work);
    loop {
        tokio::select! {
            biased;
            output = &mut work => {
                // Only a value sent since the last one seen is new.
                let newest = progress.borrow_and_update();
                let unseen = newest.has_changed().then(|| newest.clone());
                drop(newest);
                if let Some(newest) = unseen {
                    deliver(newest);
                }
                return output;
            }
            changed = progress.changed() => match changed {
                Ok(()) => {
                    let newest = progress.borrow_and_update().clone();
                    deliver(newest);
                }
                // The reporter is gone, and what it reported last was seen
                // and delivered before the watch said so.
                Err(_closed) => return work.await,
            },
        }
    }
}

/// What a hook on a [`RecordingClock`] is called with: each value the
/// reports send, once, in order.
#[cfg(feature = "test-utils")]
type OnRecord<S> = Box<dyn Fn(&Progress<S>) + Send + Sync>;

/// A clock that keeps every value reported into one watch, for a test that
/// checks steps a host reading the watch could miss. A report reads its
/// clock just before it sends, so what the watch holds at each reading,
/// with what it holds at the end, is every value sent, in order. Time and
/// waits are `clock`'s.
#[cfg(feature = "test-utils")]
pub struct RecordingClock<S> {
    clock: ClockRef,
    watch: std::sync::OnceLock<watch::Receiver<Option<Progress<S>>>>,
    seen: std::sync::Mutex<Vec<Progress<S>>>,
    on_record: std::sync::OnceLock<OnRecord<S>>,
}

#[cfg(feature = "test-utils")]
impl<S: Clone + PartialEq> RecordingClock<S> {
    /// Keep time by `clock`; nothing is recorded until [`Self::watch`].
    pub fn new(clock: ClockRef) -> Self {
        Self {
            clock,
            watch: std::sync::OnceLock::new(),
            seen: std::sync::Mutex::new(Vec::new()),
            on_record: std::sync::OnceLock::new(),
        }
    }

    /// Record the values reported into `watch`, from now on.
    pub fn watch(&self, watch: watch::Receiver<Option<Progress<S>>>) {
        assert!(self.watch.set(watch).is_ok(), "a recording clock records one watch");
    }

    /// Record the values an owner's operation reports into `progress`,
    /// from now on.
    pub fn watch_progress(&self, progress: ProgressWatch<S>) {
        self.watch(progress.0);
    }

    /// Call `hook` with each value as it is recorded, on the thread that
    /// read the clock: when that is the reporting work's, the work waits
    /// until `hook` returns.
    pub fn on_record(&self, hook: impl Fn(&Progress<S>) + Send + Sync + 'static) {
        assert!(self.on_record.set(Box::new(hook)).is_ok(), "a recording clock has one hook");
    }

    /// Every value reported so far, in order.
    pub fn seen(&self) -> Vec<Progress<S>> {
        self.record();
        self.seen.lock().unwrap().clone()
    }

    fn record(&self) {
        let Some(watch) = self.watch.get() else { return };
        let recorded = {
            // Held while the watch is read, so two readers record in the
            // order the values were sent.
            let mut seen = self.seen.lock().unwrap();
            let newest = watch.borrow().clone();
            match newest.filter(|newest| seen.last() != Some(newest)) {
                Some(newest) => {
                    seen.push(newest.clone());
                    newest
                }
                None => return,
            }
        };
        if let Some(hook) = self.on_record.get() {
            hook(&recorded);
        }
    }
}

#[cfg(feature = "test-utils")]
impl<S: Clone + PartialEq + Send + Sync> crate::clock::Clock for RecordingClock<S> {
    fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }
    fn elapsed(&self) -> Duration {
        self.record();
        self.clock.elapsed()
    }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        self.clock.sleep(duration)
    }
    fn sleep_until(
        &self,
        at: std::time::SystemTime,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), crate::clock::WallTimerError>> + Send>>
    {
        self.clock.sleep_until(at)
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
