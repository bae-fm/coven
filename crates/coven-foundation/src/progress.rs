//! How far a long operation has come, one step at a time, so a host can draw
//! a bar for it instead of a bare spinner (api.md E1).

use crate::clock::ClockRef;
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;

/// How far an operation has come: the step it is on, and how many of that
/// step's units are done out of its total. Each step names its unit. Steps
/// arrive in the order the operation runs them, and within a step `done`
/// only grows and never passes `total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress<S> {
    /// The step the operation is on.
    pub step: S,
    /// The step's units done so far.
    pub done: u64,
    /// The step's units in all, known as the step begins.
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

impl<S: Copy + PartialEq> ProgressReport<S> {
    /// Report into `sender`, pacing reports by `clock`.
    pub fn new(sender: watch::Sender<Option<Progress<S>>>, clock: ClockRef) -> Self {
        let sent = clock.elapsed();
        Self {
            sender,
            clock,
            current: None,
            sent,
        }
    }

    /// Begin `step`, with `total` units to do; reported at once. A step
    /// begins once, after the steps before it.
    pub fn begin(&mut self, step: S, total: u64) {
        debug_assert!(self.current.is_none_or(|current| current.step != step));
        self.send(Progress {
            step,
            done: 0,
            total,
        });
    }

    /// `done` of the current step's units are done.
    pub fn done(&mut self, done: u64) {
        let Some(current) = self.current else {
            debug_assert!(false, "progress is reported within a step");
            return;
        };
        debug_assert!(current.done <= done && done <= current.total);
        let next = Progress { done, ..current };
        if done == current.total || self.clock.elapsed() >= self.sent + REPORT_INTERVAL {
            self.send(next);
        } else {
            self.current = Some(next);
        }
    }

    fn send(&mut self, progress: Progress<S>) {
        self.current = Some(progress);
        self.sent = self.clock.elapsed();
        // Nobody may be watching: the newest value is kept either way.
        self.sender.send_replace(Some(progress));
    }
}

/// Run `work` to its end, calling `report` on this task with the progress
/// it reports into `progress`. A host that redraws more slowly than `work`
/// reports sees the newest value, never an older one after it; a report
/// made just before `work` ends is still delivered, and none twice.
pub async fn forward<S: Copy + PartialEq, T>(
    mut progress: watch::Receiver<Option<Progress<S>>>,
    work: impl Future<Output = T>,
    report: impl Fn(Progress<S>),
) -> T {
    let mut reported = None;
    let mut deliver = |progress: &mut watch::Receiver<Option<Progress<S>>>| {
        let newest = *progress.borrow_and_update();
        if let Some(value) = newest.filter(|_| newest != reported) {
            reported = newest;
            report(value);
        }
    };
    let mut work = std::pin::pin!(work);
    loop {
        tokio::select! {
            biased;
            output = &mut work => {
                deliver(&mut progress);
                return output;
            }
            changed = progress.changed() => match changed {
                Ok(()) => deliver(&mut progress),
                // The reporter is gone, and what it reported last is delivered.
                Err(_closed) => {
                    let output = work.await;
                    deliver(&mut progress);
                    return output;
                }
            },
        }
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
