//! Work measured within one test's asynchronous and blocking calls.

use std::{
    cell::RefCell,
    future::Future,
    sync::{Arc, Mutex},
};

/// Measured work; unrelated tests never contribute to these counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Binary decoders entered.
    pub decodes: usize,
    /// Passphrase key derivations entered.
    pub custody_derivations: usize,
    /// Snapshot records and their evidence leaves decoded, before or during installation.
    pub snapshot_decodes: usize,
    /// Snapshot records and evidence leaves decoded while the application writer was held.
    pub snapshot_decodes_with_writer: usize,
}

/// The measurements shared by a task and its explicitly bound blocking calls.
#[derive(Clone, Default)]
pub struct Counter(Arc<Mutex<Counts>>);

#[derive(Clone)]
struct Active {
    counter: Counter,
    writing: bool,
}

thread_local! {
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
}

fn enter<R>(active: Option<Active>, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<Active>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE.with(|active| *active.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(ACTIVE.with(|current| current.replace(active)));
    run()
}

impl Counter {
    /// Take the counts after all measured calls have finished.
    pub fn take(&self) -> Counts {
        std::mem::take(&mut *self.0.lock().expect("work counter poisoned"))
    }

    /// Measure a synchronous call, including calls it binds to another thread.
    pub fn scope<R>(&self, run: impl FnOnce() -> R) -> R {
        enter(
            Some(Active {
                counter: self.clone(),
                writing: false,
            }),
            run,
        )
    }

    /// Measure each poll without leaving a thread-local scope across an await.
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|context| self.scope(|| future.as_mut().poll(context))).await
    }
}

/// Preserve the initiating measurement when dispatching a blocking call.
pub fn bind<R>(run: impl FnOnce() -> R + Send + 'static) -> impl FnOnce() -> R + Send + 'static {
    let active = ACTIVE.with(|active| active.borrow().clone());
    move || enter(active, run)
}

/// Mark the actual application-writer boundary for the current measurement.
pub fn writing<R>(run: impl FnOnce() -> R) -> R {
    let active = ACTIVE
        .with(|active| active.borrow().clone())
        .map(|mut active| {
            active.writing = true;
            active
        });
    enter(active, run)
}

fn record(update: impl FnOnce(&mut Counts, bool)) {
    ACTIVE.with(|active| {
        if let Some(active) = &*active.borrow() {
            update(
                &mut active.counter.0.lock().expect("work counter poisoned"),
                active.writing,
            );
        }
    });
}

/// Record entry to a binary decoder.
pub fn decode() {
    record(|counts, _| counts.decodes += 1);
}

/// Record entry to an actual passphrase derivation.
pub fn custody_derivation() {
    record(|counts, _| counts.custody_derivations += 1);
}

/// Record decoding of snapshot records or their authenticated evidence leaves.
pub fn snapshot_record_decode() {
    record(|counts, writing| {
        counts.snapshot_decodes += 1;
        counts.snapshot_decodes_with_writer += usize::from(writing);
    });
}

#[cfg(test)]
#[path = "test_work_tests.rs"]
mod tests;
