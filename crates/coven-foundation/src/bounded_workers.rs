//! Owned synchronous workers with bounded, cancellation-aware FIFO admission.

use crate::store_dir::{HeldStoreLock, StoreOpenGuard};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};

type Job<State> = Box<dyn FnOnce(&mut State) + Send>;

/// Each worker retains one state. All workers dequeue from the same bounded
/// channel, so work is never stranded behind a busy worker's private queue.
pub struct BoundedWorkers<State> {
    inner: Arc<Workers<State>>,
}

impl<State> Clone for BoundedWorkers<State> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<State: Send + 'static> BoundedWorkers<State> {
    /// Start all workers before returning a capability that can admit work.
    pub fn start(states: Vec<State>, capacity: NonZeroUsize, name: &str) -> std::io::Result<Self> {
        assert!(
            !states.is_empty(),
            "a worker pool needs at least one worker"
        );
        let (sender, receiver) = mpsc::channel::<Job<State>>(capacity.get());
        let receiver = Arc::new(Mutex::new(receiver));
        // Each worker holds this until it has dropped its state, so the last
        // one to go closes the exit channel and releases the store lock after
        // every worker's state is gone.
        let (exit, exited) = watch::channel(());
        let store_lock = HeldStoreLock::default();
        let exit = Arc::new(WorkerExit {
            _exit: exit,
            store_lock: store_lock.clone(),
        });
        let mut joins = Vec::with_capacity(states.len());
        for (index, mut state) in states.into_iter().enumerate() {
            let receiver = receiver.clone();
            let exit = exit.clone();
            joins.push(
                std::thread::Builder::new()
                    .name(format!("{name}-{index}"))
                    .spawn(move || {
                        loop {
                            // Release this lock before running the job. It protects only
                            // dequeue; every worker executes against its own state.
                            let job = receiver
                                .lock()
                                .expect("worker queue mutex poisoned")
                                .blocking_recv();
                            match job {
                                Some(job) => job(&mut state),
                                None => break,
                            }
                        }
                        drop(state);
                        drop(exit);
                    })?,
            );
        }
        Ok(Self {
            inner: Arc::new(Workers {
                sender: Mutex::new(Some(sender)),
                exited,
                store_lock,
                joins,
            }),
        })
    }

    /// Wait for bounded admission and completion. Cancelling before execution
    /// discards the closure; running work finishes, but its reply is discarded.
    /// A closure panic resumes on the caller and leaves the worker available.
    /// Once the pool is [closed](Self::close), nothing more is admitted.
    pub async fn call<F, R>(&self, operation: F) -> Result<R, WorkersClosed>
    where
        F: FnOnce(&mut State) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        let job: Job<State> = Box::new(move |state| {
            if reply.is_closed() {
                tracing::debug!("discarding cancelled queued work");
                return;
            }
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(state)));
            // A cancelled caller deliberately abandons its reply.
            if reply.send(outcome).is_err() {
                tracing::debug!("discarding completed work for a cancelled caller");
            }
        });
        let sender = self
            .inner
            .sender
            .lock()
            .expect("worker admission mutex poisoned")
            .clone()
            .ok_or(WorkersClosed)?;
        // Workers stop receiving only once every sender is gone, and this call
        // holds one, so admission fails only if every worker has died.
        if sender.send(job).await.is_err() {
            panic!("worker pool stopped before admitting a call");
        }
        drop(sender);
        match result.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("worker pool dropped a call without responding"),
        }
    }

    /// Keep a share of the store lock until every worker has dropped its
    /// state, so the lock outlives whatever store files the states hold.
    pub fn hold_store_lock(&self, lock: Arc<StoreOpenGuard>) {
        self.inner.store_lock.hold(lock);
    }

    /// Stop admitting work, let the workers finish what was already admitted,
    /// and wait until every worker has dropped its state. Later calls on any
    /// clone fail with [`WorkersClosed`].
    pub async fn close(&self) {
        drop(
            self.inner
                .sender
                .lock()
                .expect("worker admission mutex poisoned")
                .take(),
        );
        let mut exited = self.inner.exited.clone();
        // The value never changes, so this returns only when the last worker
        // drops the sender.
        let _ = exited.changed().await;
    }
}

/// The pool was [closed](BoundedWorkers::close) before this call was admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the worker pool is closed")]
pub struct WorkersClosed;

struct Workers<State> {
    sender: Mutex<Option<mpsc::Sender<Job<State>>>>,
    exited: watch::Receiver<()>,
    store_lock: HeldStoreLock,
    joins: Vec<std::thread::JoinHandle<()>>,
}

/// Shared by the workers and dropped by the last of them to exit, after its
/// state: closing the exit channel and releasing the store lock share.
struct WorkerExit {
    _exit: watch::Sender<()>,
    store_lock: HeldStoreLock,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.store_lock.release();
    }
}

impl<State> Drop for Workers<State> {
    fn drop(&mut self) {
        // Closing admission lets workers drain cancelled jobs and drop their
        // retained state on their own thread. Never block an async executor.
        drop(
            self.sender
                .get_mut()
                .expect("worker admission mutex poisoned")
                .take(),
        );
        let current_thread = std::thread::current().id();
        let on_owned_worker = self
            .joins
            .iter()
            .any(|join| join.thread().id() == current_thread);
        if !on_owned_worker && tokio::runtime::Handle::try_current().is_err() {
            for join in self.joins.drain(..) {
                if join.join().is_err() {
                    tracing::error!("bounded worker panicked");
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "bounded_workers_tests.rs"]
mod tests;
