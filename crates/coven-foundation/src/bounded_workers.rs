//! Owned synchronous workers with bounded, cancellation-aware FIFO admission.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

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
        let mut joins = Vec::with_capacity(states.len());
        for (index, mut state) in states.into_iter().enumerate() {
            let receiver = receiver.clone();
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
                    })?,
            );
        }
        Ok(Self {
            inner: Arc::new(Workers {
                sender: Some(sender),
                joins,
            }),
        })
    }

    /// Wait for bounded admission and completion. Cancelling before execution
    /// discards the closure; running work finishes, but its reply is discarded.
    /// A closure panic resumes on the caller and leaves the worker available.
    pub async fn call<F, R>(&self, operation: F) -> R
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
        if self
            .inner
            .sender
            .as_ref()
            .expect("live workers retain their sender")
            .send(job)
            .await
            .is_err()
        {
            panic!("worker pool stopped before admitting a call");
        }
        match result.await {
            Ok(Ok(value)) => value,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("worker pool dropped a call without responding"),
        }
    }
}

struct Workers<State> {
    sender: Option<mpsc::Sender<Job<State>>>,
    joins: Vec<std::thread::JoinHandle<()>>,
}

impl<State> Drop for Workers<State> {
    fn drop(&mut self) {
        // Closing admission lets workers drain cancelled jobs and drop their
        // retained state on their own thread. Never block an async executor.
        drop(self.sender.take());
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
