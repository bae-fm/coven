use futures_util::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tracing::{debug, error, info};

use super::{SyncCommand, SyncLoopFailure, SyncLoopHandleInner, SyncLoopStatus};
use crate::sync::loop_policy::{self, LoopWait, SyncLoopReport, SyncLoopSuccess};

struct RuntimeSlotState {
    loop_thread: Option<SyncLoopThread>,
    cancelled: bool,
}

struct RuntimeSlot {
    state: Mutex<RuntimeSlotState>,
    changed: Condvar,
}

impl RuntimeSlot {
    fn new() -> Self {
        Self {
            state: Mutex::new(RuntimeSlotState {
                loop_thread: None,
                cancelled: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn install(&self, loop_thread: SyncLoopThread) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.loop_thread = Some(loop_thread);
        self.changed.notify_one();
    }

    fn cancel(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.cancelled = true;
        self.changed.notify_one();
    }

    fn run(&self, runtime: tokio::runtime::Runtime) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.loop_thread.is_none() && !state.cancelled {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        let loop_thread = state.loop_thread.take();
        drop(state);
        if let Some(loop_thread) = loop_thread {
            loop_thread.run(runtime);
        }
    }
}

/// A sync-loop OS thread whose Tokio runtime is ready but has not received a
/// Store session yet.
///
/// Setup creates this before committing credentials or initializing the Store.
/// Attaching the initialized session is then an in-memory handoff with no
/// remaining thread or runtime construction that can fail.
pub struct PreparedSyncLoopRuntime {
    slot: Arc<RuntimeSlot>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl PreparedSyncLoopRuntime {
    pub(super) fn prepare() -> Result<Self, super::SyncLoopError> {
        let slot = Arc::new(RuntimeSlot::new());
        let thread_slot = Arc::clone(&slot);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread_handle = std::thread::Builder::new()
            .name("coven-sync-loop".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(Arc::new(error)));
                        return;
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                thread_slot.run(runtime);
            })
            .map_err(super::SyncLoopError::ThreadSpawn)?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                slot,
                thread_handle: Some(thread_handle),
            }),
            Ok(Err(error)) => {
                let _ = thread_handle.join();
                Err(super::SyncLoopError::Runtime(error))
            }
            Err(_) => {
                let _ = thread_handle.join();
                Err(super::SyncLoopError::ThreadPanicked)
            }
        }
    }

    pub(super) fn install(mut self, loop_thread: SyncLoopThread) -> std::thread::JoinHandle<()> {
        self.slot.install(loop_thread);
        self.thread_handle
            .take()
            .expect("prepared sync runtime owns its thread until installation")
    }
}

impl Drop for PreparedSyncLoopRuntime {
    fn drop(&mut self) {
        let Some(thread_handle) = self.thread_handle.take() else {
            return;
        };
        self.slot.cancel();
        let _ = thread_handle.join();
    }
}

pub(super) struct SyncLoopThread {
    inner: Arc<SyncLoopHandleInner>,
    trigger_rx: tokio::sync::mpsc::Receiver<()>,
    command_rx: tokio::sync::mpsc::Receiver<SyncCommand>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
    activate_rx: tokio::sync::watch::Receiver<bool>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    eager_cache_status_tx: tokio::sync::watch::Sender<crate::sync::store::EagerCacheFillStatus>,
    running: Arc<AtomicBool>,
}

impl SyncLoopThread {
    pub(super) fn new(
        inner: Arc<SyncLoopHandleInner>,
        trigger_rx: tokio::sync::mpsc::Receiver<()>,
        command_rx: tokio::sync::mpsc::Receiver<SyncCommand>,
        stop_rx: tokio::sync::watch::Receiver<bool>,
        activate_rx: tokio::sync::watch::Receiver<bool>,
        status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
        eager_cache_status_tx: tokio::sync::watch::Sender<crate::sync::store::EagerCacheFillStatus>,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            trigger_rx,
            command_rx,
            stop_rx,
            activate_rx,
            status_tx,
            eager_cache_status_tx,
            running,
        }
    }

    fn run(mut self, runtime: tokio::runtime::Runtime) {
        let _running_guard = RunningGuard {
            running: Arc::clone(&self.running),
        };
        let status_tx = self.status_tx.clone();
        if runtime
            .block_on(std::panic::AssertUnwindSafe(self.run_loop()).catch_unwind())
            .is_err()
        {
            let failure = SyncLoopFailure::Panicked;
            error!("{failure}");
            status_tx.send_replace(SyncLoopStatus::Failed { error: failure });
            let cancelled = match self.eager_cache_status_tx.borrow().clone() {
                crate::sync::store::EagerCacheFillStatus::Scanning => {
                    Some(crate::sync::store::EagerCacheFillStatus::Cancelled(
                        crate::sync::store::EagerCacheFillProgress::empty(),
                    ))
                }
                crate::sync::store::EagerCacheFillStatus::Downloading(progress) => Some(
                    crate::sync::store::EagerCacheFillStatus::Cancelled(progress),
                ),
                _ => None,
            };
            if let Some(cancelled) = cancelled {
                self.eager_cache_status_tx.send_replace(cancelled);
            }
        }
    }

    async fn run_loop(&mut self) {
        if !self.wait_for_activation().await {
            return;
        }

        let eager_components = Arc::clone(&self.inner);
        let eager_cancel = self.stop_rx.clone();
        let eager_status = self.eager_cache_status_tx.clone();
        let eager_fill = async move {
            if let Err(error) = eager_components
                .components
                .fill_eager_cache(eager_cancel, &eager_status)
                .await
            {
                error!(%error, "post-open eager cache fill failed");
            }
        };
        tokio::pin!(eager_fill);
        let cycles = self.run_cycles();
        tokio::pin!(cycles);
        tokio::select! {
            () = &mut eager_fill => cycles.await,
            () = &mut cycles => eager_fill.await,
        }
    }

    async fn run_cycles(&mut self) {
        if !self.wait_for_first_cycle().await {
            return;
        }

        let mut consecutive_failures = 0;
        while self.running.load(Ordering::Acquire) && !*self.stop_rx.borrow() {
            self.status_tx.send_replace(SyncLoopStatus::CheckingStorage);
            let reachable = self.inner.components.probe_storage().await;
            let (decision, status) = match reachable {
                Err(error) => {
                    let error = Arc::new(error);
                    let status = storage_check_failure_status(Arc::clone(&error));
                    let failure = SyncLoopFailure::Storage(error);
                    (
                        loop_policy::after_failure(failure, consecutive_failures, 300),
                        status,
                    )
                }
                Ok(_) => {
                    self.status_tx.send_replace(SyncLoopStatus::Publishing);
                    self.run_reachable_cycle(consecutive_failures).await
                }
            };
            consecutive_failures = decision.consecutive_failures;
            self.status_tx.send_replace(status);
            if !self
                .wait_for_next_cycle(decision.wait, consecutive_failures)
                .await
            {
                break;
            }
        }
    }

    async fn wait_for_activation(&mut self) -> bool {
        loop {
            if *self.activate_rx.borrow() {
                return true;
            }
            tokio::select! {
                changed = self.activate_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                changed = self.stop_rx.changed() => {
                    if changed.is_err() || *self.stop_rx.borrow() {
                        info!("Prepared sync loop stopped before activation");
                        return false;
                    }
                }
            }
        }
    }

    async fn wait_for_first_cycle(&mut self) -> bool {
        let startup_delay = tokio::time::sleep(Duration::from_secs(3));
        tokio::pin!(startup_delay);
        loop {
            tokio::select! {
                _ = &mut startup_delay => return true,
                changed = self.stop_rx.changed() => {
                    if changed.is_err() || *self.stop_rx.borrow() {
                        info!("Sync loop stopped before first cycle");
                        return false;
                    }
                }
                message = self.trigger_rx.recv() => {
                    if message.is_none() {
                        info!("Sync trigger channel closed before first cycle");
                        return false;
                    }
                    return true;
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        info!("Sync command channel closed before first cycle");
                        return false;
                    };
                    self.inner.execute_command(command).await;
                }
            }
        }
    }

    async fn run_reachable_cycle(
        &self,
        consecutive_failures: u32,
    ) -> (loop_policy::SyncLoopDecision, SyncLoopStatus) {
        let (decision, cycle_went_offline) = match self.inner.run_single_cycle().await {
            Ok(result) => (loop_policy::after_success(result), false),
            Err(error) => {
                let offline = error.is_offline();
                let failure = SyncLoopFailure::Cycle(Arc::new(error));
                (
                    loop_policy::after_failure(failure, consecutive_failures, 300),
                    offline,
                )
            }
        };
        let status = match &decision.report {
            SyncLoopReport::Success(success) => {
                match self.inner.components.pending_blocked_writes().await {
                    Ok(writes) => current_success_status(writes, success.clone()),
                    Err(error) => SyncLoopStatus::Failed {
                        error: SyncLoopFailure::PendingWrites(Arc::new(error)),
                    },
                }
            }
            SyncLoopReport::Failure(_) if cycle_went_offline => SyncLoopStatus::Offline,
            SyncLoopReport::Failure(error) => SyncLoopStatus::Failed {
                error: error.clone(),
            },
        };
        (decision, status)
    }

    async fn wait_for_next_cycle(&mut self, wait: LoopWait, consecutive_failures: u32) -> bool {
        let duration = match wait {
            LoopWait::Immediate => Duration::ZERO,
            LoopWait::Idle => Duration::from_secs(crate::sync::backoff::backoff_secs(0, 300)),
            LoopWait::BackoffSecs(secs) => Duration::from_secs(secs),
        };
        if matches!(wait, LoopWait::BackoffSecs(_)) {
            debug!("Backing off {duration:?} after {consecutive_failures} consecutive failure(s)");
        }
        tokio::select! {
            _ = tokio::time::sleep(duration) => true,
            changed = self.stop_rx.changed() => {
                if changed.is_err() || *self.stop_rx.borrow() {
                    info!("Sync loop stop requested");
                    false
                } else {
                    true
                }
            }
            message = self.trigger_rx.recv() => {
                if message.is_none() {
                    info!("Sync trigger channel closed, stopping sync loop");
                    false
                } else {
                    true
                }
            }
            command = self.command_rx.recv() => {
                let Some(command) = command else {
                    info!("Sync command channel closed, stopping sync loop");
                    return false;
                };
                self.inner.execute_command(command).await;
                true
            }
        }
    }
}

pub(super) fn storage_check_failure_status(
    error: Arc<coven_protocol::objects::StorageError>,
) -> SyncLoopStatus {
    if error.is_transport() {
        SyncLoopStatus::Offline
    } else {
        SyncLoopStatus::Failed {
            error: SyncLoopFailure::Storage(error),
        }
    }
}

pub(super) fn current_success_status(
    writes: Vec<coven_protocol::write::PendingWrite>,
    success: SyncLoopSuccess,
) -> SyncLoopStatus {
    if writes.is_empty() {
        SyncLoopStatus::Synchronized(success)
    } else {
        SyncLoopStatus::Blocked { success, writes }
    }
}

struct RunningGuard {
    running: Arc<AtomicBool>,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}
