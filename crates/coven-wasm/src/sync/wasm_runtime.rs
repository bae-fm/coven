//! Browser sync runtime: drives sync cycles off the single JS event loop.
//!
//! The wasm counterpart of the native [`super::sync_loop`]. Native runs the loop
//! on a dedicated OS thread (a current-thread tokio runtime that `block_on`s the
//! loop) because its `Database` handle is `Send` and reached through an actor
//! thread. The browser has no threads and the wasm [`Database`] is `!Send` (it
//! holds the connection behind an `Rc<RefCell>`), so the loop runs as a
//! single-threaded task on the event loop: [`wasm_bindgen_futures::spawn_local`]
//! schedules it, and a [`gloo_timers::future::TimeoutFuture`] is the idle/backoff
//! wait between cycles in place of `tokio::time::sleep`. The loop holds the
//! clone-shareable [`Database`] / `CloudSyncStorage`, browser-local `Rc` control
//! state, and shared core handles such as the database's `Arc<Hlc>`.
//!
//! The loop mirrors the native one's shape: an initial delay so it does not race
//! page startup, then run-cycle → wait → repeat. Shared loop policy resets or
//! increments the failure count, chooses idle/backoff/immediate waits, and asks
//! for a prompt re-run while an outbox drain is mid-batch. A trigger wakes the
//! wait immediately; a `stop()` flag ends the loop after the in-flight cycle.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use futures_util::future::{select, Either};
use gloo_timers::future::TimeoutFuture;
use tracing::{debug, error, info, warn};

use crate::blob::BlobTransitionObserver;
use crate::clock::ClockRef;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

use super::cloud_storage::{CloudCipher, CloudSyncStorage};
use super::hlc::Hlc;
use super::loop_policy::{self, LoopWait, SyncLoopAlerts, SyncLoopReport};
use super::storage::SyncStorage;

/// Timing knobs for the loop. The native loop's cadence — which a host wires up —
/// is a 3 s startup grace, a 30 s idle interval, and a 300 s failure-backoff cap;
/// a headless test passes short values so convergence is observed quickly.
#[derive(Clone)]
pub struct WasmSyncSchedule {
    /// Delay before the first cycle, so the loop does not race page startup.
    pub initial_delay_ms: u32,
    /// Steady-state wait between successful cycles when no trigger fires.
    pub idle_interval_ms: u32,
    /// Upper bound on the exponential failure backoff, in seconds.
    pub backoff_cap_secs: u64,
}

/// What a sync cycle needs, owned together so the loop re-borrows them each
/// iteration. The wasm counterpart of the native loop's `SyncLoopInner`, but with
/// single-threaded types: the clone-shareable [`Database`] and
/// [`CloudSyncStorage`] are held directly, while browser-local loop state stays
/// behind `Rc`.
///
/// The whole bundle lives behind one `Rc` so [`WasmSyncRuntime::start`] can move a
/// clone into the spawned task without moving the runtime itself.
struct CycleInputs {
    storage: CloudSyncStorage,
    hlc: Arc<Hlc>,
    store_id: String,
    device_id: String,
    /// The at-rest cipher, shared with `storage` (the same `Arc<RwLock<CloudCipher>>`
    /// the [`CloudSyncStorage`] seals/opens with, via its
    /// [`shared_cipher`](CloudSyncStorage::shared_cipher)). One lock so a key
    /// rotation through either is seen by both — the cycle re-seals control objects
    /// under the rotated key while the storage reads/writes under it. `Arc` (not
    /// `Rc`) because that is the type the storage exposes; on the single-threaded
    /// browser runtime the choice is immaterial to behavior.
    cipher: Arc<RwLock<CloudCipher>>,
    db: Database,
    user_keypair: UserKeypair,
    clock: ClockRef,
    store_dir: StoreDir,
    observer: Option<Rc<dyn BlobTransitionObserver>>,
}

/// Drives [`super::cycle::run_single_sync_cycle`] repeatedly on the browser event
/// loop. Construct, then [`start`](Self::start); [`trigger`](Self::trigger) /
/// [`sync_now`](Self::sync_now) wake it immediately, [`stop`](Self::stop) ends it.
pub struct WasmSyncRuntime {
    inputs: Rc<CycleInputs>,
    schedule: WasmSyncSchedule,
    active: RefCell<Option<Rc<Cell<bool>>>>,
    /// Wakes a sleeping wait. `notify_one` stores at most one permit, so many
    /// triggers between waits collapse into a single wake — the same effect as the
    /// native loop's capacity-1 trigger channel. A permit fired while a cycle is
    /// running (nothing awaiting) is held until the next wait and consumed there.
    wake: Rc<tokio::sync::Notify>,
}

impl WasmSyncRuntime {
    /// Assemble a runtime over the cycle's inputs. The [`Database`] and
    /// [`CloudSyncStorage`] are clone-shareable; the runtime takes ownership of
    /// the clones the host hands it. Pass the storage's own cipher lock
    /// ([`CloudSyncStorage::shared_cipher`]) as `cipher` so the cycle and the
    /// storage seal/open under one rotating key. Does not start the loop — call
    /// [`start`](Self::start).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: CloudSyncStorage,
        store_id: String,
        device_id: String,
        hlc: Arc<Hlc>,
        cipher: Arc<RwLock<CloudCipher>>,
        db: Database,
        user_keypair: UserKeypair,
        clock: ClockRef,
        store_dir: StoreDir,
        observer: Option<Rc<dyn BlobTransitionObserver>>,
        schedule: WasmSyncSchedule,
    ) -> Self {
        Self {
            inputs: Rc::new(CycleInputs {
                storage,
                hlc,
                store_id,
                device_id,
                cipher,
                db,
                user_keypair,
                clock,
                store_dir,
                observer,
            }),
            schedule,
            active: RefCell::new(None),
            wake: Rc::new(tokio::sync::Notify::new()),
        }
    }

    /// Start the loop on the event loop. No-op if already running.
    ///
    /// Spawns a `!Send` task (the wasm [`Database`] is `!Send`) that waits the
    /// initial delay, then loops: run a cycle, then await either the idle/backoff
    /// timer or a trigger, and repeat until [`stop`](Self::stop). Clones of the
    /// input bundle and control state move into the task; the runtime keeps its
    /// own clones so its control methods keep working after the task is spawned.
    pub fn start(&self) {
        let running = {
            let mut active = self.active.borrow_mut();
            if active.is_some() {
                return;
            }
            let running = Rc::new(Cell::new(true));
            *active = Some(Rc::clone(&running));
            running
        };

        let inputs = Rc::clone(&self.inputs);
        let wake = Rc::clone(&self.wake);
        let schedule = self.schedule.clone();

        wasm_bindgen_futures::spawn_local(async move {
            // Initial delay so the loop does not race page startup.
            TimeoutFuture::new(schedule.initial_delay_ms).await;

            let mut consecutive_failures: u32 = 0;
            while running.get() {
                let decision = match run_one_cycle(&inputs).await {
                    Ok(result) => loop_policy::after_success(result),
                    Err(error) => loop_policy::after_failure(
                        error,
                        consecutive_failures,
                        schedule.backoff_cap_secs,
                    ),
                };
                consecutive_failures = decision.consecutive_failures;

                match &decision.report {
                    SyncLoopReport::Success(success) => log_alerts(&success.alerts),
                    SyncLoopReport::Failure(error) => warn!("Sync cycle failed: {error}"),
                }

                if !running.get() {
                    break;
                }

                let wait_ms = decision.wait.as_millis(schedule.idle_interval_ms);
                if let LoopWait::BackoffSecs(secs) = decision.wait {
                    debug!(
                        "Backing off {secs}s after {consecutive_failures} consecutive failure(s)",
                    );
                }

                // Await the wait OR a wake, whichever comes first. A wake (a
                // trigger, or the one stop() fires) returns the loop immediately
                // instead of riding out the timer.
                let timer = TimeoutFuture::new(wait_ms);
                let woken = Box::pin(wake.notified());
                match select(timer, woken).await {
                    Either::Left(((), _notified)) => {}
                    Either::Right(((), _timer)) => {}
                }
            }

            info!("Sync loop stopped");
        });
    }

    /// Whether the loop is running.
    pub fn is_running(&self) -> bool {
        self.active.borrow().is_some()
    }

    /// Wake the loop to run a cycle immediately. Collapses with any pending wake.
    /// A trigger fired while the loop is between waits is held as one stored permit
    /// and consumed at the next wait, so it is never lost; a trigger fired after
    /// `stop()` is harmless (the loop has exited).
    pub fn trigger(&self) {
        self.wake.notify_one();
    }

    /// Alias for [`trigger`](Self::trigger), matching the host-facing "sync now"
    /// verb the facade exposes.
    pub fn sync_now(&self) {
        self.trigger();
    }

    /// Stop the loop after the in-flight cycle. Sets the run flag false and wakes
    /// the loop so a sleeping wait returns at once rather than riding out the idle
    /// interval; the loop then sees the cleared flag and exits. Idempotent.
    pub fn stop(&self) {
        if let Some(running) = self.active.borrow_mut().take() {
            running.set(false);
        }
        // Wake a sleeping wait so it returns now; the loop re-checks `running`
        // after the wait and exits on the cleared flag.
        self.wake.notify_one();
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) fn active_token_for_test(&self) -> Option<Rc<Cell<bool>>> {
        self.active.borrow().clone()
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) fn hlc_is_for_test(&self, expected: &Arc<Hlc>) -> bool {
        Arc::ptr_eq(&self.inputs.hlc, expected)
    }
}

impl Drop for WasmSyncRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Run one sync cycle over the owned inputs. Re-borrows each input per call (the
/// loop owns them); no borrow spans the awaits inside the cycle.
async fn run_one_cycle(inputs: &CycleInputs) -> Result<super::cycle::SyncCycleResult, String> {
    let storage: &dyn SyncStorage = &inputs.storage;
    let cloud_home = inputs.storage.cloud_home();
    let pending_rotation = inputs.storage.shared_pending_rotation();

    let result = super::cycle::run_single_sync_cycle(
        storage,
        &inputs.store_id,
        &inputs.device_id,
        &inputs.hlc,
        inputs.clock.as_ref(),
        &inputs.db,
        &inputs.cipher,
        &pending_rotation,
        &inputs.user_keypair,
        None,
        &inputs.store_dir,
        Some(cloud_home),
        inputs.observer.as_deref(),
    )
    .await?;

    Ok(result)
}

fn log_alerts(alerts: &SyncLoopAlerts) {
    if let Some(pending) = &alerts.rotation_pending {
        warn!(
            committed_generation = pending.committed_generation,
            live_generation = pending.live_generation,
            "Sync paused: this device has not adopted a committed store-key rotation",
        );
    }
    if alerts.skipped_schema > 0 {
        error!(
            count = alerts.skipped_schema,
            "Skipped changes from a newer app version; update the app to apply them",
        );
    }
    if alerts.rejected_unauthorized > 0 {
        error!(
            count = alerts.rejected_unauthorized,
            "Rejected changes from an unauthorized device (forged or removed member)",
        );
    }
    if alerts.invalid_signatures > 0 {
        error!(
            count = alerts.invalid_signatures,
            "Stalled changes with an invalid signature (forged or corrupt)",
        );
    }
    if !alerts.held_changesets.is_empty() {
        error!(
            count = alerts.held_changesets.len(),
            "Stalled invalid changes",
        );
    }
    if alerts.asset_downloads_failed {
        warn!("Some files failed to download, will retry");
    }
}
