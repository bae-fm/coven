//! Browser sync runtime: drives sync cycles off the single JS event loop.
//!
//! The wasm counterpart of the native [`super::sync_loop`]. Native runs the loop
//! on a dedicated OS thread (a current-thread tokio runtime that `block_on`s the
//! loop) because its `Database` handle is `Send` and reached through an actor
//! thread. The browser has no threads and the wasm [`Database`] is `!Send` (it
//! holds the connection behind an `Rc<RefCell>`), so the loop runs as a
//! single-threaded task on the event loop: [`wasm_bindgen_futures::spawn_local`]
//! schedules it, and a [`gloo_timers::future::TimeoutFuture`] is the idle/backoff
//! wait between cycles in place of `tokio::time::sleep`. The loop holds only
//! single-threaded types — `Rc`/`Cell` and the clone-shareable [`Database`] /
//! `CloudSyncStorage` (no `Send`, no `Arc`, no threads).
//!
//! The loop mirrors the native one's shape: an initial delay so it does not race
//! page startup, then run-cycle → wait → repeat, with exponential backoff on
//! consecutive failures (via [`super::backoff::backoff_secs`]) and a prompt
//! re-run while an outbox drain is mid-batch. A trigger wakes the wait
//! immediately; a `stop()` flag ends the loop after the in-flight cycle.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use futures_util::future::{select, Either};
use gloo_timers::future::TimeoutFuture;
use tracing::{debug, error, info, warn};

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::clock::ClockRef;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;

use super::cloud_storage::{CloudCipher, CloudSyncStorage};
use super::hlc::Hlc;
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
/// [`CloudSyncStorage`] are held directly, the rest behind `Rc`.
///
/// The whole bundle lives behind one `Rc` so [`WasmSyncRuntime::start`] can move a
/// clone into the spawned task without moving the runtime itself.
struct CycleInputs {
    storage: CloudSyncStorage,
    hlc: Rc<Hlc>,
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
    library_dir: LibraryDir,
    blob_plan: Rc<dyn BlobPlan>,
    observer: Option<Rc<dyn BlobUploadObserver>>,
}

/// Drives [`super::cycle::run_single_sync_cycle`] repeatedly on the browser event
/// loop. Construct, then [`start`](Self::start); [`trigger`](Self::trigger) /
/// [`sync_now`](Self::sync_now) wake it immediately, [`stop`](Self::stop) ends it.
pub struct WasmSyncRuntime {
    inputs: Rc<CycleInputs>,
    schedule: WasmSyncSchedule,
    running: Rc<Cell<bool>>,
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
        device_id: String,
        hlc: Rc<Hlc>,
        cipher: Arc<RwLock<CloudCipher>>,
        db: Database,
        user_keypair: UserKeypair,
        clock: ClockRef,
        library_dir: LibraryDir,
        blob_plan: Rc<dyn BlobPlan>,
        observer: Option<Rc<dyn BlobUploadObserver>>,
        schedule: WasmSyncSchedule,
    ) -> Self {
        Self {
            inputs: Rc::new(CycleInputs {
                storage,
                hlc,
                device_id,
                cipher,
                db,
                user_keypair,
                clock,
                library_dir,
                blob_plan,
                observer,
            }),
            schedule,
            running: Rc::new(Cell::new(false)),
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
        if self.running.replace(true) {
            return;
        }

        let inputs = Rc::clone(&self.inputs);
        let running = Rc::clone(&self.running);
        let wake = Rc::clone(&self.wake);
        let schedule = self.schedule.clone();

        wasm_bindgen_futures::spawn_local(async move {
            // Initial delay so the loop does not race page startup.
            TimeoutFuture::new(schedule.initial_delay_ms).await;

            let mut consecutive_failures: u32 = 0;
            while running.get() {
                // When an outbox drain is mid-batch, run the next cycle at once so
                // each unit publishes as its blobs land. A failed cycle leaves
                // this false and falls back to the failure backoff.
                let drain_in_progress = match run_one_cycle(&inputs).await {
                    Ok(resume_drain_promptly) => {
                        consecutive_failures = 0;
                        resume_drain_promptly
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        warn!("Sync cycle failed: {e}");
                        false
                    }
                };

                if !running.get() {
                    break;
                }

                let wait_ms = if drain_in_progress {
                    // Keep draining + publishing without the inter-cycle pause. The
                    // next cycle does real upload work, so this is paced by the
                    // upload, not a busy-loop.
                    0
                } else if consecutive_failures > 0 {
                    let secs = super::backoff::backoff_secs(
                        consecutive_failures,
                        schedule.backoff_cap_secs,
                    );
                    debug!(
                        "Backing off {secs}s after {consecutive_failures} consecutive failure(s)",
                    );
                    // backoff_secs caps at backoff_cap_secs (300 in production), so
                    // the millisecond product is far inside u32 — no truncation.
                    (secs * 1_000) as u32
                } else {
                    schedule.idle_interval_ms
                };

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
        self.running.get()
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
        self.running.set(false);
        // Wake a sleeping wait so it returns now; the loop re-checks `running`
        // after the wait and exits on the cleared flag.
        self.wake.notify_one();
    }
}

/// Run one sync cycle over the owned inputs, returning whether the loop should
/// re-run promptly to finish an in-progress outbox drain. Re-borrows each input
/// per call (the loop owns them); no borrow spans the awaits inside the cycle.
async fn run_one_cycle(inputs: &CycleInputs) -> Result<bool, String> {
    let storage: &dyn SyncStorage = &inputs.storage;
    let cloud_home = inputs.storage.cloud_home();

    let result = super::cycle::run_single_sync_cycle(
        storage,
        &inputs.device_id,
        &inputs.hlc,
        inputs.clock.as_ref(),
        &inputs.db,
        &inputs.cipher,
        &inputs.user_keypair,
        &inputs.library_dir,
        Some(cloud_home),
        inputs.blob_plan.as_ref(),
        inputs.observer.as_deref(),
    )
    .await?;

    // Newer-schema skips are permanently inapplicable until the user updates the
    // app; surface them. Asset-download failures retry naturally next cycle, so a
    // log is enough rather than an error string.
    if result.skipped_schema > 0 {
        error!(
            count = result.skipped_schema,
            "Skipped changes from a newer app version; update the app to apply them",
        );
    }

    Ok(result.resume_drain_promptly)
}
