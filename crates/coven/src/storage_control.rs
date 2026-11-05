//! Connection requests, attempts and their lifetime.

use super::*;
use coven_foundation::locks::lock_unpoisoned;

impl StorageConnections {
    /// Start the loop with the store keys this session holds. Without a
    /// connection, the first pass builds the provider client from custody's
    /// credentials, failing and backing off like any pass (E5).
    pub(crate) async fn start(self: &Arc<Self>) -> Result<(), SyncError> {
        let _call = self.lock_open().await?;
        self.start_locked().await
    }

    /// Start the loop, as `start` does, for a caller holding the call lock:
    /// a setup past its point of no return starts it for the connection
    /// that finishes it (§11.6).
    pub(super) async fn start_locked(self: &Arc<Self>) -> Result<(), SyncError> {
        let keys = self.with_keys(|keys| Ok(keys.clone()))?;
        if let Some(key) = SequenceSync::missing_store_key(&self.database, &keys).await? {
            return Err(SyncError::KeyUnavailable(key));
        }
        let previous = {
            let mut attempts = lock_unpoisoned(&self.attempts);
            if attempts.as_ref().is_some_and(|attempt| {
                attempt.epoch == self.epoch.load(Ordering::SeqCst) && !attempt.task.is_finished()
            }) {
                return Ok(());
            }
            attempts.take()
        };
        if let Some(previous) = previous {
            previous.task.ended().await;
        }
        let live = self.live.lock().await;
        if let ConnectionState::Active(connected) = &*live {
            self.epoch.fetch_add(1, Ordering::SeqCst);
            return connected.sync_loop.start().await;
        }
        // Credentials may have been entered since the store opened without
        // them: record what custody holds now.
        // A setup past its point of no return connects with its own
        // credentials until it finishes (§11.6).
        let finishing = self.setup_finishing();
        if !finishing && !self.configured.load(Ordering::SeqCst) {
            self.record_custody().await?;
        }
        if !finishing && !self.configured.load(Ordering::SeqCst) {
            return Ok(());
        }
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        // The attempt is the start of the first pass: retries wait for it.
        self.waits.sync_started().await?;
        let owner = self.clone();
        let ending = self.clone();
        let parks = coven_sync::Parks::new();
        #[cfg(test)]
        let attempt_parks = parks.clone();
        let task = crate::task::spawn_owned(
            "connect to storage",
            self.database.bugs().clone(),
            async move { owner.connect_started(epoch, parks).await },
            |bug| async move { ending.attempt_panicked(bug).await },
        );
        *lock_unpoisoned(&self.attempts) = Some(ConnectionAttempt {
            epoch,
            task,
            #[cfg(test)]
            parks: attempt_parks,
        });
        Ok(())
    }

    /// The connection attempt panicked with `bug`, which has gone to the
    /// store's bug sink: the last pass is recorded failed with it, sync
    /// stops, and the retries waiting, those the attempt took among them,
    /// are answered with it (E15).
    async fn attempt_panicked(&self, bug: crate::task::Bug) {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "the attempt has ended either way; a failed write has failed the handle, which reports it"
        )]
        let _ = self.show(LastPass::failed(SyncError::Bug(bug.clone()))).await;
        if let Err(error) = self.waits.sync_panicked(bug).await {
            tracing::debug!("stopping sync on a failed handle: {error}");
        }
    }

    /// Build the started loop's first client, retrying under the connection
    /// backoff until it connects or stopping is requested. Each attempt is
    /// the start of a pass: a failed one answers the retries it took, and a
    /// connected one leaves them to the loop's first pass (IO `RetryAnswer`).
    async fn connect_started(self: Arc<Self>, epoch: u64, parks: coven_sync::Parks) {
        let _running = parks.running();
        let superseded = || self.epoch.load(Ordering::SeqCst) != epoch;
        let mut rearmed = self.waits.rearmed();
        let mut retry = match coven_sync::ConnectionRetry::open(&self.database).await {
            Ok(retry) => retry,
            Err(error) => {
                return self.give_up(coven_sync::connection_failure(self.reported(error))).await
            }
        };
        let held = retry.held(&self.waits);
        if self.waits.holds(held).await.is_err() {
            return;
        }
        if let Some(failure) = held {
            if self.show(failure.clone().into()).await.is_err() {
                return self.stopped().await;
            }
        }
        loop {
            // Register before I/O: stop may arrive while the request is in flight.
            let stopped = self.stopped.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            if superseded() {
                break;
            }
            // The app's network report ends a wait for the network, and the
            // person's retry any wait (§6.7).
            if let Err(error) =
                self.waits.end_reported_waits(coven_sync::NetworkOwner::Connection).await
            {
                return self.give_up(coven_sync::connection_failure(self.reported(error))).await;
            }
            // The cooldown's end changes what the person's retry would do,
            // even while the connection's backoff still holds. Taken before
            // the write below, so a cooldown that ends after it wakes the
            // attempt to write that.
            let cooldown = self.waits.cooldown_wake();
            // A held pass answers the retries acted on at once, and a
            // cooldown that ended is written.
            if self.waits.answer_held(retry.held(&self.waits)).await.is_err() {
                return;
            }
            let previous = match self.database.retry_wait(coven_database::RetrySubject::Sync).await
            {
                Ok(wait) => wait,
                Err(error) => {
                    return self.give_up(coven_sync::connection_failure(self.reported(error))).await
                }
            };
            if retry.held(&self.waits).is_some()
                || !self.waits.due(coven_database::RetrySubject::Sync, previous.as_ref())
            {
                let wake =
                    self.waits.next_wake(|subject| *subject == coven_database::RetrySubject::Sync);
                tokio::select! {
                    biased;
                    never = parks.polling() => match never {},
                    () = async { if let Some(wake) = wake { wake.await } else { std::future::pending().await } } => {}
                    () = async { if let Some(cooldown) = cooldown { cooldown.await } else { std::future::pending().await } } => {}
                    () = stopped => {}
                    changed = rearmed.changed() => if changed.is_err() { return self.stopped().await },
                    never = parks.parked() => match never {},
                }
                continue;
            }
            let mut live = self.live.lock().await;
            if superseded() {
                break;
            }
            // The attempt begins only as the retry effect's row lets it: a
            // cooldown that began since the plan holds it, and it plans
            // again.
            let requests = match self.waits.begin_pass().await {
                Ok(Some(requests)) => requests,
                Ok(None) => continue,
                Err(_) => return,
            };
            // Location identifies the provider even if decoding or reading its
            // credentials fails. A location read itself is a local disk failure.
            let connection_version = self.waits.connection_version();
            let result = match self.connecting_location().await {
                Ok(Some(location)) => self.connect(&mut live).await.map_err(|error| {
                    let reason = error.pending_reason(location.provider());
                    (error, reason)
                }),
                Ok(None) => Err((SyncError::NoStorage, PendingReason::NoStorage)),
                Err(error) => {
                    let (failure, source) = error.into_shared();
                    Err((SyncError::Failed { failure, source }, PendingReason::Failed(failure)))
                }
            };
            match result {
                // The stop or close that superseded the attempt waits for
                // it, then releases what it connected.
                Ok(()) if superseded() => {
                    requests.answer(&coven_sync::RetryOutcome::NotSyncing);
                    return;
                }
                Ok(()) => {
                    if self.waits.return_requests(requests).await.is_err() {
                        return;
                    }
                    if let ConnectionState::Active(connected) = &*live {
                        if let Err(error) = connected.sync_loop.start().await {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "the attempt ends either way; a failed write has failed the handle, which reports it"
                            )]
                            let _ = self.show(coven_sync::connection_failure(error).into()).await;
                        }
                    } else {
                        // Connecting installs the connection's loop; without
                        // one, nothing plans the returned retries.
                        self.stopped().await;
                    }
                    return;
                }
                Err((error, reason)) => {
                    let error = self.reported(error);
                    let failed = retry
                        .failed(
                            &self.database,
                            &self.waits,
                            reason,
                            connection_version,
                            error,
                            coven_sync::connection_failure,
                        )
                        .await;
                    drop(live);
                    match failed {
                        Ok(failure) => {
                            // What a retry would do changes with the failure
                            // before the person hears it or the row records
                            // it.
                            if let Err(error) = self.waits.holds(retry.held(&self.waits)).await {
                                let error = SyncError::from(error);
                                return requests.answer(
                                    &coven_sync::PassFailure::Failed { error: error.into() }.into(),
                                );
                            }
                            requests.answer(&failure.clone().into());
                            if superseded() {
                                break;
                            }
                            if self.show(failure.into()).await.is_err() {
                                return self.stopped().await;
                            }
                        }
                        Err(failure) => {
                            // Sync stops before the person hears why.
                            self.give_up(failure.clone()).await;
                            return requests.answer(&failure.into());
                        }
                    }
                }
            }
        }
        // Superseded: the stop or close that did so says sync isn't
        // running, and stopping records the location, once this ends.
    }

    /// Where a connection attempt connects: the recorded location, or that
    /// of a setup past its point of no return, which it finishes (§11.6).
    async fn connecting_location(&self) -> Result<Option<StorageConfig>, SyncError> {
        if let Some(location) = self.codes.location().await? {
            return Ok(Some(location));
        }
        if !self.setup_finishing() {
            return Ok(None);
        }
        Ok(coven_sync::SetupJournal::running(&self.database)
            .await?
            .and_then(|journal| journal.location().cloned()))
    }

    /// `error`, once a bug it carries has gone to the store's bug sink:
    /// nobody waits for the attempt, so its failures reach no caller (E15).
    fn reported(&self, error: impl Into<SyncError>) -> SyncError {
        crate::task::reported(self.database.bugs().as_ref(), error.into())
    }

    /// An attempt that cannot go on shows why; no pass follows it.
    async fn give_up(&self, failure: coven_sync::PassFailure) {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "the attempt ends either way; a failed write has failed the handle, which reports it"
        )]
        let _ = self.show(failure.into()).await;
        self.stopped().await;
    }

    /// No connection plans after an attempt ends: the waiting retries hear
    /// so. A failed write has failed the handle and answered them with its
    /// failure, which every call reports, so the attempt ends either way.
    async fn stopped(&self) {
        if let Err(error) = self.waits.sync_stopped().await {
            tracing::debug!("stopping sync on a failed handle: {error}");
        }
    }

    /// Stop after the active pass and transfers, release the provider
    /// client, then record the location as custody holds it; keys,
    /// credentials and location remain (E5). Retry requests no pass has
    /// taken are answered `NotSyncing` first. Stopping holds the call lock
    /// throughout, so a start asked meanwhile begins after it, and it
    /// finishes even if the caller stops waiting. A failed write has failed
    /// the handle, and a failed release is recorded as the last pass's
    /// failure; stopping goes on, and the failure is returned.
    pub(crate) async fn stop(self: &Arc<Self>) -> Result<(), SyncError> {
        let owner = self.clone();
        let stopping = async move {
            let _call = owner.lock_calls().await;
            owner.epoch.fetch_add(1, Ordering::SeqCst);
            let stopped = owner.waits.sync_stopped().await;
            owner.stopped.notify_waiters();
            let attempt = lock_unpoisoned(&owner.attempts).take();
            if let Some(attempt) = attempt {
                attempt.task.ended().await;
            }
            let released = owner.release(&mut *owner.live.lock().await).await;
            let published = owner.publish_released(released).await;
            stopped.map_err(SyncError::from).and(published)
        };
        crate::task::spawn_completing("stop sync", self.database.bugs().clone(), stopping).await?
    }

    /// Request a pass while started.
    pub(crate) fn sync_now(&self) {
        if let Some(control) = lock_unpoisoned(&self.control).as_ref() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a closed loop has nothing to sync; the sync state says why"
            )]
            let _ = control.sync_now();
        }
    }

    /// The person asked to retry everything now: the same owners end every
    /// ordinary wait they retry (§6.7), and the first pass after the request
    /// answers it, or the retry effect's row at once.
    pub(crate) async fn retry_now(&self) -> Result<coven_sync::RetryAnswer, SyncError> {
        Ok(self.waits.retry_requested().await?)
    }

    /// Hold idle passes at most `interval` apart until the guard drops.
    pub(crate) fn sync_often(&self, interval: std::time::Duration) -> coven_sync::SyncOftenGuard {
        self.publications.state.sync_often(interval)
    }

    /// Once the connection is released, record whether storage is set up
    /// here as custody holds it now; a release failure, or a failure reading
    /// custody, is recorded as the last pass's failure with its cause.
    pub(super) async fn publish_released(
        &self,
        result: Result<(), SyncError>,
    ) -> Result<(), SyncError> {
        let result = result.and_then(|()| self.credentials.location());
        match result {
            Ok(location) => self.record_location(location, None).await,
            Err(error) => {
                let (failure, source) = error.into_shared();
                self.show(LastPass::failed(coven_sync::SyncFailure::Other(source.clone()))).await?;
                Err(SyncError::Failed { failure, source })
            }
        }
    }

    /// Finish the connection's work, then release custody owners.
    pub(crate) async fn close(&self) -> Result<(), SyncError> {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        let stopped = self.waits.sync_stopped().await;
        self.stopped.notify_waiters();
        let attempts = lock_unpoisoned(&self.attempts).take();
        if let Some(attempts) = attempts {
            attempts.task.ended().await;
        }
        let _call = self.lock_calls().await;
        let released = self.release(&mut *self.live.lock().await).await;
        self.expiry.close().await;
        self.codes.close().await;
        let keys = lock_unpoisoned(&self.keys).take();
        self.authentication.lock().await.take();
        drop(keys);
        #[cfg(test)]
        let released = match lock_unpoisoned(&self.close_failure).take() {
            Some(failure) => released.and(Err(failure)),
            None => released,
        };
        released.and(stopped.map_err(SyncError::from))
    }

    /// Make the next close return `failure` once it has closed.
    #[cfg(test)]
    pub(crate) fn fail_next_close(&self, failure: SyncError) {
        *lock_unpoisoned(&self.close_failure) = Some(failure);
    }
}

#[cfg(test)]
impl StorageConnections {
    /// Wait until a call finds the call lock held and waits, after this
    /// call.
    pub(crate) fn call_waits_from_now(&self) -> impl std::future::Future<Output = ()> {
        let mut waits = self.call_waits.subscribe();
        let now = *waits.borrow_and_update();
        async move {
            coven_foundation::clock::within(waits.wait_for(|waits| *waits > now))
                .await
                .expect("a call waits for the call lock")
                .expect("the call lock's waits are kept");
        }
    }

    /// Wait until downloads have found the limit full `count` more times.
    pub(crate) fn downloads_waiting_from_now(
        &self,
        count: usize,
    ) -> impl std::future::Future<Output = ()> {
        self.transfers.downloads_waiting_from_now(count)
    }

    /// Wait until a pause is asked of the connection's sync loop after
    /// this call.
    pub(crate) fn pause_asked_from_now(&self) -> impl std::future::Future<Output = ()> {
        lock_unpoisoned(&self.control)
            .as_ref()
            .expect("a connection's sync loop")
            .pause_asked_from_now()
    }

    /// Wait until the invite expiry's wakes, the connection attempt, the
    /// connection's sync loop and the upload worker, those running, have in
    /// turn each taken everything that was ready after the one before it
    /// parked, and wait: whatever the caller and the earlier ones did has
    /// been seen.
    pub(crate) async fn parked(&self) {
        self.expiry.parked_from_now().await;
        let attempt =
            lock_unpoisoned(&self.attempts).as_ref().map(|attempt| attempt.parks.parked_from_now());
        if let Some(attempt) = attempt {
            attempt.await;
        }
        let sync = lock_unpoisoned(&self.control).as_ref().map(|control| control.parked_from_now());
        if let Some(sync) = sync {
            sync.await;
        }
        self.files.uploads_parked_from_now().await;
    }

    /// Hold the connection's passes, or let them run again, leaving sync
    /// as started or stopped as it is: while started, uploads run beside
    /// held passes, as during a connection change (§17.4).
    pub(crate) async fn hold_passes(&self, held: bool) {
        let live = self.live.lock().await;
        let ConnectionState::Active(live) = &*live else {
            panic!("passes are held on a connection");
        };
        if held {
            live.sync_loop.pause().await.unwrap();
        } else {
            live.sync_loop.resume().unwrap();
        }
    }
}
