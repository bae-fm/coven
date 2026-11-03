//! Connection requests, attempts and their lifetime.

use super::*;

impl StorageConnections {
    /// Start the loop with the store keys this session holds. Without a
    /// connection, the first pass builds the provider client from custody's
    /// credentials, failing and backing off like any pass (E5).
    pub(crate) async fn start(self: &Arc<Self>) -> Result<(), SyncError> {
        let _call = self.lock_open().await?;
        let keys = self
            .keys
            .lock()
            .expect("store keys lock poisoned")
            .clone()
            .ok_or(KeyError::StoreClosed)?;
        if let Some(key) = SequenceSync::missing_store_key(&self.database, &keys).await? {
            return Err(SyncError::KeyUnavailable(key));
        }
        let previous = {
            let mut attempts = self.attempts.lock().expect("connection attempts poisoned");
            if attempts.as_ref().is_some_and(|attempt| {
                attempt.epoch == self.epoch.load(Ordering::SeqCst) && !attempt.task.is_finished()
            }) {
                return Ok(());
            }
            attempts.take()
        };
        if let Some(previous) = previous {
            attempt_ended(previous.task).await;
        }
        let live = self.live.lock().await;
        if let ConnectionState::Active(connected) = &*live {
            self.epoch.fetch_add(1, Ordering::SeqCst);
            return connected.sync_loop.start().await;
        }
        // Credentials may have been entered since the store opened without
        // them: record what custody holds now.
        if !self.configured.load(Ordering::SeqCst) {
            self.record_custody().await?;
        }
        if !self.configured.load(Ordering::SeqCst) {
            return Ok(());
        }
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        // The attempt is the start of the first pass: retries wait for it.
        self.waits.sync_started().await?;
        let owner = self.clone();
        *self.attempts.lock().expect("connection attempts poisoned") = Some(ConnectionAttempt {
            epoch,
            task: tokio::spawn(async move { owner.connect_started(epoch).await }),
        });
        Ok(())
    }

    /// Build the started loop's first client, retrying under the connection
    /// backoff until it connects or stopping is requested. Each attempt is
    /// the start of a pass: a failed one answers the retries it took, and a
    /// connected one leaves them to the loop's first pass (IO `RetryAnswer`).
    async fn connect_started(self: Arc<Self>, epoch: u64) {
        let superseded = || self.epoch.load(Ordering::SeqCst) != epoch;
        let mut rearmed = self.waits.rearmed();
        let mut retry = match coven_sync::ConnectionRetry::open(&self.database).await {
            Ok(retry) => retry,
            Err(error) => return self.give_up(coven_sync::connection_failure(error)).await,
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
                return self.give_up(coven_sync::connection_failure(error)).await;
            }
            // A held pass answers the retries acted on at once, and a
            // cooldown that ended is written.
            if self.waits.answer_held(retry.held(&self.waits)).await.is_err() {
                return;
            }
            let previous = match self.database.retry_wait(coven_database::RetrySubject::Sync).await
            {
                Ok(wait) => wait,
                Err(error) => {
                    return self.give_up(coven_sync::connection_failure(error.into())).await
                }
            };
            if retry.held(&self.waits).is_some()
                || !self.waits.due(coven_database::RetrySubject::Sync, previous.as_ref())
            {
                let wake =
                    self.waits.next_wake(|subject| *subject == coven_database::RetrySubject::Sync);
                // The cooldown's end changes what the person's retry would
                // do, even while the connection's backoff still holds.
                let cooldown = self.waits.cooldown_wake();
                tokio::select! {
                    () = async { if let Some(wake) = wake { wake.await } else { std::future::pending().await } } => {}
                    () = async { if let Some(cooldown) = cooldown { cooldown.await } else { std::future::pending().await } } => {}
                    () = stopped => {}
                    changed = rearmed.changed() => if changed.is_err() { return self.stopped().await },
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
            let result = match self.codes.location().await {
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
                Ok(()) if superseded() => {
                    let released = self.release(&mut live).await;
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "a failed release is recorded as the last pass's failure"
                    )]
                    let _ = self.publish_released(released).await;
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
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a failed release is recorded as the last pass's failure"
        )]
        let _ = self.publish_released(Ok(())).await;
        self.stopped().await;
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

    /// Stop after the active pass and transfers, then release the provider
    /// client; keys, credentials and location remain (E5).
    /// Retry requests no pass has taken are answered `NotSyncing` before it
    /// returns. Stopping goes on even when writing that fails, which has
    /// failed the handle; the failure is returned.
    pub(crate) async fn stop(self: &Arc<Self>) -> Result<(), SyncError> {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let stopped = self.waits.sync_stopped().await;
        self.stopped.notify_waiters();
        let owner = self.clone();
        tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            let mut live = owner.live.lock().await;
            // A later start or stop decides instead.
            if owner.epoch.load(Ordering::SeqCst) != epoch {
                return;
            }
            let released = owner.release(&mut live).await;
            #[expect(
                clippy::let_underscore_must_use,
                reason = "no caller waits for stopping; a failure is recorded as the last pass's"
            )]
            let _ = owner.publish_released(released).await;
        });
        Ok(stopped?)
    }

    /// Request a pass while started.
    pub(crate) fn sync_now(&self) {
        if let Some(control) = self.control.lock().expect("loop control poisoned").as_ref() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a closed loop has nothing to sync; the sync state says why"
            )]
            let _ = control.sync_now();
        }
    }

    /// The device's network changed: the connection's owner and the upload
    /// worker each end their waits for the network (§6.7).
    pub(crate) fn network_changed(&self) {
        self.waits.network_changed();
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
        let result = result.and_then(|()| self.credentials.location(&self.settings));
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
        let attempts = self.attempts.lock().expect("connection attempts poisoned").take();
        if let Some(attempts) = attempts {
            attempt_ended(attempts.task).await;
        }
        let _call = self.calls.lock().await;
        let released = self.release(&mut *self.live.lock().await).await;
        self.expiry.close().await;
        self.codes.close().await;
        let keys = self.keys.lock().expect("store keys lock poisoned").take();
        self.authentication.lock().await.take();
        drop(keys);
        released.and(stopped.map_err(SyncError::from))
    }
}

/// Wait for a connection attempt to end. A panic in it unwinds into the
/// waiter; the runtime dropping it as it shuts down has ended it too, which
/// is all the waiter needs.
async fn attempt_ended(task: tokio::task::JoinHandle<()>) {
    if let Err(shutdown) = crate::coven::completion("connect to storage", task).await {
        tracing::debug!("{shutdown}");
    }
}
