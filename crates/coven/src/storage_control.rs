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
            crate::coven::completion(previous.task).await;
        }
        let live = self.live.lock().await;
        if let Connection::Active(connected) = &*live {
            self.epoch.fetch_add(1, Ordering::SeqCst);
            return connected.sync_loop.start();
        }
        // Credentials may have been entered since disconnecting removed them.
        if !self.configured.load(Ordering::SeqCst) && self.codes.connection().await?.is_some() {
            self.configured.store(true, Ordering::SeqCst);
        }
        if !self.configured.load(Ordering::SeqCst) {
            return Ok(());
        }
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.publications.publish(SyncStatus::Syncing);
        // The attempt is the start of the first pass: retries wait for it.
        self.waits.sync_started();
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
        // An attempt that cannot go on publishes why; no pass follows it.
        let give_up = |failure: coven_sync::PassFailure| {
            self.publications.publish(failure.into());
            self.waits.sync_stopped();
        };
        let mut rearmed = self.waits.rearmed();
        let mut retry = match coven_sync::ConnectionRetry::open(&self.database).await {
            Ok(retry) => retry,
            Err(error) => return give_up(coven_sync::connection_failure(error)),
        };
        if let Some(failure) = retry.held(&self.waits) {
            self.publications.publish(failure.clone().into());
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
            if let Err(error) = self
                .waits
                .end_reported_waits(coven_sync::NetworkOwner::Connection)
                .await
            {
                return give_up(coven_sync::connection_failure(error));
            }
            self.waits.answer_held(retry.held(&self.waits));
            let previous = match self
                .database
                .retry_wait(coven_database::RetrySubject::Sync)
                .await
            {
                Ok(wait) => wait,
                Err(error) => return give_up(coven_sync::connection_failure(error.into())),
            };
            if retry.held(&self.waits).is_some()
                || !self
                    .waits
                    .due(coven_database::RetrySubject::Sync, previous.as_ref())
            {
                let wake = self
                    .waits
                    .next_wake(|subject| *subject == coven_database::RetrySubject::Sync)
                    .or_else(|| self.waits.cooldown_wake());
                tokio::select! {
                    () = async { if let Some(wake) = wake { wake.await } else { std::future::pending().await } } => {}
                    () = stopped => {}
                    changed = rearmed.changed() => if changed.is_err() { return self.waits.sync_stopped() },
                }
                continue;
            }
            let mut live = self.live.lock().await;
            if superseded() {
                break;
            }
            let requests = self.waits.take_requests();
            self.publications.publish(SyncStatus::Syncing);
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
                    Err((
                        SyncError::Failed { failure, source },
                        PendingReason::Failed(failure),
                    ))
                }
            };
            match result {
                Ok(()) if superseded() => {
                    let released = self.release(&mut live).await;
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "a failed release is published to the status, which carries it"
                    )]
                    let _ = self.publish_released(released);
                    requests.answer(&coven_sync::RetryOutcome::NotSyncing);
                    return;
                }
                Ok(()) => {
                    self.waits.return_requests(requests);
                    if let Connection::Active(connected) = &*live {
                        if let Err(error) = connected.sync_loop.start() {
                            self.publications
                                .publish(coven_sync::connection_failure(error).into());
                        }
                    } else {
                        // Connecting installs the connection's loop; without
                        // one, nothing plans the returned retries.
                        self.waits.sync_stopped();
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
                            requests.answer(&failure.clone().into());
                            if superseded() {
                                break;
                            }
                            self.publications.publish(failure.into());
                        }
                        Err(failure) => {
                            requests.answer(&failure.clone().into());
                            return give_up(failure);
                        }
                    }
                }
            }
        }
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a failed release is published to the status, which carries it"
        )]
        let _ = self.publish_released(Ok(()));
        self.waits.sync_stopped();
    }

    /// Stop after the active pass and transfers, then release the provider
    /// client; keys, credentials and location remain (E5).
    /// Retry requests no pass has taken are answered `NotSyncing` now.
    pub(crate) fn stop(self: &Arc<Self>) {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.waits.sync_stopped();
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
                reason = "no caller waits for stopping; the status carries a failure"
            )]
            let _ = owner.publish_released(released);
        });
    }

    /// Request a pass while started.
    pub(crate) fn sync_now(&self) {
        if let Some(control) = self.control.lock().expect("loop control poisoned").as_ref() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a closed loop has nothing to sync; the status reports why"
            )]
            let _ = control.sync_now();
        }
    }

    /// The device's network changed: the connection's owner, the upload
    /// worker and the pin worker each end their waits for the network (§6.7).
    pub(crate) fn network_changed(&self) {
        self.waits.network_changed();
    }

    /// The person asked to retry everything now: the same owners end every
    /// ordinary wait they retry (§6.7), and the first pass after the request
    /// answers it.
    pub(crate) fn retry_now(&self) -> coven_sync::RetryAnswer {
        self.waits.retry_requested()
    }

    /// Hold idle passes at most `interval` apart until the guard drops.
    pub(crate) fn sync_often(&self, interval: std::time::Duration) -> coven_sync::SyncOftenGuard {
        self.publications.state.sync_often(interval)
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<SyncStatus> {
        self.publications.state.subscribe_sync()
    }

    pub(crate) fn subscribe_invites(&self) -> tokio::sync::watch::Receiver<Vec<Invite>> {
        self.publications.state.subscribe_invites()
    }

    /// Stopped with storage set up, Disconnected without; a release failure
    /// is Failed with its cause.
    pub(super) fn publish_released(&self, result: Result<(), SyncError>) -> Result<(), SyncError> {
        let result = result.and_then(|()| {
            self.credentials
                .connection(&self.settings)
                .map(|data| data.is_some())
        });
        match result {
            Ok(configured) => {
                self.configured.store(configured, Ordering::SeqCst);
                self.publications.publish(if configured {
                    SyncStatus::Stopped
                } else {
                    SyncStatus::Disconnected
                });
                Ok(())
            }
            Err(error) => {
                let (failure, source) = error.into_shared();
                self.publications.publish(SyncStatus::Failed {
                    error: coven_sync::SyncFailure::Other(source.clone()),
                });
                Err(SyncError::Failed { failure, source })
            }
        }
    }

    /// Finish the connection's work, then release custody owners.
    pub(crate) async fn close(&self) -> Result<(), SyncError> {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.waits.sync_stopped();
        self.stopped.notify_waiters();
        let attempts = self
            .attempts
            .lock()
            .expect("connection attempts poisoned")
            .take();
        if let Some(attempts) = attempts {
            crate::coven::completion(attempts.task).await;
        }
        let _call = self.calls.lock().await;
        let released = self.release(&mut *self.live.lock().await).await;
        self.codes.close().await;
        let keys = self.keys.lock().expect("store keys lock poisoned").take();
        self.authentication.lock().await.take();
        drop(keys);
        released
    }
}
