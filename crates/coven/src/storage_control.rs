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
        let owner = self.clone();
        *self.attempts.lock().expect("connection attempts poisoned") = Some(ConnectionAttempt {
            epoch,
            task: tokio::spawn(async move { owner.connect_started(epoch).await }),
        });
        Ok(())
    }

    /// Build the started loop's first client, retrying under the connection
    /// backoff until it connects or stopping is requested.
    async fn connect_started(self: Arc<Self>, epoch: u64) {
        let superseded = || self.epoch.load(Ordering::SeqCst) != epoch;
        let mut failures = 0;
        loop {
            // Register before I/O: stop may arrive while the request is in flight.
            let stopped = self.stopped.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            let mut live = self.live.lock().await;
            if superseded() {
                break;
            }
            match self.connect(&mut live).await {
                Ok(()) if superseded() => {
                    // Stopping releases the client this attempt built.
                    let released = self.release(&mut live).await;
                    let _ = self.publish_released(released);
                    return;
                }
                Ok(()) => {
                    if let Connection::Active(connected) = &*live {
                        if let Err(error) = connected.sync_loop.start() {
                            self.publications.publish(SyncStatus::Failed {
                                error: error.into(),
                            });
                        }
                    }
                    return;
                }
                Err(error) => {
                    drop(live);
                    if superseded() {
                        break;
                    }
                    self.publications.publish(SyncStatus::Failed {
                        error: error.into(),
                    });
                    let wait = coven_storage::retry_delay(failures);
                    failures = failures.saturating_add(1);
                    tokio::select! {
                        () = self.clock.sleep(wait) => {}
                        () = stopped => {}
                    }
                }
            }
        }
        let _ = self.publish_released(Ok(()));
    }

    /// Stop after the active pass and transfers, then release the provider
    /// client; keys, credentials and location remain (E5).
    pub(crate) fn stop(self: &Arc<Self>) {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
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
            // No caller waits for stopping; the status carries a failure.
            let _ = owner.publish_released(released);
        });
    }

    /// Request a pass while started.
    pub(crate) fn sync_now(&self) {
        if let Some(control) = self.control.lock().expect("loop control poisoned").as_ref() {
            // A closed loop has nothing to sync; status reports why.
            let _ = control.sync_now();
        }
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<SyncStatus> {
        self.publications.state.subscribe_sync()
    }

    pub(crate) fn subscribe_join_requests(
        &self,
    ) -> tokio::sync::watch::Receiver<Vec<coven_sync::JoinRequest>> {
        self.publications.state.subscribe_join_requests()
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
