//! An invite's expiry, a write like approving, declining or cancelling it
//! (§13.2). The handle that holds the store's writer writes it, with or
//! without a storage connection: opening writes each expiry already past,
//! and the handle then wakes when this device's wall clock reaches the
//! earliest open invite's expiry, by running or by being set past it, and
//! writes it. A clock that can't wait for it fails the handle. The write
//! ends the invite in the journal, which takes it out of `coven_invites`.
//! Deleting its request and taking back its access is the ended invite's
//! own work, which a pass does once storage is connected, as for an invite
//! ended any other way.

use super::InviteWork;
use crate::{RetryWaits, SequenceState};
use coven_database::operation_kind;
use coven_database::{Database, DbError, OperationId};
use coven_foundation::clock::ClockRef;
use std::{sync::Arc, time::SystemTime};
use tokio::sync::watch;

/// Ends this device's invites as their lifetimes end, from the store's
/// opening until its handle closes. Dropping it stops the wakes too. A
/// panic in it goes to the store's bug sink and ends the wakes, as does a
/// bug a write returns; the next open writes the expiries that passed.
pub struct InviteExpiry {
    stop: watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<coven_foundation::task::OwnedTask>>,
    /// Each time the wakes have written every expiry due and wait.
    #[cfg(any(test, feature = "test-utils"))]
    parks: crate::Parks,
}

impl InviteExpiry {
    /// End each invite whose lifetime ended while the store was closed, as
    /// it opens, before any app reads its invites. A failed write fails the
    /// handle with its cause.
    pub async fn opened(database: &Database, now: SystemTime) -> Result<(), DbError> {
        write(database, now).await.map(drop)
    }

    /// Wake at each invite's expiry and write it, until closed. A new
    /// invite brings its expiry into the wakes as it is made.
    pub fn start(
        database: Database,
        clock: ClockRef,
        waits: Arc<RetryWaits>,
        state: Arc<SequenceState>,
    ) -> Self {
        let (stop, stopping) = watch::channel(false);
        let bugs = database.bugs().clone();
        let parks = crate::Parks::new();
        #[cfg(any(test, feature = "test-utils"))]
        let expiry_parks = parks.clone();
        let task = coven_foundation::task::spawn_owned(
            "invite expiry",
            bugs,
            run(database, clock, waits, state, stopping, parks),
            |_| async {},
        );
        Self {
            stop,
            task: tokio::sync::Mutex::new(Some(task)),
            #[cfg(any(test, feature = "test-utils"))]
            parks: expiry_parks,
        }
    }

    /// Each time the wakes have written every expiry the clock has reached,
    /// and wait for the next.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn parked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.parks.parked_from_now()
    }

    /// Stop waking, once a write under way has finished.
    pub async fn close(&self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.lock().await.take() {
            task.ended().await;
        }
    }
}

async fn run(
    database: Database,
    clock: ClockRef,
    waits: Arc<RetryWaits>,
    state: Arc<SequenceState>,
    mut stopping: watch::Receiver<bool>,
    parks: crate::Parks,
) {
    let _running = parks.running();
    let mut journal = database.started_operations();
    loop {
        let next = match expire(&database, clock.now(), &waits, &state).await {
            Ok(next) => next,
            // The failed write has failed the handle, which every call and
            // live query reports, `coven_invites`' included; a bug in it
            // reaches nobody else.
            Err(error) => {
                if !coven_foundation::task::report_carried(database.bugs().as_ref(), &error) {
                    tracing::debug!("invite expiry stopped on a failed handle: {error}");
                }
                return;
            }
        };
        // The clock says when it reaches the expiry, by running or by being
        // set past it; a clock set back moves the wake later.
        let wake = match next {
            Some(at) => clock.sleep_until(at),
            None => Box::pin(std::future::pending()),
        };
        tokio::select! {
            biased;
            never = parks.polling() => match never {},
            // Closed, or dropped with its handle.
            _ = stopping.changed() => return,
            // A closed or failed handle leaves nothing to write.
            changed = journal.next() => if changed.is_err() { return },
            woke = wake => if let Err(error) = woke {
                // An invite this handle can't end in time fails the handle,
                // which every call and live query then reports.
                database.fail_expiry_wait(error);
                return;
            },
            never = parks.parked() => match never {},
        }
    }
}

/// Write the expiry of each invite whose lifetime ended by `now`, and
/// return when the next one's does. Each ended invite's work is due at
/// once, and a pass is asked for to do it.
pub(crate) async fn expire(
    database: &Database,
    now: SystemTime,
    waits: &RetryWaits,
    state: &SequenceState,
) -> Result<Option<SystemTime>, DbError> {
    let (ended, next) = write(database, now).await?;
    for id in &ended {
        waits.operation_advanced(*id);
    }
    if !ended.is_empty() {
        state.request_pass();
    }
    Ok(next)
}

/// End each invite whose lifetime ended by `now`, in one step on the
/// writer with reading them: no other write comes between. Returns the
/// ended invites' operations and the earliest expiry still to come.
async fn write(
    database: &Database,
    now: SystemTime,
) -> Result<(Vec<OperationId>, Option<SystemTime>), DbError> {
    database
        .advance_operations(operation_kind::INVITE, move |records| {
            let mut updates = Vec::new();
            let mut ended = Vec::new();
            let mut next: Option<SystemTime> = None;
            for record in records {
                let mut work: InviteWork =
                    serde_json::from_slice(&record.data).map_err(DbError::StateEncoding)?;
                if work.expired(now) {
                    work.end();
                    updates.push(
                        record.next_step(
                            serde_json::to_vec(&work).map_err(DbError::StateEncoding)?,
                        )?,
                    );
                    ended.push(record.id);
                } else if work.expires() {
                    next = Some(next.map_or(work.expires_at, |at| at.min(work.expires_at)));
                }
            }
            Ok((updates, (ended, next)))
        })
        .await
}

#[cfg(test)]
#[path = "invite_expiry_tests.rs"]
mod tests;
