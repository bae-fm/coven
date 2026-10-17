//! What each waiting operation's wait names, and the evidence that it changed
//! (§3.1, SP waiting; IO `WaitingEvidence`). A wait is due by evidence only
//! when what it names changes: its rows commit outside the attempt that ended
//! in it, the key it lacks arrives, or the storage connection it lacks changes.
//! Nothing else wakes it, so a wait whose prerequisite never changes keeps
//! §3.1's unchanged-wait rate (`unchanged_rate`).

use crate::{PendingReason, PendingRecord, Prerequisite, Retry, RetryWaits, SyncError};
use coven_database::{
    AttemptCommits, CommitScope, DatabaseChanges, DbError, OperationId, PrerequisiteRows,
    RetrySubject,
};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct Evidence {
    /// Counts storage connection changes.
    connection: u64,
    waits: BTreeMap<OperationId, Wait>,
}

enum Wait {
    /// An attempt runs in `scope`. Other work's commits since it began are
    /// kept until it names what it waits on.
    Attempt {
        scope: CommitScope,
        commits: AttemptCommits,
    },
    /// The wait an attempt ended in.
    Waiting {
        rows: DatabaseChanges,
        /// The key it lacks; whether this device held it when the wait
        /// began, unknown for a wait armed on opening.
        key: Option<(Audience, KeyId, Option<bool>)>,
        /// The connection count when a wait for storage began.
        connection: Option<u64>,
        woken: bool,
    },
}

/// What a wait for `reason` names (IO `Armed.prereq`).
#[derive(Default)]
struct Needs {
    rows: Vec<PrerequisiteRows>,
    key: Option<(Audience, KeyId)>,
    connection: bool,
}

fn object_rows(path: &ObjectPath) -> Vec<PrerequisiteRows> {
    if path.slot_number().is_some() {
        vec![PrerequisiteRows::Slot]
    } else {
        Vec::new()
    }
}

/// What a wait for `reason` watches; a reload wait also names the pending
/// reload operation, which advances or finishes (IO `PrerequisiteRows.needs`).
fn needs(reason: &PendingReason, reload: Option<OperationId>) -> Needs {
    let rows = |rows| Needs {
        rows,
        ..Needs::default()
    };
    let mut needs = match reason {
        PendingReason::Missing { path } => rows(object_rows(path)),
        PendingReason::Waits(prerequisite) => match prerequisite {
            Prerequisite::SequenceCatchUp => rows(vec![PrerequisiteRows::Slot]),
            Prerequisite::Object(path) => rows(object_rows(path)),
            Prerequisite::DeviceRegistration(device) => {
                rows(vec![PrerequisiteRows::Device(*device)])
            }
            Prerequisite::SchemaRaise { .. } => rows(vec![PrerequisiteRows::Boundaries]),
            Prerequisite::Snapshot(_) => Needs::default(),
            Prerequisite::Reload(audience) => {
                rows(vec![PrerequisiteRows::Reload(audience.clone())])
            }
        },
        PendingReason::KeyUnavailable { audience, key } => Needs {
            key: Some((audience.clone(), *key)),
            ..Needs::default()
        },
        PendingReason::ProviderPending { account, .. }
        | PendingReason::PendingOwner { account }
        | PendingReason::AccountInUse { account } => {
            rows(vec![PrerequisiteRows::ProviderAccess(account.clone())])
        }
        PendingReason::Storage { .. } | PendingReason::NoStorage => Needs {
            connection: true,
            ..Needs::default()
        },
        // Every other condition waits for time, the app or an update.
        _ => Needs::default(),
    };
    needs.rows.extend(reload.map(PrerequisiteRows::Operation));
    needs
}

impl RetryWaits {
    fn evidence(&self) -> std::sync::MutexGuard<'_, Evidence> {
        self.evidence.lock().expect("operation evidence poisoned")
    }

    /// Opening watches every persisted operation wait from now: commits after
    /// the store opened, by any work, are evidence for it. One satisfied
    /// before is unseen, and the wait is due within its persisted full wait
    /// (IO `reopened_due`, `reopen_rate`).
    pub(crate) async fn watch_persisted(&self) -> Result<(), DbError> {
        let Some(database) = &self.database else {
            return Ok(());
        };
        for (subject, _) in database.retry_waits().await? {
            let RetrySubject::Operation(id) = subject else {
                continue;
            };
            let Some(pending) = database.operation_pending::<PendingRecord>(id).await? else {
                continue;
            };
            if pending.retry() == Retry::Automatic {
                let reload = match pending.reason {
                    PendingReason::Waits(Prerequisite::Reload(_)) => database
                        .first_operation("sequence")
                        .await?
                        .map(|record| record.id),
                    _ => None,
                };
                self.arm_evidence(id, &pending.reason, None, reload);
            }
        }
        Ok(())
    }

    /// Begin an attempt at the operation: commits the attempt makes belong to
    /// the returned scope and are no evidence against the wait it ends in.
    pub(crate) fn begin_attempt(&self, id: OperationId) -> CommitScope {
        let scope = CommitScope::new();
        if let Some(database) = &self.database {
            let commits = database.attempt_commits(scope);
            self.evidence()
                .waits
                .insert(id, Wait::Attempt { scope, commits });
        }
        scope
    }

    /// The scope of the operation's running attempt, or a fresh one.
    pub(crate) fn attempt_scope(&self, id: OperationId) -> CommitScope {
        match self.evidence().waits.get(&id) {
            Some(Wait::Attempt { scope, .. }) => *scope,
            _ => CommitScope::new(),
        }
    }

    /// The attempt ended waiting on `reason`: watch what that names. Other
    /// work's commits to it while the attempt ran wake the wait at once (IO
    /// `racing_commit_wakes`). `held` says whether this device holds a key
    /// the reason lacks; `reload` is the reload operation a reload wait names.
    pub(crate) fn arm_evidence(
        &self,
        id: OperationId,
        reason: &PendingReason,
        held: Option<bool>,
        reload: Option<OperationId>,
    ) {
        let Some(database) = &self.database else {
            return;
        };
        let needs = needs(reason, reload);
        let mut evidence = self.evidence();
        let commits = match evidence.waits.remove(&id) {
            Some(Wait::Attempt { commits, .. }) => commits,
            _ => database.attempt_commits(CommitScope::new()),
        };
        let connection = needs.connection.then_some(evidence.connection);
        evidence.waits.insert(
            id,
            Wait::Waiting {
                rows: commits.watch(&needs.rows),
                key: needs.key.map(|(audience, key)| (audience, key, held)),
                connection,
                woken: false,
            },
        );
    }

    /// What a wait for `reason` watches: its prerequisites' rows, the key it
    /// lacks, and whether it waits for the storage connection; `reload` is the
    /// reload operation a reload wait names (IO `PrerequisiteRows.needs`).
    pub(crate) fn watches(
        reason: &PendingReason,
        reload: Option<OperationId>,
    ) -> (Vec<PrerequisiteRows>, Option<(Audience, KeyId)>, bool) {
        let needs = needs(reason, reload);
        (needs.rows, needs.key, needs.connection)
    }

    /// The key the operation's wait lacks, if it lacks one.
    pub(crate) fn lacks_key(reason: &PendingReason) -> Option<(Audience, KeyId)> {
        Self::watches(reason, None).1
    }

    pub(crate) fn clear_evidence(&self, id: OperationId) {
        self.evidence().waits.remove(&id);
    }

    pub(crate) fn retain_evidence(&self, live: &BTreeSet<OperationId>) {
        self.evidence().waits.retain(|id, _| live.contains(id));
    }

    /// Whether what the operation's wait names changed since it began: its
    /// rows, the key it lacks (`holds` reads this device's keys), or the
    /// connection. It stays woken until the next attempt begins.
    pub(crate) fn evidence_woken(
        &self,
        id: OperationId,
        holds: impl Fn(&Audience, KeyId) -> Result<bool, SyncError>,
    ) -> Result<bool, SyncError> {
        let mut evidence = self.evidence();
        let connection = evidence.connection;
        let Some(Wait::Waiting {
            rows,
            key,
            connection: began,
            woken,
        }) = evidence.waits.get_mut(&id)
        else {
            return Ok(false);
        };
        *woken |= rows.take_changed()?;
        if let Some((audience, key, held)) = key {
            if *held != Some(true) {
                *woken |= holds(audience, *key)?;
            }
        }
        *woken |= began.is_some_and(|began| began != connection);
        Ok(*woken)
    }

    /// The storage connection changed: waits for storage are woken.
    pub fn connection_changed(&self) {
        self.evidence().connection += 1;
        self.rearmed
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// The credential/connection version a connection failure waits to change.
    pub fn connection_version(&self) -> u64 {
        self.evidence().connection
    }

    /// Wakes for waits not yet woken, when their rows commit.
    pub(crate) fn evidence_wakes(&self) -> Vec<crate::retry_waits::Wake> {
        self.evidence()
            .waits
            .values()
            .filter_map(|wait| match wait {
                Wait::Waiting {
                    rows, woken: false, ..
                } => Some(Box::pin(rows.changed()) as crate::retry_waits::Wake),
                _ => None,
            })
            .collect()
    }
}
