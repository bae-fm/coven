//! What the sync state is computed from and how (§19.1, api.md E5): the facts
//! the database reads (`Facts`), and the one candidate state each names, as
//! `state` of the Lean model in `spec/proofs/io/CovenIO/SyncState.lean`
//! computes it. `sync_status.rs` holds the state's types and what each failure
//! and pending reason names.

use super::{named_provider, Named, SyncActivity, SyncCause, SyncState, SyncWait};
use crate::{LastPass, PendingReason, PendingRecord, PendingSubject, StatusFailure};
use coven_database::{DbError, PassMove, RecordedLocation, RetrySync};
use coven_storage::CloudProvider;
use std::time::SystemTime;

/// Where one upload in this device's queue stands (§17.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// An attempt is sending it now.
    Moving,
    /// Storage holds its bytes; it waits for the rest of its move.
    Stored,
    /// Queued, and no attempt is sending it.
    Queued,
}

/// One upload in this device's queue.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Upload {
    pub(crate) progress: Progress,
    /// A bound reference's, which holds the writes behind it and which the
    /// pause doesn't hold (§17.4). The state reads bound uploads only.
    pub(crate) bound: bool,
    /// Its last attempt failed, and it retries then (§6.7).
    pub(crate) retry_at: Option<SystemTime>,
    /// Its own pending record, if it has one.
    pub(crate) record: Option<PendingRecord>,
}

/// What the state is computed from, as the model's `Facts` names it; the
/// time of the last finished sync is read beside it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Facts {
    pub(crate) location: RecordedLocation<CloudProvider>,
    /// How the last pass ended; none until a pass ends here, which counts
    /// as synced, since setup, joining and restoring read the store to its
    /// end.
    pub(crate) last: Option<LastPass>,
    /// What the running pass moved last, a slot it read that it hadn't or
    /// one it claimed; none while it has moved nothing.
    pub(crate) moved: Option<PassMove>,
    /// The bound uploads that the state can show, in queue order: the first
    /// one sending, the first queued one with no record, and every one with
    /// a record. The rest change no candidate: a later one of a rank never
    /// shows over the first, and a stored one names none. The database
    /// reads no trailing upload for the state (`trailing_keeps_state`).
    pub(crate) uploads: Vec<Upload>,
    /// The upload queue holds a bound upload with bytes to send, whether
    /// `uploads` lists it or not.
    pub(crate) queued: bool,
    /// The send queue holds a write.
    pub(crate) unsent: bool,
    /// The pending records the state reads, in the order they arose, but
    /// the connection's, which is the last pass's failure kept for its
    /// retry. The database leaves out those about a trailing upload, a
    /// download or a pin coven keeps.
    pub(crate) records: Vec<PendingRecord>,
    /// Whether a started connection will plan, and the cooldown holding
    /// every request.
    pub(crate) sync: RetrySync,
    /// The app paused trailing uploads.
    pub(crate) paused: bool,
    /// The app moves files only on unmetered networks.
    pub(crate) unmetered_only: bool,
    /// The store's watch says the device's network is metered.
    pub(crate) metered: bool,
    /// A self-replacement of this member's keys pauses writes (§12.3).
    pub(crate) keys_changing: bool,
    /// When the next try of the pass's failure is, if its backoff is armed:
    /// the sync scope's, or, for coven's own operation, the operation's.
    pub(crate) pass_retry: Option<SystemTime>,
}

/// A state that may show, its cause made only for the one that does.
enum Candidate<'a> {
    State(SyncState),
    Named(Named, &'a PendingRecord),
}

impl Candidate<'_> {
    fn rank(&self) -> u8 {
        match self {
            Self::State(state) => state.rank(),
            Self::Named(named, _) => named.rank(),
        }
    }

    fn state(self, provider: CloudProvider) -> SyncState {
        match self {
            Self::State(state) => state,
            Self::Named(named, record) => named.state(SyncCause::Pending(record.clone()), provider),
        }
    }
}

impl Facts {
    /// A write or an upload waits to go out.
    fn unsent_work(&self) -> bool {
        self.unsent || self.queued
    }

    /// What holds every transfer, the pass's, each upload's and a queued
    /// write's alike: sync stopped, a provider's cooldown, or a slot that
    /// can't apply yet (the model's `Facts.heldAll`).
    pub(crate) fn held_all(&self) -> Option<SyncWait> {
        match self.sync {
            RetrySync::Stopped => Some(SyncWait::NotSyncing),
            RetrySync::Started { cooling_until: Some(until) } => {
                Some(SyncWait::CoolingDown { until })
            }
            RetrySync::Started { cooling_until: None } => match &self.last {
                // A slot's failure that clears by itself retries instead, and a
                // newer floor is the next pass's turn: neither is a wait.
                Some(LastPass::Blocked { reason, .. }) => reason.slot_wait(),
                _ => None,
            },
        }
    }

    /// The key the first write in the send queue is recorded as waiting for,
    /// when it can't seal it (the model's `Facts.writeKey`).
    pub(super) fn write_key(&self) -> Option<SyncWait> {
        self.records.iter().find_map(|record| match (&record.subject, &record.reason) {
            (PendingSubject::Write(_), PendingReason::KeyUnavailable { audience, key }) => {
                Some(SyncWait::KeyCopy { audience: audience.clone(), key: *key })
            }
            _ => None,
        })
    }

    /// What the next pass waits for: what holds every transfer, else the
    /// backoff of a failure that clears by itself; with neither it waits for
    /// nothing, and its turn has come. A queued write waits for the same (the
    /// model's `Facts.passWait`).
    fn pass_wait(&self) -> Option<SyncWait> {
        match (self.held_all(), self.pass_retry) {
            (Some(held), _) => Some(held),
            (None, Some(at)) if self.last.as_ref().is_some_and(LastPass::retries) => {
                Some(SyncWait::Retrying { at })
            }
            (None, _) => None,
        }
    }

    /// What one queued bound upload waits for: what holds every transfer
    /// first, then its backoff; with neither its turn has come (the model's
    /// `Facts.uploadWait`).
    fn upload_wait(&self, upload: &Upload) -> Option<SyncWait> {
        self.held_all().or_else(|| upload.retry_at.map(|at| SyncWait::Retrying { at }))
    }

    /// What a bound upload names: syncing while it moves; once queued, what
    /// its record names, else waiting to sync while something holds it or it
    /// retries, else syncing, for its turn; nothing once stored.
    fn upload<'a>(&self, upload: &'a Upload) -> Option<Candidate<'a>> {
        match upload.progress {
            Progress::Moving => {
                Some(Candidate::State(SyncState::Syncing(SyncActivity::UploadingBoundFile)))
            }
            Progress::Stored => None,
            Progress::Queued => Some(
                match upload
                    .record
                    .as_ref()
                    .and_then(|record| Some((record.reason.names()?, record)))
                {
                    Some((named, record)) => Candidate::Named(named, record),
                    None => Candidate::State(self.upload_wait(upload).map_or(
                        SyncState::Syncing(SyncActivity::UploadingBoundFile),
                        SyncState::WaitingToSync,
                    )),
                },
            ),
        }
    }

    /// The state shown: not synced with no location recorded, else the
    /// highest-ranked candidate. Among candidates of one state the first
    /// shows, so the cause is the last pass, or else the first record, in
    /// the order they arose, that names the state, and waiting to sync names
    /// the held-back pass's wait, else the first waiting upload's, else a
    /// queued write's. Signed out with no credentials comes first of all,
    /// with no cause.
    pub(crate) fn state(&self) -> SyncState {
        let (provider, credentials) = match self.location {
            RecordedLocation::None => return SyncState::NotSynced,
            RecordedLocation::SignedOut(provider) => (provider, false),
            RecordedLocation::Connected(provider) => (provider, true),
        };
        let signed_out = (!credentials)
            .then_some(Candidate::State(SyncState::SignedOut { provider, cause: None }));
        // What the pass moved comes first among the syncing candidates. The
        // next pass's turn, which shows syncing only for want of anything
        // else to do, comes after the bound files and the writes (the model's
        // `Facts.activities`).
        let (looping, turn) = match (self.moved, &self.last) {
            (Some(moved), _) => (
                Some(Candidate::State(SyncState::Syncing(match moved {
                    PassMove::Read => SyncActivity::Receiving,
                    PassMove::Claimed => SyncActivity::Sending,
                }))),
                None,
            ),
            (None, None) => (Some(Candidate::State(SyncState::Synced)), None),
            (None, Some(last)) => {
                let ended =
                    Candidate::State(last.ended(self.unsent_work(), self.pass_wait(), provider));
                if matches!(ended, Candidate::State(SyncState::Syncing(_))) {
                    (None, Some(ended))
                } else {
                    (Some(ended), None)
                }
            }
        };
        let records = self
            .records
            .iter()
            .filter_map(|record| Some(Candidate::Named(record.reason.names()?, record)));
        let uploads: Vec<Candidate> =
            self.uploads.iter().filter_map(|upload| self.upload(upload)).collect();
        // A write goes out with the next pass and once the files it waits for
        // are stored, so it waits for what holds every pass, else for what
        // holds the first waiting bound file, else it is next in line (the
        // model's `Facts.writes`).
        let file_wait = uploads.iter().find_map(|upload| match upload {
            Candidate::State(SyncState::WaitingToSync(wait)) => Some(wait.clone()),
            Candidate::State(_) | Candidate::Named(..) => None,
        });
        let writes = self.unsent.then(|| {
            Candidate::State(
                self.pass_wait()
                    .or(file_wait)
                    .or_else(|| self.write_key())
                    .map_or(SyncState::Syncing(SyncActivity::Sending), SyncState::WaitingToSync),
            )
        });
        signed_out
            .into_iter()
            .chain(looping)
            .chain(records)
            .chain(uploads)
            .chain(writes)
            .chain(turn)
            .reduce(|shown, next| if next.rank() > shown.rank() { next } else { shown })
            .expect("the loop's candidate, or its turn, is always one")
            .state(provider)
    }

    /// The facts as the database read them, and when sync last finished.
    pub(crate) fn read(
        facts: coven_database::SyncFacts<PendingRecord>,
    ) -> Result<(Self, Option<SystemTime>), DbError> {
        let row = facts.row;
        let location = match row.location {
            RecordedLocation::None => RecordedLocation::None,
            RecordedLocation::SignedOut(name) => {
                RecordedLocation::SignedOut(named_provider(&name)?)
            }
            RecordedLocation::Connected(name) => {
                RecordedLocation::Connected(named_provider(&name)?)
            }
        };
        let last = row
            .last_pass
            .map(|bytes| serde_json::from_slice(&bytes).map_err(DbError::StateEncoding))
            .transpose()?;
        let pass_retry = match last.as_ref() {
            Some(LastPass::Failed { failure: StatusFailure::Operation(record), .. }) => {
                match record.subject {
                    crate::PendingSubject::Operation { id, .. } => facts
                        .operation_retries
                        .iter()
                        .find_map(|(operation, at)| (*operation == id).then_some(*at)),
                    _ => facts.pass_retry,
                }
            }
            _ => facts.pass_retry,
        };
        let uploads = facts
            .uploads
            .into_iter()
            .map(|upload| Upload {
                progress: if upload.sending {
                    Progress::Moving
                } else if upload.stored {
                    Progress::Stored
                } else {
                    Progress::Queued
                },
                bound: true,
                retry_at: upload.retry_at,
                record: upload.record,
            })
            .collect();
        let facts = Self {
            location,
            last,
            moved: row.moved,
            uploads,
            queued: facts.queued,
            unsent: facts.unsent,
            records: facts
                .records
                .into_iter()
                .filter(|record| record.subject != crate::PendingSubject::Connection)
                .collect(),
            sync: facts.retry.sync,
            paused: facts.paused,
            unmetered_only: facts.unmetered_only,
            metered: facts.metered,
            keys_changing: facts.keys_changing,
            pass_retry,
        };
        Ok((facts, row.last_synced))
    }
}

#[cfg(test)]
#[path = "sync_status_facts_tests.rs"]
mod tests;
