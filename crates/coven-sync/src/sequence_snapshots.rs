//! Snapshot requests follow the sealed slots a new device would replay, and
//! missing retention coverage.
use crate::{
    object_input::ObjectInput,
    sequence_sync::sequence_operations::{Request, SequenceOperation},
    sequence_sync::snapshot_capture::SnapshotRequest,
    sequence_sync::{SequenceSync, Session},
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_format::sequence::Purpose;
use coven_merge::Audience;
use std::collections::BTreeSet;

/// The sealed slot bytes that request an audience's first snapshot (§16.2).
const FIRST_SNAPSHOT_BYTES: u64 = 1024 * 1024;
/// k, in percent: after a snapshot of S bytes, the next is requested once
/// the sealed slot bytes since it reach k × S (§16.2). It trades upload work
/// against restore work at every library size: snapshots cost 1/k of what is
/// written, and a new device replays at most k × S of slots after loading
/// one. The two balance at k = a / b, a being what installing a snapshot
/// costs per byte and b what replaying slots costs per byte; coven's
/// `snapshot_and_replay_cost_per_byte` test measures both. 100 until b is
/// measured again.
pub(crate) const SNAPSHOT_GAP_PERCENT: u64 = 100;

/// The journal kind a breaking migration commits with its schema (§18.3).
const SCHEMA_RAISE: &str = "schema-raise";

impl SequenceSync {
    /// Supply to `DatabaseBuilder::migration_operation` at the composition
    /// root: the breaking migration's version commits with its schema, and
    /// passes raise each audience it captured to that version (§18.3).
    pub fn migration_operation(
        version: u32,
    ) -> Result<coven_database::NewOperation, coven_database::DbError> {
        Ok(coven_database::NewOperation {
            kind: SCHEMA_RAISE.into(),
            data: serde_json::to_vec(&version)
                .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))?,
            started_by: "coven".into(),
            pauses_writes: false,
        })
    }

    /// Raise every captured audience to its migration's version through the
    /// operation path, then finish the migration's journal. An audience
    /// another device already raised to that version keeps no capture.
    pub(crate) async fn publish_raises(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let records: Vec<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == SCHEMA_RAISE)
            .collect();
        if records.is_empty() || !self.registered(session) {
            return Ok(());
        }
        let pending: BTreeSet<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == "sequence")
            .map(|record| serde_json::from_slice::<SequenceOperation>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|work| match work.request {
                Request::Snapshot(request) if matches!(request.purpose, Purpose::Raise(_)) => {
                    Some(request.audience)
                }
                _ => None,
            })
            .collect();
        for capture in self.database.raise_captures().await? {
            if session
                .history
                .state
                .schema
                .get(&capture.audience)
                .is_some_and(|current| current.number >= capture.version)
            {
                self.database
                    .discard_raise_capture(capture.audience, capture.version)
                    .await?;
                continue;
            }
            if pending.contains(&capture.audience) {
                continue;
            }
            // The raise's snapshot covers the captured writes and the
            // marker, so it starts once they have landed. A write a bound
            // upload holds keeps it waiting until the file is stored, and
            // the upload's own record says why (§17.4, §18.5).
            if self
                .database
                .unconfirmed_own_prefix(capture.through)
                .await?
                .is_some()
            {
                continue;
            }
            let result = self
                .start_request(
                    session,
                    ring,
                    member,
                    Request::Snapshot(SnapshotRequest {
                        audience: capture.audience.clone(),
                        purpose: Purpose::Raise(capture.version),
                    }),
                    "coven",
                )
                .await
                .map(drop);
            self.own_work(result, |request| {
                matches!(request, Request::Snapshot(raise) if raise.audience == capture.audience)
            })
            .await?;
        }
        if self.database.raise_captures().await?.is_empty() {
            for record in records {
                self.database.finish_operation(record.id).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn request_snapshots(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if !session
            .history
            .state
            .devices
            .get(&self.device)
            .is_some_and(|device| device.active())
            || self.database.current_claim().await?.is_some()
        {
            return Ok(());
        }
        let pending: BTreeSet<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == "sequence")
            .map(|record| serde_json::from_slice::<SequenceOperation>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|work| match work.request {
                Request::Snapshot(request) => Some(request.audience),
                _ => None,
            })
            .collect();
        let audiences: Vec<_> = std::iter::once(Audience::Store)
            .chain(
                session
                    .history
                    .state
                    .readable_circles(&member.member())
                    .into_iter()
                    .map(Audience::Circle),
            )
            .collect();
        for audience in audiences {
            if pending.contains(&audience) {
                continue;
            }
            let previous = session
                .history
                .announcements
                .iter()
                .rev()
                .find(|(_, snapshot)| {
                    snapshot.snapshot.audience == audience && session.history.usable(snapshot)
                });
            // After the first, a snapshot follows k times as many bytes of
            // slots as the last one holds, at any size.
            let (covers, threshold) = match previous {
                Some((_, snapshot)) => {
                    let path = crate::snapshot_object::path(self.store(), &snapshot.snapshot)?;
                    (
                        snapshot.covers,
                        self.immutable_metadata(&path)
                            .await?
                            .size
                            .saturating_mul(SNAPSHOT_GAP_PERCENT)
                            / 100,
                    )
                }
                None => (0, FIRST_SNAPSHOT_BYTES),
            };
            let growth = self
                .database
                .snapshot_slot_bytes(audience.clone(), covers)
                .await?;
            let next = covers
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?
                .max(
                    session
                        .floor
                        .as_ref()
                        .map_or(1, |floor| floor.header.first.0),
                );
            // History older than grace also asks for coverage, so retention can
            // delete it even where the slots never reach the threshold: a
            // store of few small writes may stay below it.
            // Age comes from the exact slot observation already made by reading
            // or claiming it. An idle pass never probes metadata to find work.
            let aged = if next <= session.through {
                match ObjectInput::retained(&self.database, &self.path(next)?).await? {
                    Some(input) => self.past_grace(input.object()),
                    None => false,
                }
            } else {
                false
            };
            if growth >= threshold || aged {
                let result = self
                    .start_request(
                        session,
                        ring,
                        member,
                        Request::Snapshot(SnapshotRequest {
                            audience: audience.clone(),
                            purpose: Purpose::Ordinary,
                        }),
                        "coven",
                    )
                    .await
                    .map(drop);
                self.own_work(result, |request| {
                    matches!(request, Request::Snapshot(snapshot) if snapshot.audience == audience)
                })
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_snapshots_tests.rs"]
mod tests;
