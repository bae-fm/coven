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
use coven_database::operation_kind;
use coven_format::sequence::Purpose;
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

impl SequenceSync {
    pub(crate) async fn request_snapshots(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if !session.history.state.devices.get(&self.device).is_some_and(|device| device.active())
            || self.database.current_claim().await?.is_some()
        {
            return Ok(());
        }
        let pending: BTreeSet<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == operation_kind::SEQUENCE)
            .map(|record| serde_json::from_slice::<SequenceOperation>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|work| match work.request {
                Request::Snapshot(request) => Some(request.audience),
                _ => None,
            })
            .collect();
        // An audience that hasn't reached this device's newest breaking
        // migration, as while its own raise waits for its captured writes,
        // gets no ordinary snapshot: it would carry this device's newer schema
        // version before the history holds it, and its announcement would not
        // count (§16.2, §16.3).
        let breaking = self.database.breaking_version().await?;
        for audience in session.history.state.readable_audiences(&member.member()) {
            if pending.contains(&audience) || session.history.state.reached(&audience) < breaking {
                continue;
            }
            let previous = session.history.announcements.iter().rev().find(|(_, snapshot)| {
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
            let growth = self.database.snapshot_slot_bytes(audience.clone(), covers).await?;
            let next = covers
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?
                .max(session.floor.as_ref().map_or(1, |floor| floor.header.first.0));
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
                        operation_kind::COVEN,
                    )
                    .await
                    .and_then(crate::operation_outcome::into_slot)
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
