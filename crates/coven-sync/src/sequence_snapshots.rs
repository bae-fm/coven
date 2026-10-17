//! Snapshot requests follow counted part growth and missing retention coverage.
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

impl SequenceSync {
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
            let (covers, threshold) = match previous {
                Some((_, snapshot)) => {
                    let path = crate::snapshot_object::path(self.store(), &snapshot.snapshot)?;
                    (snapshot.covers, self.immutable_metadata(&path).await?.size)
                }
                None => (0, 1024 * 1024),
            };
            let growth = self
                .database
                .snapshot_part_bytes(audience.clone(), covers)
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
            if growth > threshold || aged {
                self.start_request(
                    session,
                    ring,
                    member,
                    Request::Snapshot(SnapshotRequest {
                        audience,
                        purpose: Purpose::Ordinary,
                    }),
                    "coven",
                )
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_snapshots_tests.rs"]
mod tests;
