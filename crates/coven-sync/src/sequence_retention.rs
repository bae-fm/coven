//! A confirmed conditional floor write authorizes each journaled deletion.
use crate::sequence_sync::OperationState;
use crate::{
    sequence_sync::{next_slot, SequenceSync, Session},
    OperationKind, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::operation_kind;
use coven_database::{NewOperation, OperationRecord};
use coven_format::sequence::Purpose;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Slot};
use coven_storage::{ObjectPath, StorageFailure, StoredObject};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

#[path = "floor_capture.rs"]
mod floor_capture;
use floor_capture::{FloorArtifact, FloorCapture};
#[path = "floor_upload.rs"]
mod floor_upload;
#[path = "retention_maturity.rs"]
mod maturity;

pub(crate) const GRACE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Serialize, Deserialize)]
enum Retention {
    Snapshot(
        #[serde(with = "crate::pending_encoding::snapshot_id")] coven_format::admin::SnapshotId,
    ),
    /// Clear what a device gone for good left besides its files: end its
    /// uploads, then list its snapshot folders, deleting every body there
    /// that no announcement names.
    GoneListing(DeviceId),
    Capture(FloorCapture),
    Floor(FloorArtifact),
    Delete {
        #[serde(with = "coven_database::slot_json")]
        next: Slot,
        #[serde(with = "coven_database::slot_json")]
        first: Slot,
    },
}
impl OperationState for Retention {}

impl SequenceSync {
    /// New floor work does not wait for an earlier floor's failed deletions.
    pub(crate) async fn retain_sequence(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if !session.history.state.devices.get(&self.device).is_some_and(|device| device.active()) {
            return Ok(());
        }
        let records = self.database.operations().await?;
        let pending_floor = records
            .iter()
            .filter(|record| record.kind == operation_kind::SEQUENCE_RETENTION)
            .map(|record| serde_json::from_slice::<Retention>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|work| matches!(work, Retention::Capture(_) | Retention::Floor(_)));
        if !pending_floor {
            if let Some(proposal) = self.propose_floor(session, ring).await? {
                self.database
                    .start_operation(NewOperation {
                        kind: operation_kind::SEQUENCE_RETENTION.into(),
                        data: serde_json::to_vec(&proposal)?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    })
                    .await?;
            }
        }
        self.resume_retention(session, ring, member).await
    }

    async fn resume_retention(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != operation_kind::SEQUENCE_RETENTION
                || !self.operation_ready(&record, ring).await?
            {
                continue;
            }
            let scope = self.waits.begin_attempt(record.id);
            let result =
                scope.run(self.apply_retention(session, ring, member, record.clone())).await;
            self.settle_resumed(record.id, OperationKind::RetainSequence, result, ring).await?;
        }
        Ok(())
    }

    /// Delete the snapshot bodies of this device, and of each device gone
    /// for good whose files it deletes (§17.10), that nothing still loads:
    /// not a floor or pending floor snapshot, not the body of a raise or
    /// reset applied after the floor's coverage, and replaced by a newer
    /// usable announcement covering at least as much whose slot has passed
    /// grace. A gone device's folders are listed once, too, for bodies no
    /// announcement names: it never runs the undo that would delete them.
    pub(crate) async fn retain_snapshots(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if !session.history.state.devices.get(&self.device).is_some_and(|device| device.active()) {
            return Ok(());
        }
        let history = &session.history;
        let first_kept = session.floor.as_ref().map_or(Slot::FIRST, |floor| floor.header.first);
        // The devices gone for good whose files this device deletes: their
        // stored bytes are its to delete, and they never return to delete
        // their own (§16.6).
        let gone: BTreeSet<DeviceId> = history
            .state
            .devices
            .keys()
            .copied()
            .filter(|device| {
                *device != self.device && history.deletes_for(self.device, member.member(), *device)
            })
            .collect();
        let mut kept: BTreeSet<Slot> = session
            .floor
            .as_ref()
            .map(|floor| floor.header.snapshots.iter().copied().collect())
            .unwrap_or_default();
        let mut pending_snapshots = Vec::new();
        let mut pending_listings = BTreeSet::new();
        for record in self.database.operations().await? {
            if record.kind != operation_kind::SEQUENCE_RETENTION {
                continue;
            }
            match serde_json::from_slice(&record.data)? {
                Retention::Capture(capture) => kept.extend(capture.announcements),
                Retention::Floor(artifact) => kept.extend(artifact.capture.announcements),
                Retention::Snapshot(id) => pending_snapshots.push(id),
                Retention::GoneListing(device) => {
                    pending_listings.insert(device);
                }
                Retention::Delete { .. } => (),
            }
        }
        if !gone.is_empty() {
            let listed = self.database.gone_storage_cleared().await?;
            for device in
                gone.difference(&listed).filter(|device| !pending_listings.contains(device))
            {
                self.database
                    .start_operation(NewOperation {
                        kind: operation_kind::SEQUENCE_RETENTION.into(),
                        data: serde_json::to_vec(&Retention::GoneListing(*device))?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    })
                    .await?;
            }
        }
        let floor_covers = |audience: &Audience| {
            session
                .floor
                .as_ref()
                .and_then(|floor| {
                    floor
                        .header
                        .snapshots
                        .iter()
                        .filter_map(|slot| history.announcements.get(slot))
                        .find(|announcement| announcement.snapshot.audience == *audience)
                        .map(|announcement| announcement.covers)
                })
                .unwrap_or(0)
        };
        for (slot, announcement) in &history.announcements {
            let id = &announcement.snapshot;
            if (id.device != self.device && !gone.contains(&id.device))
                || kept.contains(slot)
                || pending_snapshots.contains(id)
            {
                continue;
            }
            if history.boundaries.iter().any(|coverage| {
                coverage.boundary.snapshot() == id
                    && coverage.boundary.entry().get() > floor_covers(&id.audience)
            }) {
                continue;
            }
            if self.database.snapshot_absent(id.clone()).await? {
                continue;
            }
            let replacement = self
                .newest_mature_snapshot(
                    first_kept,
                    history
                        .announcements
                        .range((std::ops::Bound::Excluded(*slot), std::ops::Bound::Unbounded))
                        .rev()
                        .filter(|(_, newer)| {
                            newer.snapshot.audience == id.audience
                                && newer.covers >= announcement.covers
                                && history.usable(newer)
                        }),
                )
                .await?;
            if replacement.is_none() {
                continue;
            }
            self.database
                .start_operation(NewOperation {
                    kind: operation_kind::SEQUENCE_RETENTION.into(),
                    data: serde_json::to_vec(&Retention::Snapshot(id.clone()))?,
                    started_by: operation_kind::COVEN.into(),
                    pauses_writes: false,
                })
                .await?;
        }
        self.resume_retention(session, ring, member).await
    }

    async fn propose_floor(
        &self,
        session: &Session,
        ring: &StoreKeyring,
    ) -> Result<Option<Retention>, SyncError> {
        // Every introduced audience needs coverage, even without data writes:
        // its membership and boundary history also crosses the floor.
        let audiences = session.history.introduced_audiences();
        let previous_first = session.floor.as_ref().map_or(Slot::FIRST, |floor| floor.header.first);
        // Two selections per audience. The floor names the newest applicable
        // snapshot, mature or not, so a new device starts from it at once.
        // `first`, which says what may be deleted, follows only snapshots past
        // grace, so a device away a short time still finds its slots.
        let mut named = Vec::new();
        let mut mature = Vec::new();
        for audience in audiences {
            // The proved floor loader starts from ordinary snapshots. An active
            // preparation is followed by an ordinary capture before retention.
            let candidates = || {
                session.history.announcements.iter().rev().filter(|(_, snapshot)| {
                    snapshot.snapshot.audience == audience
                        && snapshot.purpose == Purpose::Ordinary
                        && session.history.usable(snapshot)
                })
            };
            // The floor names every introduced audience; one without any
            // snapshot leaves the current floor as it is.
            let Some((slot, snapshot)) = candidates().next() else {
                return Ok(None);
            };
            if let Some(floor) = &session.floor {
                if floor
                    .announcements
                    .iter()
                    .filter_map(|original| session.history.announcements.get(&original.slot))
                    .any(|previous| {
                        previous.snapshot.audience == audience && previous.covers > snapshot.covers
                    })
                {
                    return Ok(None);
                }
            }
            named.push(*slot);
            mature.push(
                self.newest_mature_snapshot(previous_first, candidates())
                    .await?
                    .map(|(_, snapshot)| snapshot.covers),
            );
        }
        // Every audience needs a mature snapshot before `first` moves; until
        // then it stays where it is. It never moves back.
        let first = match mature.into_iter().collect::<Option<Vec<_>>>() {
            Some(covers) => {
                next_slot(covers.into_iter().min().expect("store audience"))?.max(previous_first)
            }
            None => previous_first,
        };
        let names_advance =
            session.floor.as_ref().is_none_or(|floor| floor.header.snapshots != named);
        if first == previous_first && !names_advance {
            return Ok(None);
        }

        let key = session.history.metadata_key(ring)?;
        let copies = session.history.copies.iter().filter(|copy|
            matches!(&copy.prefix.kind, coven_format::sealed_slot::SlotKind::KeyCopy(copy) if copy.routing.audience == Audience::Store && copy.routing.key == key))
            .map(|copy| copy.path.slot_number().expect("copy slot")).collect();
        Ok(Some(Retention::Capture(FloorCapture {
            previous: session.floor.as_ref().map(|floor| floor.object().clone()),
            previous_first,
            first,
            announcements: named,
            at: session.through,
            key,
            copies,
            name: format!("floor-{}.sealed", self.ids.new_id()),
        })))
    }

    async fn apply_retention(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        mut record: OperationRecord,
    ) -> Result<(), SyncError> {
        let mut work: Retention = serde_json::from_slice(&record.data)?;
        if let Retention::Snapshot(id) = work {
            self.state.check()?;
            self.storage.delete(&crate::snapshot_object::path(self.store(), &id)?).await?;
            self.database.record_absent_snapshot(id, BTreeSet::new()).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::GoneListing(device) = work {
            self.clear_gone_storage(session, device).await?;
            self.database.record_gone_storage_cleared(device).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::Capture(capture) = &work {
            let artifact = self.capture_floor(capture, ring, member).await?;
            work = Retention::Floor(artifact);
            let update = work.update(&record)?;
            let next = update.last_step;
            self.database.advance_operation(update).await?;
            record.last_step = next;
        }
        if let Retention::Floor(artifact) = &mut work {
            let first = artifact.capture.first;
            let previous_first = artifact.capture.previous_first;
            let previous = artifact.capture.previous.clone();
            self.state.check()?;
            let mut confirmed = None;
            if record.last_step > 1 {
                // A sent conditional request may have committed despite its
                // missing reply. This observation alone authorizes deletion.
                // Another device may have replaced the floor since, past the
                // slots this one has applied: reading forward checks it
                // against the history through its `at`, or reloads from it
                // when its `first` has passed the next slot, whose slots
                // may be deleted (§16.4, §16.6).
                self.read_forward(session, ring, member).await?;
                // A changed floor whose `first` reaches this one's authorizes
                // the deletions. An unchanged one never does: a write that
                // moves only the names keeps the previous `first`.
                let same_revision =
                    session.floor.as_ref().map(|floor| floor.object()) == previous.as_ref();
                if let Some(floor) = session.floor.as_ref().filter(|_| !same_revision) {
                    if floor.header.first >= first {
                        confirmed = Some(floor.header.first);
                    }
                }
                if confirmed.is_none() && !same_revision {
                    self.cleanup_floor(artifact).await?;
                    self.database.finish_operation(record.id).await?;
                    return Ok(());
                }
            }
            if confirmed.is_none() {
                // Record the attempt before a request can publish or create
                // provider state that must be recovered after a lost reply.
                let update = Retention::Floor(artifact.clone()).update(&record)?;
                let next = update.last_step;
                self.database.advance_operation(update).await?;
                record.last_step = next;
                let result = self.upload_floor(&mut record, artifact).await;
                match result {
                    Ok(()) => {
                        confirmed = Some(first);
                        session.floor = None;
                    }
                    Err(error)
                        if matches!(&error, SyncError::Storage(storage) if matches!(
                            storage.own_failure(),
                            Some(StorageFailure::Conflict | StorageFailure::AlreadyExists)
                        )) =>
                    {
                        self.read_forward(session, ring, member).await?;
                        self.cleanup_floor(artifact).await?;
                        self.database.finish_operation(record.id).await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
            self.cleanup_floor(artifact).await?;
            work = Retention::Delete {
                next: previous_first,
                first: confirmed.expect("successful or observed floor"),
            };
            let update = work.update(&record)?;
            record.last_step = update.last_step;
            self.database.advance_operation(update).await?;
        }
        let Retention::Delete { mut next, first } = work else {
            unreachable!("floor settled before deletion");
        };
        while next < first {
            self.state.check()?;
            self.storage.delete(&self.path(next)).await?;
            next = next_slot(next.get())?;
            let update = Retention::Delete { next, first }.update(&record)?;
            record.last_step = update.last_step;
            self.database.advance_operation(update).await?;
        }
        // A claim's session left at a slot this device has applied, or a
        // floor session begun before the floor now published, can never
        // publish, whoever began it, and nothing else would end it (§17.10).
        self.state.check()?;
        self.storage.end_settled_claims(self.store(), session.through).await?;
        self.storage.end_superseded_floor_sessions(self.store()).await?;
        self.database.finish_operation(record.id).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_retention_tests.rs"]
mod tests;
