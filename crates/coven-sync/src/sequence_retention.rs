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
    /// that no kept announcement names.
    GoneListing(DeviceId),
    /// List this device's own snapshot folders, deleting every body there
    /// that no kept announcement and no open operation names: what its
    /// forgotten announcements alone named (§16.8).
    OwnListing,
    Capture(FloorCapture),
    Floor(FloorArtifact),
    /// A floor body the floor names, of this device or of a device gone for
    /// good whose files this device deletes: delete it once the floor names
    /// another (§16.6).
    LiveBody {
        device: DeviceId,
        #[serde(with = "coven_database::slot_json")]
        number: Slot,
    },
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
        // A floor body is deleted once the floor names another, so those
        // checks follow any floor write this pass makes.
        let mut records = self.database.operations().await?;
        records.sort_by_key(|record| {
            let live = serde_json::from_slice::<Retention>(&record.data)
                .is_ok_and(|work| matches!(work, Retention::LiveBody { .. }));
            (record.kind == operation_kind::SEQUENCE_RETENTION && live, record.id)
        });
        for record in records {
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

    /// Forget the announcements no rule reads any more, then delete the
    /// snapshot bodies of this device, and of each device gone for good
    /// whose files it deletes (§17.10), that nothing still loads: not a
    /// floor or pending floor snapshot, not the body of a raise or reset
    /// applied after the floor's coverage, and replaced by a newer usable
    /// announcement covering at least as much whose slot has passed grace.
    /// A gone device's folders are listed once, too, for bodies no
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
        let first_kept = session.floor.as_ref().map_or(Slot::FIRST, |floor| floor.header.first);
        // The devices gone for good whose files this device deletes: their
        // stored bytes are its to delete, and they never return to delete
        // their own (§16.6).
        let gone: BTreeSet<DeviceId> = session
            .history
            .state
            .devices
            .keys()
            .copied()
            .filter(|device| {
                *device != self.device
                    && session.history.deletes_for(self.device, member.member(), *device)
            })
            .collect();
        let mut kept: BTreeSet<Slot> = session
            .floor
            .as_ref()
            .map(|floor| floor.header.snapshots.iter().copied().collect())
            .unwrap_or_default();
        // What rebuilds of circles below the store's version read: the store
        // snapshot they start from, which is never forgotten, and the bodies
        // of the store's raises and resets they replay (§16.6, §16.8).
        let rebuilds = session.history.rebuild_reads();
        kept.extend(rebuilds.iter().filter_map(|read| read.snapshot.map(|(slot, _)| slot)));
        let replayed: BTreeSet<coven_format::admin::SnapshotId> =
            rebuilds.iter().flat_map(|read| read.replay.iter().cloned()).collect();
        let mut pending_snapshots = Vec::new();
        let mut pending_listings = BTreeSet::new();
        let mut own_listing = false;
        let mut live_bodies = BTreeSet::new();
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
                Retention::OwnListing => own_listing = true,
                Retention::LiveBody { device, number } => {
                    live_bodies.insert((device, number));
                }
                Retention::Delete { .. } => (),
            }
        }
        // A floor body of a device gone for good that the floor names is
        // deleted once the floor names another.
        if let Some((device, number)) = session.floor.as_ref().map(|floor| floor.body()) {
            if gone.contains(&device) && live_bodies.insert((device, number)) {
                self.database
                    .start_operation(NewOperation {
                        kind: operation_kind::SEQUENCE_RETENTION.into(),
                        data: serde_json::to_vec(&Retention::LiveBody { device, number })?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    })
                    .await?;
            }
        }
        if !gone.is_empty() {
            let listed = self.database.gone_storage_cleared().await?;
            let unlisted: Vec<DeviceId> = gone
                .difference(&listed)
                .filter(|device| !pending_listings.contains(device))
                .copied()
                .collect();
            for device in unlisted {
                self.database
                    .start_operation(NewOperation {
                        kind: operation_kind::SEQUENCE_RETENTION.into(),
                        data: serde_json::to_vec(&Retention::GoneListing(device))?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    })
                    .await?;
                pending_listings.insert(device);
            }
        }
        // The floor and a journaled floor write name their snapshots, so
        // those announcements stay, and the walk below keeps their bodies.
        self.forget_announcements(
            session,
            first_kept,
            &kept,
            &gone,
            &pending_listings,
            own_listing,
        )
        .await?;
        let history = &session.history;
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
                || replayed.contains(id)
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

    /// Forget the announcements no rule reads any more (§16.8). Their
    /// original slots go, and the absence recorded for each body only they
    /// named. A body only they named is found by listing its folder, which
    /// is journaled in the same transaction for this device's own and for
    /// each gone device in `gone`, unless one is journaled already
    /// (`own_listing`, `listings`). `named` holds the announcements the
    /// floor and each journaled floor write name, which stay.
    async fn forget_announcements(
        &self,
        session: &mut Session,
        first: Slot,
        named: &BTreeSet<Slot>,
        gone: &BTreeSet<DeviceId>,
        listings: &BTreeSet<DeviceId>,
        own_listing: bool,
    ) -> Result<(), SyncError> {
        let forgotten = session.history.forgettable(first, named);
        // A device whose store waits for its rebuild knows the store's
        // announcements from the floor's snapshot but holds none of their
        // original slots: the rebuild's install brings them (§18.3).
        if forgotten.is_empty()
            || self.database.rebuilds_waiting().await?.contains_key(&Audience::Store)
        {
            return Ok(());
        }
        let history = &session.history;
        let named: BTreeSet<&coven_format::admin::SnapshotId> = history
            .announcements
            .iter()
            .filter(|(slot, _)| !forgotten.contains(slot))
            .map(|(_, announcement)| &announcement.snapshot)
            .collect();
        let bodies: BTreeSet<coven_format::admin::SnapshotId> = forgotten
            .iter()
            .map(|slot| &history.announcements[slot].snapshot)
            .filter(|id| !named.contains(id))
            .cloned()
            .collect();
        let devices: BTreeSet<DeviceId> = bodies.iter().map(|id| id.device).collect();
        let mut work = Vec::new();
        if devices.contains(&self.device) && !own_listing {
            work.push(Retention::OwnListing);
        }
        for device in devices.intersection(gone).filter(|device| !listings.contains(device)) {
            work.push(Retention::GoneListing(*device));
        }
        let listings = work
            .iter()
            .map(|work| {
                Ok(NewOperation {
                    kind: operation_kind::SEQUENCE_RETENTION.into(),
                    data: serde_json::to_vec(work)?,
                    started_by: operation_kind::COVEN.into(),
                    pauses_writes: false,
                })
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        self.database.forget_announcements(forgotten.clone(), bodies, listings).await?;
        session.history.forget(&forgotten);
        Ok(())
    }

    /// The snapshot bodies open operations own, read before a listing in the
    /// same pass: one being captured, uploaded, announced or activated, a
    /// raise or reset waiting for its entry or its undo, and one queued for
    /// deletion. A listing of this device's folders leaves them (§16.8).
    async fn journaled_snapshots(
        &self,
    ) -> Result<BTreeSet<coven_format::admin::SnapshotId>, SyncError> {
        let mut journaled = BTreeSet::new();
        for record in self.database.operations().await? {
            match record.kind.as_str() {
                operation_kind::SEQUENCE => {
                    let work: super::sequence_operations::SequenceOperation =
                        serde_json::from_slice(&record.data)?;
                    journaled.extend(work.snapshot().cloned());
                }
                operation_kind::SEQUENCE_RETENTION => {
                    if let Retention::Snapshot(id) = serde_json::from_slice(&record.data)? {
                        journaled.insert(id);
                    }
                }
                _ => (),
            }
        }
        Ok(journaled)
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
            // The floor names every introduced audience that holds a part or
            // has a snapshot; one of those without a usable snapshot leaves
            // the current floor as it is. One with neither has an empty body
            // that nothing below `first` can hold a part of (§16.4).
            let Some((slot, snapshot)) = candidates().next() else {
                let announced = session
                    .history
                    .announcements
                    .values()
                    .any(|announced| announced.snapshot.audience == audience);
                if audience == Audience::Store
                    || announced
                    || self.database.audience_holds_parts(audience.clone()).await?
                {
                    return Ok(None);
                }
                continue;
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
        // A circle below the store's version is rebuilt from the store's
        // snapshot at its version, so `first` stays at or before the slot
        // after that snapshot's coverage, and never moves back (§16.6).
        let first = session
            .history
            .rebuild_reads()
            .iter()
            .map(|read| read.cap())
            .fold(first, |first, cap| first.min(cap))
            .max(previous_first);
        let names_advance =
            session.floor.as_ref().is_none_or(|floor| floor.header.snapshots != named);
        if first == previous_first && !names_advance {
            return Ok(None);
        }

        let key = session.history.metadata_key(ring)?;
        // The slots carrying copies of the floor's key: key-copy slots and
        // admissions, each once (D9).
        let copies: BTreeSet<Slot> = session
            .history
            .copies
            .iter()
            .filter(|copy| copy.route().audience == Audience::Store && copy.route().key == key)
            .map(|copy| copy.path.slot_number().expect("copy slot"))
            .collect();
        let copies = copies.into_iter().collect();
        let number = Slot::from(self.database.reserve_snapshot_number().await?);
        Ok(Some(Retention::Capture(FloorCapture {
            previous: session.floor.as_ref().map(|floor| floor.object().clone()),
            number,
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
            // Only a kept announcement's body is looked up as absent; one
            // forgotten since its deletion was journaled names no body.
            if session.history.announcements.values().any(|announced| announced.snapshot == id) {
                self.database.record_absent_snapshot(id, BTreeSet::new()).await?;
            }
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::OwnListing = work {
            let journaled = self.journaled_snapshots().await?;
            self.list_own_snapshots(&session.history, &journaled).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::GoneListing(device) = work {
            self.clear_gone_storage(session, device).await?;
            self.database.record_gone_storage_cleared(device).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::LiveBody { device, number } = work {
            // The floor was read by this pass. A body it still names stays; once
            // it names another, nothing can read this one again.
            if session.floor.as_ref().is_some_and(|floor| floor.body() == (device, number)) {
                return Ok(());
            }
            self.state.check()?;
            self.storage
                .delete(&ObjectPath::floor_body(self.store(), device, number.into()))
                .await?;
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
                    self.cleanup_floor(artifact, session.floor.as_ref()).await?;
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
                        // The rest of the pass deletes by the floor storage
                        // now holds: the bodies it names stay, and the slots
                        // below its `first` are gone (§16.6). Reading forward
                        // checks it, and reloads from another device's floor
                        // that has already replaced it past the next slot.
                        self.read_forward(session, ring, member).await?;
                    }
                    Err(error)
                        if matches!(&error, SyncError::Storage(storage) if matches!(
                            storage.own_failure(),
                            Some(StorageFailure::Conflict | StorageFailure::AlreadyExists)
                        )) =>
                    {
                        self.read_forward(session, ring, member).await?;
                        self.cleanup_floor(artifact, session.floor.as_ref()).await?;
                        self.database.finish_operation(record.id).await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
            self.cleanup_floor(artifact, session.floor.as_ref()).await?;
            // The floor names this body until another replaces it, and then
            // this device deletes it.
            if session
                .floor
                .as_ref()
                .is_some_and(|floor| floor.body() == (self.device, artifact.capture.number))
            {
                self.database
                    .start_operation(NewOperation {
                        kind: operation_kind::SEQUENCE_RETENTION.into(),
                        data: serde_json::to_vec(&Retention::LiveBody {
                            device: self.device,
                            number: artifact.capture.number,
                        })?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    })
                    .await?;
            }
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
        // A claim's session left at a slot this device has applied can never
        // publish, whoever began it, and nothing else would end it (§17.10).
        self.state.check()?;
        self.storage.end_settled_claims(self.store(), session.through).await?;
        self.database.finish_operation(record.id).await?;
        Ok(())
    }
}

/// Whether a journaled operation is a floor write in progress.
#[cfg(test)]
pub(crate) fn is_floor_write(record: &OperationRecord) -> bool {
    matches!(
        serde_json::from_slice::<Retention>(&record.data),
        Ok(Retention::Capture(_) | Retention::Floor(_))
    )
}

/// Whether a journaled operation only waits to delete the floor body the
/// floor named when it was journaled.
#[cfg(test)]
pub(crate) fn is_live_body(record: &OperationRecord) -> bool {
    matches!(serde_json::from_slice::<Retention>(&record.data), Ok(Retention::LiveBody { .. }))
}

#[cfg(test)]
#[path = "sequence_retention_tests.rs"]
mod tests;
