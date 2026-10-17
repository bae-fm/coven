//! A confirmed conditional floor write authorizes each journaled deletion.
use crate::{
    floor_object::FloorObject,
    object_input::ObjectInput,
    sealed_stream::SealedStream,
    sequence_sync::{SequenceSync, Session},
    OperationKind, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{NewOperation, OperationRecord, OperationUpdate};
use coven_format::{
    floor::{FloorFrames, FloorHeader},
    sealed_floor::FloorPrefixEncoder,
    sequence::Purpose,
};
use coven_merge::{Audience, Slot};
use coven_storage::{ObjectPath, StorageFailure, StoredObject};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    time::{Duration, UNIX_EPOCH},
};

pub(crate) const GRACE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Serialize, Deserialize)]
enum Retention {
    Snapshot(
        #[serde(with = "crate::pending_encoding::snapshot_id")] coven_format::admin::SnapshotId,
    ),
    Floor {
        previous: Option<StoredObject>,
        first: u64,
        announcements: Vec<u64>,
        bytes: Vec<u8>,
    },
    Delete {
        next: u64,
        first: u64,
    },
}
impl Retention {
    fn update(&self, record: &OperationRecord) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: record
                .last_step
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?,
            data: serde_json::to_vec(self)?,
        })
    }
}

impl SequenceSync {
    /// One immutable observation supplies publication age for all later passes.
    pub(crate) async fn immutable_metadata(
        &self,
        path: &ObjectPath,
    ) -> Result<StoredObject, SyncError> {
        if let Some(input) = ObjectInput::retained(&self.database, path).await? {
            return Ok(input.object().clone());
        }
        let object = self
            .storage
            .status(path)
            .await?
            .ok_or(StorageFailure::NotFound)?;
        ObjectInput::observe(&self.database, &object).await?;
        Ok(object)
    }

    pub(crate) fn past_grace(&self, object: &StoredObject) -> bool {
        let Ok(now) = self.clock.now().duration_since(UNIX_EPOCH) else {
            return false;
        };
        object
            .stored_at
            .elapsed_since_epoch()
            .checked_add(object.time_resolution)
            .and_then(|at| at.checked_add(GRACE))
            .is_some_and(|deadline| deadline <= now)
    }

    /// New floor work does not wait for an earlier floor's failed deletions.
    pub(crate) async fn retain_sequence(
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
        {
            return Ok(());
        }
        let records = self.database.operations().await?;
        let pending_floor = records
            .iter()
            .filter(|record| record.kind == "sequence-retention")
            .map(|record| serde_json::from_slice::<Retention>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|work| matches!(work, Retention::Floor { .. }));
        if !pending_floor {
            if let Some(proposal) = self.propose_floor(session, ring, member).await? {
                self.database
                    .start_operation(NewOperation {
                        kind: "sequence-retention".into(),
                        data: serde_json::to_vec(&proposal)?,
                        started_by: "coven".into(),
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
            if record.kind != "sequence-retention" || !self.operation_ready(&record, ring).await? {
                continue;
            }
            let scope = self.waits.begin_attempt(record.id);
            if let Err(error) = scope
                .run(self.apply_retention(session, ring, member, record.clone()))
                .await
            {
                self.record_operation_error(record.id, OperationKind::RetainSequence, &error, ring)
                    .await?;
                self.state.check()?;
            } else {
                self.waits.clear_evidence(record.id);
                self.waits
                    .forget(coven_database::RetrySubject::Operation(record.id));
            }
        }
        Ok(())
    }

    /// Delete this device's snapshot bodies that nothing still loads: not a
    /// floor or pending floor snapshot, not the body of a raise or reset
    /// applied after the floor's coverage, and replaced by a newer usable
    /// announcement covering at least as much whose slot has passed grace.
    pub(crate) async fn retain_snapshots(
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
        {
            return Ok(());
        }
        let history = &session.history;
        let mut kept: BTreeSet<Slot> = session
            .floor
            .as_ref()
            .map(|floor| floor.header.snapshots.iter().copied().collect())
            .unwrap_or_default();
        let mut pending_snapshots = Vec::new();
        for record in self.database.operations().await? {
            if record.kind != "sequence-retention" {
                continue;
            }
            match serde_json::from_slice(&record.data)? {
                Retention::Floor { announcements, .. } => {
                    kept.extend(announcements.into_iter().map(Slot))
                }
                Retention::Snapshot(id) => pending_snapshots.push(id),
                Retention::Delete { .. } => (),
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
            if id.device != self.device || kept.contains(slot) || pending_snapshots.contains(id) {
                continue;
            }
            if history.boundaries.iter().any(|coverage| {
                coverage.boundary.snapshot() == id
                    && coverage.boundary.entry().0 > floor_covers(&id.audience)
            }) {
                continue;
            }
            let Some((replacement, _)) = history
                .announcements
                .range((std::ops::Bound::Excluded(*slot), std::ops::Bound::Unbounded))
                .rev()
                .find(|(_, newer)| {
                    newer.snapshot.audience == id.audience
                        && newer.covers >= announcement.covers
                        && history.usable(newer)
                })
            else {
                continue;
            };
            if self.database.snapshot_absent(id.clone()).await? {
                continue;
            }
            if !self.past_grace(&self.immutable_metadata(&self.path(replacement.0)?).await?) {
                continue;
            }
            self.database
                .start_operation(NewOperation {
                    kind: "sequence-retention".into(),
                    data: serde_json::to_vec(&Retention::Snapshot(id.clone()))?,
                    started_by: "coven".into(),
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
        member: &MemberIdentity,
    ) -> Result<Option<Retention>, SyncError> {
        let audiences: BTreeSet<_> = std::iter::once(Audience::Store)
            .chain(
                session
                    .history
                    .introductions
                    .iter()
                    .map(|intro| intro.audience.clone()),
            )
            .collect();
        let mut selected = Vec::new();
        for audience in audiences {
            let Some((slot, snapshot)) =
                session
                    .history
                    .announcements
                    .iter()
                    .rev()
                    .find(|(_, snapshot)| {
                        snapshot.snapshot.audience == audience && session.history.usable(snapshot)
                    })
            else {
                return Ok(None);
            };
            // The proved floor loader starts from ordinary snapshots. An active
            // preparation is followed by an ordinary capture before retention.
            if snapshot.purpose != Purpose::Ordinary {
                return Ok(None);
            }
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
            selected.push((*slot, snapshot));
        }
        let first = selected
            .iter()
            .map(|(_, snapshot)| snapshot.covers)
            .min()
            .expect("store audience")
            .checked_add(1)
            .ok_or(SyncError::SequenceExhausted)?;
        if first
            <= session
                .floor
                .as_ref()
                .map_or(1, |floor| floor.header.first.0)
        {
            return Ok(None);
        }
        for (slot, _) in &selected {
            if !self.past_grace(&self.immutable_metadata(&self.path(slot.0)?).await?) {
                return Ok(None);
            }
        }
        let originals: BTreeMap<_, _> = self
            .database
            .original_slots()
            .await?
            .into_iter()
            .map(|original| (original.slot, original))
            .collect();
        let key = session.history.metadata_key(ring)?;
        let copies: Vec<_> = session.history.copies.iter().filter(|copy|
            matches!(&copy.prefix.kind, coven_format::sealed_slot::SlotKind::KeyCopy(copy) if copy.routing.audience == Audience::Store && copy.routing.key == key))
            .map(|copy| &originals[&copy.path.slot_number().expect("copy slot")]).collect();
        let (mut routing, mut prefix) = FloorPrefixEncoder::start(
            key,
            copies
                .len()
                .try_into()
                .map_err(|_| coven_format::Error::Truncated)?,
        )?;
        for copy in copies {
            prefix.extend(routing.copy(copy)?);
        }
        routing.finish()?;
        let header = FloorHeader {
            author: self.device,
            at: session.through,
            first: Slot(first),
            snapshots: selected.iter().map(|(slot, _)| *slot).collect(),
        };
        let (mut frames, mut plaintext) = FloorFrames::start(header)?;
        for (slot, _) in &selected {
            plaintext.extend(frames.announcement(&originals[slot])?);
        }
        plaintext.extend(frames.end()?);
        let mut sealed = SealedStream::new(
            Cursor::new(plaintext),
            ObjectPath::floor(self.store()),
            prefix,
            None,
            crate::write_seal::derive(ring, &Audience::Store, key)?,
            member.clone(),
        );
        let mut bytes = Vec::new();
        sealed
            .by_ref()
            .take(self.storage.single_request_limit().saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(SyncError::disk)?;
        if bytes.len() as u64 > self.storage.single_request_limit() {
            return Err(StorageFailure::SingleRequestTooLarge {
                size: bytes.len() as u64,
                limit: self.storage.single_request_limit(),
            }
            .into());
        }
        Ok(Some(Retention::Floor {
            previous: session.floor.as_ref().map(|floor| floor.object().clone()),
            first,
            announcements: selected.into_iter().map(|(slot, _)| slot.0).collect(),
            bytes,
        }))
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
            self.storage
                .delete(&crate::snapshot_object::path(self.store(), &id)?)
                .await?;
            self.database
                .record_absent_snapshot(id, BTreeSet::new())
                .await?;
            self.database.finish_operation(record.id).await?;
            return Ok(());
        }
        if let Retention::Floor {
            previous,
            first,
            bytes,
            ..
        } = &work
        {
            self.state.check()?;
            let mut confirmed = None;
            if record.last_step != 0 {
                // A sent conditional request may have committed despite its
                // missing reply. This observation alone authorizes deletion.
                let current = FloorObject::read(
                    &self.database,
                    self.storage.as_ref(),
                    &ObjectPath::floor(self.store()),
                    session.floor.as_ref(),
                    ring,
                    member,
                )
                .await?;
                if let Some(floor) = &current {
                    self.authenticate_floor(floor, &session.history, ring, member)
                        .await?;
                }
                if let Some(floor) = &current {
                    if floor.header.first.0 >= *first {
                        confirmed = Some(floor.header.first.0);
                    }
                }
                let same_revision =
                    current.as_ref().map(|floor| floor.object()) == previous.as_ref();
                session.floor = current;
                if confirmed.is_none() && !same_revision {
                    self.database.finish_operation(record.id).await?;
                    return Ok(());
                }
            }
            if confirmed.is_none() {
                let update = work.update(&record)?;
                record.last_step = update.last_step;
                self.database.advance_operation(update).await?;
                let result = match previous {
                    Some(previous) => self.storage.replace(previous, bytes).await,
                    None => self
                        .storage
                        .create(&ObjectPath::floor(self.store()), bytes)
                        .await
                        .map(drop),
                };
                match result {
                    Ok(()) => {
                        confirmed = Some(*first);
                        session.floor = None;
                    }
                    Err(error)
                        if matches!(
                            error.failure(),
                            StorageFailure::Conflict | StorageFailure::AlreadyExists
                        ) =>
                    {
                        self.read_forward(session, ring, member).await?;
                        self.database.finish_operation(record.id).await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            work = Retention::Delete {
                next: 1,
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
            self.storage.delete(&self.path(next)?).await?;
            next += 1;
            let update = Retention::Delete { next, first }.update(&record)?;
            record.last_step = update.last_step;
            self.database.advance_operation(update).await?;
        }
        self.database.finish_operation(record.id).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_retention_tests.rs"]
mod tests;
