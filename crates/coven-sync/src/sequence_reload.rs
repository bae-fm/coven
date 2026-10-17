//! Fixed-target reloads, selected by authenticated announcements and the floor.

use crate::{
    chunk_reader::{PartPlaintext, SlotPlaintext},
    floor_object::FloorObject,
    history::History,
    sequence_sync::{SequenceSync, Session},
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_authorship::SnapshotAuthorship,
    snapshot_object::{self, SnapshotObject},
    PendingRecord, Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{IncomingSlot, SlotContent, SnapshotReload, SnapshotSource};
use coven_format::sequence::{Purpose, SlotFrame, SnapshotAnnouncement};
use coven_merge::{Audience, Slot};
use coven_storage::ObjectPath;
use std::sync::Arc;

pub(crate) struct SelectedSnapshot {
    announcement: SnapshotAnnouncement,
    object: SnapshotObject,
    plaintext: SlotPlaintext,
}

impl SelectedSnapshot {
    pub(crate) fn announcement(&self) -> &SnapshotAnnouncement {
        &self.announcement
    }
    /// Its original slots and claim records, tentative until it authenticates.
    pub(crate) fn history(&self) -> Result<snapshot_object::SnapshotHistory, SyncError> {
        let history = snapshot_object::history(
            self.plaintext.clone(),
            self.object.prefix(),
            self.object.path(),
        )?;
        if history.header.id != self.announcement.snapshot {
            return Err(crate::write_object::damaged(
                self.object.path(),
                Refusal::WrongIdentity { cause: None },
            ));
        }
        Ok(history)
    }
    /// Check the body against the announced digest, then its signature and prefix.
    pub(crate) async fn authenticate(
        &self,
        history: &History,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        self.object
            .authenticate(
                history,
                &self.announcement.snapshot,
                &self.announcement.digest,
            )
            .await
    }
    fn into_source(
        self,
        author: coven_foundation::id_source::MemberId,
    ) -> SnapshotSource<SlotPlaintext> {
        SnapshotSource::Stored {
            id: self.announcement.snapshot,
            prefix: self.object.into_prefix(),
            input: self.plaintext,
            author,
        }
    }
}

impl SequenceSync {
    pub(crate) async fn authenticate_floor(
        &self,
        floor: &FloorObject,
        history: &History,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for copy in &floor.copies {
            if copy.slot.0 > floor.header.at {
                return Err(crate::write_object::damaged(
                    &ObjectPath::floor(self.store()),
                    Refusal::WrongIdentity { cause: None },
                ));
            }
            snapshot_object::open_routing(copy, self.store(), ring, member)?;
            slot_object::open_bounded(
                copy.object.clone(),
                &self.path(copy.slot.0)?,
                ring,
                history,
            )?;
        }
        let required: std::collections::BTreeSet<_> = std::iter::once(Audience::Store)
            .chain(
                history
                    .introductions
                    .iter()
                    .filter(|intro| intro.slot.0 <= floor.header.at)
                    .map(|intro| intro.audience.clone()),
            )
            .collect();
        let mut included = std::collections::BTreeSet::new();
        let mut previous = None;
        for original in &floor.announcements {
            let received = slot_object::open_bounded(
                original.object.clone(),
                &self.path(original.slot.0)?,
                ring,
                history,
            )?;
            let SlotBody::Bounded {
                frame: SlotFrame::Announcement(announcement),
                ..
            } = received.into_parts().1
            else {
                unreachable!("floor format requires announcements");
            };
            let audience = &announcement.snapshot.audience;
            if previous.as_ref().is_some_and(|prior| prior >= audience)
                || (previous.is_none() && *audience != Audience::Store)
                || announcement.covers < floor.header.first.0 - 1
                || history.announcements.get(&original.slot) != Some(&announcement)
                || !history.usable_at(&announcement, floor.header.at)
            {
                return Err(crate::write_object::damaged(
                    &ObjectPath::floor(self.store()),
                    Refusal::WrongIdentity { cause: None },
                ));
            }
            included.insert(audience.clone());
            previous = Some(audience.clone());
        }
        if included != required {
            return Err(crate::write_object::damaged(
                &ObjectPath::floor(self.store()),
                Refusal::WrongIdentity { cause: None },
            ));
        }
        floor.authenticate(history)
    }

    async fn select(
        &self,
        announcement: SnapshotAnnouncement,
        ring: &StoreKeyring,
    ) -> Result<SelectedSnapshot, SyncError> {
        let path = snapshot_object::path(self.store(), &announcement.snapshot)?;
        let object =
            SnapshotObject::read_prefix(&self.database, self.storage.as_ref(), &path).await?;
        if object.prefix().covers != announcement.covers
            || object.prefix().purpose != announcement.purpose
        {
            return Err(crate::write_object::damaged(
                &path,
                Refusal::WrongIdentity { cause: None },
            ));
        }
        let plaintext = object.plaintext(self.storage.as_ref(), ring).await?;
        Ok(SelectedSnapshot {
            announcement,
            object,
            plaintext,
        })
    }

    /// Bootstrap the floor's history from its store snapshot. Its announcement
    /// is routing until the original slot and floor signatures have verified.
    pub(crate) async fn floor_history(
        &self,
        floor: &FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(History, SelectedSnapshot), SyncError> {
        let original = floor
            .announcements
            .first()
            .expect("floor requires snapshots");
        let (_, frame) =
            slot_object::bounded_plaintext(&original.object, &self.path(original.slot.0)?, ring)?;
        let SlotFrame::Announcement(announcement) = frame else {
            unreachable!("floor announcement kind");
        };
        if announcement.snapshot.audience != Audience::Store {
            return Err(crate::write_object::damaged(
                &ObjectPath::floor(self.store()),
                Refusal::WrongIdentity { cause: None },
            ));
        }
        let selected = self.select(announcement, ring).await?;
        let metadata = selected.history()?;
        let mut history = History::restore(&metadata.originals, self.store(), ring, member)?;
        selected.authenticate(&history).await?;
        for number in (selected.announcement().covers + 1)..=floor.header.at {
            let path = self.path(number)?;
            let input = self.fetch(&path).await?.ok_or_else(|| {
                crate::write_object::damaged(&path, Refusal::Parse { cause: None })
            })?;
            let slot = slot_object::open(
                input.local_bytes().await?,
                &path,
                ring,
                &history,
                member.member(),
            )?;
            history.consume(slot, member.member());
            history.acquire(ring, member)?;
        }
        self.authenticate_floor(floor, &history, ring, member)
            .await?;
        Ok((history, selected))
    }

    pub(crate) async fn reload_floor(
        &self,
        session: &mut Session,
        mut floor: FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        // An already blocked kept slot cannot be bypassed by a later floor.
        if let Some(record) = &session.blocked {
            return Err(SyncError::Pending(Box::new(record.clone())));
        }
        loop {
            let mut tentative_ring = ring.clone();
            let (history, store_snapshot) = self
                .floor_history(&floor, &mut tentative_ring, member)
                .await?;
            let mut selected = vec![store_snapshot];
            for original in floor.announcements.iter().skip(1) {
                let announcement = history
                    .announcements
                    .get(&original.slot)
                    .expect("authenticated floor announcement");
                if crate::effects::belongs(
                    &history.state,
                    &announcement.snapshot.audience,
                    &member.member(),
                ) {
                    selected.push(self.select(announcement.clone(), &tentative_ring).await?);
                }
            }
            let target = selected
                .iter()
                .map(|snapshot| snapshot.announcement().covers)
                .max()
                .expect("store snapshot")
                .max(session.through);
            match self
                .install_selected(
                    session,
                    selected,
                    Vec::new(),
                    target,
                    history,
                    &mut tentative_ring,
                    member,
                    Some(&floor),
                )
                .await?
            {
                Some(newer) => {
                    floor = newer;
                }
                None => {
                    *ring = tentative_ring;
                    return Ok(());
                }
            }
        }
    }

    /// Reload readable audiences from the floor's snapshots, or else from
    /// their newest usable announced snapshots, and the following slots,
    /// keeping this device's id and waiting writes (§19.2). It cannot bypass
    /// a blocked slot.
    pub async fn reload_from_snapshot(&self) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        if let Some(floor) = session.floor.clone() {
            return self
                .reload_floor(&mut session, floor, &mut ring, &member)
                .await;
        }
        let history = session.history.clone();
        let readable: Vec<_> = std::iter::once(Audience::Store)
            .chain(
                history
                    .state
                    .readable_circles(&member.member())
                    .into_iter()
                    .map(Audience::Circle),
            )
            .collect();
        let mut selected = Vec::new();
        let mut preserved = Vec::new();
        for audience in readable {
            match history.announcements.values().rev().find(|announcement| {
                announcement.snapshot.audience == audience && history.usable(announcement)
            }) {
                Some(announcement) => {
                    selected.push(self.select(announcement.clone(), &ring).await?)
                }
                None => preserved.push(audience),
            }
        }
        if selected.is_empty() {
            return Err(coven_storage::StorageFailure::NotFound
                .with_source("no usable snapshot is announced for a readable audience")
                .into());
        }
        let target = session.through;
        if let Some(newer) = self
            .install_selected(
                &mut session,
                selected,
                preserved,
                target,
                history,
                &mut ring,
                &member,
                None,
            )
            .await?
        {
            return self
                .reload_floor(&mut session, newer, &mut ring, &member)
                .await;
        }
        Ok(())
    }

    pub(crate) async fn reload_activation(
        &self,
        session: &mut Session,
        slot: Slot,
        required: &[Audience],
        history: History,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let mut selected = Vec::new();
        for audience in required {
            let announcement = history
                .announcements
                .iter()
                .rev()
                .find(|(_, announcement)| {
                    announcement.snapshot.audience == *audience && history.usable(announcement)
                })
                .map(|(_, announcement)| announcement.clone())
                .ok_or_else(|| SyncError::Rejected(crate::OperationRejection::NotAllowed))?;
            selected.push(self.select(announcement, ring).await?);
        }
        let preserved = std::iter::once(Audience::Store)
            .chain(
                history
                    .state
                    .readable_circles(&member.member())
                    .into_iter()
                    .map(Audience::Circle),
            )
            .filter(|audience| !required.contains(audience))
            .collect();
        let target = selected
            .iter()
            .map(|snapshot| snapshot.announcement().covers)
            .max()
            .expect("required audience")
            .max(slot.0);
        let mut ring = ring.clone();
        let floor = session.floor.clone();
        if let Some(newer) = self
            .install_selected(
                session,
                selected,
                preserved,
                target,
                history,
                &mut ring,
                member,
                floor.as_ref(),
            )
            .await?
        {
            // This activation remains pending; a later pass retries its exact
            // required reload using the newly observed floor.
            session.floor = Some(newer);
            return Err(SyncError::Pending(Box::new(crate::PendingRecord {
                subject: crate::PendingSubject::Slot(slot),
                reason: crate::PendingReason::Waits(crate::Prerequisite::SequenceCatchUp),
                reported_by: self.device,
            })));
        }
        Ok(())
    }

    /// Returns a newer floor if retention removed a required suffix. No local
    /// state has changed in that case; the fixed target's data is selected again.
    async fn install_selected(
        &self,
        session: &mut Session,
        selected: Vec<SelectedSnapshot>,
        preserved: Vec<Audience>,
        target: u64,
        history: History,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
    ) -> Result<Option<FloorObject>, SyncError> {
        let first = selected
            .iter()
            .map(|snapshot| snapshot.announcement().covers)
            .chain((!preserved.is_empty()).then_some(session.through))
            .min()
            .expect("reload audience");
        let (mut at_target, store_covers) = match selected
            .iter()
            .find(|snapshot| snapshot.announcement().snapshot.audience == Audience::Store)
        {
            Some(store_snapshot) => {
                let metadata = store_snapshot.history()?;
                (
                    History::restore(&metadata.originals, self.store(), ring, member)?,
                    store_snapshot.announcement().covers,
                )
            }
            None => (session.history.clone(), session.through),
        };
        let mut slots = Vec::new();
        let mut missing = None;
        for number in (first + 1)..=target {
            let path = self.path(number)?;
            let Some(input) = self.fetch(&path).await? else {
                missing = Some(number);
                break;
            };
            let received = slot_object::open(
                input.local_bytes().await?,
                &path,
                ring,
                &history,
                member.member(),
            )?;
            if number > session.through && number > store_covers {
                self.check_copy(&received, &path).await?;
            }
            let incoming = if number > store_covers {
                let incoming = at_target.consume(received, member.member());
                at_target.acquire(ring, member)?;
                incoming
            } else {
                retained_slot(received)
            };
            slots.push(incoming);
        }
        let floor = FloorObject::read(
            &self.database,
            self.storage.as_ref(),
            &ObjectPath::floor(self.store()),
            previous_floor,
            ring,
            member,
        )
        .await?;
        if floor
            .as_ref()
            .is_some_and(|floor| floor.header.first.0 > first + 1)
        {
            return Ok(floor);
        }
        if let Some(number) = missing {
            return Err(crate::write_object::damaged(
                &self.path(number)?,
                Refusal::Parse { cause: None },
            ));
        }
        if let Some(floor) = &floor {
            self.authenticate_floor(floor, &history, ring, member)
                .await?;
        }
        let mut sources: Vec<_> = preserved.into_iter().map(SnapshotSource::Current).collect();
        for selected in selected {
            let author = selected.authenticate(&history).await?;
            sources.push(selected.into_source(author));
        }
        self.keys.persist(ring)?;
        let authorship = Arc::new(SnapshotAuthorship {
            store: self.store(),
            history: history.clone(),
            ring: ring.clone(),
        });
        self.database
            .load_snapshots::<PendingRecord, _, _>(SnapshotReload {
                snapshots: sources,
                slots,
                target,
                state: at_target.state.clone(),
                cutoffs: at_target.cutoffs.clone(),
                boundaries: at_target.boundaries.clone(),
                provider: at_target
                    .provider_changes(member.member(), &self.invited_accounts().await?)?,
                operation: None,
                authorship,
            })
            .await?;
        session.history = at_target;
        session.through = target;
        session.floor = floor;
        self.check_binding(session, member)?;
        Ok(None)
    }
}

fn retained_slot(
    received: ReceivedSlot,
) -> IncomingSlot<coven_database::DownloadedWriteStream<PartPlaintext>> {
    let (received, body) = received.into_parts();
    let content = match body {
        SlotBody::Writes(writes) => SlotContent::Writes(writes),
        SlotBody::Bounded {
            frame: SlotFrame::Admin(_),
            original,
        } => SlotContent::Admin {
            original,
            effect: None,
        },
        SlotBody::Bounded {
            frame: SlotFrame::KeyCopy,
            original,
        } => SlotContent::KeyCopy(original),
        SlotBody::Bounded {
            frame: SlotFrame::Announcement(_),
            original,
        } => SlotContent::Announcement(original),
    };
    IncomingSlot {
        slot: Slot(received.header.after + 1),
        header: received.header,
        digest: received.digest,
        content,
    }
}

impl History {
    pub(crate) fn usable(&self, announcement: &SnapshotAnnouncement) -> bool {
        self.usable_at(announcement, u64::MAX)
    }

    pub(crate) fn usable_at(&self, announcement: &SnapshotAnnouncement, through: u64) -> bool {
        let boundaries: Vec<_> = self
            .boundaries
            .iter()
            .filter(|boundary| {
                boundary.boundary.entry().0 <= through
                    && boundary.boundary.snapshot().audience == announcement.snapshot.audience
            })
            .collect();
        let selected = match announcement.purpose {
            Purpose::Ordinary => true,
            Purpose::Raise(version) => boundaries
                .iter()
                .rev()
                .find_map(|coverage| match &coverage.boundary {
                    coven_database::Boundary::SchemaChange(current) => {
                        Some(current.number == version && current.snapshot == announcement.snapshot)
                    }
                    _ => None,
                })
                .unwrap_or(false),
            Purpose::Reset => boundaries
                .iter()
                .rev()
                .find_map(|coverage| match &coverage.boundary {
                    coven_database::Boundary::Reset { snapshot, .. } => {
                        Some(snapshot == &announcement.snapshot)
                    }
                    _ => None,
                })
                .unwrap_or(false),
        };
        selected
            && boundaries.iter().all(|coverage| {
                coverage.boundary.entry().0 <= announcement.covers
                    || (announcement.purpose != Purpose::Ordinary
                        && coverage.boundary.snapshot() == &announcement.snapshot)
            })
    }
}

#[cfg(test)]
#[path = "sequence_reload_tests.rs"]
mod tests;
