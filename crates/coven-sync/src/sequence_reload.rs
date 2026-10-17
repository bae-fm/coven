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
use coven_database::{IncomingSlot, OperationUpdate, SlotContent, SnapshotReload, SnapshotSource};
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

/// Where one audience's reloaded prefix begins.
enum ReloadStart {
    /// A selected announced snapshot.
    Stored(SelectedSnapshot),
    /// Slot zero: no slot is deleted and no snapshot is announced.
    Empty(Audience),
    /// This device's committed prefix, applied to `Session::through`.
    Current(Audience),
}

struct ReloadInputs {
    starts: Vec<ReloadStart>,
    target: u64,
    history: History,
    operation: Option<OperationUpdate>,
}

#[path = "sequence_reload_operation.rs"]
mod operation;

impl ReloadStart {
    fn audience(&self) -> &Audience {
        match self {
            ReloadStart::Stored(snapshot) => &snapshot.announcement().snapshot.audience,
            ReloadStart::Empty(audience) | ReloadStart::Current(audience) => audience,
        }
    }

    fn covers(&self, through: u64) -> u64 {
        match self {
            ReloadStart::Stored(snapshot) => snapshot.announcement().covers,
            ReloadStart::Empty(_) => 0,
            ReloadStart::Current(_) => through,
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
        self.advance_history(
            &mut history,
            selected.announcement().covers,
            floor.header.at,
            ring,
            member,
        )
        .await?;
        self.authenticate_floor(floor, &history, ring, member)
            .await?;
        Ok((history, selected))
    }

    /// Extend `history`, which holds slots through `after`, with the kept
    /// slots through `through`.
    async fn advance_history(
        &self,
        history: &mut History,
        after: u64,
        through: u64,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for number in (after + 1)..=through {
            let path = self.path(number)?;
            let input = self.fetch(&path).await?.ok_or_else(|| {
                crate::write_object::damaged(&path, Refusal::Parse { cause: None })
            })?;
            let slot = slot_object::open(
                input.local_bytes().await?,
                &path,
                ring,
                history,
                |audience| crate::effects::belongs(&history.state, audience, &member.member()),
            )?;
            history.consume(slot, member.member());
            history.acquire(ring, member)?;
        }
        Ok(())
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
            let mut starts = vec![ReloadStart::Stored(store_snapshot)];
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
                    starts.push(ReloadStart::Stored(
                        self.select(announcement.clone(), &tentative_ring).await?,
                    ));
                }
            }
            let target = starts
                .iter()
                .map(|start| start.covers(session.through))
                .max()
                .expect("store snapshot")
                .max(session.through);
            match self
                .install_selected(
                    session,
                    ReloadInputs {
                        starts,
                        target,
                        history,
                        operation: None,
                    },
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

    /// Load the audiences an applied admin slot made `required`: a raise's or
    /// reset's audience from its prepared body, a newly readable circle from
    /// its newest usable snapshot. Where that start's following slots lie
    /// below the floor's first kept slot, or a newly readable circle has
    /// none, the floor's snapshot for that audience is the start (§16.5).
    pub(crate) async fn reload_activation(
        &self,
        session: &mut Session,
        slot: Slot,
        required: &[Audience],
        history: History,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let mut ring = ring.clone();
        let floor = session.floor.clone();
        // History through the floor, read only when the floor supplies a start.
        let mut through_floor: Option<History> = None;
        let mut starts = Vec::new();
        for audience in required {
            let boundary = history.boundaries.iter().any(|coverage| {
                coverage.boundary.entry() == slot
                    && coverage.boundary.snapshot().audience == *audience
            });
            let own = history
                .announcements
                .values()
                .rev()
                .find(|announcement| {
                    announcement.snapshot.audience == *audience && history.usable(announcement)
                })
                .cloned();
            // The floor supplies the start when the audience's own start
            // needs slots below its first kept slot, or has none.
            let from_floor = match (&own, &floor) {
                (Some(own), Some(floor)) if own.covers + 1 >= floor.header.first.0 => None,
                (_, floor) => floor.as_ref(),
            };
            let start = match (own, from_floor) {
                (Some(own), None) => ReloadStart::Stored(self.select(own, &ring).await?),
                (own, Some(floor)) => {
                    let later = match &mut through_floor {
                        Some(later) => later,
                        None => {
                            let mut later = history.clone();
                            if floor.header.at > slot.0 {
                                self.advance_history(
                                    &mut later,
                                    slot.0,
                                    floor.header.at,
                                    &mut ring,
                                    member,
                                )
                                .await?;
                            }
                            self.authenticate_floor(floor, &later, &mut ring, member)
                                .await?;
                            through_floor.insert(later)
                        }
                    };
                    let named = floor
                        .announcements
                        .iter()
                        .filter_map(|original| later.announcements.get(&original.slot))
                        .find(|announcement| announcement.snapshot.audience == *audience)
                        .cloned();
                    match (named, own) {
                        // A raise or reset is replaced only by a snapshot
                        // announced after it, which covers it.
                        (Some(named), _) if !boundary || named.covers >= slot.0 => {
                            ReloadStart::Stored(self.select(named, &ring).await?)
                        }
                        (_, Some(own)) => {
                            return Err(crate::write_object::damaged(
                                &self.path(own.covers + 1)?,
                                Refusal::Parse { cause: None },
                            ))
                        }
                        (_, None) => {
                            return Err(SyncError::Pending(Box::new(PendingRecord {
                                subject: crate::PendingSubject::Slot(slot),
                                reason: crate::PendingReason::Waits(crate::Prerequisite::Reload(
                                    audience.clone(),
                                )),
                            })))
                        }
                    }
                }
                // No slot is deleted: a newly readable circle without a
                // snapshot loads every slot from the start.
                (None, None) => ReloadStart::Empty(audience.clone()),
            };
            starts.push(start);
        }
        starts.extend(
            std::iter::once(Audience::Store)
                .chain(
                    history
                        .state
                        .readable_circles(&member.member())
                        .into_iter()
                        .map(Audience::Circle),
                )
                .filter(|audience| !required.contains(audience))
                .map(ReloadStart::Current),
        );
        let target = starts
            .iter()
            .map(|start| start.covers(session.through))
            .max()
            .expect("required audience")
            .max(slot.0);
        let history = through_floor.unwrap_or(history);
        if let Some(newer) = self
            .install_selected(
                session,
                ReloadInputs {
                    starts,
                    target,
                    history,
                    operation: None,
                },
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
            })));
        }
        Ok(())
    }

    /// Returns a newer floor if retention removed a required suffix. No local
    /// state has changed in that case; the fixed target's data is selected again.
    async fn install_selected(
        &self,
        session: &mut Session,
        inputs: ReloadInputs,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
    ) -> Result<Option<FloorObject>, SyncError> {
        let ReloadInputs {
            starts,
            target,
            history,
            operation,
        } = inputs;
        let first = starts
            .iter()
            .map(|start| start.covers(session.through))
            .min()
            .expect("reload audience");
        let (mut at_target, store_covers) = match starts.iter().find_map(|start| match start {
            ReloadStart::Stored(snapshot)
                if snapshot.announcement().snapshot.audience == Audience::Store =>
            {
                Some(snapshot)
            }
            _ => None,
        }) {
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
                |audience| starts.iter().any(|start| start.audience() == audience),
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
        let mut sources = Vec::new();
        for start in starts {
            sources.push(match start {
                ReloadStart::Current(audience) => SnapshotSource::Current(audience),
                ReloadStart::Empty(audience) => SnapshotSource::Empty(audience),
                ReloadStart::Stored(selected) => {
                    let author = selected.authenticate(&history).await?;
                    selected.into_source(author)
                }
            });
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
                operation,
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
