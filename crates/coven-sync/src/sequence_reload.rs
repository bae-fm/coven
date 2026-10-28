//! Fixed-target reloads, selected by authenticated announcements and the floor.

use crate::{
    chunk_reader::PartPlaintext,
    credential_custody::cleanup_error,
    floor_object::FloorObject,
    history::History,
    sequence_sync::{SequenceSync, Session},
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_authorship::SnapshotAuthorship,
    snapshot_object::{self, SnapshotBody, SnapshotObject},
    write_object::database_failure,
    PendingRecord, Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{
    IncomingSlot, OperationUpdate, SlotContent, SnapshotReload, SnapshotSource, SnapshotStaging,
    StagedRecords, StagedSnapshot,
};
use coven_format::sequence::{Purpose, SlotFrame, SnapshotAnnouncement};
use coven_merge::{Audience, Slot};
use coven_storage::ObjectPath;
use std::sync::Arc;

/// A snapshot whose body streamed once, before it authenticates.
pub(crate) struct ReadSnapshot {
    announcement: SnapshotAnnouncement,
    object: SnapshotObject,
    body: SnapshotBody,
}

impl ReadSnapshot {
    pub(crate) fn announcement(&self) -> &SnapshotAnnouncement {
        &self.announcement
    }
    /// Its header, original slots and claim records, tentative until it
    /// authenticates.
    pub(crate) fn body(&self) -> &SnapshotBody {
        &self.body
    }
    /// Check the body against the announced digest, then its signature and prefix.
    pub(crate) fn authenticate(
        &self,
        history: &History,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        self.object.authenticate(
            history,
            &self.announcement.snapshot,
            &self.body.digest,
            &self.announcement.digest,
        )
    }
}

/// A selected snapshot read once, with its records staged for the one load
/// that installs it after it authenticates (§16.2).
pub(crate) struct SelectedSnapshot {
    read: ReadSnapshot,
    staged: StagedSnapshot,
}

impl SelectedSnapshot {
    pub(crate) fn announcement(&self) -> &SnapshotAnnouncement {
        self.read.announcement()
    }
    pub(crate) fn body(&self) -> &SnapshotBody {
        self.read.body()
    }
    pub(crate) fn authenticate(
        &self,
        history: &History,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        self.read.authenticate(history)
    }
    fn source(
        &self,
        author: coven_foundation::id_source::MemberId,
    ) -> SnapshotSource<StagedRecords> {
        SnapshotSource::Stored {
            id: self.announcement().snapshot.clone(),
            input: self.staged.records(),
            author,
        }
    }
    /// Remove its staged records, unless the load that installed it took them.
    async fn discard(self) -> Result<(), SyncError> {
        Ok(self.staged.discard().await?)
    }
}

/// End a reload with `result`: remove the staged records of every selected
/// start the reload did not install. A failure to remove them is reported,
/// beside the reload's own failure if it failed too (§16.2).
async fn release<T>(
    starts: Vec<ReloadStart>,
    result: Result<T, SyncError>,
) -> Result<T, SyncError> {
    let mut result = result;
    for start in starts {
        if let ReloadStart::Stored(selected) = start {
            if let Err(cleanup) = selected.discard().await {
                result = Err(match result {
                    Ok(_) => cleanup,
                    Err(error) => cleanup_error(error, cleanup),
                });
            }
        }
    }
    result
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

/// Everything a reload installs besides its starts.
struct ReloadInputs {
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

    /// A snapshot's signed prefix, checked against its announcement.
    async fn snapshot_prefix(
        &self,
        announcement: &SnapshotAnnouncement,
    ) -> Result<SnapshotObject, SyncError> {
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
        Ok(object)
    }

    /// Stream the body once, staging its plaintext into `staging` if given.
    /// Only the prefix it read is kept, released for eviction.
    async fn read_snapshot(
        &self,
        announcement: SnapshotAnnouncement,
        object: SnapshotObject,
        ring: &StoreKeyring,
        staging: Option<&mut SnapshotStaging>,
    ) -> Result<ReadSnapshot, SyncError> {
        let started = self.clock.elapsed();
        let body = object
            .read_body(self.storage.as_ref(), ring, &announcement.digest, staging)
            .await;
        let body = match (body, object.release().await) {
            (Ok(body), Ok(())) => body,
            (Ok(_), Err(cleanup)) => return Err(cleanup),
            (Err(error), Ok(())) => return Err(error),
            (Err(error), Err(cleanup)) => return Err(cleanup_error(error, cleanup)),
        };
        if body.header.id != announcement.snapshot {
            return Err(crate::write_object::damaged(
                object.path(),
                Refusal::WrongIdentity { cause: None },
            ));
        }
        tracing::info!(
            audience = ?announcement.snapshot.audience,
            covers = announcement.covers,
            bytes = object.size(),
            time = ?(self.clock.elapsed() - started),
            "streamed snapshot"
        );
        Ok(ReadSnapshot {
            announcement,
            object,
            body,
        })
    }

    /// Read a selected snapshot once and stage its records for loading. A
    /// failed read removes what it staged before reporting (§16.2).
    async fn select(
        &self,
        announcement: SnapshotAnnouncement,
        ring: &StoreKeyring,
    ) -> Result<SelectedSnapshot, SyncError> {
        let object = self.snapshot_prefix(&announcement).await?;
        let path = object.path().clone();
        let mut staging = self
            .database
            .stage_snapshot(announcement.snapshot.clone(), object.prefix().clone())
            .await?;
        let read = self
            .read_snapshot(announcement, object, ring, Some(&mut staging))
            .await;
        let staged = match read {
            Ok(read) => match staging.finish().await {
                Ok(staged) => return Ok(SelectedSnapshot { read, staged }),
                Err(error) => database_failure(&path, error),
            },
            Err(error) => error,
        };
        Err(match staging.discard().await {
            Ok(()) => staged,
            Err(cleanup) => cleanup_error(staged, cleanup.into()),
        })
    }

    /// The floor's store snapshot announcement: routing until its original
    /// slot and the floor's signature verify.
    fn floor_store_announcement(
        &self,
        floor: &FloorObject,
        ring: &StoreKeyring,
    ) -> Result<SnapshotAnnouncement, SyncError> {
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
        Ok(announcement)
    }

    /// Bootstrap the floor's history from its store snapshot, read once,
    /// then authenticate the snapshot and the floor against it.
    async fn history_from(
        &self,
        floor: &FloorObject,
        snapshot: &ReadSnapshot,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<History, SyncError> {
        let restoring = self.clock.elapsed();
        let originals = &snapshot.body().originals;
        let mut history = History::restore(originals, self.store(), ring, member)?;
        let authenticating = self.clock.elapsed();
        snapshot.authenticate(&history)?;
        tracing::info!(
            originals = originals.len(),
            restoring = ?(authenticating - restoring),
            authenticating = ?(self.clock.elapsed() - authenticating),
            "read the history in the floor's store snapshot"
        );
        self.advance_history(
            &mut history,
            snapshot.announcement().covers,
            floor.header.at,
            ring,
            member,
        )
        .await?;
        self.authenticate_floor(floor, &history, ring, member)
            .await?;
        Ok(history)
    }

    /// The floor's history, from its store snapshot read once and staged
    /// nowhere: for a caller that installs none of its rows.
    pub(crate) async fn floor_history(
        &self,
        floor: &FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(History, ReadSnapshot), SyncError> {
        let announcement = self.floor_store_announcement(floor, ring)?;
        let object = self.snapshot_prefix(&announcement).await?;
        let snapshot = self.read_snapshot(announcement, object, ring, None).await?;
        let history = self.history_from(floor, &snapshot, ring, member).await?;
        Ok((history, snapshot))
    }

    /// The floor's history and its store snapshot, staged to install.
    async fn floor_start(
        &self,
        floor: &FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(History, SelectedSnapshot), SyncError> {
        let announcement = self.floor_store_announcement(floor, ring)?;
        let selected = self.select(announcement, ring).await?;
        match self.history_from(floor, &selected.read, ring, member).await {
            Ok(history) => Ok((history, selected)),
            Err(error) => release(vec![ReloadStart::Stored(selected)], Err(error)).await,
        }
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
        let started = self.clock.elapsed();
        let mut bytes = 0;
        let mut reads = crate::sequence_sync::SlotReads::new(self, after + 1, through);
        for number in (after + 1)..=through {
            let path = self.path(number)?;
            let input = reads.next().await?.ok_or_else(|| {
                crate::write_object::damaged(&path, Refusal::Parse { cause: None })
            })?;
            let slot = slot_object::open(
                input.local_bytes().await?,
                &path,
                ring,
                history,
                |audience| crate::effects::belongs(&history.state, audience, &member.member()),
            )?;
            bytes += input.object().size;
            history.consume(slot, member.member());
            history.acquire(ring, member)?;
        }
        if through > after {
            tracing::info!(
                slots = through - after,
                bytes,
                time = ?(self.clock.elapsed() - started),
                "read the slots between a snapshot and the floor"
            );
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
                .floor_start(&floor, &mut tentative_ring, member)
                .await?;
            let mut starts = vec![ReloadStart::Stored(store_snapshot)];
            let selected = async {
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
                Ok(())
            }
            .await;
            if let Err(error) = selected {
                return release(starts, Err(error)).await;
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
                    starts,
                    ReloadInputs {
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
        let selected = async {
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
                                return Err(SyncError::Pending(Box::new(PendingRecord::new(
                                    crate::PendingSubject::Slot(slot),
                                    crate::PendingReason::Waits(crate::Prerequisite::Reload(
                                        audience.clone(),
                                    )),
                                ))))
                            }
                        }
                    }
                    // No slot is deleted: a newly readable circle without a
                    // snapshot loads every slot from the start.
                    (None, None) => ReloadStart::Empty(audience.clone()),
                };
                starts.push(start);
            }
            Ok(())
        }
        .await;
        if let Err(error) = selected {
            return release(starts, Err(error)).await;
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
                starts,
                ReloadInputs {
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
            return Err(SyncError::Pending(Box::new(crate::PendingRecord::new(
                crate::PendingSubject::Slot(slot),
                crate::PendingReason::Waits(crate::Prerequisite::SequenceCatchUp),
            ))));
        }
        Ok(())
    }

    /// Returns a newer floor if retention removed a required suffix. No local
    /// state has changed in that case; the fixed target's data is selected again.
    /// Every selected start's staged records are gone when this returns:
    /// installing took them, or they were removed (§16.2).
    async fn install_selected(
        &self,
        session: &mut Session,
        starts: Vec<ReloadStart>,
        inputs: ReloadInputs,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
    ) -> Result<Option<FloorObject>, SyncError> {
        let installed = self
            .install_starts(session, &starts, inputs, ring, member, previous_floor)
            .await;
        release(starts, installed).await
    }

    async fn install_starts(
        &self,
        session: &mut Session,
        starts: &[ReloadStart],
        inputs: ReloadInputs,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
    ) -> Result<Option<FloorObject>, SyncError> {
        let ReloadInputs {
            target,
            history,
            operation,
        } = inputs;
        let first = starts
            .iter()
            .map(|start| start.covers(session.through))
            .min()
            .expect("reload audience");
        let mut rows = 0;
        let (mut at_target, store_covers) = match starts.iter().find_map(|start| match start {
            ReloadStart::Stored(snapshot)
                if snapshot.announcement().snapshot.audience == Audience::Store =>
            {
                Some(snapshot)
            }
            _ => None,
        }) {
            Some(store_snapshot) => {
                let body = store_snapshot.body();
                rows = body.header.synced_rows();
                (
                    History::restore(&body.originals, self.store(), ring, member)?,
                    store_snapshot.announcement().covers,
                )
            }
            None => (session.history.clone(), session.through),
        };
        let started = self.clock.elapsed();
        let mut bytes = 0;
        let mut slots = Vec::new();
        let mut missing = None;
        let mut reads = crate::sequence_sync::SlotReads::new(self, first + 1, target);
        for number in (first + 1)..=target {
            let path = self.path(number)?;
            let Some(input) = reads.next().await? else {
                missing = Some(number);
                break;
            };
            bytes += input.object().size;
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
        let fetched = slots.len();
        let reading = self.clock.elapsed() - started;
        let authenticating = self.clock.elapsed();
        let mut sources = Vec::new();
        for start in starts {
            sources.push(match start {
                ReloadStart::Current(audience) => SnapshotSource::Current(audience.clone()),
                ReloadStart::Empty(audience) => SnapshotSource::Empty(audience.clone()),
                ReloadStart::Stored(selected) => {
                    let author = selected.authenticate(&history)?;
                    selected.source(author)
                }
            });
        }
        self.follow_synced_entries(&session.history, &at_target, member)?;
        self.keys.persist(ring)?;
        let authenticating = self.clock.elapsed() - authenticating;
        let loading = self.clock.elapsed();
        let authorship = Arc::new(SnapshotAuthorship {
            store: self.store(),
            history: history.clone(),
            ring: ring.clone(),
        });
        self.database
            .load_snapshots::<PendingRecord, _>(SnapshotReload {
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
        tracing::info!(
            store_rows = rows,
            slots = fetched,
            bytes,
            target,
            reading = ?reading,
            authenticating = ?authenticating,
            loading = ?(self.clock.elapsed() - loading),
            "loaded snapshots and the slots after them"
        );
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
        SlotBody::Writes { size, writes } => SlotContent::Writes { size, writes },
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
