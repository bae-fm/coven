//! Fixed-target reloads, selected by authenticated announcements and the floor.

use crate::{
    chunk_reader::PartPlaintext,
    floor_object::FloorObject,
    history::History,
    sequence_sync::{next_slot, SequenceSync, Session},
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_authorship::SnapshotAuthorship,
    snapshot_object::{self, SnapshotObject},
    PendingReason, PendingRecord, PendingSubject, Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{
    IncomingSlot, LoadStep, OperationUpdate, SlotContent, SnapshotReload, SnapshotSource,
};
use coven_format::sequence::{Purpose, SlotFrame, SnapshotAnnouncement};
use coven_foundation::progress::ProgressReport;
use coven_merge::{Audience, Slot, SlotRange};
use coven_storage::ObjectPath;
use std::sync::Arc;

/// End a reload that ends before its load with `result`: remove the staged
/// records of every selected start. A failure to remove them is reported,
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
                    Err(error) => SyncError::cleanup(error, cleanup),
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
    /// Nothing of the audience exists up to the slot given: slot zero when no
    /// slot is deleted and no snapshot is announced, or the floor's `at` for a
    /// circle the floor leaves unnamed because it holds no part (§16.4).
    Empty(Audience, u64),
    /// This device's committed prefix, applied to `Session::through`.
    Current(Audience),
}

/// Everything a reload installs besides its starts.
struct ReloadInputs {
    target: u64,
    history: History,
    /// The slot `history` holds the slots through.
    through: u64,
    operation: Option<OperationUpdate>,
    /// Audiences the load leaves for their rebuild (§18.3).
    rebuilds: Vec<Audience>,
    /// The floor's store snapshot when the store is among `rebuilds`: read
    /// for its history and staged nowhere, since the database can't hold it.
    store_history: Option<ReadSnapshot>,
}

#[path = "sequence_reload_evidence.rs"]
mod evidence;

#[path = "sequence_reload_operation.rs"]
mod operation;

#[path = "sequence_reload_rebuild.rs"]
mod rebuild;

#[path = "sequence_reload_snapshot.rs"]
mod snapshot;

use snapshot::{ReadSnapshot, SelectedSnapshot};

impl ReloadStart {
    fn audience(&self) -> &Audience {
        match self {
            ReloadStart::Stored(snapshot) => &snapshot.announcement().snapshot.audience,
            ReloadStart::Empty(audience, _) | ReloadStart::Current(audience) => audience,
        }
    }

    fn covers(&self, through: u64) -> u64 {
        match self {
            ReloadStart::Stored(snapshot) => snapshot.announcement().covers,
            ReloadStart::Empty(_, empty_through) => *empty_through,
            ReloadStart::Current(_) => through,
        }
    }
}

impl SequenceSync {
    /// The store's floor as storage holds it now; `previous` is the one
    /// last read, reused while the object is unchanged.
    pub(crate) async fn read_floor(
        &self,
        previous: Option<&FloorObject>,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<FloorObject>, SyncError> {
        FloorObject::read(
            &self.database,
            self.storage.as_ref(),
            &ObjectPath::floor(self.store()),
            previous,
            ring,
            member,
        )
        .await
    }

    /// The floor's store snapshot announcement: routing until its original
    /// slot and the floor's signature verify.
    fn floor_store_announcement(
        &self,
        floor: &FloorObject,
        ring: &StoreKeyring,
    ) -> Result<SnapshotAnnouncement, SyncError> {
        let original = floor.announcements.first().expect("floor requires snapshots");
        let (_, frame) =
            slot_object::bounded_plaintext(&original.object, &self.path(original.slot), ring)?;
        let SlotFrame::Announcement(announcement) = frame else {
            unreachable!("floor announcement kind");
        };
        if announcement.snapshot.audience != Audience::Store {
            return Err(crate::write_object::wrong_identity(&ObjectPath::floor(self.store())));
        }
        Ok(announcement)
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
        let snapshot = self
            .read_snapshot(announcement, object, ring, None, None)
            .await
            .map_err(|failure| failure.error)?;
        let history = self.history_from(floor, &snapshot, ring, member).await?;
        Ok((history, snapshot))
    }

    /// The floor's history and its store snapshot, staged to install.
    async fn floor_start(
        &self,
        floor: &FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<(History, SelectedSnapshot), SyncError> {
        let announcement = self.floor_store_announcement(floor, ring)?;
        let selected = self.select(announcement, ring, progress).await?;
        match self.history_from_selected(floor, &selected, ring, member).await {
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
        let from = next_slot(after)?;
        let range = SlotRange::new(from, through);
        let mut reads = crate::sequence_sync::SlotReads::new(self, range);
        for number in range {
            let path = self.path(number);
            let input = reads.next().await?.ok_or_else(|| {
                crate::write_object::damaged(&path, Refusal::Parse { cause: None })
            })?;
            let slot =
                slot_object::open(input.local_bytes().await?, &path, ring, history, |audience| {
                    crate::effects::belongs(&history.state, audience, &member.member())
                })?;
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

    /// Load the floor's snapshots and the slots after them to a common
    /// target, reporting loading's steps to `progress` when given (§16.5).
    pub(crate) async fn reload_floor(
        &self,
        session: &mut Session,
        mut floor: FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        mut progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<(), SyncError> {
        // A blocked slot can't be bypassed by a later floor, except one
        // waiting for a circle key once retention has deleted it: the floor's
        // snapshots cover it, and the key's copy is in the floor's history or
        // in a kept slot, which reading in order never reaches (§6.1, §12.1).
        let waiting = match &session.blocked {
            None => None,
            Some(PendingRecord {
                subject: PendingSubject::Slot(slot),
                reason: PendingReason::KeyUnavailable { audience: Audience::Circle(circle), key },
                ..
            }) => Some((*slot, *circle, *key)),
            Some(record) => return Err(SyncError::Pending(Box::new(record.clone()))),
        };
        if let Some((_, circle, key)) = waiting {
            if let Some(hash) = session.history.commitment(&Audience::Circle(circle), key) {
                let start = floor.header.first;
                if let Some(found) =
                    self.scan_for_stuck_key(session, member, start, circle, key, &hash).await?
                {
                    found.insert(ring)?;
                    self.keys.persist(ring)?;
                }
            }
        }
        let breaking = self.database.breaking_version().await?;
        loop {
            let mut tentative_ring = ring.clone();
            // A store snapshot below this database's version can't be staged
            // in it: the history is read from it alone (§18.3).
            let below = breaking > 0
                && self.floor_store_announcement(&floor, &tentative_ring)?.breaking_version
                    < breaking;
            let mut store_history = None;
            let mut store_snapshot = None;
            let history = if below {
                let (history, read) =
                    self.floor_history(&floor, &mut tentative_ring, member).await?;
                if history.state.rebuilt_by(&Audience::Store, breaking) {
                    store_history = Some(read);
                    history
                } else {
                    let (history, selected) = self
                        .floor_start(&floor, &mut tentative_ring, member, progress.as_deref_mut())
                        .await?;
                    store_snapshot = Some(selected);
                    history
                }
            } else {
                let (history, selected) = self
                    .floor_start(&floor, &mut tentative_ring, member, progress.as_deref_mut())
                    .await?;
                store_snapshot = Some(selected);
                history
            };
            // An audience below this database's version takes no snapshot of
            // the floor, which holds it at that lower version: it waits for
            // its rebuild, as one made readable by a slot does (§18.3).
            let rebuilds: Vec<Audience> = history
                .state
                .readable_audiences(&member.member())
                .into_iter()
                .filter(|audience| history.state.rebuilt_by(audience, breaking))
                .collect();
            let mut starts: Vec<ReloadStart> =
                store_snapshot.into_iter().map(ReloadStart::Stored).collect();
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
                    ) && !rebuilds.contains(&announcement.snapshot.audience)
                    {
                        starts.push(ReloadStart::Stored(
                            self.select(
                                announcement.clone(),
                                &tentative_ring,
                                progress.as_deref_mut(),
                            )
                            .await?,
                        ));
                    }
                }
                // A circle the floor doesn't name holds no part below `first`
                // (§16.4): it starts empty there.
                for audience in history.state.readable_audiences(&member.member()) {
                    if !starts.iter().any(|start| *start.audience() == audience)
                        && !rebuilds.contains(&audience)
                    {
                        starts.push(ReloadStart::Empty(audience, floor.header.at));
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
                .chain(store_history.iter().map(|read| read.announcement().covers))
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
                        through: floor.header.at,
                        operation: None,
                        rebuilds: rebuilds.clone(),
                        store_history,
                    },
                    &mut tentative_ring,
                    member,
                    Some(&floor),
                    progress.is_some(),
                )
                .await?
            {
                // The slots the snapshots needed are gone: loading starts
                // over from the newer floor's snapshots.
                Some(newer) => {
                    floor = newer;
                    if let Some(progress) = progress.as_deref_mut() {
                        progress.restart();
                    }
                }
                None => {
                    *ring = tentative_ring;
                    if let Some((slot, _, _)) = waiting {
                        self.database.clear_pending(PendingSubject::Slot(slot)).await?;
                        session.blocked = None;
                    }
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
                    (Some(own), Some(floor))
                        if own.covers.saturating_add(1) >= floor.header.first.get() =>
                    {
                        None
                    }
                    (_, floor) => floor.as_ref(),
                };
                let start = match (own, from_floor) {
                    (Some(own), None) => ReloadStart::Stored(self.select(own, &ring, None).await?),
                    (own, Some(floor)) => {
                        let later = match &mut through_floor {
                            Some(later) => later,
                            None => {
                                let mut later = history.clone();
                                if floor.header.at > slot.get() {
                                    self.advance_history(
                                        &mut later,
                                        slot.get(),
                                        floor.header.at,
                                        &mut ring,
                                        member,
                                    )
                                    .await?;
                                }
                                self.authenticate_floor(floor, &later, &mut ring, member).await?;
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
                            (Some(named), _) if !boundary || named.covers >= slot.get() => {
                                ReloadStart::Stored(self.select(named, &ring, None).await?)
                            }
                            (_, Some(own)) => {
                                return Err(crate::write_object::damaged(
                                    &self.path(next_slot(own.covers)?),
                                    Refusal::Parse { cause: None },
                                ))
                            }
                            // The floor names every audience that holds a part or
                            // a snapshot, so an unnamed circle holds nothing below
                            // its first kept slot (§16.4).
                            (_, None) if !boundary => {
                                ReloadStart::Empty(audience.clone(), floor.header.at)
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
                    (None, None) => ReloadStart::Empty(audience.clone(), 0),
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
            history
                .state
                .readable_audiences(&member.member())
                .into_iter()
                .filter(|audience| !required.contains(audience))
                .map(ReloadStart::Current),
        );
        let target = starts
            .iter()
            .map(|start| start.covers(session.through))
            .max()
            .expect("required audience")
            .max(slot.get());
        let (history, through) = match (through_floor, &floor) {
            (Some(later), Some(floor)) => (later, floor.header.at.max(slot.get())),
            _ => (history, slot.get()),
        };
        if let Some(newer) = self
            .install_selected(
                session,
                starts,
                ReloadInputs {
                    target,
                    history,
                    through,
                    operation: None,
                    rebuilds: Vec::new(),
                    store_history: None,
                },
                &mut ring,
                member,
                floor.as_ref(),
                false,
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
    /// the load took them, or the reload removed them (§16.2).
    async fn install_selected(
        &self,
        session: &mut Session,
        mut starts: Vec<ReloadStart>,
        inputs: ReloadInputs,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
        reporting: bool,
    ) -> Result<Option<FloorObject>, SyncError> {
        let installed = self
            .install_starts(session, &mut starts, inputs, ring, member, previous_floor, reporting)
            .await;
        release(starts, installed).await
    }

    /// Read the slots the starts need, then install them and the starts in
    /// one transaction. When `reporting`, the install reports its steps on
    /// the database's writer thread, into the same watch as the steps
    /// before it (§13.4). The load takes the starts, leaving none, as the
    /// last phase of their stagings; a return before it leaves them all.
    async fn install_starts(
        &self,
        session: &mut Session,
        starts: &mut Vec<ReloadStart>,
        inputs: ReloadInputs,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        previous_floor: Option<&FloorObject>,
        reporting: bool,
    ) -> Result<Option<FloorObject>, SyncError> {
        let ReloadInputs { target, history, through, operation, rebuilds, store_history } = inputs;
        let first = starts
            .iter()
            .map(|start| start.covers(session.through))
            .chain(store_history.iter().map(|read| read.announcement().covers))
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
                rows = body.header.merge_rows();
                (
                    History::restore(&body.originals, self.store(), ring, member)?,
                    store_snapshot.announcement().covers,
                )
            }
            None => match &store_history {
                Some(read) => (
                    History::restore(&read.body().originals, self.store(), ring, member)?,
                    read.announcement().covers,
                ),
                None => (session.history.clone(), session.through),
            },
        };
        let started = self.clock.elapsed();
        let mut bytes = 0;
        let mut slots = Vec::new();
        let mut missing = None;
        let from = next_slot(first)?;
        let range = SlotRange::new(from, target);
        let mut reads = crate::sequence_sync::SlotReads::new(self, range);
        for number in range {
            let path = self.path(number);
            let Some(input) = reads.next().await? else {
                missing = Some(number);
                break;
            };
            bytes += input.object().size;
            let received =
                slot_object::open(input.local_bytes().await?, &path, ring, &history, |audience| {
                    starts.iter().any(|start| start.audience() == audience)
                })?;
            if number.get() > session.through && number.get() > store_covers {
                self.check_copy(&received, &path).await?;
            }
            let incoming = if number.get() > store_covers {
                let incoming = at_target.consume(received, member.member());
                at_target.acquire(ring, member)?;
                incoming
            } else {
                retained_slot(received)
            };
            slots.push(incoming);
        }
        let floor = self.read_floor(previous_floor, ring, member).await?;
        if floor.as_ref().is_some_and(|floor| floor.header.first > from) {
            return Ok(floor);
        }
        if let Some(number) = missing {
            return Err(crate::write_object::damaged(
                &self.path(number),
                Refusal::Parse { cause: None },
            ));
        }
        // This floor can be newer than the target: one written after a later
        // announcement names it. It is checked against the history through
        // its `at` (§16.4, §16.5).
        if let Some(floor) = &floor {
            self.authenticate_floor_from(floor, &history, through, ring, member).await?;
        }
        let fetched = slots.len();
        let reading = self.clock.elapsed() - started;
        let authenticating = self.clock.elapsed();
        // Each start's signer, in the starts' order; only stored starts have one.
        let authors = starts
            .iter()
            .map(|start| match start {
                ReloadStart::Stored(selected) => selected.authenticate(&history).map(Some),
                ReloadStart::Current(_) | ReloadStart::Empty(..) => Ok(None),
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        self.follow_person_entries(&session.history, &at_target, member)?;
        self.keys.persist(ring)?;
        // A failure in a stored start's body is judged at its object, once a
        // second staging agrees (§19.1).
        let stored: Vec<_> = starts
            .iter()
            .filter_map(|start| match start {
                ReloadStart::Stored(selected) => {
                    Some((selected.announcement().clone(), selected.path().clone()))
                }
                ReloadStart::Current(_) | ReloadStart::Empty(..) => None,
            })
            .collect();
        let sources = std::mem::take(starts)
            .into_iter()
            .zip(authors)
            .map(|(start, author)| match (start, author) {
                (ReloadStart::Current(audience), _) => SnapshotSource::Current(audience),
                (ReloadStart::Empty(audience, covers), _) => {
                    SnapshotSource::Empty(audience, covers)
                }
                (ReloadStart::Stored(selected), Some(author)) => selected.into_source(author),
                (ReloadStart::Stored(_), None) => {
                    unreachable!("every stored start authenticated above")
                }
            })
            .collect();
        let authenticating = self.clock.elapsed() - authenticating;
        let loading = self.clock.elapsed();
        let authorship = Arc::new(SnapshotAuthorship {
            store: self.store(),
            history: history.clone(),
            ring: ring.clone(),
        });
        let loaded = self
            .database
            .load_snapshots::<PendingRecord, _>(
                SnapshotReload {
                    snapshots: sources,
                    slots,
                    target,
                    state: at_target.state.clone(),
                    cutoffs: at_target
                        .cutoffs
                        .iter()
                        .map(|cutoff| (cutoff.slot, cutoff.change.clone()))
                        .collect(),
                    boundaries: at_target.boundaries.clone(),
                    provider_access: at_target.provider_access(member.member())?,
                    operation,
                    rebuilds: rebuilds
                        .into_iter()
                        .map(|audience| {
                            let covers = match (&audience, &store_history) {
                                (Audience::Store, Some(read)) => read.announcement().covers,
                                _ => target,
                            };
                            (audience, covers)
                        })
                        .collect(),
                    authorship,
                },
                reporting.then(|| ProgressReport::of(&self.progress, self.clock.clone())),
            )
            .await;
        if let Err(error) = loaded {
            return Err(self.judge_staged_body(error, &stored, ring, SyncError::from).await);
        }
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
/// A slot at or below the store snapshot's coverage: the snapshot, or this
/// device when none is loaded, already holds its kept originals.
pub(crate) fn retained_slot(
    received: ReceivedSlot,
) -> IncomingSlot<coven_database::DownloadedWriteStream<PartPlaintext>> {
    let (received, body) = received.into_parts();
    let content = match body {
        SlotBody::Writes { size, writes } => SlotContent::Writes { size, writes },
        SlotBody::Bounded { frame: SlotFrame::Admin(_), original } => {
            SlotContent::Admin { original, effect: None }
        }
        SlotBody::Bounded { frame: SlotFrame::KeyCopy, original } => SlotContent::KeyCopy(original),
        SlotBody::Bounded { frame: SlotFrame::Announcement(_), original } => {
            SlotContent::Announcement { original, accepted: None }
        }
    };
    IncomingSlot {
        slot: received.slot(),
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
                boundary.boundary.entry().get() <= through
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
                coverage.boundary.entry().get() <= announcement.covers
                    || (announcement.purpose != Purpose::Ordinary
                        && coverage.boundary.snapshot() == &announcement.snapshot)
            })
    }
}

#[cfg(test)]
#[path = "sequence_reload_tests.rs"]
pub(crate) mod tests;
