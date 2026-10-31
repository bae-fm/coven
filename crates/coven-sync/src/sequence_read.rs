//! Read forward: the slots after the applied prefix, until one is missing,
//! then the floor, before applying any of them (§6.1, §6.2).

use super::{SequenceSync, Session, SlotReading, SlotReads};
use crate::{
    floor_object::FloorObject, slot_object, PendingRecord, Refusal, SequenceStatus, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{LoadStep, SlotContent};
use coven_foundation::progress::ProgressReport;
use coven_merge::Slot;
use coven_storage::ObjectPath;

impl SequenceSync {
    /// Exponential existence checks bracket the end; binary checks narrow it.
    /// These observations never supply data or advance the applied prefix.
    async fn locate_end(&self, start: u64) -> Result<u64, SyncError> {
        let mut low = start;
        let mut step = 1u64;
        let high = loop {
            let Some(next) = start.checked_add(step) else {
                break u64::MAX;
            };
            if self.storage.status(&self.path(next)?).await?.is_none() {
                break next;
            }
            low = next;
            let Some(next_step) = step.checked_mul(2) else {
                break u64::MAX;
            };
            step = next_step;
        };
        let mut high = high;
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if self.storage.status(&self.path(middle)?).await?.is_some() {
                low = middle;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    pub(crate) async fn read_forward(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        self.read_forward_reporting(session, ring, member, None)
            .await
    }

    /// Read forward, reporting loading's steps to `progress` when given: a
    /// new installation's floor snapshots and their install, then the slots
    /// after them. Reporting finds the end of those slots before reading
    /// them, so the Reading step has a total (§13.4).
    pub(crate) async fn read_forward_reporting(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        mut progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<(), SyncError> {
        if let SequenceStatus::Stopped(_) = &*self.state.subscribe().borrow() {
            return self.state.check();
        }
        self.resume_reload(session, ring, member).await?;
        if let Some(record) = &session.blocked {
            if !self
                .database
                .pending_can_retry::<PendingRecord>(record.subject.clone())
                .await?
            {
                return Err(SyncError::Pending(Box::new(record.clone())));
            }
        }
        // A new installation starts from the floor's snapshots whenever a
        // floor exists, even while `first` is still slot 1 (§16.4).
        if session.through == 0 {
            let floor = FloorObject::read(
                &self.database,
                self.storage.as_ref(),
                &ObjectPath::floor(self.store()),
                session.floor.as_ref(),
                ring,
                member,
            )
            .await?;
            if let Some(floor) = floor {
                self.reload_floor(session, floor, ring, member, progress.as_deref_mut())
                    .await?;
            }
        }
        loop {
            let first = session
                .through
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?;
            let mut next = first;
            let mut known_end = session.through;
            // Slots read so far, out of the end known so far, for the
            // Reading step.
            let mut reading = ReadingStep::default();
            if let Some(progress) = progress.as_deref_mut() {
                known_end = self.locate_end(session.through).await?;
                reading.count(progress, 0, known_end - session.through);
            }
            let mut virtual_history = session.history.clone();
            let mut tentative_ring = ring.clone();
            let mut slots = Vec::new();
            let mut failure = None;
            let mut timing = SlotReading::new(self.clock.elapsed());
            let floor = loop {
                let mut reads = SlotReads::new(self, next, u64::MAX);
                loop {
                    let path = self.path(next)?;
                    let fetching = self.clock.elapsed();
                    let fetched = reads.next().await;
                    timing.fetched(self.clock.elapsed() - fetching, &fetched);
                    let input = match fetched {
                        Ok(Some(input)) => input,
                        Ok(None) => {
                            if next <= known_end {
                                failure = Some((
                                    Slot(next),
                                    crate::write_object::damaged(
                                        &path,
                                        Refusal::Parse { cause: None },
                                    ),
                                ));
                            }
                            break;
                        }
                        Err(error) => {
                            failure = Some((Slot(next), error));
                            break;
                        }
                    };
                    let opening = self.clock.elapsed();
                    let received = slot_object::open(
                        input.local_bytes().await?,
                        &path,
                        &tentative_ring,
                        &virtual_history,
                        |audience| {
                            crate::effects::belongs(
                                &virtual_history.state,
                                audience,
                                &member.member(),
                            )
                        },
                    );
                    timing.opening += self.clock.elapsed() - opening;
                    let received = match received {
                        Ok(slot) => slot,
                        Err(error) => {
                            failure = Some((Slot(next), error));
                            break;
                        }
                    };
                    if let Err(error) = self.check_copy(&received, &path).await {
                        failure = Some((Slot(next), error));
                        break;
                    }
                    let metadata = !received.writes();
                    let incoming = virtual_history.consume(received, member.member());
                    if let Err(error) = virtual_history.acquire(&mut tentative_ring, member) {
                        failure = Some((Slot(next), error));
                        break;
                    }
                    slots.push((incoming, metadata.then(|| virtual_history.clone()), input));
                    known_end = known_end.max(next);
                    if slots.len() == 32 {
                        let locating = self.clock.elapsed();
                        known_end = self.locate_end(next).await?;
                        timing.locating += self.clock.elapsed() - locating;
                    }
                    if let Some(progress) = progress.as_deref_mut() {
                        reading.count(
                            progress,
                            next - session.through,
                            known_end - session.through,
                        );
                    }
                    next = next.checked_add(1).ok_or(SyncError::SequenceExhausted)?;
                }
                // The final floor observation follows every input read. A
                // floor written after the ending miss may name new slots: read
                // them before deciding that a hole or signer is missing.
                let reading_floor = self.clock.elapsed();
                let floor = FloorObject::read(
                    &self.database,
                    self.storage.as_ref(),
                    &ObjectPath::floor(self.store()),
                    session.floor.as_ref(),
                    &mut tentative_ring,
                    member,
                )
                .await?;
                timing.floor += self.clock.elapsed() - reading_floor;
                if floor
                    .as_ref()
                    .is_some_and(|floor| floor.header.first.0 > first)
                {
                    break floor;
                }
                if failure.is_none() && floor.as_ref().is_some_and(|floor| floor.header.at >= next)
                {
                    known_end = floor.as_ref().expect("observed floor").header.at;
                    if let Some(progress) = progress.as_deref_mut() {
                        reading.count(
                            progress,
                            next - 1 - session.through,
                            known_end - session.through,
                        );
                    }
                    continue;
                }
                if let Some(floor) = floor
                    .as_ref()
                    .filter(|floor| session.checked_floor.as_ref() != Some(floor.object()))
                {
                    self.authenticate_floor(floor, &virtual_history, &mut tentative_ring, member)
                        .await?;
                    session.checked_floor = Some(floor.object().clone());
                }
                break floor;
            };
            if floor
                .as_ref()
                .is_some_and(|floor| floor.header.first.0 > first)
            {
                for (_, _, input) in slots {
                    input.release().await?;
                }
                // What was read went stale: loading starts over from the
                // newer floor's snapshots.
                if let Some(progress) = progress.as_deref_mut() {
                    progress.restart();
                }
                self.reload_floor(
                    session,
                    floor.expect("floor passed first"),
                    ring,
                    member,
                    progress.as_deref_mut(),
                )
                .await?;
                continue;
            }
            // A failed keychain change fails this pass before any of its
            // slots applies; it is not a fault of the slot, so nothing blocks.
            self.follow_person_entries(&session.history, &virtual_history, member)?;
            session.floor = floor;
            let applying = self.clock.elapsed();
            let applied = slots.len();
            if let Some(progress) = progress.as_deref_mut().filter(|_| applied > 0) {
                progress.begin(LoadStep::Applying, applied as u64);
            }
            let writes: usize = slots
                .iter()
                .map(|(incoming, _, _)| match &incoming.content {
                    SlotContent::Writes { writes, .. } => writes.len(),
                    _ => 0,
                })
                .sum();
            for (incoming, history, input) in slots {
                let at = incoming.slot;
                if let Err(error) = self
                    .apply(
                        session,
                        incoming,
                        history.unwrap_or_else(|| session.history.clone()),
                        &tentative_ring,
                        member,
                    )
                    .await
                {
                    // A local failure that clears by itself, such as a locked
                    // keychain, fails only this pass; the next one applies
                    // the slot again (§19.1).
                    if error.local_failure_clears_by_itself() {
                        return Err(error);
                    }
                    return Err(self.block(session, at, error).await);
                }
                input.release().await?;
                if let Some(progress) = progress.as_deref_mut() {
                    progress.advance();
                }
            }
            if applied > 0 {
                timing.log(applied, writes, applying, self.clock.elapsed());
            }
            *ring = tentative_ring;
            if let Some((slot, error)) = failure {
                // A failed slot read supplies no bytes to apply, and a local
                // failure says nothing about the slot. A failure during
                // application, including its required reload, blocks.
                if matches!(error, SyncError::Storage(_)) || error.local_failure_clears_by_itself()
                {
                    return Err(error);
                }
                return Err(self.block(session, slot, error).await);
            }
            if next <= known_end {
                let error =
                    crate::write_object::damaged(&self.path(next)?, Refusal::Parse { cause: None });
                return Err(self.block(session, Slot(next), error).await);
            }
            session.blocked = None;
            self.check_binding(session, member)?;
            self.state.set(SequenceStatus::Synced {
                through: session.through,
            });
            return Ok(());
        }
    }
}

/// The Reading step's count: the slots read past the applied prefix, out of
/// the end known so far. The step begins once there is a slot to read, and
/// its total grows as later slots turn up (§13.4).
#[derive(Default)]
struct ReadingStep {
    total: u64,
}

impl ReadingStep {
    fn count(&mut self, progress: &mut ProgressReport<LoadStep>, read: u64, end: u64) {
        let total = self.total.max(read).max(end);
        if self.total == 0 && total > 0 {
            progress.begin(LoadStep::Reading, total);
        } else if total > self.total {
            progress.grow(total);
        }
        self.total = total;
        if read > 0 {
            progress.done(read);
        }
    }
}
