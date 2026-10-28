//! An explicit reload retains its selected bodies and common target (§16.5, E6).
use super::*;
use crate::sequence_sync::OperationState;
use coven_database::{NewOperation, OperationRecord};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum ReloadWork {
    Loading {
        target: u64,
        history_through: u64,
        selections: Vec<Selection>,
    },
    Installed,
}

#[derive(Serialize, Deserialize)]
enum Selection {
    Stored(#[serde(with = "announcement")] (Slot, SnapshotAnnouncement)),
    Current(#[serde(with = "crate::pending_encoding::audience")] Audience),
}

impl OperationState for ReloadWork {}

fn selections(
    history: &History,
    floor: Option<&FloorObject>,
    member: &MemberIdentity,
) -> Vec<Selection> {
    std::iter::once(Audience::Store)
        .chain(
            history
                .state
                .readable_circles(&member.member())
                .into_iter()
                .map(Audience::Circle),
        )
        .map(|audience| {
            let selected = match floor {
                Some(floor) => floor.header.snapshots.iter().find_map(|slot| {
                    let announcement = &history.announcements[slot];
                    (announcement.snapshot.audience == audience).then_some((*slot, announcement))
                }),
                None => history
                    .announcements
                    .iter()
                    .rev()
                    .find(|(_, announcement)| {
                        announcement.snapshot.audience == audience && history.usable(announcement)
                    })
                    .map(|(slot, announcement)| (*slot, announcement)),
            };
            match selected {
                Some((slot, announcement)) => Selection::Stored((slot, announcement.clone())),
                None => Selection::Current(audience),
            }
        })
        .collect()
}

impl SequenceSync {
    /// Reload readable audiences from selected snapshots and retained slots,
    /// preserving waiting writes. Repeated calls resume the same journal (§16.5).
    pub async fn reload_from_snapshot(&self) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        if let crate::SequenceStatus::Stopped(_) = &*self.state.subscribe().borrow() {
            return self.state.check();
        }
        let member = self
            .member
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        let record = match self.database.first_operation("sequence-reload").await? {
            Some(record) => {
                self.retry_operation(record.id).await?;
                record
            }
            None => {
                self.read_forward(&mut session, &mut ring, &member).await?;
                self.settle_open(&mut session, &mut ring, &member).await?;
                // An explicit reload chooses the newest applicable announcements
                // (§16.2). The floor selects starts only after deletion (§16.5).
                let selections = selections(&session.history, None, &member);
                if !selections
                    .iter()
                    .any(|selection| matches!(selection, Selection::Stored(_)))
                {
                    return Err(coven_storage::StorageFailure::NotFound
                        .with_source("no usable snapshot is announced for a readable audience")
                        .into());
                }
                let data = serde_json::to_vec(&ReloadWork::Loading {
                    target: session.through,
                    history_through: session.through,
                    selections,
                })?;
                let kind = "sequence-reload".to_owned();
                let started_by = "reload_from_snapshot".to_owned();
                let id = self
                    .database
                    .start_operation(NewOperation {
                        kind: kind.clone(),
                        data: data.clone(),
                        started_by: started_by.clone(),
                        pauses_writes: false,
                    })
                    .await?;
                OperationRecord {
                    id,
                    kind,
                    last_step: 0,
                    data,
                    started_by,
                }
            }
        };
        Box::pin(self.run_reload(&mut session, &mut ring, &member, record)).await
    }

    /// Finish a retained reload before advancing its applied prefix or claiming.
    pub(crate) async fn resume_reload(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if let Some(record) = self.database.first_operation("sequence-reload").await? {
            // Reload staging lives on the heap; ordinary readers and their
            // callers need not reserve its future on every stack frame.
            Box::pin(self.run_reload(session, ring, member, record)).await?;
        }
        Ok(())
    }

    async fn run_reload(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: OperationRecord,
    ) -> Result<(), SyncError> {
        if !self.operation_ready(&record, ring).await? {
            return Err(SyncError::ReloadPending(record.id));
        }
        let id = record.id;
        let scope = self.waits.begin_attempt(id);
        if let Err(error) = scope
            .run(self.advance_reload(session, ring, member, record))
            .await
        {
            self.record_operation_error(
                id,
                crate::OperationKind::ReloadFromSnapshot,
                crate::sequence_sync::OperationFailure::Returned(&error),
                ring,
            )
            .await?;
            return Err(error);
        }
        self.waits.operation_advanced(id);
        Ok(())
    }

    async fn advance_reload(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        mut record: OperationRecord,
    ) -> Result<(), SyncError> {
        let mut work: ReloadWork = serde_json::from_slice(&record.data)?;
        loop {
            let ReloadWork::Loading {
                target,
                history_through,
                selections: chosen,
            } = &work
            else {
                // Cancellation may have followed the database commit before its
                // in-memory installation. Reconstruct committed state on resume.
                session.history = History::restore(
                    &self.database.original_slots().await?,
                    self.store(),
                    ring,
                    member,
                )?;
                session.through = self.database.applied_slot().await?;
                session.floor = None;
                self.database.finish_operation(record.id).await?;
                return Ok(());
            };
            let floor = FloorObject::read(
                &self.database,
                self.storage.as_ref(),
                &ObjectPath::floor(self.store()),
                session.floor.as_ref(),
                ring,
                member,
            )
            .await?;
            let first = chosen
                .iter()
                .map(|selection| match selection {
                    Selection::Stored((_, announcement)) => announcement.covers,
                    Selection::Current(_) => session.through,
                })
                .min()
                .expect("readable store audience");
            let mut starts = Vec::new();
            let selected = async {
                if floor
                    .as_ref()
                    .is_some_and(|floor| floor.header.first.0 > first + 1)
                {
                    return Ok(Some(None));
                }
                for selection in chosen {
                    starts.push(match selection {
                        Selection::Stored((slot, announcement)) => {
                            match self.select(announcement.clone(), ring).await {
                                Ok(snapshot) => ReloadStart::Stored(snapshot),
                                Err(error)
                                    if matches!(&error, SyncError::Storage(error)
                                    if error.failure() == coven_storage::StorageFailure::NotFound)
                                        && floor.as_ref().is_some_and(|floor| {
                                            !floor.header.snapshots.contains(slot)
                                        }) =>
                                {
                                    return Ok(Some(Some((slot, announcement, error))));
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        Selection::Current(audience) => ReloadStart::Current(audience.clone()),
                    });
                }
                Ok(None)
            }
            .await;
            let missing = match selected {
                Ok(missing) => missing,
                Err(error) => return release(starts, Err(error)).await,
            };
            if let Some(missing) = missing {
                // Retention removed what this selection needs: its bodies
                // are selected again from the floor.
                release(std::mem::take(&mut starts), Ok(())).await?;
                let floor = floor.as_ref().expect("retention requires a floor");
                let (mut history, _) = self.floor_history(floor, ring, member).await?;
                let history_through = floor.header.at.max(*target);
                self.advance_history(&mut history, floor.header.at, history_through, ring, member)
                    .await?;
                let selections = selections(&history, Some(floor), member);
                if let Some((slot, announcement, error)) = missing {
                    // A missing body can move only to its authenticated
                    // replacement, never to an older floor selection (§16.5).
                    if !floor.header.snapshots.iter().any(|next_slot| {
                        let next = &history.announcements[next_slot];
                        next_slot > slot
                            && next.snapshot.audience == announcement.snapshot.audience
                            && next.covers >= announcement.covers
                    }) {
                        return Err(error);
                    }
                }
                let target = selections
                    .iter()
                    .map(|selection| match selection {
                        Selection::Stored((_, announcement)) => announcement.covers,
                        Selection::Current(_) => session.through,
                    })
                    .max()
                    .expect("readable store audience")
                    // The reselected audiences belong to this prefix;
                    // install its membership together with their rows.
                    .max(history_through);
                work = ReloadWork::Loading {
                    target,
                    history_through,
                    selections,
                };
                let update = work.update(&record)?;
                record.last_step = update.last_step;
                self.database.advance_operation(update).await?;
                continue;
            }
            let history = async {
                let mut history = if *history_through <= session.through {
                    session.history.clone()
                } else {
                    let snapshot = starts
                        .iter()
                        .find_map(|start| match start {
                            ReloadStart::Stored(snapshot)
                                if snapshot.announcement().snapshot.audience == Audience::Store =>
                            {
                                Some(snapshot)
                            }
                            _ => None,
                        })
                        .expect("floor selects a store snapshot");
                    let mut history =
                        History::restore(&snapshot.body().originals, self.store(), ring, member)?;
                    self.advance_history(
                        &mut history,
                        snapshot.announcement().covers,
                        *history_through,
                        ring,
                        member,
                    )
                    .await?;
                    history
                };
                if let Some(floor) = floor
                    .as_ref()
                    .filter(|floor| floor.header.at > *history_through)
                {
                    // A later floor can require newer authentication history without
                    // changing the selected bodies or their installation target.
                    history = self.floor_history(floor, ring, member).await?.0;
                }
                Ok(history)
            }
            .await;
            let history = match history {
                Ok(history) => history,
                Err(error) => return release(starts, Err(error)).await,
            };
            let operation = match ReloadWork::Installed.update(&record) {
                Ok(operation) => operation,
                Err(error) => return release(starts, Err(error)).await,
            };
            let inputs = ReloadInputs {
                target: *target,
                history,
                operation: Some(operation),
            };
            match self
                .install_selected(session, starts, inputs, ring, member, floor.as_ref())
                .await?
            {
                Some(newer) => session.floor = Some(newer),
                None => {
                    self.database.finish_operation(record.id).await?;
                    return Ok(());
                }
            }
        }
    }
}

mod announcement {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &(Slot, SnapshotAnnouncement),
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        (
            value.0 .0,
            value.1.encode(value.0).map_err(serde::ser::Error::custom)?,
        )
            .serialize(serializer)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<(Slot, SnapshotAnnouncement), D::Error> {
        let (slot, bytes) = <(u64, Vec<u8>)>::deserialize(deserializer)?;
        let slot = Slot(slot);
        Ok((
            slot,
            SnapshotAnnouncement::decode(&bytes, slot).map_err(serde::de::Error::custom)?,
        ))
    }
}

#[cfg(test)]
#[path = "sequence_reload_operation_tests.rs"]
mod tests;
