//! The rebuild of a circle, or of the store itself, this device reads below
//! its version: in a store of its own, the audience and the store as they were
//! at the slot the device's migration marker read through, migrated, read
//! forward to the applied slot, then installed as the audience's snapshot
//! (§18.3).

use super::{release, retained_slot, ReloadStart};
use crate::{
    history::History,
    sequence_sync::{next_slot, SequenceSync, Session},
    slot_object,
    snapshot_authorship::SnapshotAuthorship,
    PendingRecord, Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{RebuildDatabase, SnapshotReload, SnapshotSource};
use coven_format::{sequence::SnapshotAnnouncement, write::WriteHeader};
use coven_merge::{Audience, SlotRange};
use std::sync::Arc;

type RebuildSlot = coven_database::IncomingSlot<
    coven_database::DownloadedWriteStream<crate::chunk_reader::PartPlaintext>,
>;

/// A rebuild's failure, kept with its cause as a database failure.
fn failed(error: coven_database::RebuildError) -> SyncError {
    SyncError::Failed { failure: crate::LocalFailure::Database, source: Arc::new(error) }
}

impl SequenceSync {
    /// Rebuild every circle waiting for it whose migration marker has landed
    /// (§18.3). A rebuild that fails leaves the circle waiting, the real store
    /// as it was and nothing of the rebuild behind; the pass reports the
    /// failure and the next one runs it again.
    pub(crate) async fn resume_rebuilds(
        &self,
        session: &mut Session,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for (audience, marker) in self.database.rebuilds_waiting().await? {
            let Some((header, Some(_))) = self.database.write_header(marker).await? else {
                continue;
            };
            self.rebuild(session, ring, member, audience, header).await?;
        }
        Ok(())
    }

    async fn rebuild(
        &self,
        session: &mut Session,
        ring: &StoreKeyring,
        member: &MemberIdentity,
        audience: Audience,
        header: WriteHeader,
    ) -> Result<(), SyncError> {
        let version = session.history.state.reached(&audience);
        let starts = session.history.rebuild_starts(&audience, version, header.read_through);
        // Retention may have removed the slots after the starts while the
        // marker was in flight: a new marker reads through the slot applied
        // now, and the next pass rebuilds from it.
        // The store's own rebuild reads only the store's start.
        let own = (audience != Audience::Store).then_some(&starts.circle);
        let needed = own
            .into_iter()
            .chain([&starts.store])
            .map(|start| start.as_ref().map_or(0, |announcement| announcement.covers))
            .min()
            .unwrap_or(0)
            + 1;
        if session.floor.as_ref().is_some_and(|floor| floor.header.first.get() > needed) {
            self.database.restart_rebuilds().await?;
            return Ok(());
        }
        let mut ring = ring.clone();
        // A circle can be ahead of the store, whose newest snapshot is then
        // below the circle's version: the rebuild starts at the store's.
        let store_version = starts
            .store
            .as_ref()
            .map(|announcement| announcement.breaking_version)
            .filter(|store| *store < version && audience != Audience::Store);
        let mut rebuild = self
            .database
            .open_rebuild(audience.clone(), store_version.unwrap_or(version))
            .await
            .map_err(failed)?;
        let migrated = self
            .migrated_circle(
                session,
                &mut ring,
                member,
                &audience,
                &header,
                &starts,
                (store_version.is_some()).then_some(version),
                &mut rebuild,
            )
            .await;
        if let Err(error) = migrated {
            return match rebuild.discard().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(SyncError::cleanup(error, failed(cleanup))),
            };
        }
        let id = coven_format::admin::SnapshotId {
            audience: audience.clone(),
            device: self.device,
            number: self.database.reserve_snapshot_number().await?.get(),
        };
        let key = session.history.sending_key(&ring, member.member(), &audience)?;
        let staging = rebuild.export(id.clone(), key, &self.database).await.map_err(failed)?;
        let authorship = Arc::new(SnapshotAuthorship {
            store: self.store(),
            history: session.history.clone(),
            ring: ring.clone(),
        });
        self.database
            .install_rebuilt::<PendingRecord, crate::chunk_reader::PartPlaintext>(
                SnapshotReload {
                    snapshots: vec![SnapshotSource::Stored {
                        id,
                        input: staging,
                        author: member.member(),
                    }],
                    slots: Vec::new(),
                    target: session.through,
                    state: session.history.state.clone(),
                    cutoffs: Self::cutoffs(&session.history),
                    provider_access: session.history.provider_access(member.member())?,
                    boundaries: session.history.boundaries.clone(),
                    operation: None,
                    rebuilds: Vec::new(),
                    authorship,
                },
                audience,
                header.position,
            )
            .await?;
        Ok(())
    }

    fn cutoffs(history: &History) -> Vec<(coven_merge::Slot, coven_format::admin::StoreChange)> {
        history.cutoffs.iter().map(|cutoff| (cutoff.slot, cutoff.change.clone())).collect()
    }

    /// Load the circle and the store at the slot the marker read through into
    /// `rebuild`, migrate there under the marker's header, and read on to the
    /// applied slot.
    async fn migrated_circle(
        &self,
        session: &Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        audience: &Audience,
        header: &WriteHeader,
        starts: &crate::history_rebuild::RebuildStarts,
        circle_version: Option<u32>,
        rebuild: &mut RebuildDatabase,
    ) -> Result<(), SyncError> {
        let input = header.read_through;
        let mut selected: Vec<ReloadStart> = Vec::new();
        let first_starts = [&starts.store, &starts.circle];
        let first_starts = match circle_version {
            // The store alone, at its own version.
            Some(_) => &first_starts[..1],
            None => &first_starts[..],
        };
        for announcement in first_starts {
            let Some(announcement) = announcement else {
                continue;
            };
            match self.select_into(announcement.clone(), ring, None, Some(rebuild)).await {
                Ok(snapshot) => selected.push(ReloadStart::Stored(snapshot)),
                Err(error) => return release(selected, Err(error)).await,
            }
        }
        let stored: Vec<_> = selected
            .iter()
            .filter_map(|start| match start {
                ReloadStart::Stored(snapshot) => {
                    Some((snapshot.announcement().clone(), snapshot.path().clone()))
                }
                ReloadStart::Current(_) | ReloadStart::Empty(..) => None,
            })
            .collect();
        let first_audience = circle_version.map_or(audience, |_| &Audience::Store);
        let reloaded = self
            .first_reload(session, ring, member, first_audience, starts, input, &mut selected)
            .await;
        let (reload, mut at) = match reloaded {
            Ok(reloaded) => reloaded,
            Err(error) => return release(selected, Err(error)).await,
        };
        if let Err(error) = rebuild.load::<PendingRecord, _>(reload, starts.store_through).await {
            return Err(self.judge_staged_body(error, &stored, ring, SyncError::from).await);
        }
        if let Some(version) = circle_version {
            rebuild.migrate_store_to(audience.clone(), version).await.map_err(failed)?;
            self.load_circle_over_store(
                session, ring, member, audience, starts, &at, input, rebuild,
            )
            .await?;
        }
        rebuild.migrate(header.clone()).await.map_err(failed)?;
        let target = session.through;
        let mut slots = Vec::new();
        self.read_rebuild_slots(
            session, ring, member, audience, &mut at, 0, input, target, true, &mut slots,
        )
        .await?;
        let forward = SnapshotReload {
            snapshots: std::iter::once(Audience::Store)
                .chain((*audience != Audience::Store).then(|| audience.clone()))
                .map(SnapshotSource::Current)
                .collect(),
            slots,
            target,
            state: at.state.clone(),
            cutoffs: Self::cutoffs(&at),
            provider_access: Vec::new(),
            boundaries: at.boundaries.clone(),
            operation: None,
            rebuilds: Vec::new(),
            authorship: Arc::new(SnapshotAuthorship {
                store: self.store(),
                history: session.history.clone(),
                ring: ring.clone(),
            }),
        };
        rebuild
            .load::<PendingRecord, _>(forward, starts.store_through_at(target))
            .await
            .map_err(SyncError::from)?;
        Ok(())
    }

    /// Load the circle into a rebuild whose store already holds the store at
    /// the circle's version: the circle's newest snapshot and the circle's
    /// parts in the slots after it, through the slot the store was read to.
    async fn load_circle_over_store(
        &self,
        session: &Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        audience: &Audience,
        starts: &crate::history_rebuild::RebuildStarts,
        at: &History,
        input: u64,
        rebuild: &mut RebuildDatabase,
    ) -> Result<(), SyncError> {
        let mut snapshots = vec![SnapshotSource::Current(Audience::Store)];
        let mut stored = Vec::new();
        let mut covers = 0;
        match &starts.circle {
            Some(announcement) => {
                covers = announcement.covers;
                let snapshot =
                    self.select_into(announcement.clone(), ring, None, Some(rebuild)).await?;
                let author = match snapshot.authenticate(&session.history) {
                    Ok(author) => author,
                    Err(error) => {
                        return release(vec![ReloadStart::Stored(snapshot)], Err(error)).await
                    }
                };
                stored.push((snapshot.announcement().clone(), snapshot.path().clone()));
                snapshots.push(snapshot.into_source(author));
            }
            None => snapshots.push(SnapshotSource::Empty(audience.clone())),
        }
        let mut slots = Vec::new();
        let mut at = at.clone();
        self.read_rebuild_slots(
            session,
            ring,
            member,
            audience,
            &mut at,
            u64::MAX,
            covers,
            input,
            false,
            &mut slots,
        )
        .await?;
        let reload = SnapshotReload {
            snapshots,
            slots,
            target: input,
            state: at.state.clone(),
            cutoffs: Self::cutoffs(&at),
            provider_access: Vec::new(),
            boundaries: at.boundaries.clone(),
            operation: None,
            rebuilds: Vec::new(),
            authorship: Arc::new(SnapshotAuthorship {
                store: self.store(),
                history: session.history.clone(),
                ring: ring.clone(),
            }),
        };
        if let Err(error) = rebuild.load::<PendingRecord, _>(reload, starts.store_through).await {
            return Err(self.judge_staged_body(error, &stored, ring, SyncError::from).await);
        }
        Ok(())
    }

    /// The input to the first load: the starts' stagings, the slots after
    /// them through `input`, and the history at `input`, which the forward
    /// load continues from.
    async fn first_reload(
        &self,
        session: &Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        audience: &Audience,
        starts: &crate::history_rebuild::RebuildStarts,
        input: u64,
        selected: &mut Vec<ReloadStart>,
    ) -> Result<
        (
            SnapshotReload<
                coven_database::SnapshotStaging,
                coven_database::DownloadedWriteStream<crate::chunk_reader::PartPlaintext>,
            >,
            History,
        ),
        SyncError,
    > {
        let store_start = selected.iter().find_map(|start| match start {
            ReloadStart::Stored(snapshot)
                if snapshot.announcement().snapshot.audience == Audience::Store =>
            {
                Some(snapshot)
            }
            _ => None,
        });
        let (mut at, store_covers) = match store_start {
            Some(snapshot) => (
                History::restore(&snapshot.body().originals, self.store(), ring, member)?,
                snapshot.announcement().covers,
            ),
            None => (History::default(), 0),
        };
        // The store's own rebuild has no circle: its slots start where the
        // store's snapshot ends.
        let circle_covers = if *audience == Audience::Store {
            store_covers
        } else {
            starts.circle.as_ref().map_or(0, |a: &SnapshotAnnouncement| a.covers)
        };
        let mut slots = Vec::new();
        self.read_rebuild_slots(
            session,
            ring,
            member,
            audience,
            &mut at,
            store_covers,
            circle_covers.min(starts.store.as_ref().map_or(0, |a| a.covers)),
            input,
            true,
            &mut slots,
        )
        .await?;
        let authors = selected
            .iter()
            .map(|start| match start {
                ReloadStart::Stored(snapshot) => snapshot.authenticate(&session.history).map(Some),
                ReloadStart::Current(_) | ReloadStart::Empty(..) => Ok(None),
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        let mut snapshots = Vec::new();
        if starts.store.is_none() {
            snapshots.push(SnapshotSource::Empty(Audience::Store, 0));
        }
        if starts.circle.is_none() && *audience != Audience::Store {
            snapshots.push(SnapshotSource::Empty(audience.clone(), 0));
        }
        for (start, author) in std::mem::take(selected).into_iter().zip(authors) {
            match (start, author) {
                (ReloadStart::Stored(snapshot), Some(author)) => {
                    snapshots.push(snapshot.into_source(author));
                }
                _ => unreachable!("every selected start is stored and authenticated"),
            }
        }
        // The starts were taken; a failure from here on has nothing to release.
        let reload = SnapshotReload {
            snapshots,
            slots,
            target: input,
            state: at.state.clone(),
            cutoffs: Self::cutoffs(&at),
            provider_access: Vec::new(),
            boundaries: at.boundaries.clone(),
            operation: None,
            rebuilds: Vec::new(),
            authorship: Arc::new(SnapshotAuthorship {
                store: self.store(),
                history: session.history.clone(),
                ring: ring.clone(),
            }),
        };
        Ok((reload, at))
    }

    /// The slots after `first` through `last`, opened for the circle and the
    /// store, and consumed into `at` where the store's snapshot, which holds
    /// the kept originals through `store_covers`, doesn't.
    async fn read_rebuild_slots(
        &self,
        session: &Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        audience: &Audience,
        at: &mut History,
        store_covers: u64,
        first: u64,
        last: u64,
        store_parts: bool,
        slots: &mut Vec<RebuildSlot>,
    ) -> Result<(), SyncError> {
        let range = SlotRange::new(next_slot(first)?, last);
        let mut reads = crate::sequence_sync::SlotReads::new(self, range);
        for number in range {
            let path = self.path(number);
            let input = reads.next().await?.ok_or_else(|| {
                crate::write_object::damaged(&path, Refusal::Parse { cause: None })
            })?;
            let received = slot_object::open(
                input.local_bytes().await?,
                &path,
                ring,
                &session.history,
                |opened| (store_parts && *opened == Audience::Store) || opened == audience,
            )?;
            slots.push(if number.get() > store_covers {
                let incoming = at.consume(received, member.member());
                at.acquire(ring, member)?;
                incoming
            } else {
                retained_slot(received)
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_reload_rebuild_tests.rs"]
mod tests;
