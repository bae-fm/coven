//! Deletion follows authenticated coverage, the finality of the entries it
//! depends on, posted positions or storage age, and ownership (§9, §15).

use super::{catalog::inconsistent, prefix::failure, snapshot_path, StoreLogSync};
use crate::SyncError;
use coven_database::{EntryOutcome, StoreDevice, StoreLog};
use coven_format::{
    store_log::StoreChange,
    value::{StorageTime, WritePositions},
};
use coven_foundation::id_source::DeviceId;
use coven_storage::{CloudProvider, ObjectPath, ObjectPrefix, StoredObject};
use dependencies::Dependencies;
use std::collections::{BTreeMap, BTreeSet};
use waits::Facts;
use witness::Threshold;

#[path = "snapshot_retention_dependencies.rs"]
pub(crate) mod dependencies;
#[path = "snapshot_retention_waits.rs"]
pub(crate) mod waits;
#[path = "snapshot_witness.rs"]
pub(crate) mod witness;

impl StoreLogSync {
    pub(super) async fn retain_snapshot_objects(&self) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        let readable = self.snapshot_audiences(&local.log)?;
        let dependencies = Dependencies::new(&local.log);
        let mut coverage = BTreeMap::new();
        // Each readable audience's current newest counting snapshot, or none,
        // once its newest is known (§15).
        let mut current = BTreeMap::new();
        let mut superseded = Vec::new();
        let mut pinned = BTreeSet::new();
        for pending in self.database.operations().await? {
            let data = crate::operation_data::Data::read(&pending)?;
            if let crate::operation_data::Data::Entry(crate::operation_data::EntryWork {
                intent: crate::operation_data::Intent::Reset { snapshot },
                ..
            }) = &data
            {
                pinned.insert(snapshot_path(self.directory.id(), snapshot)?);
            }
            // An unfinished publication's prepared attempt keeps its path,
            // numbered from the snapshot sequence, not the operation's id.
            if let crate::operation_data::Data::Snapshots(task) = &data {
                if let Some(id) = task.published() {
                    pinned.insert(snapshot_path(self.directory.id(), &id)?);
                }
            }
        }
        for boundary in local.log.boundaries() {
            let snapshot = boundary.snapshot();
            pinned.insert(snapshot_path(self.directory.id(), snapshot)?);
        }
        for audience in &readable {
            let catalog = self.snapshot_candidates(audience, &local.log).await?;
            // Log deletion judges by the current newest counting snapshot:
            // the newest verified prefix, whatever this device found in its
            // body or whether it accepts its key. A version that loads that
            // body selects it and needs every write it leaves out (§15,
            // §19.1; storelog-data `retention_ignores_bodies`).
            // A listed prefix this device has not verified, because it
            // waits or this version refused it, may be the newest for a
            // later version, and need writes the chosen snapshot covers:
            // deleting this audience's parts waits until it verifies or is
            // no longer listed (§15, §19.1; `waiting_retention_safe`,
            // storelog-data `holding_safe`, `refused_prefix_holds`). That
            // prefix's record says why. A refused body never holds.
            if catalog.newest_unknown.is_none() {
                current.insert(
                    audience.clone(),
                    catalog
                        .verified
                        .iter()
                        .find(|snapshot| snapshot.follows_boundary(&local.log))
                        .map(|newest| newest.prefix.writes.clone()),
                );
            }
            if catalog.newest_unknown.is_some() {
                tracing::debug!(
                    ?audience,
                    "unverified snapshot prefix holds back log retention"
                );
            // Coverage must hold under every boundary replay could still
            // select, including a listed raise snapshot no entry names yet
            // (§15; io `RaiseHistory.deleted_stays_covered`).
            } else if let Some(selections) = super::held::held_coverage(
                &local.log,
                audience,
                &catalog.verified,
                self.finality
                    .lock()
                    .expect("finality lock poisoned")
                    .entries_received_through(),
            ) {
                coverage.insert(audience.clone(), selections);
            }
            // An own snapshot goes once a newer one this device can load
            // covers everything it covers, and the entries that make that
            // replacement usable are final (§15).
            let candidates = catalog.candidates;
            let chosen = candidates.iter().find(|c| {
                super::boundaries::allows(
                    &local.log,
                    &c.object
                        .path
                        .snapshot_id()
                        .expect("validated snapshot path"),
                    &c.prefix,
                )
            });
            if let Some(chosen) = chosen {
                for older in &candidates {
                    if older.object.path != chosen.object.path
                        && older.object.path.device() == Some(local.device)
                        && !pinned.contains(&older.object.path)
                        && older
                            .prefix
                            .writes
                            .0
                            .iter()
                            .all(|id| chosen.prefix.writes.covers(*id))
                    {
                        superseded.push((
                            older.object.clone(),
                            Facts::for_snapshot(
                                &local.log,
                                dependencies.audience(audience).collect(),
                            ),
                        ));
                    }
                }
            }
        }
        let positions = self.retention_positions().await?;
        // Age compares storage times only: the provider time T observed
        // before this pass's reads against the object's first stored time
        // and its witnesses' landings. A device clock never establishes age;
        // it schedules the next observation through SP step 1 (§9, §15).
        let storage_time = self.storage_time();
        let witnesses = self.database.snapshot_witnesses().await?;
        let mut threshold = Threshold::default();
        let mut stuck = crate::pending_logs::local_refusals(&self.database).await?;
        let mut retained = Vec::new();
        let mut failures = Vec::new();
        let mut waits = Vec::new();
        let mut due = BTreeMap::new();
        let mut held = false;
        for object in storage
            .list(&ObjectPrefix::device_logs(self.directory.id()))
            .await?
        {
            // A waiting write holds back the rest of log retention, and the
            // first failed deletion ends this evaluation's deletions: they stay.
            if held || !failures.is_empty() {
                retained.push(object);
                continue;
            }
            let id = object
                .path
                .write_id()
                .ok_or_else(|| inconsistent("device listing has another path layout"))?;
            let owned = self.can_delete_device(&local.log, local.device, id.device)?;
            if stuck
                .iter()
                .any(|record| record.blocks(coven_database::LogObject::Write(id)))
            {
                retained.push(object);
                continue;
            }
            let header = match self.open_write_header(&object).await {
                Ok(header) => header,
                // The write's first prerequisite is this object's record.
                Err(error) if waiting(&object.path, &error) => {
                    if owned {
                        waits.push((object.path.clone(), self.pending_reason(&error).await?));
                        held = true;
                    }
                    retained.push(object);
                    continue;
                }
                // Opening the header recorded a refusal as the write's own
                // record; it stops that log here as well.
                Err(error) => {
                    if let SyncError::Damaged(damage) = &error {
                        stuck.push(coven_database::LogRefusal {
                            object: coven_database::LogObject::Write(id),
                            failure: (&damage.failure).into(),
                        });
                    }
                    failure(&object.path, error)?;
                    retained.push(object);
                    continue;
                }
            };
            // Every device asks for coverage of a final write past its reader
            // gate, owned or not, in the store and each readable part audience
            // whose current newest counting snapshot misses it, even one with
            // no snapshot (§15; io `RetentionDeadline.refreshDue`).
            let holders = witness::holders(header.parts.iter().map(|part| &part.audience));
            let gate = |time| {
                Facts::for_write(
                    &local.log,
                    &dependencies,
                    BTreeSet::new(),
                    id,
                    object.stored_at,
                    time,
                    &positions,
                )
            };
            let gated = gate(storage_time);
            threshold.age(&gated, object.stored_at);
            for audience in &holders {
                if let Some(newest) = current.get(*audience) {
                    if waits::refresh_due(&gated, true, newest.as_ref(), id) {
                        due.entry((*audience).clone()).or_insert(id);
                    }
                }
            }
            if !owned {
                retained.push(object);
                continue;
            }
            // Each holder's newest counting snapshot must cover the write
            // under every boundary replay could still select, and a counting
            // snapshot of each covering it must have landed strictly more
            // than one storage day ago: a witness kept past that snapshot's
            // supersession or deletion. Without one known, the write waits
            // for a known covering snapshot to mature (§15).
            match witness::coverage(
                holders.iter().copied(),
                id,
                &coverage,
                &witnesses,
                storage_time,
            ) {
                witness::Coverage::Mature => (),
                witness::Coverage::Missing => {
                    retained.push(object);
                    continue;
                }
                witness::Coverage::Maturing(landed) => {
                    threshold.maturity(landed);
                    retained.push(object);
                    continue;
                }
            }
            // Coverage already holds under every boundary replay could still
            // select, so no boundary's fate decides it; the removal that lets
            // this device delete another's objects does (§9, §15).
            let entries = dependencies.deleting(local.device, id.device);
            let facts = |time| {
                Facts::for_write(
                    &local.log,
                    &dependencies,
                    entries.clone(),
                    id,
                    object.stored_at,
                    time,
                    &positions,
                )
            };
            let known = facts(storage_time);
            threshold.age(&known, object.stored_at);
            match known.first_unmet() {
                None => {
                    // Another uploader's write this device never checked
                    // names files no candidate may cover yet: establish its
                    // references before it goes (§16.5).
                    let deleted = if id.device == local.device {
                        self.delete_retained(&object).await
                    } else {
                        match self.establish_references(&object, &local.log).await {
                            Ok(()) => self.delete_retained(&object).await,
                            Err(error) => Err(error),
                        }
                    };
                    if let Err(error) = deleted {
                        failures.push((object.path.clone(), error));
                        retained.push(object);
                    }
                }
                Some(prerequisite) => {
                    waits.push((
                        object.path.clone(),
                        crate::PendingReason::Waits(prerequisite),
                    ));
                    retained.push(object);
                }
            }
        }
        for (object, facts) in superseded {
            if !failures.is_empty() {
                break;
            }
            if let Some(prerequisite) = facts.first_unmet() {
                waits.push((object.path, crate::PendingReason::Waits(prerequisite)));
                continue;
            }
            if let Err(error) = self.delete_retained(&object).await {
                failures.push((object.path, error));
            }
        }
        // The next pass's first phase observes storage time once a device
        // clock says one of these ages may have been reached (SP step 1).
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .retention_threshold(threshold.0);
        if failures.is_empty() {
            self.retain_uploaded_files(&retained, &mut failures).await?;
        }
        // One request per audience, run in this pass's operation work or the
        // next; an unchanged one is the same operation, under its own backoff.
        for (audience, write) in due {
            self.start_snapshot_task(crate::snapshot_data::SnapshotJob::Write {
                audience,
                device: local.device,
                trigger: crate::snapshot_data::SnapshotTrigger::Coverage {
                    writer: write.device,
                    number: write.number,
                },
                attempt: None,
                abandoned: Vec::new(),
            })
            .await?;
        }
        self.record_retention(waits, failures).await
    }

    /// A completed evaluation's waits and failed deletions become exactly
    /// this device's `Retention { path }` records, each naming the object's
    /// first unmet condition: a path deleted since, or no longer a covered
    /// candidate, loses its record in the same transaction, and an unchanged
    /// set writes nothing. Each object stays protected, and the next
    /// evaluation derives its eligibility again (§15, §18.1, SP step 8). The
    /// first failure still fails the pass.
    async fn record_retention(
        &self,
        waits: Vec<(ObjectPath, crate::PendingReason)>,
        failures: Vec<(ObjectPath, SyncError)>,
    ) -> Result<(), SyncError> {
        let own = self.database.local_store_log().await?.device;
        let mut records = Vec::new();
        for (path, reason) in waits {
            records.push(crate::PendingRecord {
                subject: crate::PendingSubject::Retention { path },
                reason,
                reported_by: own,
            });
        }
        for (path, error) in &failures {
            records.push(crate::PendingRecord {
                subject: crate::PendingSubject::Retention { path: path.clone() },
                reason: self.pending_reason(error).await?,
                reported_by: own,
            });
        }
        self.database
            .replace_local_pending(
                |record: &crate::PendingRecord| {
                    matches!(record.subject, crate::PendingSubject::Retention { .. })
                },
                records,
            )
            .await?;
        match failures.into_iter().next() {
            Some((_, error)) => Err(error),
            None => Ok(()),
        }
    }

    /// Delete a log write or snapshot. The files its checked references named
    /// lose that protection, so they become retention candidates first: a
    /// candidate is only a reason to check, while a deletion committed
    /// without one would leave its files unconsidered (§16.5). This device's
    /// own writes and snapshots need no check: it captured them from its own
    /// rows, whose releases already made their files candidates.
    async fn delete_retained(&self, object: &StoredObject) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let evidence = crate::object_input::ObjectInput::observe(&self.database, object)
            .await?
            .files()
            .await?;
        if let Some(coven_database::FileEvidence::Complete(files)) = evidence {
            self.database.add_file_candidates(files).await?;
        }
        storage.delete(&object.path).await?;
        Ok(())
    }

    /// The newest provider time this session knows, a lower bound on storage's.
    pub(super) fn storage_time(&self) -> Option<StorageTime> {
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
    }

    pub(super) fn can_delete_device(
        &self,
        log: &StoreLog,
        own: DeviceId,
        uploader: DeviceId,
    ) -> Result<bool, SyncError> {
        dependencies::deletes_for(log, own, uploader, |device| self.entitled(log, device))
    }

    /// Whether this device may delete a removed device's objects: its own
    /// member's, or a removed member's whose storage it owns (§13).
    fn entitled(&self, log: &StoreLog, device: &StoreDevice) -> Result<bool, SyncError> {
        let me = self.operation_member()?.member();
        if device.member == me {
            return Ok(true);
        }
        let removed = log
            .replay
            .state
            .members
            .get(&device.member)
            .is_some_and(|m| m.removed);
        let provider = self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .config()
            .provider();
        let owner = match provider {
            // S3 access-key ids do not identify their provider account. The
            // creation entry identifies its member; an admin role does not.
            CloudProvider::S3 => log.entries.iter().any(|entry| {
                matches!(entry.entry.change, StoreChange::CreateStore { .. })
                    && log.replay.entries[&entry.entry.position] == EntryOutcome::Kept
                    && entry.entry.author == me
            }),
            _ => self.owns_storage(log, &me),
        };
        Ok(removed && owner)
    }

    async fn retention_positions(&self) -> Result<BTreeMap<DeviceId, WritePositions>, SyncError> {
        let local = self.database.local_store_log().await?;
        let ring = self.store_keys.read()?;
        let observation = crate::posted_positions::observe(
            &self.database,
            &self.reads,
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            &local,
            ring.as_ref(),
        )
        .await?;
        Ok(observation
            .positions
            .into_iter()
            .map(|(device, positions)| (device, positions.writes))
            .collect())
    }
}

// Retention cannot prove coverage or absence until a write's inputs arrive.
pub(super) fn waiting(path: &coven_storage::ObjectPath, error: &SyncError) -> bool {
    let waiting = matches!(
        error,
        SyncError::KeyUnavailable(_)
            | SyncError::Database(coven_database::DbError::Snapshot(
                coven_database::SnapshotError::WriteWaiting(_)
            ))
    );
    if waiting {
        tracing::debug!(
            path = path.as_str(),
            ?error,
            "waiting write holds back retention"
        );
    }
    waiting
}
