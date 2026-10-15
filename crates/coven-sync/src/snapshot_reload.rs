//! Choose each audience's start and read to their common point
//! (`snapshot_common_point.rs`), download, then replace all selected
//! audiences and the journal together.

use super::catalog::{inconsistent, Catalog};
use super::common_point::{common_point, Origin, Outcome, Reads, Start, Unknown};
use super::{io, prefix::Candidate, StoreLogSync};
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    replay_cache::ReplayCache,
    snapshot_data::*,
    SyncError,
};
use coven_database::{
    DownloadedPartStream, DownloadedWriteStream, OperationRecord, SnapshotReload, SnapshotSource,
    StoreLog,
};
use coven_format::{
    sealed_snapshot::SnapshotObjectPrefix, value::WritePositions, write_stream::WriteHeaderFrame,
};
use coven_merge::{Audience, WriteId};
use coven_storage::StoredObject;
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    pub(super) async fn reload_snapshot_step(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
    ) -> Result<Progress, SyncError> {
        match record.last_step {
            0 => {
                self.clear_snapshot_files(record, &mut task).await?;
                let local = self.database.local_store_log().await?;
                self.check_stopped(&local, &self.operation_member()?)?;
                let readable = self.snapshot_audiences(&local.log)?;
                let mut audiences = readable.clone();
                let SnapshotJob::Reload { scope, .. } = &task.job else {
                    unreachable!()
                };
                if let ReloadScope::Changed(changed) = scope {
                    let required = self.database.reloading_audiences().await?;
                    audiences.retain(|audience| {
                        changed.contains(audience) && required.contains(audience)
                    });
                }
                if audiences.is_empty() {
                    self.database.finish_operation(record.id).await?;
                    return Ok(Progress::Finished(Output::Unit));
                }
                let schema = self.database.schema_version().await?;
                if let Some((_, version)) =
                    local
                        .log
                        .replay
                        .state
                        .schema
                        .iter()
                        .find(|(audience, version)| {
                            audiences.contains(audience) && version.number > schema
                        })
                {
                    return Err(SyncError::UpdateRequired(
                        crate::RequiredUpdate::AppSchema {
                            version: version.number,
                        },
                    ));
                }
                let boundaries = self.snapshot_boundaries(&local.log).await?;
                let expected_entries = local
                    .log
                    .entries
                    .iter()
                    .map(|e| (e.entry.position.device, e.entry.position.number))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let mut catalogs = Vec::new();
                for audience in &audiences {
                    catalogs.push((
                        audience,
                        self.snapshot_candidates(audience, &local.log).await?,
                    ));
                }
                // A newer snapshot may be the one to load: the reload waits
                // for the update rather than choosing without it.
                if let Some(update) = catalogs.iter().find_map(|(_, catalog)| catalog.update) {
                    return Err(SyncError::UpdateRequired(update));
                }
                // Each audience loads its current newest counting snapshot:
                // the newest verified prefix that follows its boundary,
                // whatever this device found in its body or key. One it cannot
                // load makes the reload wait, never pass it over for an older
                // one whose coverage would skip a deleted write's part in that
                // audience (§15, §19.1; storelog-data `ReloadWait.choose`,
                // `deleted_part_never_skipped`).
                let mut starts = BTreeMap::new();
                let mut unknown_empty = BTreeSet::new();
                for (audience, mut catalog) in catalogs {
                    let (positions, origin) = match catalog.take_current(&local.log) {
                        Ok(Some(candidate)) => {
                            (candidate.prefix.writes.clone(), Origin::Selected(candidate))
                        }
                        Err(reason) => {
                            return Err(SyncError::Pending(Box::new(
                                self.operation_condition(record, reason).await?,
                            )))
                        }
                        Ok(None) if self.has_snapshot_boundary(&local.log, audience) => {
                            return Err(inconsistent(
                                "no valid snapshot follows the selected reset or version boundary",
                            ))
                        }
                        Ok(None) => {
                            if catalog.newest_unknown.is_some() {
                                unknown_empty.insert(audience.clone());
                            }
                            (WritePositions(Vec::new()), Origin::Empty)
                        }
                    };
                    let start = Start {
                        positions,
                        selected_at: 0,
                        origin,
                    };
                    starts.insert(audience.clone(), start);
                }
                // Unchanged audiences keep their state at the local positions,
                // which also join the frozen requirement (`committed_safe`).
                if audiences.len() < readable.len() {
                    let positions = self.database.sync_state(Vec::new()).await?.positions;
                    for audience in readable.difference(&audiences) {
                        let start = Start {
                            positions: positions.clone(),
                            selected_at: 0,
                            origin: Origin::Kept,
                        };
                        starts.insert(audience.clone(), start);
                    }
                }
                let (required, waiting) = self.database.reload_positions().await?;
                let waiting: BTreeSet<_> = waiting.into_iter().collect();
                let mut reads = ReloadReads {
                    sync: self,
                    record,
                    log: &local.log,
                    readable: &readable,
                    refused: crate::pending_logs::local_refusals(&self.database).await?,
                    objects: BTreeMap::new(),
                    catalogs: BTreeMap::new(),
                };
                let plan = match common_point(&mut reads, &local.log, starts, &required, &waiting)
                    .await?
                {
                    Outcome::Complete(plan) => plan,
                    // Unresolved history waits for the write, or for a snapshot
                    // showing its absence is a deletion (§15).
                    Outcome::Unpublished(id) => {
                        let path = crate::write_seal::path(self.directory.id(), id);
                        let reason = crate::PendingReason::Waits(crate::Prerequisite::Object(path));
                        return Err(SyncError::Pending(Box::new(
                            self.operation_condition(record, reason).await?,
                        )));
                    }
                    Outcome::Unknown(Unknown::Update(update)) => {
                        return Err(SyncError::UpdateRequired(update))
                    }
                    Outcome::Unknown(Unknown::Unverified(path)) => {
                        let reason = crate::PendingReason::Waits(crate::Prerequisite::Object(path));
                        return Err(SyncError::Pending(Box::new(
                            self.operation_condition(record, reason).await?,
                        )));
                    }
                };
                // An unverified prefix may cover deleted history. An empty
                // audience is established only while some log still starts
                // at its first write.
                let empty_unknown = plan.starts.iter().any(|(audience, start)| {
                    matches!(start.origin, Origin::Empty) && unknown_empty.contains(audience)
                });
                if empty_unknown && plan.present.is_empty() {
                    let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
                    let mut present = false;
                    for device in local.log.replay.state.devices.keys() {
                        let first = WriteId {
                            device: *device,
                            number: 1,
                        };
                        let path = crate::write_seal::path(self.directory.id(), first);
                        if storage.status(&path).await?.is_some() {
                            present = true;
                            break;
                        }
                    }
                    if !present {
                        return Err(inconsistent("unreadable snapshot prefixes and missing logs cannot establish an empty audience"));
                    }
                }
                let objects = std::mem::take(&mut reads.objects);
                let mut files = ReloadFiles {
                    snapshots: Vec::new(),
                    empty: Vec::new(),
                    absent: plan
                        .deleted
                        .iter()
                        .map(|id| (id.device, id.number))
                        .collect(),
                    writes: Vec::new(),
                    boundaries,
                    expected_entries,
                };
                let starts: BTreeMap<_, _> = plan
                    .starts
                    .iter()
                    .map(|(audience, start)| (audience.clone(), start.positions.clone()))
                    .collect();
                let mut selections = Vec::new();
                for (audience, start) in plan.starts {
                    match start.origin {
                        Origin::Selected(candidate) => selections.push(candidate),
                        Origin::Empty => files.empty.push(audience),
                        Origin::Kept => (),
                    }
                }
                let mut names = self
                    .reserve_snapshot_files(record, &mut task, selections.len())
                    .await?
                    .into_iter();
                for candidate in selections {
                    let file = names.next().expect("reserved snapshot files");
                    match self.download_snapshot(candidate, file).await? {
                        Ok(snapshot) => files.snapshots.push(snapshot),
                        Err(reason) => {
                            return Err(SyncError::Pending(Box::new(
                                self.operation_condition(record, reason).await?,
                            )))
                        }
                    }
                }
                // The committing round's own reads: each write some audience
                // lacks, retained as checked bytes until the commit. Only the
                // parts of audiences whose start lacks the write are opened.
                let needed: Vec<_> = plan
                    .present
                    .iter()
                    .map(|id| {
                        let opens: BTreeSet<_> = readable
                            .iter()
                            .filter(|audience| {
                                !starts.get(*audience).is_some_and(|start| start.covers(*id))
                            })
                            .cloned()
                            .collect();
                        (objects[id].clone(), opens)
                    })
                    .collect();
                let mut replays = ReplayCache::new(&local.log);
                let mut count = 0;
                for (object, opens) in &needed {
                    match self.open_write_header(object).await {
                        Ok(header) => {
                            count += header
                                .parts
                                .iter()
                                .filter(|part| opens.contains(&part.audience))
                                .count()
                        }
                        // The recorded stop changes which optional tail writes can
                        // be downloaded. Select again before installing any rows.
                        Err(SyncError::Damaged(_)) => return Ok(Progress::Advanced),
                        Err(error) => return Err(error),
                    }
                }
                let mut names = self
                    .reserve_snapshot_files(record, &mut task, count)
                    .await?
                    .into_iter();
                for (object, opens) in &needed {
                    match self
                        .open_snapshot_write(object, &local.log, &mut replays, opens, &mut names)
                        .await
                    {
                        Ok(write) => files.writes.push(write),
                        Err(SyncError::Damaged(_)) => return Ok(Progress::Advanced),
                        Err(error) => return Err(error),
                    }
                }
                let SnapshotJob::Reload { files: saved, .. } = &mut task.job else {
                    unreachable!()
                };
                *saved = Some(files);
                self.save_snapshot_task(record, &task, 1).await?;
            }
            1 => {
                let SnapshotJob::Reload {
                    files: Some(files), ..
                } = &task.job
                else {
                    return Err(coven_database::DbError::Invariant {
                        what: "a reload past its first step has its files",
                    }
                    .into());
                };
                let mut snapshots = Vec::new();
                for snapshot in &files.snapshots {
                    snapshots.push(SnapshotSource::Stored {
                        id: snapshot.object.path.snapshot_id().ok_or_else(|| {
                            inconsistent("recorded snapshot path has another layout")
                        })?,
                        prefix: SnapshotObjectPrefix::decode(&snapshot.prefix)?,
                        input: io::SnapshotInput::open(&self.directory, &snapshot.file)?,
                        author: snapshot.author,
                    });
                }
                snapshots.extend(files.empty.iter().cloned().map(SnapshotSource::Empty));
                let writes = files
                    .writes
                    .iter()
                    .map(|write| self.open_saved_write(write))
                    .collect::<Result<Vec<_>, _>>()?;
                let reload = SnapshotReload {
                    snapshots,
                    writes,
                    absent: files
                        .absent
                        .iter()
                        .map(|(device, number)| WriteId {
                            device: *device,
                            number: *number,
                        })
                        .collect(),
                    boundaries: Some(
                        files
                            .boundaries
                            .iter()
                            .map(super::boundaries::decode)
                            .collect::<Result<_, _>>()?,
                    ),
                    expected_entries: Some(
                        files
                            .expected_entries
                            .iter()
                            .map(|(device, number)| coven_format::value::EntryId {
                                device: *device,
                                number: *number,
                            })
                            .collect(),
                    ),
                    operation: Some(Data::Snapshots(task.clone()).update(record, 2)?),
                    authorship: self.evidence_keys().await?,
                };
                // Whether or not it commits, earlier write misses and the
                // catch-up they completed no longer describe this device (§15).
                self.reads.reloaded();
                let result = match self
                    .database
                    .load_snapshots::<crate::PendingRecord, _, _>(reload)
                    .await
                {
                    Err(coven_database::DbError::Snapshot(
                        coven_database::SnapshotError::Write { write, error },
                    )) => {
                        match crate::write_object::database_failure(
                            &crate::write_seal::path(self.directory.id(), write),
                            *error,
                        ) {
                            SyncError::Damaged(damage) => {
                                // Its complete authenticated bytes were staged before
                                // the atomic reload refused this particular write.
                                crate::pending_logs::record_refusal(
                                    &self.database,
                                    coven_database::LogRefusal {
                                        object: coven_database::LogObject::Write(write),
                                        failure: (&damage.failure).into(),
                                    },
                                )
                                .await?;
                                tracing::warn!(?write, error = %damage, "reload write log is stuck");
                                let SnapshotJob::Reload { files, .. } = &mut task.job else {
                                    unreachable!()
                                };
                                *files = None;
                                self.save_snapshot_task(record, &task, 0).await?;
                                return Ok(Progress::Advanced);
                            }
                            SyncError::Database(error) => Err(error),
                            error => return Err(error),
                        }
                    }
                    Err(coven_database::DbError::Snapshot(
                        coven_database::SnapshotError::Body { snapshot, error },
                    )) => {
                        let object = files
                            .snapshots
                            .iter()
                            .find(|saved| {
                                saved.object.path.snapshot_id().as_ref() == Some(&snapshot)
                            })
                            .ok_or(coven_database::DbError::Invariant {
                                what: "a reload's failed snapshot is one it supplied",
                            })?
                            .object
                            .clone();
                        // Nothing loaded. Retry selects again: the refused
                        // body is then waited on, never reloaded.
                        let SnapshotJob::Reload { files, .. } = &mut task.job else {
                            unreachable!()
                        };
                        *files = None;
                        self.save_snapshot_task(record, &task, 0).await?;
                        // A schema outside the app's supported range fails the
                        // caller, as does a snapshot made under another
                        // retirement verdict: §15 prepares only final verdicts,
                        // so it means a damaged snapshot or database (§10). Any
                        // other verdict on the body refuses it, and the reload
                        // waits on that snapshot rather than passing it over for
                        // an older one (§15, §19.1; storelog-data
                        // `ReloadWait.choose`).
                        let error = match *error {
                            error @ coven_database::DbError::Snapshot(
                                coven_database::SnapshotError::Schema { .. }
                                | coven_database::SnapshotError::RetirementVerdict { .. },
                            ) => return Err(error.into()),
                            // Another member's write the snapshot carries is
                            // sealed with a key, or names store-log entries,
                            // that have not arrived: it waits, unrefused (§8.8).
                            coven_database::DbError::Snapshot(
                                coven_database::SnapshotError::KeyUnavailable { audience, key },
                            ) => {
                                let reason = crate::PendingReason::KeyUnavailable { audience, key };
                                return Err(SyncError::Pending(Box::new(
                                    self.operation_condition(record, reason).await?,
                                )));
                            }
                            error @ coven_database::DbError::Snapshot(
                                coven_database::SnapshotError::StoreLog { .. },
                            ) => return Err(error.into()),
                            error => error,
                        };
                        self.refuse_snapshot(&object, error.into()).await?;
                        let reason =
                            crate::PendingReason::Waits(crate::Prerequisite::Object(object.path));
                        return Err(SyncError::Pending(Box::new(
                            self.operation_condition(record, reason).await?,
                        )));
                    }
                    result => result,
                };
                match result {
                    Err(coven_database::DbError::StoreLogEntriesChanged) => {
                        // No database state changed: discard this selection and
                        // download against the new replay before trying to commit.
                        let SnapshotJob::Reload { files, .. } = &mut task.job else {
                            unreachable!()
                        };
                        *files = None;
                        self.save_snapshot_task(record, &task, 0).await?;
                    }
                    Err(
                        error @ coven_database::DbError::Snapshot(
                            coven_database::SnapshotError::MissingWrites { .. },
                        ),
                    ) => {
                        // A write assumed to remain queued can finish uploading
                        // during the download. Retry must select and fetch
                        // again, rather than repeatedly loading the same stale
                        // inputs.
                        let SnapshotJob::Reload { files, .. } = &mut task.job else {
                            unreachable!()
                        };
                        *files = None;
                        self.save_snapshot_task(record, &task, 0).await?;
                        return Err(error.into());
                    }
                    result => {
                        // The parse that loaded each body also found the files
                        // it names; file retention reuses them (§16.5).
                        for loaded in result? {
                            let saved = files
                                .snapshots
                                .iter()
                                .find(|saved| {
                                    saved.object.path.snapshot_id().as_ref() == Some(&loaded.id)
                                })
                                .ok_or(coven_database::DbError::Invariant {
                                    what: "a reload's loaded snapshot is one it supplied",
                                })?;
                            crate::object_input::ObjectInput::observe(
                                &self.database,
                                &saved.object,
                            )
                            .await?
                            .keep_files(coven_database::FileEvidence::Complete(loaded.files))
                            .await?;
                        }
                    }
                }
            }
            2 => {
                let SnapshotJob::Reload {
                    files: Some(files), ..
                } = &task.job
                else {
                    return Err(coven_database::DbError::Invariant {
                        what: "a reload past its first step has its files",
                    }
                    .into());
                };
                let mut paths: Vec<_> = files
                    .snapshots
                    .iter()
                    .map(|s| s.object.path.as_str().to_owned())
                    .collect();
                for write in &files.writes {
                    let header = WriteHeaderFrame::decode(&write.header)?;
                    paths.push(
                        crate::write_seal::path(self.directory.id(), header.header.position)
                            .as_str()
                            .to_owned(),
                    );
                }
                self.database.release_input_ranges(paths).await?;
                self.clear_snapshot_files(record, &mut task).await?;
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
            _ => {
                return Err(coven_database::DbError::Invariant {
                    what: "a reload has three steps",
                }
                .into())
            }
        }
        Ok(Progress::Advanced)
    }
    pub(super) fn open_saved_write(
        &self,
        write: &SavedWrite,
    ) -> Result<DownloadedWriteStream<io::SnapshotInput>, SyncError> {
        let parts = write
            .parts
            .iter()
            .map(|file| match file {
                Some(file) => Ok(DownloadedPartStream::Opened(io::SnapshotInput::open(
                    &self.directory,
                    file,
                )?)),
                None => Ok(DownloadedPartStream::Skipped),
            })
            .collect::<Result<_, SyncError>>()?;
        let header = WriteHeaderFrame::decode(&write.header)?;
        let opens: Vec<_> = write.parts.iter().map(Option::is_some).collect();
        let ring = self
            .store_keys
            .read()?
            .ok_or_else(|| coven_database::DbError::Invariant {
                what: "a saved reload write was opened with held keys",
            })?;
        let authorship =
            crate::write_object::authorship(&ring, write.evidence.evidence()?, &header, &opens)?;
        Ok(DownloadedWriteStream {
            header,
            parts,
            authorship,
            published: coven_database::Published {
                stored_at: write.stored_at,
                signer: coven_crypto::SigningKey::from_bytes(write.signer)
                    .map_err(|_| coven_database::DbError::DamagedDatabase)?,
            },
        })
    }
}

/// A reload's reads of storage: exact write statuses and fresh catalogs.
struct ReloadReads<'a> {
    sync: &'a StoreLogSync,
    record: &'a OperationRecord,
    log: &'a StoreLog,
    readable: &'a BTreeSet<Audience>,
    /// This device's permanent log refusals.
    refused: Vec<coven_database::LogRefusal>,
    /// Each present write's object, from its latest status.
    objects: BTreeMap<WriteId, StoredObject>,
    /// The readable audiences' catalogs from the latest refresh.
    catalogs: BTreeMap<Audience, Catalog>,
}

impl Reads for ReloadReads<'_> {
    type Body = Candidate;

    async fn present(&mut self, id: WriteId) -> Result<bool, SyncError> {
        // The reload waits on the refused write, whose own record is the
        // refusal. Recording the refusal here would retry only after an
        // update, so a snapshot covering the gap would never be tried (§15;
        // storelog-data `refused_write_reload_counterexample`,
        // `waiting_on_the_write_loads`).
        let blocked = self
            .refused
            .iter()
            .find_map(|refusal| match refusal.object {
                coven_database::LogObject::Write(refused)
                    if refusal.blocks(coven_database::LogObject::Write(id)) =>
                {
                    Some(refused)
                }
                _ => None,
            });
        if let Some(refused) = blocked {
            let reason = crate::PendingReason::Waits(crate::Prerequisite::Object(
                crate::write_seal::path(self.sync.directory.id(), refused),
            ));
            return Err(SyncError::Pending(Box::new(
                self.sync.operation_condition(self.record, reason).await?,
            )));
        }
        let storage = self.sync.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let path = crate::write_seal::path(self.sync.directory.id(), id);
        Ok(match storage.status(&path).await? {
            Some(object) => {
                self.objects.insert(id, object);
                true
            }
            None => {
                self.objects.remove(&id);
                false
            }
        })
    }

    async fn refresh(&mut self) -> Result<super::common_point::Fresh, SyncError> {
        let (fresh, catalogs) = self.sync.fresh_catalog(self.readable, self.log).await?;
        self.catalogs = catalogs;
        Ok(fresh)
    }

    async fn select(&mut self, audience: &Audience) -> Result<Candidate, SyncError> {
        let catalog = self
            .catalogs
            .get_mut(audience)
            .expect("a replaced audience is readable");
        match catalog.take_current(self.log) {
            Ok(Some(candidate)) => Ok(candidate),
            Ok(None) => Err(inconsistent("a replacement has no current snapshot")),
            Err(reason) => Err(SyncError::Pending(Box::new(
                self.sync.operation_condition(self.record, reason).await?,
            ))),
        }
    }
}

#[cfg(test)]
#[path = "snapshot_reload_tests.rs"]
mod tests;
