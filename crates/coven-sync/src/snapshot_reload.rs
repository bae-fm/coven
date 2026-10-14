//! Download first, then replace all selected audiences and the journal together.

use super::{catalog::inconsistent, io, point, StoreLogSync};
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    replay_cache::ReplayCache,
    snapshot_data::*,
    SyncError,
};
use coven_database::{
    DownloadedPartStream, DownloadedWriteStream, OperationRecord, SnapshotReload, SnapshotSource,
};
use coven_format::{sealed_snapshot::SnapshotObjectPrefix, write_stream::WriteHeaderFrame};
use coven_merge::WriteId;
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
                let mut files = ReloadFiles {
                    snapshots: Vec::new(),
                    empty: Vec::new(),
                    absent: Vec::new(),
                    writes: Vec::new(),
                    boundaries: self.snapshot_boundaries(&local.log).await?,
                    expected_entries: local
                        .log
                        .entries
                        .iter()
                        .map(|e| (e.entry.position.device, e.entry.position.number))
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                };
                let mut highest = BTreeMap::new();
                let mut unknown_empty = false;
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
                // audience (§15; storelog-data `missingDecision`,
                // `holding_never_skips`).
                let mut selections = Vec::new();
                for (audience, mut catalog) in catalogs {
                    super::include_positions(&mut highest, &catalog.required_positions.0);
                    let candidate = match catalog.take_current(&local.log) {
                        Ok(Some(candidate)) => candidate,
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
                            unknown_empty |= catalog.newest_unknown;
                            files.empty.push(audience.clone());
                            continue;
                        }
                    };
                    selections.push(candidate);
                }
                let mut names = self
                    .reserve_snapshot_files(record, &mut task, selections.len())
                    .await?
                    .into_iter();
                for candidate in selections {
                    let file = names.next().expect("reserved snapshot files");
                    match self.load_snapshot(candidate, file).await? {
                        Ok(snapshot) => files.snapshots.push(snapshot),
                        Err(reason) => {
                            return Err(SyncError::Pending(Box::new(
                                self.operation_condition(record, reason).await?,
                            )))
                        }
                    }
                }
                let selected = files
                    .snapshots
                    .iter()
                    .map(|snapshot| {
                        SnapshotObjectPrefix::decode(&snapshot.prefix).map(|p| p.writes)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut positions = selected.clone();
                // Unchanged audiences keep their local frontier. Download the
                // shared headers and any still-unread parts needed to bring
                // those audiences through the selected snapshots' positions.
                if audiences.len() < readable.len() {
                    positions.push(self.database.sync_state(Vec::new()).await?.positions);
                }
                let (required, waiting) = self.database.reload_positions().await?;
                let waiting: BTreeSet<_> = waiting.into_iter().collect();
                super::include_positions(
                    &mut highest,
                    positions.iter().flat_map(|p| &p.0).chain(&required.0),
                );
                let stuck = crate::pending_logs::local_refusals(&self.database).await?;
                let readable = readable.into_iter().collect();
                let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
                let mut replays = ReplayCache::new(&local.log);
                let mut needed = Vec::new();
                for (device, highest) in highest {
                    let lowest = if files.empty.is_empty() {
                        positions
                            .iter()
                            .map(|p| point(p, device))
                            .min()
                            .expect("a readable store audience")
                    } else {
                        0
                    };
                    for previous in lowest..highest {
                        let number = previous + 1;
                        let id = WriteId { device, number };
                        if waiting.contains(&id) {
                            continue;
                        }
                        if let Some(record) = stuck
                            .iter()
                            .find(|record| record.blocks(coven_database::LogObject::Write(id)))
                        {
                            return Err(
                                crate::pending_logs::required(&self.database, *record).await?
                            );
                        }
                        // Read to the fixed target by exact number, never a
                        // listing of the moving log tail.
                        let path = crate::write_seal::path(self.directory.id(), id);
                        match storage.status(&path).await? {
                            Some(object) => needed.push(object),
                            // Only a selected snapshot accounts for a missing
                            // write. The local frontier passed it for unchanged
                            // audiences, while a returning circle may have
                            // skipped its part there (§14.4).
                            None if selected.iter().any(|p| p.covers(id)) => {
                                files.absent.push((device, number))
                            }
                            None => {
                                return Err(coven_database::DbError::Snapshot(
                                    coven_database::SnapshotError::MissingWrites {
                                        missing: vec![id],
                                    },
                                )
                                .into())
                            }
                        }
                    }
                }
                // An unverified prefix may cover deleted history. An empty
                // audience is established only while some log still starts
                // at its first write.
                if unknown_empty && needed.is_empty() {
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
                let mut count = 0;
                for object in &needed {
                    match self.open_write_header(object).await {
                        Ok(header) => count += header.parts.len(),
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
                for object in &needed {
                    match self
                        .open_snapshot_write(
                            object,
                            &local.log,
                            &mut replays,
                            &readable,
                            &mut names,
                        )
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
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                let mut snapshots = Vec::new();
                for snapshot in &files.snapshots {
                    snapshots.push(SnapshotSource::Stored {
                        id: snapshot.path.snapshot_id().ok_or_else(|| {
                            inconsistent("recorded snapshot path has another layout")
                        })?,
                        prefix: SnapshotObjectPrefix::decode(&snapshot.prefix)?,
                        input: io::SnapshotInput::open(&self.directory, &snapshot.file)?,
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
                };
                let result = match self.database.load_snapshots(reload).await {
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
                        // during the download. Retry must fetch its stored copy,
                        // rather than repeatedly loading the same stale inputs.
                        let SnapshotJob::Reload { files, .. } = &mut task.job else {
                            unreachable!()
                        };
                        *files = None;
                        self.save_snapshot_task(record, &task, 0).await?;
                        return Err(error.into());
                    }
                    result => result?,
                }
            }
            2 => {
                let SnapshotJob::Reload {
                    files: Some(files), ..
                } = &task.job
                else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                let mut paths: Vec<_> = files
                    .snapshots
                    .iter()
                    .map(|s| s.path.as_str().to_owned())
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
            _ => return Err(coven_database::DbError::DamagedDatabase.into()),
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
        Ok(DownloadedWriteStream {
            header: WriteHeaderFrame::decode(&write.header)?,
            parts,
        })
    }
}

#[cfg(test)]
#[path = "snapshot_reload_tests.rs"]
mod tests;
