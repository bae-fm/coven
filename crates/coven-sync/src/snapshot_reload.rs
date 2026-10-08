//! Download first, then replace all selected audiences and the journal together.

use super::{catalog::inconsistent, io, point, StoreLogSync};
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    replay_cache::ReplayCache,
    snapshot_data::*,
    DamagedObject, SyncError,
};
use coven_database::{
    DownloadedPartStream, DownloadedWriteStream, OperationRecord, SnapshotReload, SnapshotSource,
};
use coven_format::{sealed_snapshot::SnapshotObjectPrefix, write_stream::WriteHeaderFrame};
use coven_merge::WriteId;
use coven_storage::ObjectPrefix;
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    pub(super) async fn reload_snapshot_step(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        damages: &mut Vec<DamagedObject>,
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
                    audiences.retain(|audience, _| changed.contains(audience));
                }
                if audiences.is_empty() {
                    self.database.finish_operation(record.id).await?;
                    return Ok(Progress::Finished(Output::Unit));
                }
                let schema = self.database.schema_version().await?;
                if local
                    .log
                    .replay
                    .state
                    .schema
                    .iter()
                    .any(|(audience, version)| {
                        audiences.contains_key(audience) && version.number > schema
                    })
                {
                    return Err(crate::SyncFailure::UpdateRequired.into());
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
                for audience in audiences.keys() {
                    catalogs.push((
                        audience,
                        self.current_snapshot_candidates(audience, &local.log, damages)
                            .await?,
                    ));
                }
                let count = catalogs
                    .iter()
                    .map(|(_, catalog)| catalog.candidates.len())
                    .sum();
                let mut names = self
                    .reserve_snapshot_files(record, &mut task, count)
                    .await?
                    .into_iter();
                for (audience, candidates) in catalogs {
                    for id in &candidates.required_positions.0 {
                        highest
                            .entry(id.device)
                            .and_modify(|n: &mut u64| *n = (*n).max(id.number))
                            .or_insert(id.number);
                    }
                    let unreadable_prefix = candidates.unreadable_prefix;
                    match self
                        .load_snapshot(candidates.candidates, &mut names, damages)
                        .await?
                    {
                        Some(snapshot) => files.snapshots.push(snapshot),
                        None if self.has_snapshot_boundary(&local.log, audience) => {
                            return Err(inconsistent(
                                "no valid snapshot follows the selected reset or version boundary",
                            ))
                        }
                        None => {
                            unknown_empty |= unreadable_prefix;
                            files.empty.push(audience.clone());
                        }
                    }
                }
                let mut positions = files
                    .snapshots
                    .iter()
                    .map(|snapshot| {
                        SnapshotObjectPrefix::decode(&snapshot.prefix).map(|p| p.writes)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // Unchanged audiences keep their local frontier. Download the
                // shared headers and any still-unread parts needed to bring
                // those audiences through the selected snapshots' positions.
                if audiences.len() < readable.len() {
                    positions.push(self.database.sync_state(Vec::new()).await?.positions);
                }
                let (required, waiting) = self.database.reload_positions().await?;
                let waiting: BTreeSet<_> = waiting.into_iter().collect();
                for id in positions.iter().flat_map(|p| &p.0).chain(&required.0) {
                    highest
                        .entry(id.device)
                        .and_modify(|n: &mut u64| *n = (*n).max(id.number))
                        .or_insert(id.number);
                }
                let mut objects = BTreeMap::new();
                for object in self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .list(&ObjectPrefix::device_logs())
                    .await?
                {
                    let id = object
                        .path
                        .write_id()
                        .ok_or_else(|| inconsistent("device listing has another path layout"))?;
                    highest
                        .entry(id.device)
                        .and_modify(|n| *n = (*n).max(id.number))
                        .or_insert(id.number);
                    objects.insert(id, object);
                }
                if unknown_empty && objects.is_empty() {
                    return Err(inconsistent("unreadable snapshot prefixes and missing logs cannot establish an empty audience"));
                }
                let readable = readable.into_keys().collect();
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
                        match objects.get(&id) {
                            Some(object) => needed.push(object),
                            None if positions.iter().any(|p| p.covers(id)) => {
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
                let mut count = 0;
                for object in &needed {
                    count += self.open_write_header(object).await?.parts.len();
                }
                let mut names = self
                    .reserve_snapshot_files(record, &mut task, count)
                    .await?
                    .into_iter();
                for object in needed {
                    files.writes.push(
                        self.open_snapshot_write(
                            object,
                            &local.log,
                            &mut replays,
                            &readable,
                            &mut names,
                        )
                        .await?,
                    );
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
                    .collect::<Result<_, _>>()?;
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
                match self.database.load_snapshots(reload).await {
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
