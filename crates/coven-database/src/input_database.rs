//! Durable remote observations, independently committed while application writes run.

use super::{with_locked, Database};
use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::id_source::{DeviceId, FileId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

pub(crate) struct InputDatabase {
    reader: Mutex<DatabaseConnection>,
    writer: Mutex<DatabaseConnection>,
    active: Arc<Mutex<BTreeMap<i64, Weak<InputRead>>>>,
}

struct InputRead {
    id: i64,
    active: Weak<Mutex<BTreeMap<i64, Weak<InputRead>>>>,
    receiving: tokio::sync::Mutex<()>,
}

impl Drop for InputRead {
    fn drop(&mut self) {
        if let Some(active) = self.active.upgrade() {
            let mut active = active.lock().expect("active input lock poisoned");
            // A concurrent new reader may already have replaced this expired weak entry.
            if active
                .get(&self.id)
                .is_some_and(|saved| saved.strong_count() == 0)
            {
                active.remove(&self.id);
            }
        }
    }
}

/// One observed object version. A replacement invalidates the preceding handle.
#[derive(Clone)]
pub struct RetainedInput {
    database: Database,
    id: i64,
    reading: Arc<InputRead>,
}

/// Encrypted bytes received for one provider observation; completion is separate.
pub struct RetainedRange {
    /// Byte offset in the encrypted object.
    pub offset: u64,
    /// The exact provider bytes.
    pub bytes: Vec<u8>,
}

/// Retained encrypted bytes have a maintained total and explicit eviction count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputBudget {
    /// Maximum retained encrypted bytes, excluding checked metadata and references.
    pub maximum: u64,
    /// Currently retained encrypted bytes.
    pub retained: u64,
    /// Bytes explicitly evicted since this catalog was created.
    pub evicted: u64,
}

impl InputDatabase {
    fn release(&self, ids: Vec<i64>) -> Result<(), DbError> {
        let pending = self.read(|db| {
            let mut pending = Vec::new();
            for id in ids {
                if !db.query_row(
                    "SELECT released FROM _coven_input_objects WHERE id=?1",
                    [id],
                    |r| r.get::<_, bool>(0),
                )? {
                    pending.push(id);
                }
            }
            Ok(pending)
        })?;
        if pending.is_empty() {
            return Ok(());
        }
        self.write(|db| {
            for id in pending {
                db.internal_execute(
                    "UPDATE _coven_input_objects SET released=1 WHERE id=?1",
                    [id],
                )?;
            }
            Ok(())
        })
    }
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn test_track_work(&self, counter: &crate::test_utils::WorkCounter) {
        for connection in [&self.reader, &self.writer] {
            *connection
                .lock()
                .expect("input connection lock poisoned")
                .work
                .borrow_mut() = Some(counter.clone());
        }
    }
    pub(crate) fn new(reader: DatabaseConnection, writer: DatabaseConnection) -> Self {
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn reading(&self, id: i64) -> Arc<InputRead> {
        let mut active = self.active.lock().expect("active input lock poisoned");
        if let Some(reading) = active.get(&id).and_then(Weak::upgrade) {
            return reading;
        }
        let reading = Arc::new(InputRead {
            id,
            active: Arc::downgrade(&self.active),
            receiving: tokio::sync::Mutex::new(()),
        });
        active.insert(id, Arc::downgrade(&reading));
        reading
    }

    pub(crate) fn close(self) -> Vec<DbError> {
        [self.reader, self.writer]
            .into_iter()
            .filter_map(|connection| {
                connection
                    .into_inner()
                    .expect("input connection lock poisoned")
                    .close()
                    .err()
            })
            .collect()
    }

    fn read<R>(
        &self,
        run: impl FnOnce(&DatabaseConnection) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        with_locked(&self.reader, |reader| {
            reader.read_transaction(|| run(reader))
        })
    }

    fn write<R>(
        &self,
        run: impl FnOnce(&DatabaseConnection) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        with_locked(&self.writer, |writer| {
            #[cfg(any(test, feature = "test-utils"))]
            writer.record_work(|work| work.writer_holds += 1);
            writer.transaction(run)
        })
    }
}

fn sql_offset(value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_| DbError::TooLarge {
        field: "retained input bytes",
        actual: value,
        maximum: i64::MAX as u64,
    })
}

fn observation(db: &DatabaseConnection, path: &str) -> Result<Option<(i64, Vec<u8>)>, DbError> {
    Ok(db
        .query(
            "SELECT id,identity FROM _coven_input_objects WHERE path=?1",
            [path],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .into_iter()
        .next())
}

// Nonoverlapping ranges need at most the immediately preceding range plus
// ranges beginning inside the requested interval. Seek to that boundary.
const RANGES: &str = "SELECT max(start,?2),substr(bytes,max(start,?2)-start+1,min(end,?3)-max(start,?2)) FROM _coven_input_ranges WHERE object=?1 AND start>=coalesce((SELECT start FROM _coven_input_ranges WHERE object=?1 AND start<=?2 ORDER BY start DESC LIMIT 1),?2) AND start<?3 AND end>?2 ORDER BY start";

const RANGE_OVERLAP: &str = "SELECT EXISTS(SELECT 1 FROM _coven_input_ranges WHERE object=?1 AND start=(SELECT max(start) FROM _coven_input_ranges WHERE object=?1 AND start<?3) AND end>?2)";

fn budget(db: &DatabaseConnection) -> Result<InputBudget, DbError> {
    db.query_row(
        "SELECT maximum,retained,evicted FROM _coven_input_budget WHERE singleton=1",
        [],
        |r| {
            Ok(InputBudget {
                maximum: r.get::<_, i64>(0)? as u64,
                retained: r.get::<_, i64>(1)? as u64,
                evicted: r.get::<_, i64>(2)? as u64,
            })
        },
    )
}

fn evict(
    db: &DatabaseConnection,
    active: &Mutex<BTreeMap<i64, Weak<InputRead>>>,
) -> Result<(), DbError> {
    // A released object can be borrowed by another consumer. Keep its bytes
    // until that consumer drops its handle, including across network awaits.
    let active = active.lock().expect("active input lock poisoned");
    let before = budget(db)?;
    let mut retained = before.retained;
    let mut after = 0;
    while retained > before.maximum {
        let next = db.query(
            "SELECT r.id,r.object FROM _coven_input_ranges r JOIN _coven_input_objects o ON o.id=r.object WHERE o.released=1 AND r.id>?1 ORDER BY r.id LIMIT 1",
            [after], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,i64>(1)?)),
        )?.into_iter().next();
        let Some((id, object)) = next else {
            return Err(DbError::InputBudgetExceeded {
                required: retained,
                maximum: before.maximum,
            });
        };
        after = id;
        if active
            .get(&object)
            .is_some_and(|input| input.strong_count() > 0)
        {
            continue;
        }
        // Evict a complete input together: retaining only its suffix would
        // turn the next read into separate requests around an internal gap.
        db.internal_execute("DELETE FROM _coven_input_ranges WHERE object=?1", [object])?;
        retained = budget(db)?.retained;
    }
    let evicted = before.retained - retained;
    if evicted > 0 {
        db.internal_execute(
            "UPDATE _coven_input_budget SET evicted=evicted+?1 WHERE singleton=1",
            [sql_offset(evicted)?],
        )?;
        tracing::debug!(
            bytes = evicted,
            retained,
            "evicted retained remote input bytes"
        );
    }
    Ok(())
}

impl Database {
    /// Release encrypted inputs after a snapshot reload commits, before retiring
    /// its journal record. A failed release leaves that step available to retry.
    pub async fn release_input_ranges(&self, paths: Vec<String>) -> Result<(), DbError> {
        self.call(move |inner| {
            let ids = inner.inputs.read(|db| {
                paths
                    .into_iter()
                    .map(|path| {
                        observation(db, &path)?
                            .map(|(id, _)| id)
                            .ok_or(DbError::Invariant {
                                what: "a retained input path has its observation",
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            inner.inputs.release(ids)
        })
        .await
    }
    /// The retained provider observation of an exact path, if any. A drained
    /// writer's discovered objects are found here instead of by probing (§6).
    pub async fn retained_observation(&self, path: String) -> Result<Option<Vec<u8>>, DbError> {
        self.call(move |inner| {
            inner.inputs.read(|db| {
                Ok(db
                    .query(
                        "SELECT observation FROM _coven_input_objects WHERE path=?1",
                        [&path],
                        |r| r.get(0),
                    )?
                    .into_iter()
                    .next())
            })
        })
        .await
    }

    /// Retain a provider observation before reading it. Immutable identity changes
    /// return `None` and preserve the preceding evidence. A replacement discards
    /// only that path's preceding version. Equal observations acquire no writer.
    pub async fn observe_input(
        &self,
        path: String,
        identity: Vec<u8>,
        observation_bytes: Vec<u8>,
        replaceable: bool,
    ) -> Result<Option<RetainedInput>, DbError> {
        let id = self.call(move |inner| {
            let previous = inner.inputs.read(|db| observation(db, &path))?;
            match previous {
                Some((id, saved)) if saved == identity => return Ok(Some(id)),
                Some(_) if !replaceable => return Ok(None),
                _ => (),
            }
            inner.inputs.write(|db| {
                match observation(db, &path)? {
                    Some((id, saved)) if saved == identity => return Ok(Some(id)),
                    Some(_) if !replaceable => return Ok(None),
                    _ => (),
                }
                db.internal_execute("DELETE FROM _coven_input_objects WHERE path=?1", [&path])?;
                db.query_row("INSERT INTO _coven_input_objects(path,identity,observation) VALUES(?1,?2,?3) RETURNING id", (&path, &identity, &observation_bytes), |r| r.get(0)).map(Some)
            })
        }).await?;
        match id {
            None => Ok(None),
            Some(id) => {
                let reading = self
                    .call(move |inner| Ok::<_, DbError>(inner.inputs.reading(id)))
                    .await?;
                Ok(Some(RetainedInput {
                    database: self.clone(),
                    id,
                    reading,
                }))
            }
        }
    }

    /// Configure retained encrypted input independently of application file caches.
    /// Lowering the budget evicts oldest completed inputs atomically with accounting.
    /// Checked facts and file protection are never evicted with their bytes.
    pub async fn set_input_budget(&self, maximum: u64) -> Result<InputBudget, DbError> {
        self.call(move |inner| {
            let previous = inner.inputs.read(budget)?;
            if previous.maximum == maximum {
                return Ok(previous);
            }
            inner.inputs.write(|db| {
                db.internal_execute(
                    "UPDATE _coven_input_budget SET maximum=?1 WHERE singleton=1",
                    [sql_offset(maximum)?],
                )?;
                evict(db, &inner.inputs.active)?;
                budget(db)
            })
        })
        .await
    }

    /// Report byte retention and explicit eviction without scanning cached chunks.
    pub async fn input_budget(&self) -> Result<InputBudget, DbError> {
        self.call(|inner| inner.inputs.read(budget)).await
    }
}

impl RetainedInput {
    /// The first completed provider observation for this identity, including
    /// native metadata whose representation can differ in later listings.
    pub async fn observation(&self) -> Result<Vec<u8>, DbError> {
        self.read(|db, id| {
            db.query_row(
                "SELECT observation FROM _coven_input_objects WHERE id=?1",
                [id],
                |r| r.get(0),
            )
        })
        .await
    }
    /// Share an in-flight range across consumers of the same durable observation.
    /// The guard spans checking retained ranges, fetching gaps, and committing them.
    pub async fn receiving(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.reading.receiving.lock().await
    }
    async fn read<R: Send + 'static>(
        &self,
        run: impl FnOnce(&DatabaseConnection, i64) -> Result<R, DbError> + Send + 'static,
    ) -> Result<R, DbError> {
        let id = self.id;
        self.database
            .call(move |inner| inner.inputs.read(|db| run(db, id)))
            .await
    }

    async fn write<R: Send + 'static>(
        &self,
        run: impl FnOnce(&DatabaseConnection, i64) -> Result<R, DbError> + Send + 'static,
    ) -> Result<R, DbError> {
        let id = self.id;
        self.database
            .call(move |inner| inner.inputs.write(|db| run(db, id)))
            .await
    }

    /// Cached portions of a requested interval, clipped so memory is bounded by it.
    pub async fn ranges(&self, start: u64, end: u64) -> Result<Vec<RetainedRange>, DbError> {
        let start = sql_offset(start)?;
        let end = sql_offset(end)?;
        self.read(move |db, id| {
            db.query(RANGES, (id, start, end), |r| {
                Ok(RetainedRange {
                    offset: r.get::<_, i64>(0)? as u64,
                    bytes: r.get(1)?,
                })
            })
        })
        .await
    }

    /// Commit newly fetched, nonoverlapping bytes. Refilling an evicted range is
    /// a new insertion, while retained bytes must be consumed without fetching.
    pub async fn keep_range(&self, offset: u64, bytes: Vec<u8>) -> Result<(), DbError> {
        self.keep_received_range(offset, bytes, false).await
    }

    /// Whether a whole response (or all of its bounded ranges) reached checked
    /// transport EOF. This records receipt, never content authentication.
    pub async fn received(&self) -> Result<bool, DbError> {
        self.read(|db, id| {
            db.query_row(
                "SELECT received FROM _coven_input_objects WHERE id=?1",
                [id],
                |r| r.get(0),
            )
        })
        .await
    }

    /// Commit received bytes and, at successful EOF, the transport completion
    /// fact in one transaction. Neither fact can survive a failed byte commit.
    pub async fn keep_received_range(
        &self,
        offset: u64,
        bytes: Vec<u8>,
        complete: bool,
    ) -> Result<(), DbError> {
        if bytes.is_empty() && (!complete || self.received().await?) {
            return Ok(());
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(DbError::Invariant {
                what: "an input range's end fits u64",
            })?;
        let offset = sql_offset(offset)?;
        let end = sql_offset(end)?;
        let id = self.id;
        self.database.call(move |inner| inner.inputs.write(|db| {
            if !bytes.is_empty() {
                let overlap: bool = db.query_row(RANGE_OVERLAP, (id,offset,end), |r| r.get(0))?;
                if overlap { return Err(DbError::Invariant { what: "input ranges never overlap" }); }
                db.internal_execute("INSERT INTO _coven_input_ranges(object,start,end,bytes) VALUES(?1,?2,?3,?4)", (id,offset,end,bytes))?;
            }
            if complete {
                db.internal_execute("UPDATE _coven_input_objects SET received=1 WHERE id=?1", [id])?;
            }
            evict(db, &inner.inputs.active)
        })).await
    }

    /// The complete input has been applied (with reversible inputs committed),
    /// or permanently refused. Release its encrypted ranges for explicit eviction;
    /// checked metadata, file protection, and the refusal remain durable.
    pub async fn release_ranges(self) -> Result<(), DbError> {
        let id = self.id;
        self.database
            .call(move |inner| inner.inputs.release(vec![id]))
            .await
    }

    /// The retained header or signed prefix; its interpretation belongs to sync.
    /// A header alone never records complete-object signature authentication.
    pub async fn metadata(&self) -> Result<Option<Vec<u8>>, DbError> {
        self.read(|db, id| {
            db.query_row(
                "SELECT metadata FROM _coven_input_objects WHERE id=?1",
                [id],
                |r| r.get(0),
            )
        })
        .await
    }

    /// Keep checked metadata independently of evictable transport ranges.
    pub async fn keep_metadata(&self, bytes: Vec<u8>) -> Result<(), DbError> {
        if self.metadata().await?.as_ref() == Some(&bytes) {
            return Ok(());
        }
        self.write(move |db, id| {
            db.internal_execute(
                "UPDATE _coven_input_objects SET metadata=?2 WHERE id=?1",
                (id, bytes),
            )?;
            Ok(())
        })
        .await
    }

    /// Unread, partially checked, or completely checked file protection.
    pub async fn files(&self) -> Result<Option<Option<BTreeSet<(DeviceId, FileId)>>>, DbError> {
        self.read(|db,id| {
            let complete: Option<bool> = db.query_row("SELECT files_complete FROM _coven_input_objects WHERE id=?1", [id], |r| r.get(0))?;
            match complete {
                None => Ok(None),
                Some(false) => Ok(Some(None)),
                Some(true) => Ok(Some(Some(db.query("SELECT device,file FROM _coven_input_files WHERE object=?1 ORDER BY device,file", [id], |r| Ok((DeviceId(u64::from_be_bytes(r.get(0)?)),FileId(uuid::Uuid::from_bytes(r.get(1)?)))))?.into_iter().collect()))),
            }
        }).await
    }

    /// Record references only after complete authentication. Incomplete knowledge
    /// remains protective and can be retried when keys or schema become available.
    pub async fn keep_files(
        &self,
        files: Option<BTreeSet<(DeviceId, FileId)>>,
    ) -> Result<(), DbError> {
        if self.files().await?.as_ref() == Some(&files) {
            return Ok(());
        }
        self.write(move |db, id| {
            db.internal_execute("DELETE FROM _coven_input_files WHERE object=?1", [id])?;
            db.internal_execute(
                "UPDATE _coven_input_objects SET files_complete=?2 WHERE id=?1",
                (id, files.is_some()),
            )?;
            if let Some(files) = files {
                for (device, file) in files {
                    db.internal_execute(
                        "INSERT INTO _coven_input_files(object,device,file) VALUES(?1,?2,?3)",
                        (
                            id,
                            device.0.to_be_bytes().as_slice(),
                            file.0.as_bytes().as_slice(),
                        ),
                    )?;
                }
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
#[path = "input_database_tests.rs"]
mod tests;
