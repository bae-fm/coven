//! Test transport support using the actual local-write queue.

use super::Database;
use crate::{test_utils::StatementLog, DbError};

impl Database {
    /// Record the statements every read context of this database runs into
    /// `log` (`query` and `query_row`, in reads and live queries' runs), as
    /// SQLite reads them with their bound values written in. Writes and
    /// migrations are not recorded. Install while the database is quiescent.
    pub async fn record_statements(&self, log: StatementLog) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.test_record_statements(log.clone());
                Ok::<_, DbError>(())
            })?;
            inner.access.readers.test_record_statements(&log);
            Ok::<_, DbError>(())
        })
        .await
    }

    /// Count executed work on every connection without including counter reads.
    /// Install while the database is quiescent, before starting the measured pass.
    pub async fn test_track_work(
        &self,
        counter: crate::test_utils::WorkCounter,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.test_track_work(counter.clone());
                Ok::<_, DbError>(())
            })?;
            inner.access.readers.test_track_work(&counter);
            inner.inputs.test_track_work(&counter);
            Ok(())
        })
        .await
    }

    /// The plaintext bytes each section of a snapshot `id` would hold now:
    /// what the sealed object's size can't tell apart.
    pub async fn test_snapshot_section_bytes(
        &self,
        id: coven_format::admin::SnapshotId,
    ) -> Result<
        [u64; coven_format::snapshot::SECTIONS],
        crate::SnapshotWriteError<std::convert::Infallible>,
    > {
        let sections =
            std::sync::Arc::new(std::sync::Mutex::new([0; coven_format::snapshot::SECTIONS]));
        let counted = sections.clone();
        self.write_snapshot(
            id,
            |_| Ok(()),
            move |frame| {
                if let Some(section) = coven_format::snapshot::frame_section(&frame) {
                    counted.lock().expect("no test panics counting")[section] += frame.len() as u64;
                }
                Ok(())
            },
        )
        .await?;
        let bytes = *sections.lock().expect("no test panics counting");
        Ok(bytes)
    }

    /// Observe transaction completion and file cleanup on the writer thread.
    /// Install after setup; the callback may terminate a subprocess to simulate
    /// a crash without Rust destructors or SQLite connection shutdown.
    pub async fn test_on_write_checkpoint(
        &self,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.on_write_checkpoint(callback);
                Ok(())
            })
        })
        .await
    }

    /// Panic in the next write the writer judges, before it commits, as a
    /// bug in it would: the write rolls back, and its call fails with
    /// `DbError::Bug` carrying `message` (E15).
    pub async fn test_panic_in_next_write(&self, message: &'static str) -> Result<(), DbError> {
        let armed = std::sync::atomic::AtomicBool::new(true);
        self.test_on_write_checkpoint(move |point| {
            if point == crate::test_utils::WriteCheckpoint::Judged
                && armed.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                panic!("{message}");
            }
        })
        .await
    }

    /// Make every later write of how the passes go fail, as a full disk
    /// would: their move, their end and the last synced time. Counting a
    /// start still writes, so the loop starts and its first pass fails.
    pub async fn test_fail_sync_status(&self) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.batch(
                    "CREATE TEMP TRIGGER fail_sync_status
                     BEFORE UPDATE OF last_pass,moved,last_synced ON coven_sync_status
                     BEGIN SELECT RAISE(ABORT,'injected sync status failure'); END;",
                )
            })
        })
        .await
    }

    /// Make every later write of the reset notice fail, as a full disk would.
    pub async fn test_fail_device_reset(&self) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.batch(
                    "CREATE TEMP TRIGGER fail_device_reset BEFORE INSERT ON _coven_device_reset
                     BEGIN SELECT RAISE(ABORT,'injected reset notice failure'); END;",
                )
            })
        })
        .await
    }

    /// Make every later write of the retry effect fail, as a full disk would.
    pub async fn test_fail_retry_effect(&self) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.batch(
                    "CREATE TEMP TRIGGER fail_retry_effect BEFORE UPDATE ON coven_retry_effect
                     BEGIN SELECT RAISE(ABORT,'injected retry effect failure'); END;",
                )
            })
        })
        .await
    }

    /// Test transport: read actual queued records without reauthoring their writes.
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer
                    .query("SELECT record FROM _coven_uploads ORDER BY device,number", [], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })?
                    .iter()
                    .map(|bytes| {
                        coven_format::write_stream::decode_plaintext(bytes).map_err(Into::into)
                    })
                    .collect()
            })
        })
        .await
    }
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
#[cfg(test)]
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_gone_storage_cleared SELECT n FROM fixture;
         INSERT INTO _coven_requests_ended SELECT n FROM fixture;",
    )
    .unwrap();
}
