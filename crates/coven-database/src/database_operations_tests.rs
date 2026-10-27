//! Test transport support using the actual local-write queue.

use super::Database;
use crate::DbError;

impl Database {
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

    /// Model a database last judged by another library release before reopening it.
    pub async fn test_pending_version(&self, version: &'static str) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute("UPDATE _coven_pending SET coven_version=?1", [version])?;
                    Ok(())
                })
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
                    .query(
                        "SELECT record FROM _coven_uploads ORDER BY device,number",
                        [],
                        |r| r.get::<_, Vec<u8>>(0),
                    )?
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
