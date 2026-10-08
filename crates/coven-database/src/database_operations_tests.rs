//! Test transport support using the actual local-write queue.

use super::Database;
use crate::DbError;

impl Database {
    /// Observe transaction completion and file cleanup on the writer thread.
    /// Install after setup; the callback may terminate a subprocess to simulate
    /// a crash without Rust destructors or SQLite connection shutdown.
    pub async fn test_on_write_checkpoint(
        &self,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| writer.on_write_checkpoint(callback));
            Ok(())
        })
        .await
    }

    /// Inject an agreement fault without changing positions or application rows.
    /// Recovery tests use the actual incremental sum and production reload path.
    pub async fn test_damage_fingerprint(
        &self,
        audience: coven_merge::Audience,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute("INSERT INTO _coven_fingerprint_sums(audience,sum) VALUES(?1,?2) ON CONFLICT(audience) DO UPDATE SET sum=excluded.sum",
                        (crate::write_encoding::audience_text(&audience), [37u8;32].as_slice()))?;
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
            inner.with_writer(|writer| {
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

    /// Test transport: acknowledge a write after transferring it to the peer.
    pub async fn test_acknowledge_write(&self, write: coven_merge::WriteId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "DELETE FROM _coven_uploads WHERE device=?1 AND number=?2",
                        (
                            write.device.0.to_be_bytes().as_slice(),
                            write.number.to_be_bytes().as_slice(),
                        ),
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }
}
