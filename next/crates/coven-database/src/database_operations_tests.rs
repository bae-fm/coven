//! Test transport support using the actual local-write queue.

use super::{finish_blocking, Database};
use crate::DbError;

impl Database {
    /// Inject an agreement fault without changing positions or application rows.
    /// Recovery tests use the actual incremental sum and production reload path.
    pub async fn test_damage_fingerprint(
        &self,
        audience: coven_merge::Audience,
    ) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(tokio::task::spawn_blocking(move || {
            let slot = owner.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            let writer = inner.writer.lock().expect("writer lock poisoned");
            writer.transaction(|db| {
                db.internal_execute("INSERT INTO coven_fingerprint_sums(audience,sum) VALUES(?1,?2) ON CONFLICT(audience) DO UPDATE SET sum=excluded.sum",
                    (crate::write_encoding::audience_text(&audience), [37u8;32].as_slice()))?;
                Ok(())
            })
        }).await)
    }

    /// Test transport: read actual queued records without reauthoring their writes.
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer
                    .query(
                        "SELECT record FROM coven_uploads ORDER BY device,number",
                        [],
                        |r| r.get::<_, Vec<u8>>(0),
                    )?
                    .iter()
                    .map(|bytes| {
                        coven_format::write_stream::decode_plaintext(bytes).map_err(Into::into)
                    })
                    .collect()
            })
            .await,
        )
    }

    /// Test transport: acknowledge a write after transferring it to the peer.
    pub async fn test_acknowledge_write(&self, write: coven_merge::WriteId) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| {
                    db.internal_execute(
                        "DELETE FROM coven_uploads WHERE device=?1 AND number=?2",
                        (
                            write.device.0.to_be_bytes().as_slice(),
                            write.number.to_be_bytes().as_slice(),
                        ),
                    )?;
                    Ok(())
                })
            })
            .await,
        )
    }
}
