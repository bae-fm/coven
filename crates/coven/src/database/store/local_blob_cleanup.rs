use tracing::debug;

use super::*;

impl StoreDatabase {
    /// Drain every committed cleanup obligation. A filesystem or database
    /// failure leaves the intent durable and fails the operation. `true` means
    /// every remaining intent is blocked by an active Store-write lease.
    pub(crate) async fn drain_local_blob_cleanup(
        &self,
        store_dir: &crate::store_dir::StoreDir,
    ) -> Result<bool, DbError> {
        #[cfg(test)]
        self.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupRequested)
            .await;
        let _cleanup_guard = self.runtime.local_blob_cleanup.clone().lock_owned().await;
        #[cfg(test)]
        self.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupAcquired)
            .await;

        let intents = self
            .connection
            .call(|connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT intent.namespace, intent.blob_id, intent.copy_identity, EXISTS (
                             SELECT 1 FROM store_write_blob_leases lease
                             WHERE lease.namespace = intent.namespace
                               AND lease.blob_id = intent.blob_id
                               AND intent.copy_identity = 'local'
                         )
                         FROM local_cleanup_intents intent
                         ORDER BY namespace, blob_id,
                                  CASE WHEN copy_identity = 'local' THEN 1 ELSE 0 END,
                                  copy_identity",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            crate::blob::local_cleanup::LocalBlobCleanupIntent::from_persisted(
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            )
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    2,
                                    rusqlite::types::Type::Text,
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        error,
                                    )),
                                )
                            })?,
                            row.get::<_, bool>(3)?,
                        ))
                    })
                    .map_err(DbError::from)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
            })
            .await?;

        let mut pending = false;
        for (intent, leased) in intents {
            if leased {
                pending = true;
                debug!(
                    namespace = %intent.namespace(),
                    blob_id = %intent.blob_id(),
                    "local blob cleanup is blocked by an active Store-write lease"
                );
                continue;
            }
            #[cfg(test)]
            self.reach_test_point(
                crate::database::DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
                    namespace: intent.namespace().to_string(),
                    blob_id: intent.blob_id().to_string(),
                },
            )
            .await;
            let persisted_identity = intent.persisted_identity()?;
            intent.apply(store_dir).await?;

            let namespace = intent.namespace().to_string();
            let blob_id = intent.blob_id().to_string();
            self.connection
                .call(move |connection| {
                    connection
                        .execute(
                            "DELETE FROM local_cleanup_intents
                             WHERE namespace = ?1 AND blob_id = ?2 AND copy_identity = ?3",
                            (&namespace, &blob_id, &persisted_identity),
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                })
                .await?;
        }
        #[cfg(test)]
        self.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupFinished)
            .await;
        Ok(pending)
    }
}
