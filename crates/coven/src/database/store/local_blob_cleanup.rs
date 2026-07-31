use tracing::debug;

use super::*;

impl StoreDatabase {
    pub(crate) async fn drain_published_blob_drop_intents(
        &self,
        store_dir: &crate::store_dir::StoreDir,
        max_seq: u64,
    ) -> Result<(), String> {
        let intents = self
            .published_blob_drop_intents(max_seq)
            .await
            .map_err(|error| format!("Failed to load published blob drop intents: {error}"))?;
        for intent in intents {
            apply_published_blob_drop_intent(self, store_dir, &intent).await?;
            self.clear_published_blob_drop_intent(&intent)
                .await
                .map_err(|error| format!("Failed to clear published blob drop intent: {error}"))?;
        }
        Ok(())
    }

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

async fn apply_published_blob_drop_intent(
    database: &StoreDatabase,
    store_dir: &crate::store_dir::StoreDir,
    intent: &crate::database::PublishedBlobDropIntent,
) -> Result<(), String> {
    apply_deferred_local_blob_drop(database, store_dir, &intent.drop)
        .await
        .map_err(|error| error.to_string())
}

async fn apply_deferred_local_blob_drop(
    database: &StoreDatabase,
    store_dir: &crate::store_dir::StoreDir,
    deferred: &crate::sync::cycle::DeferredLocalBlobDrop,
) -> Result<(), crate::sync::store::StorePreparationError> {
    let local = crate::blob::local_files::path_if_present(
        store_dir,
        &deferred.namespace,
        &deferred.id,
        deferred.size,
    )
    .await
    .map_err(|error| crate::sync::store::StorePreparationError::AssetUpload(error.to_string()))?;
    match (deferred.disposition, local) {
        (crate::sync::cycle::DeferredLocalBlobDisposition::Pin, Some(source)) => {
            crate::blob::cache::populate_pinned_from_file(
                store_dir,
                &deferred.namespace,
                deferred.locator_hash,
                deferred.size,
                deferred.plaintext_hash,
                &source,
            )
            .await
            .map_err(|error| {
                crate::sync::store::StorePreparationError::AssetUpload(error.to_string())
            })?;
        }
        (crate::sync::cycle::DeferredLocalBlobDisposition::Cache, Some(source)) => {
            crate::blob::cache::write_blob_from_file(
                database,
                store_dir,
                &deferred.namespace,
                deferred.locator_hash,
                deferred.size,
                deferred.plaintext_hash,
                &source,
            )
            .await
            .map_err(|error| {
                crate::sync::store::StorePreparationError::AssetUpload(error.to_string())
            })?;
        }
        (crate::sync::cycle::DeferredLocalBlobDisposition::Drop, _) => {}
        (crate::sync::cycle::DeferredLocalBlobDisposition::Pin, None) => {
            let pinned = store_dir
                .pinned_blob_path(&deferred.namespace, deferred.locator_hash)
                .map_err(|error| {
                    crate::sync::store::StorePreparationError::AssetUpload(error.to_string())
                })?;
            return recognize_applied_disposition_or_fail(&pinned, deferred).await;
        }
        (crate::sync::cycle::DeferredLocalBlobDisposition::Cache, None) => {
            let cached = store_dir
                .cache_blob_path(&deferred.namespace, deferred.locator_hash)
                .map_err(|error| {
                    crate::sync::store::StorePreparationError::AssetUpload(error.to_string())
                })?;
            return recognize_applied_disposition_or_fail(&cached, deferred).await;
        }
    }
    store_dir
        .remove_local_blob(&deferred.namespace, &deferred.id)
        .await
        .map(|_| ())
        .map_err(|error| crate::sync::store::StorePreparationError::AssetUpload(error.to_string()))
}

async fn recognize_applied_disposition_or_fail(
    destination: &std::path::Path,
    deferred: &crate::sync::cycle::DeferredLocalBlobDrop,
) -> Result<(), crate::sync::store::StorePreparationError> {
    match crate::local_blob::exists(destination).await {
        Ok(true) => {
            let (size, hash) = crate::local_blob::exact_file_facts(destination)
                .await
                .map_err(crate::sync::store::StorePreparationError::AssetUpload)?;
            if size == deferred.size && hash == deferred.plaintext_hash {
                return Ok(());
            }
        }
        Ok(false) => {}
        Err(error) => {
            return Err(crate::sync::store::StorePreparationError::AssetUpload(
                error,
            ))
        }
    }
    Err(crate::sync::store::StorePreparationError::AssetUpload(
        format!(
            "published blob {}/{} is missing from both the local store and its {:?} destination",
            deferred.namespace, deferred.id, deferred.disposition
        ),
    ))
}
