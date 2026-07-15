//! Durable deletion of a blob's on-device copies after its last row reference.

use rusqlite::Connection;
use tracing::{debug, warn};

use crate::blob::decl::BlobDecls;
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;

/// A committed obligation to remove every on-device copy of an unreferenced blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBlobCleanupIntent {
    namespace: String,
    blob_id: String,
}

impl LocalBlobCleanupIntent {
    pub fn new(namespace: impl Into<String>, blob_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn blob_id(&self) -> &str {
        &self.blob_id
    }
}

/// Record `intent` only when no row references the blob in this transaction.
/// The caller must use the transaction that mutates the carrying rows, so a
/// cleanup intent can never commit beside a live reference.
pub fn record_if_unreferenced_on(
    conn: &Connection,
    decls: &BlobDecls,
    intent: &LocalBlobCleanupIntent,
) -> Result<bool, DbError> {
    if decls
        .row_for_blob_in_namespace(conn, intent.namespace(), intent.blob_id())
        .map_err(|error| DbError::Message(error.to_string()))?
        .is_some()
    {
        return Ok(false);
    }
    let inserted = conn
        .execute(
            "INSERT OR IGNORE INTO local_cleanup_intents (namespace, blob_id) VALUES (?1, ?2)",
            (intent.namespace(), intent.blob_id()),
        )
        .map_err(DbError::from)?;
    if inserted == 0 {
        debug!(
            namespace = %intent.namespace(),
            blob_id = %intent.blob_id(),
            "local blob cleanup intent already exists"
        );
    }
    Ok(true)
}

/// Drain every committed cleanup obligation. A filesystem failure leaves that
/// intent durable and returns `true`; database failures are surfaced.
pub async fn drain(db: &Database, store_dir: &StoreDir) -> Result<bool, DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupRequested)
        .await;
    let _cleanup_guard = db.lock_local_blob_cleanup().await;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupAcquired)
        .await;

    let intents = db
        .call(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT intent.namespace, intent.blob_id, EXISTS (\
                         SELECT 1 FROM store_write_blob_leases lease \
                         WHERE lease.namespace = intent.namespace \
                           AND lease.blob_id = intent.blob_id\
                     ) \
                     FROM local_cleanup_intents intent \
                     ORDER BY namespace, blob_id",
                )
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        LocalBlobCleanupIntent::new(
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                        ),
                        row.get::<_, bool>(2)?,
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
            continue;
        }
        #[cfg(any(test, feature = "test-utils"))]
        db.reach_test_point(
            crate::database::DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
                namespace: intent.namespace().to_string(),
                blob_id: intent.blob_id().to_string(),
            },
        )
        .await;
        if let Err(error) = crate::blob::cache::drop_all_local_copies(
            store_dir,
            intent.namespace(),
            intent.blob_id(),
        )
        .await
        {
            warn!(
                namespace = %intent.namespace(),
                blob_id = %intent.blob_id(),
                error = %error,
                "local blob cleanup intent remains pending"
            );
            pending = true;
            continue;
        }

        let namespace = intent.namespace;
        let blob_id = intent.blob_id;
        db.call(move |conn| {
            conn.execute(
                "DELETE FROM local_cleanup_intents WHERE namespace = ?1 AND blob_id = ?2",
                (&namespace, &blob_id),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await?;
    }
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::LocalBlobCleanupFinished)
        .await;
    Ok(pending)
}
