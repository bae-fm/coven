//! Durable deletion of a blob's on-device copies after its last row reference.

#[cfg(test)]
use std::sync::Arc;

use rusqlite::Connection;
use tracing::warn;

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
        .map_err(|error| DbError(error.to_string()))?
        .is_some()
    {
        return Ok(false);
    }
    conn.execute(
        "INSERT OR IGNORE INTO local_cleanup_intents (namespace, blob_id) VALUES (?1, ?2)",
        (intent.namespace(), intent.blob_id()),
    )
    .map_err(DbError::from)?;
    Ok(true)
}

/// Drain every committed cleanup obligation. A filesystem failure leaves that
/// intent durable and returns `true`; database failures are surfaced.
pub async fn drain(db: &Database, store_dir: &StoreDir) -> Result<bool, DbError> {
    let intents = db
        .call(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT namespace, blob_id FROM local_cleanup_intents \
                     ORDER BY namespace, blob_id",
                )
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(LocalBlobCleanupIntent::new(
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
        })
        .await?;

    let mut pending = false;
    for intent in intents {
        #[cfg(test)]
        pause_before_filesystem_if_armed(&intent).await;
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
    Ok(pending)
}

#[cfg(test)]
struct LocalBlobCleanupPause {
    intent: LocalBlobCleanupIntent,
    reached_filesystem: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static LOCAL_BLOB_CLEANUP_PAUSE: std::sync::Mutex<Option<LocalBlobCleanupPause>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn pause_before_filesystem(
    namespace: &str,
    blob_id: &str,
) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
    let reached_filesystem = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *LOCAL_BLOB_CLEANUP_PAUSE.lock().unwrap() = Some(LocalBlobCleanupPause {
        intent: LocalBlobCleanupIntent::new(namespace, blob_id),
        reached_filesystem: reached_filesystem.clone(),
        resume: resume.clone(),
    });
    (reached_filesystem, resume)
}

#[cfg(test)]
async fn pause_before_filesystem_if_armed(intent: &LocalBlobCleanupIntent) {
    let pause = {
        let mut armed = LOCAL_BLOB_CLEANUP_PAUSE.lock().unwrap();
        if armed.as_ref().is_some_and(|pause| pause.intent == *intent) {
            armed.take()
        } else {
            None
        }
    };
    if let Some(pause) = pause {
        pause.reached_filesystem.notify_one();
        pause.resume.notified().await;
    }
}
