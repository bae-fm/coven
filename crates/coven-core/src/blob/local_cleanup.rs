//! Durable deletion of a blob's on-device copies after its last row reference.

use rusqlite::Connection;
use tracing::debug;

use crate::blob::decl::BlobDecls;
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;

/// A committed obligation to remove every on-device copy of an unreferenced blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBlobCleanupIntent {
    namespace: String,
    blob_id: String,
    identity: LocalBlobCleanupIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalBlobCleanupIdentity {
    Local,
    Row { table: String, row_id: String },
    Exact(crate::sync::store_commit::ObjectHash),
}

impl LocalBlobCleanupIntent {
    pub fn local(namespace: impl Into<String>, blob_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
            identity: LocalBlobCleanupIdentity::Local,
        }
    }

    pub fn for_row(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        table: impl Into<String>,
        row_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
            identity: LocalBlobCleanupIdentity::Row {
                table: table.into(),
                row_id: row_id.into(),
            },
        }
    }

    fn exact(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        locator_hash: crate::sync::store_commit::ObjectHash,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
            identity: LocalBlobCleanupIdentity::Exact(locator_hash),
        }
    }

    fn persisted_identity(&self) -> Result<String, DbError> {
        match &self.identity {
            LocalBlobCleanupIdentity::Local => Ok("local".to_string()),
            LocalBlobCleanupIdentity::Exact(locator_hash) => Ok(locator_hash.to_string()),
            LocalBlobCleanupIdentity::Row { .. } => Err(DbError::Message(
                "row-bound local cleanup identity is not durable".to_string(),
            )),
        }
    }

    fn from_persisted(
        namespace: String,
        blob_id: String,
        identity: String,
    ) -> Result<Self, String> {
        if identity == "local" {
            return Ok(Self::local(namespace, blob_id));
        }
        let locator_hash = identity
            .parse()
            .map_err(|error| format!("invalid exact local cleanup identity: {error}"))?;
        Ok(Self::exact(namespace, blob_id, locator_hash))
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
    let durable = match &intent.identity {
        LocalBlobCleanupIdentity::Local => intent.clone(),
        LocalBlobCleanupIdentity::Exact(_) => {
            return Err(DbError::Message(
                "exact local cleanup identity is already durable".to_string(),
            ));
        }
        LocalBlobCleanupIdentity::Row { table, row_id } => {
            let mut statement = conn
                .prepare(
                    "SELECT locator.locator_hash
                     FROM row_blob_locators AS binding
                     JOIN blob_locators AS locator
                       ON locator.remote_object_id = binding.remote_object_id
                     WHERE binding.table_name = ?1 AND binding.row_id = ?2",
                )
                .map_err(DbError::from)?;
            let locator_hashes = statement
                .query_map((table, row_id), |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<Result<std::collections::BTreeSet<_>, _>>()
                .map_err(DbError::from)?;
            match locator_hashes.len() {
                0 => LocalBlobCleanupIntent::local(&intent.namespace, &intent.blob_id),
                1 => {
                    let locator_hash = locator_hashes
                        .iter()
                        .next()
                        .expect("one exact locator hash")
                        .parse::<crate::sync::store_commit::ObjectHash>()
                        .map_err(|error| {
                            DbError::Message(format!("parse local cleanup locator hash: {error}"))
                        })?;
                    LocalBlobCleanupIntent::exact(&intent.namespace, &intent.blob_id, locator_hash)
                }
                count => {
                    return Err(DbError::Message(format!(
                        "local cleanup for {table}.{row_id} has {count} distinct exact locator bindings"
                    )));
                }
            }
        }
    };
    let persisted_identity = durable.persisted_identity()?;
    let inserted = conn
        .execute(
            "INSERT OR IGNORE INTO local_cleanup_intents (namespace, blob_id, copy_identity)
             VALUES (?1, ?2, ?3)",
            (intent.namespace(), intent.blob_id(), persisted_identity),
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

/// Drain every committed cleanup obligation. A filesystem or database failure
/// leaves the intent durable and fails the operation. `true` means every
/// remaining intent is blocked by an active Store-write lease.
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
                    "SELECT intent.namespace, intent.blob_id, intent.copy_identity, EXISTS (\
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
                        LocalBlobCleanupIntent::from_persisted(
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
        let persisted_identity = intent.persisted_identity()?;
        let locator_hash = match &intent.identity {
            LocalBlobCleanupIdentity::Local => None,
            LocalBlobCleanupIdentity::Exact(locator_hash) => Some(*locator_hash),
            LocalBlobCleanupIdentity::Row { .. } => {
                return Err(DbError::Message(
                    "persisted local cleanup intent is row-bound".to_string(),
                ));
            }
        };
        if let Err(error) = crate::blob::cache::drop_all_local_copies(
            store_dir,
            intent.namespace(),
            intent.blob_id(),
            locator_hash,
        )
        .await
        {
            return Err(DbError::Message(format!(
                "remove local copies for {}/{}: {error}",
                intent.namespace(),
                intent.blob_id()
            )));
        }

        let namespace = intent.namespace;
        let blob_id = intent.blob_id;
        db.call(move |conn| {
            conn.execute(
                "DELETE FROM local_cleanup_intents
                 WHERE namespace = ?1 AND blob_id = ?2 AND copy_identity = ?3",
                (&namespace, &blob_id, &persisted_identity),
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
