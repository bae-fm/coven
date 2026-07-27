//! Durable deletion of obsolete logical-source and exact-locator copies.

use rusqlite::Connection;
use tracing::debug;

use crate::blob::decl::BlobDecls;
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;

/// A transaction-local request that resolves into committed copy-specific cleanup
/// obligations.
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

/// Whether any live row still carries this logical blob ID. Author-requested
/// deletion uses this as its transaction-local policy gate before recording
/// copy-specific cleanup obligations.
pub fn logical_blob_is_referenced_on(
    conn: &Connection,
    decls: &BlobDecls,
    namespace: &str,
    blob_id: &str,
) -> Result<bool, DbError> {
    decls
        .blob_id_is_referenced(conn, namespace, blob_id)
        .map_err(|error| DbError::Message(error.to_string()))
}

/// Record cleanup obligations for each copy identity no live row needs in this
/// transaction. The caller must use the transaction that mutates the carrying
/// rows, so each obligation commits atomically with the state that makes that
/// copy obsolete. A caller enforcing logical-ID deletion policy checks that
/// policy before calling; obsolete exact copies are independent of that policy.
pub fn record_obsolete_copy_intents_on(
    conn: &Connection,
    decls: &BlobDecls,
    intent: &LocalBlobCleanupIntent,
) -> Result<(), DbError> {
    match &intent.identity {
        LocalBlobCleanupIdentity::Local => {
            let local_referenced = decls
                .local_copy_is_referenced(conn, intent.namespace(), intent.blob_id())
                .map_err(|error| DbError::Message(error.to_string()))?;
            if !local_referenced {
                record_durable_intent(conn, intent)?;
            }
        }
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
            let exact_locator_hash = match locator_hashes.len() {
                0 => None,
                1 => {
                    let locator_hash = locator_hashes
                        .iter()
                        .next()
                        .expect("one exact locator hash")
                        .parse::<crate::sync::store_commit::ObjectHash>()
                        .map_err(|error| {
                            DbError::Message(format!("parse local cleanup locator hash: {error}"))
                        })?;
                    Some(locator_hash)
                }
                count => {
                    return Err(DbError::Message(format!(
                        "local cleanup for {table}.{row_id} has {count} distinct exact locator bindings"
                    )));
                }
            };
            if let Some(locator_hash) = exact_locator_hash {
                let exact =
                    LocalBlobCleanupIntent::exact(&intent.namespace, &intent.blob_id, locator_hash);
                let referenced = decls
                    .exact_copy_is_referenced(
                        conn,
                        exact.namespace(),
                        exact.blob_id(),
                        locator_hash,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if !referenced {
                    record_durable_intent(conn, &exact)?;
                }
            }
            let local_referenced = decls
                .local_copy_is_referenced(conn, intent.namespace(), intent.blob_id())
                .map_err(|error| DbError::Message(error.to_string()))?;
            if !local_referenced {
                record_durable_intent(
                    conn,
                    &LocalBlobCleanupIntent::local(intent.namespace(), intent.blob_id()),
                )?;
            }
        }
    }
    Ok(())
}

fn record_durable_intent(
    conn: &Connection,
    intent: &LocalBlobCleanupIntent,
) -> Result<(), DbError> {
    let persisted_identity = intent.persisted_identity()?;
    let inserted = crate::database::with_coven_sql_authority(|| {
        conn.execute(
            "INSERT OR IGNORE INTO local_cleanup_intents (namespace, blob_id, copy_identity)
             VALUES (?1, ?2, ?3)",
            (intent.namespace(), intent.blob_id(), persisted_identity),
        )
        .map_err(DbError::from)
    })?;
    if inserted == 0 {
        debug!(
            namespace = %intent.namespace(),
            blob_id = %intent.blob_id(),
            "local blob cleanup intent already exists"
        );
    }
    Ok(())
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
                           AND lease.blob_id = intent.blob_id \
                           AND intent.copy_identity = 'local'\
                     ) \
                     FROM local_cleanup_intents intent \
                     ORDER BY namespace, blob_id,
                              CASE WHEN copy_identity = 'local' THEN 1 ELSE 0 END,
                              copy_identity",
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
        let cleanup = match &intent.identity {
            LocalBlobCleanupIdentity::Local => {
                crate::blob::local_files::drop_blob(store_dir, intent.namespace(), intent.blob_id())
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
            LocalBlobCleanupIdentity::Exact(locator_hash) => {
                crate::blob::cache::drop_cached_locator(
                    store_dir,
                    intent.namespace(),
                    *locator_hash,
                )
                .await
                .map_err(|error| error.to_string())
            }
            LocalBlobCleanupIdentity::Row { .. } => {
                return Err(DbError::Message(
                    "persisted local cleanup intent is row-bound".to_string(),
                ));
            }
        };
        if let Err(error) = cleanup {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{CacheFill, Provenance};
    use crate::sync::session::BlobDecl;
    use crate::sync::store_commit::ObjectHash;
    use crate::sync::test_helpers::open_test_db_with_blob;

    #[tokio::test]
    async fn a_live_same_id_row_with_another_locator_does_not_suppress_exact_cleanup() {
        let db = open_test_db_with_blob(
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
                .with_id_column("blob_id"),
        );
        let removed_locator = ObjectHash::digest(b"removed locator");
        let live_locator = ObjectHash::digest(b"live locator");
        let removed_object = ObjectHash::digest(b"removed object");
        let live_object = ObjectHash::digest(b"live object");
        let decls = db.blob_decls();

        db.call(move |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES ('parent', 'parent', 1, '0000000001000-0000-test', '2026-01-01');
                 INSERT INTO note_photos
                    (id, note_id, kind, size, hash, blob_id, _updated_at, created_at)
                 VALUES
                    ('removed-row', 'parent', 'cover', 5, '{hash}', 'shared-id',
                     '0000000001000-0000-test', '2026-01-01'),
                    ('live-row', 'parent', 'cover', 5, '{hash}', 'shared-id',
                     '0000000001001-0000-test', '2026-01-01');",
                hash = crate::blob::content_hash(b"bytes"),
            ))
            .map_err(DbError::from)?;
            for (object, locator) in [
                (removed_object, removed_locator),
                (live_object, live_locator),
            ] {
                conn.execute(
                    "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
                    [object.to_string()],
                )
                .map_err(DbError::from)?;
                conn.execute(
                    "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)",
                    (object.to_string(), locator.to_string()),
                )
                .map_err(DbError::from)?;
            }
            for (row_id, row_stamp, object) in [
                ("removed-row", "0000000001000-0000-test", removed_object),
                ("live-row", "0000000001001-0000-test", live_object),
            ] {
                conn.execute(
                    "INSERT INTO row_blob_locators
                     (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                     VALUES ('note_photos', ?1, 'blob_id', ?2, '\"store\"', ?3)",
                    (row_id, row_stamp, object.to_string()),
                )
                .map_err(DbError::from)?;
            }
            conn.execute("DELETE FROM note_photos WHERE id = 'removed-row'", [])
                .map_err(DbError::from)?;

            let intent = LocalBlobCleanupIntent::for_row(
                "photos",
                "shared-id",
                "note_photos",
                "removed-row",
            );
            record_obsolete_copy_intents_on(conn, &decls, &intent)?;
            let mut statement = conn
                .prepare(
                    "SELECT copy_identity FROM local_cleanup_intents ORDER BY copy_identity",
                )
                .map_err(DbError::from)?;
            let identities = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(identities)
        })
        .await
        .map(|identities| {
            assert_eq!(
                identities,
                [removed_locator.to_string(), "local".to_string()]
            );
        })
        .expect("record exact cleanup despite a live same-id row");
    }
}
