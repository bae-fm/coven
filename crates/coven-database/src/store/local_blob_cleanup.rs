use tracing::debug;

use super::*;
use crate::local_blob_cleanup_intents::{LocalBlobCleanupIdentity, LocalBlobCleanupIntent};
use crate::BlobDecls;

/// Record cleanup obligations for each copy identity no live row needs in this
/// transaction. The transaction mutating the carrying rows must also record the
/// obligation, so the obsolete state and its cleanup commit together.
pub(crate) fn record_obsolete_copy_intents_on(
    conn: &rusqlite::Connection,
    decls: &BlobDecls,
    intent: &LocalBlobCleanupIntent,
) -> Result<(), DbError> {
    match intent.identity() {
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
                        .parse::<coven_protocol::store_commit::ObjectHash>()
                        .map_err(|error| {
                            DbError::context("parse local cleanup locator hash", error)
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
                let exact = LocalBlobCleanupIntent::exact(
                    intent.namespace(),
                    intent.blob_id(),
                    locator_hash,
                );
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
    conn: &rusqlite::Connection,
    intent: &LocalBlobCleanupIntent,
) -> Result<(), DbError> {
    let persisted_identity = intent.persisted_identity()?;
    let inserted = crate::with_coven_sql_authority(|| {
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

pub(crate) fn local_blob_cleanup_intents_on(
    conn: &rusqlite::Connection,
) -> Result<Vec<(LocalBlobCleanupIntent, bool)>, DbError> {
    let mut statement = conn
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
                LocalBlobCleanupIntent::from_persisted(
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                )
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
                    )
                })?,
                row.get::<_, bool>(3)?,
            ))
        })
        .map_err(DbError::from)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
}

pub(crate) fn complete_local_blob_cleanup_on(
    conn: &rusqlite::Connection,
    namespace: &str,
    blob_id: &str,
    persisted_identity: &str,
) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM local_cleanup_intents
                 WHERE namespace = ?1 AND blob_id = ?2 AND copy_identity = ?3",
        (namespace, blob_id, persisted_identity),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub struct LocalBlobCleanup<'operation> {
    database: &'operation StoreDatabase,
}

impl<'operation> LocalBlobCleanup<'operation> {
    pub fn new(database: &'operation StoreDatabase) -> Self {
        Self { database }
    }

    /// Drain every committed cleanup obligation. A filesystem or database
    /// failure leaves the intent durable and fails the operation. `true` means
    /// every remaining intent is blocked by an active Store-write lease.
    pub async fn drain(&self) -> Result<bool, DbError> {
        let database = self.database;
        #[cfg(any(test, feature = "test-utils"))]
        database
            .reach_test_point(crate::DatabaseTestPoint::LocalBlobCleanupRequested)
            .await;
        let _cleanup_guard = database.local_blob_cleanup_permit().await;
        #[cfg(any(test, feature = "test-utils"))]
        database
            .reach_test_point(crate::DatabaseTestPoint::LocalBlobCleanupAcquired)
            .await;

        let intents = database
            .call_database(|session| session.local_blob_cleanup_intents())
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
            #[cfg(any(test, feature = "test-utils"))]
            database
                .reach_test_point(crate::DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
                    namespace: intent.namespace().to_string(),
                    blob_id: intent.blob_id().to_string(),
                })
                .await;
            let persisted_identity = intent.persisted_identity()?;
            database.apply_local_blob_cleanup_intent(&intent).await?;

            let namespace = intent.namespace().to_string();
            let blob_id = intent.blob_id().to_string();
            database
                .call_database(move |session| {
                    session.complete_local_blob_cleanup(&namespace, &blob_id, &persisted_identity)
                })
                .await?;
        }
        #[cfg(any(test, feature = "test-utils"))]
        database
            .reach_test_point(crate::DatabaseTestPoint::LocalBlobCleanupFinished)
            .await;
        Ok(pending)
    }
}

#[cfg(test)]
impl StoreSession<'_> {
    fn record_obsolete_copy_intent_for_test(
        &self,
        intent: &LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        record_obsolete_copy_intents_on(self.conn, self.blob_decls, intent)
    }
}

#[cfg(test)]
impl StoreDatabase {
    async fn record_obsolete_copy_intent_for_test(
        &self,
        intent: LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.record_obsolete_copy_intent_for_test(&intent))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthetic_store::open_test_db_with_blob;
    use coven_protocol::blob::{CacheFill, Provenance};
    use coven_protocol::store_commit::ObjectHash;
    use coven_protocol::synced_schema::BlobDecl;

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
        let database = StoreDatabase::new(&db);

        db.test_sql(move |conn| {
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
                hash = coven_protocol::blob::content_hash(b"bytes"),
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

            Ok(())
        })
        .await
        .expect("seed removed and live blob bindings");

        database
            .record_obsolete_copy_intent_for_test(LocalBlobCleanupIntent::for_row(
                "photos",
                "shared-id",
                "note_photos",
                "removed-row",
            ))
            .await
            .expect("record obsolete row copies");

        db.test_sql(|conn| {
            conn.query(
                "SELECT copy_identity FROM local_cleanup_intents ORDER BY copy_identity",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(DbError::from)
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
