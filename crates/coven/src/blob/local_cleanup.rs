//! Durable deletion of obsolete logical-source and exact-locator copies.

use rusqlite::Connection;
use tracing::debug;

use crate::blob::decl::BlobDecls;
use crate::database::DbError;

pub(crate) fn intents_from_changes(
    blob_decls: &crate::blob::decl::BlobDecls,
    old_changes: &[crate::changeset::RowChange],
    new_changes: &[crate::changeset::RowChange],
) -> Result<Vec<LocalBlobCleanupIntent>, crate::blob::decl::BlobDeclError> {
    if old_changes.len() != new_changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: new_changes.len(),
        });
    }
    let mut intents = Vec::new();
    for (old, new) in old_changes.iter().zip(new_changes) {
        let old_blob_to_drop = match old.op {
            crate::changeset::ChangeOp::Delete => blob_decls.ref_from_change(old)?,
            crate::changeset::ChangeOp::Update => {
                let Some(old_blob) = blob_decls.ref_from_change(old)? else {
                    continue;
                };
                let should_drop = match blob_decls.ref_from_change(new)? {
                    Some(new_blob) => {
                        old_blob.namespace != new_blob.namespace || old_blob.id != new_blob.id
                    }
                    None => true,
                };
                should_drop.then_some(old_blob)
            }
            crate::changeset::ChangeOp::Insert => None,
        };
        if let Some(blob) = old_blob_to_drop {
            let row_id = old.pk().ok_or_else(|| {
                crate::blob::decl::BlobDeclError::MissingPublicationPrimaryKey {
                    table: old.table.clone(),
                }
            })?;
            intents.push(LocalBlobCleanupIntent::for_row(
                blob.namespace,
                blob.id,
                old.table.clone(),
                row_id,
            ));
        }
    }
    Ok(intents)
}
use crate::store_dir::StoreDir;

/// A transaction-local request that resolves into committed copy-specific cleanup
/// obligations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalBlobCleanupIntent {
    namespace: String,
    blob_id: String,
    identity: LocalBlobCleanupIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalBlobCleanupIdentity {
    Local,
    Row { table: String, row_id: String },
    Exact(crate::protocol::store_commit::ObjectHash),
}

impl LocalBlobCleanupIntent {
    pub(crate) fn local(namespace: impl Into<String>, blob_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
            identity: LocalBlobCleanupIdentity::Local,
        }
    }

    pub(crate) fn for_row(
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
        locator_hash: crate::protocol::store_commit::ObjectHash,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            blob_id: blob_id.into(),
            identity: LocalBlobCleanupIdentity::Exact(locator_hash),
        }
    }

    pub(crate) fn persisted_identity(&self) -> Result<String, DbError> {
        match &self.identity {
            LocalBlobCleanupIdentity::Local => Ok("local".to_string()),
            LocalBlobCleanupIdentity::Exact(locator_hash) => Ok(locator_hash.to_string()),
            LocalBlobCleanupIdentity::Row { .. } => Err(DbError::Message(
                "row-bound local cleanup identity is not durable".to_string(),
            )),
        }
    }

    pub(crate) fn from_persisted(
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

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn blob_id(&self) -> &str {
        &self.blob_id
    }

    pub(crate) async fn apply(&self, store_dir: &StoreDir) -> Result<(), DbError> {
        let cleanup = match &self.identity {
            LocalBlobCleanupIdentity::Local => store_dir
                .remove_local_blob(self.namespace(), self.blob_id())
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            LocalBlobCleanupIdentity::Exact(locator_hash) => store_dir
                .remove_cached_locator(self.namespace(), *locator_hash)
                .await
                .map_err(|error| error.to_string()),
            LocalBlobCleanupIdentity::Row { .. } => {
                return Err(DbError::Message(
                    "persisted local cleanup intent is row-bound".to_string(),
                ));
            }
        };
        cleanup.map_err(|error| {
            DbError::Message(format!(
                "remove local copies for {}/{}: {error}",
                self.namespace(),
                self.blob_id()
            ))
        })
    }
}

/// Record cleanup obligations for each copy identity no live row needs in this
/// transaction. The caller must use the transaction that mutates the carrying
/// rows, so each obligation commits atomically with the state that makes that
/// copy obsolete. A caller enforcing logical-ID deletion policy checks that
/// policy before calling; obsolete exact copies are independent of that policy.
pub(crate) fn record_obsolete_copy_intents_on(
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
                        .parse::<crate::protocol::store_commit::ObjectHash>()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{CacheFill, Provenance};
    use crate::protocol::store_commit::ObjectHash;
    use crate::sync::session::BlobDecl;
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
