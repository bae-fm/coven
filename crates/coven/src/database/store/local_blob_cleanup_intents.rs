//! Durable deletion of obsolete logical-source and exact-locator copies.

use crate::database::DbError;

pub(crate) fn intents_from_changes(
    blob_decls: &crate::database::BlobDecls,
    old_changes: &[crate::changeset::RowChange],
    new_changes: &[crate::changeset::RowChange],
) -> Result<Vec<LocalBlobCleanupIntent>, crate::database::BlobDeclError> {
    if old_changes.len() != new_changes.len() {
        return Err(crate::database::BlobDeclError::ChangesetWalkMismatch {
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
                crate::database::BlobDeclError::MissingPublicationPrimaryKey {
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
pub(crate) enum LocalBlobCleanupIdentity {
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

    pub(crate) fn exact(
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

    pub(crate) fn identity(&self) -> &LocalBlobCleanupIdentity {
        &self.identity
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
