use std::sync::Arc;

use crate::blob::CacheFill;
use crate::database::StoreDatabase;
use crate::database::TableSchema;
use crate::database::ValidatedChangeset;
use crate::protocol::audience_package::AudiencePackage;
use crate::storage::BlobSpoolProtection;
use crate::store_dir::StoreDir;
use crate::sync::store::blob::RemoteBlobSource;

use super::pull::{
    BlobDownloadFailure, BlobDownloadFailures, HeldStorePositionReason,
    PreparedMergeMaterializationPackage, StorePullError,
};

pub(super) struct PullPackageMaterializer<'storage> {
    database: StoreDatabase,
    blob_source: RemoteBlobSource<'storage>,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    store_dir: StoreDir,
    schema: Arc<TableSchema>,
}

impl<'storage> PullPackageMaterializer<'storage> {
    pub(super) fn new(
        database: StoreDatabase,
        blob_source: RemoteBlobSource<'storage>,
        blob_cache: crate::sync::store::blob::StoreBlobCache,
        store_dir: StoreDir,
        schema: Arc<TableSchema>,
    ) -> Self {
        Self {
            blob_cache,
            database,
            blob_source,
            store_dir,
            schema,
        }
    }

    pub(super) fn store_blob_protection(
        &self,
    ) -> Result<BlobSpoolProtection, crate::storage::StorageError> {
        self.blob_source.store_protection()
    }

    pub(super) async fn prepare(
        &self,
        package: AudiencePackage,
        blob_protection: BlobSpoolProtection,
    ) -> Result<Result<PreparedMergeMaterializationPackage, HeldStorePositionReason>, StorePullError>
    {
        let changeset =
            match ValidatedChangeset::new(package.changeset().to_vec(), self.schema.clone()) {
                Ok(changeset) => changeset,
                Err(crate::database::ChangesetIdentityError::Row(error)) => {
                    return Ok(Err(HeldStorePositionReason::InvalidRowIdentity {
                        table: error.table().to_string(),
                        reason: error.to_string(),
                    }))
                }
                Err(error) => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                        error.to_string(),
                    )))
                }
            };
        let changes = match crate::database::walk_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => return Ok(Err(HeldStorePositionReason::InvalidChangeset(error))),
        };
        let old_changes = match crate::database::walk_old_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => return Ok(Err(HeldStorePositionReason::InvalidChangeset(error))),
        };
        let blob_decls = self.database.blob_decls();
        let mut eager = Vec::new();
        for change in &changes {
            if change.op == crate::changeset::ChangeOp::Delete {
                continue;
            }
            let blob = match blob_decls.ref_from_change(change) {
                Ok(blob) => blob,
                Err(error) => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                        error.to_string(),
                    )))
                }
            };
            let Some(blob) = blob else {
                continue;
            };
            if blob.fill != CacheFill::CacheEager {
                continue;
            }
            let row_id = match change.pk() {
                Some(row_id) => row_id,
                None => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangeset(format!(
                        "blob-bearing incoming row {:?} has no primary key",
                        change.table
                    ))))
                }
            };
            let matches = package
                .blob_bindings()
                .iter()
                .filter(|binding| {
                    binding.table() == change.table
                        && binding.row_id() == row_id
                        && binding.blob().locator().namespace() == blob.namespace
                        && binding.blob().locator().blob_id() == blob.id
                })
                .collect::<Vec<_>>();
            let [binding] = matches.as_slice() else {
                return Ok(Err(HeldStorePositionReason::InvalidChangeset(format!(
                    "incoming eager blob row {:?}/{row_id:?} has {} exact locator bindings",
                    change.table,
                    matches.len()
                ))));
            };
            eager.push(binding.blob().clone());
        }
        let mut verified = Vec::new();
        let mut failures = Vec::new();
        for binding in package.blob_bindings() {
            let stored = binding.blob();
            if verified.iter().any(|candidate| candidate == stored) {
                continue;
            }
            verified.push(stored.clone());
            let locator = stored.locator();
            let retain = eager.iter().any(|download| download == stored);
            if let Err(cause) = self
                .blob_source
                .verify_plaintext_with_protection(
                    &self.blob_cache,
                    stored,
                    blob_protection.clone(),
                    retain,
                )
                .await
            {
                failures.push(BlobDownloadFailure {
                    namespace: locator.namespace().to_string(),
                    id: locator.blob_id().to_string(),
                    cause,
                });
            }
        }
        if !failures.is_empty() {
            let failures = BlobDownloadFailures::new(failures);
            if failures.has_transport_failure() {
                return Err(StorePullError::BlobDownloads(failures));
            }
            return Ok(Err(HeldStorePositionReason::BlobDownloadFailed));
        }
        if let Err(error) =
            crate::blob::local_cleanup::intents_from_changes(&blob_decls, &old_changes, &changes)
        {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )));
        }
        Ok(Ok(PreparedMergeMaterializationPackage {
            package,
            changeset,
        }))
    }

    pub(super) async fn finish_cleanup(&self) -> Result<bool, crate::database::DbError> {
        self.database
            .drain_local_blob_cleanup(&self.store_dir)
            .await
    }
}
