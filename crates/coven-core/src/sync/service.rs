//! Full sync orchestrator: gate + push local changes, pull remote changes.
//!
//! Protocol within a cycle:
//! 1. The caller drained the outgoing changeset from the pending-changeset journal
//!    and passes the bytes in.
//! 2. Gate the captured changeset (cut gated-false rows, re-emit on flip).
//! 3. Push our changeset's blobs, then build the signed envelope to push.
//! 4. Pull incoming changesets and apply them. An apply is a plain connection
//!    write, never a journaled one, so applied rows are never recorded as this
//!    device's own outgoing changes — while a host write during the network phases
//!    journals normally.
//! 5. The caller runs snapshot policy.
//!
//! All connection access goes through the owned [`Database`]; only a host write
//! wrapped in a journaled transaction is ever captured, so applies need no special
//! handling.

use crate::database::{
    Database, StoreBatchCompletion, StoreBatchLocalCleanup, StoreWriteBlobFacts,
};
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

use super::membership::{MembershipChain, MembershipGrantCreationAuthority};
use super::storage::StorageError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeferredLocalBlobDisposition {
    Drop,
    Cache,
    Pin,
}

impl DeferredLocalBlobDisposition {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Cache => "cache",
            Self::Pin => "pin",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredLocalBlobDrop {
    pub namespace: String,
    pub id: String,
    pub size: u64,
    pub disposition: DeferredLocalBlobDisposition,
}

pub(crate) struct PreparedStorePayload {
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
    pub membership_authority: Option<MembershipGrantCreationAuthority>,
}

/// Upload the blobs referenced by exact staged package bytes and persist every
/// fact needed to retry their publication without re-deriving it from later rows.
pub(crate) async fn prepare_store_payload(
    blob_facts: &StoreWriteBlobFacts,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership_chain: Option<&MembershipChain>,
) -> Result<PreparedStorePayload, SyncCycleError> {
    let mut drops = std::collections::BTreeMap::new();
    for fact in &blob_facts.blobs {
        if fact.blob.provenance != crate::blob::Provenance::HostProvided {
            continue;
        }
        let present = crate::blob::local_files::path_if_present(
            store_dir,
            &fact.blob.namespace,
            &fact.blob.id,
            fact.plaintext_size,
        )
        .await
        .map_err(|error| SyncCycleError::AssetScan(error.to_string()))?;
        if present.is_none() {
            continue;
        }
        let disposition = match fact.blob.fill {
            crate::blob::CacheFill::CacheEager => DeferredLocalBlobDisposition::Cache,
            crate::blob::CacheFill::CacheLazy => DeferredLocalBlobDisposition::Drop,
        };
        let drop = DeferredLocalBlobDrop {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
            size: fact.plaintext_size,
            disposition,
        };
        let key = (drop.namespace.clone(), drop.id.clone());
        if let Some(prior) = drops.insert(key, drop.clone()) {
            if prior != drop {
                return Err(SyncCycleError::AssetScan(format!(
                    "captured Store write gives blob {}/{} conflicting local cleanup facts",
                    drop.namespace, drop.id,
                )));
            }
        }
    }
    Ok(PreparedStorePayload {
        local_cleanup: StoreBatchLocalCleanup {
            drops: drops.into_values().collect(),
        },
        completion: StoreBatchCompletion {},
        membership_authority: resolve_write_authority(membership_chain, keypair),
    })
}

/// The storage coordinate of the membership entry that authorizes this device
/// to write. `None` means a pre-initialization caller supplied no chain or the
/// current identity has no write grant; an initialized authorized writer has a
/// coordinate.
/// Embedded in the outgoing changeset so a puller can resolve a
/// membership-propagation gap. Read off the cycle's once-loaded chain, so it
/// judges the same membership state as the rest of the cycle rather than
/// re-listing (the very disagreement that once had a puller skip the write it was
/// meant to accept).
fn resolve_write_authority(
    membership_chain: Option<&MembershipChain>,
    keypair: &UserKeypair,
) -> Option<MembershipGrantCreationAuthority> {
    let our_pubkey = hex::encode(keypair.public_key());
    membership_chain.and_then(|chain| chain.write_grant_authority(&our_pubkey))
}

pub async fn apply_deferred_local_blob_drop(
    db: &Database,
    store_dir: &StoreDir,
    deferred: &DeferredLocalBlobDrop,
) -> Result<(), SyncCycleError> {
    let local = crate::blob::local_files::path_if_present(
        store_dir,
        &deferred.namespace,
        &deferred.id,
        deferred.size,
    )
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    match (deferred.disposition, local) {
        (DeferredLocalBlobDisposition::Pin, Some(source)) => {
            let pinned = store_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            crate::local_blob::copy_atomic(&source, &pinned)
                .await
                .map_err(SyncCycleError::AssetUpload)?;
        }
        (DeferredLocalBlobDisposition::Cache, Some(source)) => {
            crate::blob::cache::write_blob_from_file(
                db,
                store_dir,
                &deferred.namespace,
                &deferred.id,
                &source,
            )
            .await
            .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
        }
        (DeferredLocalBlobDisposition::Drop, _) => {}
        // The source is gone. This disposition (copy to a destination, then drop the
        // source) is applied in one step but its intent clears in a separate commit,
        // so a crash in that window leaves the blob correctly placed with the intent
        // still pending. Recognize that finished work by its destination — Ok clears
        // the intent — and fail loud only when the destination is ALSO empty.
        (DeferredLocalBlobDisposition::Pin, None) => {
            let pinned = store_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&pinned, deferred).await;
        }
        (DeferredLocalBlobDisposition::Cache, None) => {
            let cached = store_dir
                .cache_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&cached, deferred).await;
        }
    }
    crate::blob::local_files::drop_blob(store_dir, &deferred.namespace, &deferred.id)
        .await
        .map(|_| ())
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))
}

/// A Pin/Cache disposition whose local-store source is gone is either already applied
/// — a prior drain copied the blob to `destination` and dropped the source, then
/// crashed before clearing its intent — or a genuine loss. Ok when the destination
/// holds the blob at its expected size (the work is done, so the caller clears the
/// intent); a loud Err when the destination is also empty (the bytes are gone, so the
/// intent stays pending and retries).
async fn recognize_applied_disposition_or_fail(
    destination: &std::path::Path,
    deferred: &DeferredLocalBlobDrop,
) -> Result<(), SyncCycleError> {
    let present = crate::local_blob::exists(destination)
        .await
        .map_err(SyncCycleError::AssetUpload)?
        && crate::local_blob::file_len(destination)
            .await
            .map_err(SyncCycleError::AssetUpload)?
            == deferred.size;
    if present {
        return Ok(());
    }
    Err(SyncCycleError::AssetUpload(format!(
        "published blob {}/{} is missing from both the local store and its {:?} destination",
        deferred.namespace, deferred.id, deferred.disposition
    )))
}

#[derive(Debug)]
pub enum SyncCycleError {
    Database(crate::database::DbError),
    Gate(String),
    AssetScan(String),
    AssetUpload(String),
    Storage {
        operation: &'static str,
        source: StorageError,
    },
    /// An outgoing changeset still names a user-owned local file.
    LocalUserBlob {
        namespace: String,
        id: String,
    },
    /// An outgoing changeset references bytes that are absent from their required
    /// publication location.
    MissingPreparedBlob {
        namespace: String,
        id: String,
    },
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Database(error) => write!(f, "database error: {error}"),
            SyncCycleError::Gate(e) => write!(f, "gate error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
            SyncCycleError::Storage { operation, source } => {
                write!(f, "{operation}: {source}")
            }
            SyncCycleError::LocalUserBlob { namespace, id } => {
                write!(
                    f,
                    "user-provided blob {namespace}/{id} still has a local external ref"
                )
            }
            SyncCycleError::MissingPreparedBlob { namespace, id } => {
                write!(
                    f,
                    "blob {namespace}/{id} has no prepared exact publication object"
                )
            }
        }
    }
}

impl std::error::Error for SyncCycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(source) => Some(source),
            Self::Storage { source, .. } => Some(source),
            Self::Gate(_)
            | Self::AssetScan(_)
            | Self::AssetUpload(_)
            | Self::LocalUserBlob { .. }
            | Self::MissingPreparedBlob { .. } => None,
        }
    }
}
