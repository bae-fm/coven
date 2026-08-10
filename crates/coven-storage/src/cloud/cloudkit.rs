//! CloudKit-backed `CloudHome` implementation.
//!
//! CloudKit's CKAsset has a 50MB limit, so large files are split into 10MB
//! chunks stored as tokened part records plus a manifest record.
//!
//! The `CloudKitOps` trait defines synchronous record operations implemented by
//! a host bridge to its CloudKit driver. `CloudKitCloudHome` wraps these ops,
//! adds chunking logic, and implements `CloudHome`.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use coven_foundation::id_provider::{IdRef, UuidProvider};

use super::{
    combine_cleanup_failure, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, CloudObjectVersion, CloudVersionedObject, ExactSlotStorage, RevokeOutcome,
    UploadProgress,
};
use coven_protocol::objects::ObjectSlot;

const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
const CHUNK_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-chunk-manifest-v1\0";
const CHUNK_MANIFEST_SUFFIX: &str = ".manifest";

mod chunking;
use chunking::*;
use part_sink::CloudKitPartSink;
mod exact;
mod part_sink;

/// Synchronous interface for raw CloudKit record operations.
/// Implemented by a host bridge to its platform CloudKit driver.
/// Methods block the calling thread while CloudKit async operations complete.
pub trait CloudKitOps: Send + Sync {
    /// Stable CloudKit namespace and principal facts for the selected zone.
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError>;

    /// Fetch the accepted CKShare for a shared scope and return its exact
    /// canonical record bytes plus the participant facts verified by the host.
    fn accepted_read_write_share(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError>;

    fn write_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), CloudHomeError>;
    fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    fn list_records(
        &self,
        scope: &CloudKitScope,
        prefix: &str,
    ) -> Result<Vec<String>, CloudHomeError>;
    fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError>;
    fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError>;
    /// Read the exact CKRecord and return its opaque `recordChangeTag` with the bytes.
    fn read_versioned_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
    ) -> Result<CloudVersionedObject, CloudHomeError>;
    /// Open a host-owned local staging batch. Staging never creates CloudKit
    /// records; the host keeps payloads in temporary CKAsset files until commit.
    fn begin_atomic_create(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError>;
    /// Stage one bounded record payload in the host-owned batch.
    fn stage_atomic_create_record(
        &self,
        scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
        record: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError>;
    /// Create every staged record as one atomic custom-zone modification. Every
    /// record uses CloudKit's create-only save policy. A known precommit failure
    /// leaves no record present. If the commit response is lost, the whole batch
    /// may be present; preserve those records so the caller can read back every
    /// requested key and settle the outcome.
    /// Returned versions follow staging order when the response is received.
    fn commit_atomic_create(
        &self,
        scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError>;
    /// Discard host-local staging without deleting any CloudKit records the batch
    /// may have committed. This is idempotent. On failure, return an error naming
    /// the batch; the caller surfaces it and does not hide or retry it.
    fn discard_atomic_create(
        &self,
        scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError>;
    /// Delete exactly these fetched record versions as one CloudKit atomic zone
    /// modification. A changed or missing record fails the whole deletion.
    fn delete_record_versions(
        &self,
        scope: &CloudKitScope,
        records: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError>;
    fn share_for_member(
        &self,
        member_pubkey: &str,
    ) -> Result<Option<CloudKitShare>, CloudHomeError>;
    fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError>;
    fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError>;
    fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitRecordVersion {
    pub key: String,
    pub version: CloudObjectVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitRecordCreate {
    pub key: String,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CloudKitAtomicCreateBatch(String);

impl CloudKitAtomicCreateBatch {
    pub fn from_provider(value: String) -> Result<Self, CloudHomeError> {
        if value.is_empty() {
            return Err(CloudHomeError::Transport(
                "CloudKit returned an empty atomic-create batch id".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_provider(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CloudKitScope {
    Private,
    Shared {
        owner_name: String,
        zone_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitProviderIdentity {
    pub container_id: String,
    pub environment: coven_protocol::objects::CloudKitEnvironment,
    pub owner_name: String,
    pub zone_name: String,
    pub current_user_record_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitShare {
    pub share_url: String,
    pub owner_name: String,
    pub zone_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitAcceptedShareRecord {
    pub share_record_name: String,
    pub owner_name: String,
    pub zone_name: String,
    pub participant_record_name: String,
    pub permission: CloudKitSharePermission,
    pub acceptance: CloudKitShareAcceptance,
    pub canonical_record: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudKitSharePermission {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudKitShareAcceptance {
    Pending,
    Accepted,
}

/// CloudKit-backed cloud home with automatic chunking for large files.
#[derive(Clone)]
pub struct CloudKitCloudHome {
    ops: Arc<dyn CloudKitOps>,
    ids: IdRef,
    scope: CloudKitScope,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
}

impl CloudKitCloudHome {
    pub fn new_private(
        ops: Arc<dyn CloudKitOps>,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self::new_private_with_ids(ops, Arc::new(UuidProvider), exact_upload_verification)
    }

    pub(crate) fn new_private_with_ids(
        ops: Arc<dyn CloudKitOps>,
        ids: IdRef,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self {
            ops,
            ids,
            scope: CloudKitScope::Private,
            exact_upload_verification,
        }
    }

    pub fn new_shared(
        ops: Arc<dyn CloudKitOps>,
        owner_name: String,
        zone_name: String,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self {
            ops,
            ids: Arc::new(UuidProvider),
            scope: CloudKitScope::Shared {
                owner_name,
                zone_name,
            },
            exact_upload_verification,
        }
    }

    async fn begin_atomic_create(&self) -> Result<Arc<CloudKitStagingCleanup>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        tokio::task::spawn_blocking(move || {
            let batch = ops.begin_atomic_create(&scope)?;
            Ok(Arc::new(CloudKitStagingCleanup::new(ops, scope, batch)))
        })
        .await
        .map_err(|error| {
            CloudHomeError::Transport(format!(
                "CloudKit atomic-create staging task failed: {error}"
            ))
        })?
    }

    async fn settle_atomic_create_response_loss(
        &self,
        manifest_key: String,
    ) -> Result<AtomicCreateReadback, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        blocking(
            move || match ops.read_versioned_record(&scope, &manifest_key) {
                Ok(record) => {
                    exact::decode_exact_manifest(&record.bytes)?;
                    Ok(AtomicCreateReadback::Created)
                }
                Err(CloudHomeError::NotFound(_)) => Ok(AtomicCreateReadback::Absent),
                Err(error) => Err(error),
            },
        )
        .await
    }

    async fn exact_manifest(
        &self,
        slot: &ObjectSlot,
    ) -> Result<exact::ExactManifest, CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = slot.logical_key().to_string();
        blocking(move || {
            let record = ops.read_versioned_record(&scope, &key)?;
            exact::decode_exact_manifest(&record.bytes)
        })
        .await
    }

    async fn verify_exact_upload(
        &self,
        upload: &super::ExactUpload<'_>,
        created_response_was_observed: bool,
    ) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        match self.exact_upload_verification {
            ExactUploadVerification::UploadChecksum => Err(CloudHomeError::Configuration(
                "CloudKit does not accept a caller-supplied upload checksum".to_string(),
            )),
            ExactUploadVerification::MetadataHash => {
                let manifest = self.exact_manifest(upload.object().slot()).await?;
                if manifest.total_len as u64 != upload.object().stored_size()
                    || manifest.stored_hash != upload.object().stored_hash()
                {
                    return Err(CloudHomeError::SlotCollision(
                        upload.object().slot().logical_key().to_string(),
                    ));
                }
                Ok(())
            }
            ExactUploadVerification::Readback => {
                let ops = self.ops.clone();
                let scope = self.scope.clone();
                let key = upload.object().slot().logical_key().to_string();
                let bytes = blocking(move || {
                    exact::read_exact_cloudkit_object(&*ops, &scope, &key).map(|value| value.0)
                })
                .await?;
                upload.verify_stored_bytes(&bytes)
            }
            ExactUploadVerification::Unchecked => {
                super::exact_upload::accept_unchecked_create_response(
                    created_response_was_observed,
                    upload.object(),
                )
            }
        }
    }
}

pub async fn accept_share(
    ops: Arc<dyn CloudKitOps>,
    share_url: String,
) -> Result<CloudKitShare, CloudHomeError> {
    blocking(move || ops.accept_share(&share_url)).await
}

/// Run a synchronous CloudKit op on the blocking pool, mapping a join failure to a
/// storage error. The Swift bridge methods block, so every `CloudHome` method
/// wraps its call this way — one helper instead of the same `spawn_blocking(...)
/// .await.map_err(...)` in each.
async fn blocking<T, F>(f: F) -> Result<T, CloudHomeError>
where
    F: FnOnce() -> Result<T, CloudHomeError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| CloudHomeError::Transport(format!("spawn_blocking failed: {e}")))?
}

struct BlockingState<T> {
    result: std::sync::Mutex<Option<std::thread::Result<T>>>,
    ready: std::sync::Condvar,
    notify: tokio::sync::Notify,
}

struct BlockingCompletion<T> {
    state: Arc<BlockingState<T>>,
    consumed: bool,
}

impl<T> Drop for BlockingCompletion<T> {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        let mut result = self.state.result.lock().expect("lock blocking result");
        while result.is_none() {
            result = self
                .state
                .ready
                .wait(result)
                .expect("wait for blocking result");
        }
    }
}

async fn cancellation_safe_blocking<T, F>(f: F) -> Result<T, CloudHomeError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let state = Arc::new(BlockingState {
        result: std::sync::Mutex::new(None),
        ready: std::sync::Condvar::new(),
        notify: tokio::sync::Notify::new(),
    });
    let worker_state = state.clone();
    tokio::task::spawn_blocking(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        worker_state
            .result
            .lock()
            .expect("lock blocking result")
            .replace(result);
        worker_state.ready.notify_all();
        worker_state.notify.notify_one();
    });
    let mut completion = BlockingCompletion {
        state,
        consumed: false,
    };
    let result = loop {
        let notified = completion.state.notify.notified();
        if let Some(result) = completion
            .state
            .result
            .lock()
            .expect("lock blocking result")
            .take()
        {
            break result;
        }
        notified.await;
    };
    completion.consumed = true;
    result.map_err(|_| CloudHomeError::Transport("CloudKit blocking task panicked".to_string()))
}

#[async_trait]
impl CloudHome for CloudKitCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let k = key.to_string();
        blocking(move || ops.write_record(&scope, &k, data)).await?;

        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let k = key.to_string();
        blocking(move || delete_chunk_layout(&*ops, &scope, &k)).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<super::BoxPartSink<'a>, CloudHomeError> {
        let total_len = usize::try_from(total_len).map_err(|_| {
            CloudHomeError::Transport(format!(
                "CloudKit object {key} is too large for this platform"
            ))
        })?;
        Ok(Box::new(CloudKitPartSink::new(
            self.ops.clone(),
            self.scope.clone(),
            key.to_string(),
            self.ids.new_id(),
            total_len,
        )))
    }

    fn multipart_threshold(&self) -> u64 {
        CHUNK_SIZE as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || {
            match ops.read_record(&scope, &key) {
                Ok(data) => return Ok(data),
                Err(CloudHomeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }

            match ops.read_record(&scope, &chunk_manifest_key(&key)) {
                Ok(data) => {
                    let manifest = decode_chunk_manifest(&data)?;
                    return read_chunked_object(&*ops, &scope, &key, manifest);
                }
                Err(CloudHomeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }

            Err(missing_or_unassembled(&*ops, &scope, key))
        })
        .await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        if end <= start {
            return Ok(Vec::new());
        }

        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || {
            let start = start as usize;
            let end = end as usize;

            match ops.read_record(&scope, &key) {
                Ok(data) => {
                    if end > data.len() {
                        return Err(CloudHomeError::Transport(format!(
                            "range {start}..{end} exceeds file size {}",
                            data.len()
                        )));
                    }
                    return Ok(data[start..end].to_vec());
                }
                Err(CloudHomeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }

            match ops.read_record(&scope, &chunk_manifest_key(&key)) {
                Ok(data) => {
                    let manifest = decode_chunk_manifest(&data)?;
                    let chunks = list_numbered_chunks(&*ops, &scope, &key, &manifest)?;
                    verify_chunk_manifest(&key, &manifest, &chunks)?;
                    if end > manifest.total_len {
                        return Err(CloudHomeError::Transport(format!(
                            "range {start}..{end} exceeds file size {}",
                            manifest.total_len
                        )));
                    }

                    let first_chunk = start / CHUNK_SIZE;
                    let last_chunk = (end - 1) / CHUNK_SIZE;
                    let mut result = Vec::with_capacity(end - start);
                    for (i, chunk_key) in chunks
                        .iter()
                        .filter(|(i, _)| (first_chunk..=last_chunk).contains(i))
                    {
                        let chunk = read_chunk(&*ops, &scope, &key, &manifest, *i, chunk_key)?;
                        let chunk_start = i * CHUNK_SIZE;
                        let slice_start = if *i == first_chunk {
                            start - chunk_start
                        } else {
                            0
                        };
                        let slice_end = if *i == last_chunk {
                            end - chunk_start
                        } else {
                            chunk.len()
                        };
                        result.extend_from_slice(&chunk[slice_start..slice_end]);
                    }
                    return Ok(result);
                }
                Err(CloudHomeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }

            Err(missing_or_unassembled(&*ops, &scope, key))
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let prefix = prefix.to_string();
        blocking(move || {
            let raw_keys = ops.list_records(&scope, &prefix)?;

            // A base key exists only when its single record or its manifest is
            // present — the manifest is what makes a chunked object readable. Part
            // records with no manifest are an incomplete or aborted upload, which
            // `read` cannot assemble, so they are not reported.
            let present: HashSet<&str> = raw_keys.iter().map(String::as_str).collect();
            let mut base_keys: Vec<String> = raw_keys
                .iter()
                .map(|k| strip_part_suffix(k))
                .filter(|&base| {
                    present.contains(base) || present.contains(chunk_manifest_key(base).as_str())
                })
                .map(str::to_string)
                .collect();
            base_keys.sort();
            base_keys.dedup();
            Ok(base_keys)
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || delete_all_variants(&*ops, &scope, &key)).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || {
            if ops.record_exists(&scope, &key)? {
                return Ok(true);
            }
            let manifest = match ops.read_record(&scope, &chunk_manifest_key(&key)) {
                Ok(data) => decode_chunk_manifest(&data)?,
                Err(CloudHomeError::NotFound(_)) => return Ok(false),
                Err(e) => return Err(e),
            };
            let chunks = list_numbered_chunks(&*ops, &scope, &key, &manifest)?;
            Ok(verify_chunk_manifest(&key, &manifest, &chunks).is_ok())
        })
        .await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let ops = self.ops.clone();
        match desired {
            CloudAccessState::Present { member_pubkey, .. } => {
                // CloudKit shares bind the joiner's identity at URL-accept time,
                // so no provider email is required.
                let lookup_ops = ops.clone();
                let lookup_member = member_pubkey.clone();
                let existing =
                    blocking(move || lookup_ops.share_for_member(&lookup_member)).await?;
                let expected = match existing {
                    Some(share) => share,
                    None => {
                        let grant_ops = ops.clone();
                        let grant_member = member_pubkey.clone();
                        blocking(move || grant_ops.grant_share(&grant_member)).await?
                    }
                };
                let verified = blocking(move || ops.share_for_member(&member_pubkey))
                    .await?
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "CloudKit member share is absent after setting it present".to_string(),
                        )
                    })?;
                if verified != expected {
                    return Err(CloudHomeError::Transport(
                        "CloudKit member share changed while verifying present access".to_string(),
                    ));
                }
                Ok(CloudAccessOutcome::Present(
                    CloudHomeJoinInfo::CloudKitShare {
                        share_url: verified.share_url,
                        owner_name: verified.owner_name,
                        zone_name: verified.zone_name,
                    },
                ))
            }
            CloudAccessState::Absent { member_pubkey, .. } => {
                let lookup_ops = ops.clone();
                let lookup_member = member_pubkey.clone();
                if blocking(move || lookup_ops.share_for_member(&lookup_member))
                    .await?
                    .is_some()
                {
                    let revoke_ops = ops.clone();
                    let revoke_member = member_pubkey.clone();
                    blocking(move || revoke_ops.revoke_share(&revoke_member)).await?;
                }
                if blocking(move || ops.share_for_member(&member_pubkey))
                    .await?
                    .is_some()
                {
                    return Err(CloudHomeError::Transport(
                        "CloudKit member share remains after setting access absent".to_string(),
                    ));
                }
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[cfg(test)]
mod tests;
