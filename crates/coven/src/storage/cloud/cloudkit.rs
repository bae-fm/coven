//! CloudKit-backed `CloudHome` implementation.
//!
//! CloudKit's CKAsset has a 50MB limit, so large files are split into 10MB
//! chunks stored as tokened part records plus a manifest record.
//!
//! The `CloudKitOps` trait defines synchronous record operations implemented by
//! a host bridge to its CloudKit driver. `CloudKitCloudHome` wraps these ops,
//! adds chunking logic, and implements `CloudHome`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::id_provider::{IdRef, UuidProvider};

use super::{
    AppendedListing, AppendedObject, BlobBody, CloudAccessOutcome, CloudAccessState,
    CloudHeadCreateError, CloudHeadReplaceError, CloudHeadStorage, CloudHeadVersion, CloudHome,
    CloudHomeError, CloudHomeJoinInfo, CloudVersionedHead, ImmutableCopyStorage, ListingCoverage,
    RevokeOutcome, UploadProgress,
};

const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
const CHUNK_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-chunk-manifest-v1\0";
const CHUNK_MANIFEST_SUFFIX: &str = ".manifest";

/// Synchronous interface for raw CloudKit record operations.
/// Implemented by a host bridge to its platform CloudKit driver.
/// Methods block the calling thread while CloudKit async operations complete.
pub trait CloudKitOps: Send + Sync {
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
    /// Read the exact CKRecord and return its opaque `recordChangeTag` with the
    /// bytes. A later replacement must mutate this fetched record.
    fn read_versioned_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
    ) -> Result<CloudVersionedHead, CloudHomeError>;
    fn create_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        data: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError>;
    /// Replace by saving the previously fetched CKRecord with
    /// `ifServerRecordUnchanged`; copying its tag into a new CKRecord is invalid.
    fn replace_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        expected: &CloudHeadVersion,
        data: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError>;
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
    /// may be present; preserve those records so `record_changes` can report them.
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
    /// Return one ordered page of the custom zone's record history. `after=None`
    /// starts a complete enumeration; the terminal page carries the server token
    /// that bounds the resulting live-record view.
    fn record_changes(
        &self,
        scope: &CloudKitScope,
        after: Option<&CloudKitChangeToken>,
    ) -> Result<CloudKitRecordChangesPage, CloudHomeError>;
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
pub struct CloudKitChangeToken(String);

impl CloudKitChangeToken {
    pub fn from_provider(value: String) -> Result<Self, CloudHomeError> {
        if value.is_empty() {
            return Err(CloudHomeError::Transport(
                "CloudKit returned an empty zone change token".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_provider(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitRecordVersion {
    pub key: String,
    pub version: CloudHeadVersion,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudKitRecordChange {
    Present(CloudKitRecordVersion),
    Deleted { key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudKitRecordChangesContinuation {
    More(CloudKitChangeToken),
    Complete(CloudKitChangeToken),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKitRecordChangesPage {
    pub changes: Vec<CloudKitRecordChange>,
    pub continuation: CloudKitRecordChangesContinuation,
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
pub struct CloudKitShare {
    pub share_url: String,
    pub owner_name: String,
    pub zone_name: String,
}

/// CloudKit-backed cloud home with automatic chunking for large files.
#[derive(Clone)]
pub(crate) struct CloudKitCloudHome {
    ops: Arc<dyn CloudKitOps>,
    ids: IdRef,
    scope: CloudKitScope,
}

#[async_trait]
impl CloudHeadStorage for CloudKitCloudHome {
    async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || ops.read_versioned_record(&scope, &key)).await
    }

    async fn create_head(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || ops.create_record(&scope, &key, bytes))
            .await
            .map_err(|error| {
                CloudHeadCreateError::Storage(CloudHomeError::Transport(format!(
                    "spawn_blocking failed: {error}"
                )))
            })?
    }

    async fn replace_head(
        &self,
        key: &str,
        expected: &CloudHeadVersion,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        let expected = expected.clone();
        tokio::task::spawn_blocking(move || ops.replace_record(&scope, &key, &expected, bytes))
            .await
            .map_err(|error| {
                CloudHeadReplaceError::Storage(CloudHomeError::Transport(format!(
                    "spawn_blocking failed: {error}"
                )))
            })?
    }

    async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = key.to_string();
        blocking(move || ops.delete_record(&scope, &key)).await
    }
}

impl CloudKitCloudHome {
    pub(crate) fn new_private(ops: Arc<dyn CloudKitOps>) -> Self {
        Self::new_private_with_ids(ops, Arc::new(UuidProvider))
    }

    pub(crate) fn new_private_with_ids(ops: Arc<dyn CloudKitOps>, ids: IdRef) -> Self {
        Self {
            ops,
            ids,
            scope: CloudKitScope::Private,
        }
    }

    pub(crate) fn new_shared(
        ops: Arc<dyn CloudKitOps>,
        owner_name: String,
        zone_name: String,
    ) -> Self {
        Self::new_shared_with_ids(ops, Arc::new(UuidProvider), owner_name, zone_name)
    }

    pub(crate) fn new_shared_with_ids(
        ops: Arc<dyn CloudKitOps>,
        ids: IdRef,
        owner_name: String,
        zone_name: String,
    ) -> Self {
        Self {
            ops,
            ids,
            scope: CloudKitScope::Shared {
                owner_name,
                zone_name,
            },
        }
    }
}

pub(crate) async fn accept_share(
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

/// If a key ends with CloudKit layout metadata, strip that suffix to get the
/// base object key.
fn strip_part_suffix(key: &str) -> &str {
    if let Some(base) = key.strip_suffix(CHUNK_MANIFEST_SUFFIX) {
        return base;
    }
    if let Some(idx) = key.rfind(".part") {
        let after = &key[idx + 5..];
        let digits = after
            .as_bytes()
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if digits > 0 && (digits == after.len() || after.as_bytes()[digits] == b'.') {
            return &key[..idx];
        }
    }
    key
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChunkManifest {
    part_count: usize,
    total_len: usize,
    upload_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CloudKitStoredRecordId {
    key: String,
    version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "layout", rename_all = "snake_case", deny_unknown_fields)]
enum CloudKitAppendedId {
    Single {
        record: CloudKitStoredRecordId,
    },
    Chunked {
        manifest: CloudKitStoredRecordId,
        parts: Vec<CloudKitStoredRecordId>,
    },
}

impl CloudKitStoredRecordId {
    fn from_record(record: &CloudKitRecordVersion) -> Self {
        Self {
            key: record.key.clone(),
            version: record.version.as_provider().to_string(),
        }
    }

    fn to_record(&self) -> Result<CloudKitRecordVersion, CloudHomeError> {
        if self.key.is_empty() {
            return Err(CloudHomeError::Configuration(
                "CloudKit appended record key is empty".to_string(),
            ));
        }
        Ok(CloudKitRecordVersion {
            key: self.key.clone(),
            version: CloudHeadVersion::from_provider(self.version.clone())?,
        })
    }
}

impl CloudKitAppendedId {
    fn encode(&self) -> Result<String, CloudHomeError> {
        serde_json::to_string(self).map_err(|error| {
            CloudHomeError::Configuration(format!(
                "encode CloudKit appended object identity: {error}"
            ))
        })
    }

    fn decode(object: &AppendedObject) -> Result<Self, CloudHomeError> {
        let identity: Self =
            serde_json::from_str(object.opaque_provider_id()).map_err(|error| {
                CloudHomeError::Configuration(format!(
                    "decode CloudKit appended object identity for {:?}: {error}",
                    object.logical_key()
                ))
            })?;
        identity.validate_for(object.logical_key())?;
        Ok(identity)
    }

    fn validate_for(&self, logical_key: &str) -> Result<(), CloudHomeError> {
        match self {
            Self::Single { record } if record.key == logical_key => Ok(()),
            Self::Chunked { manifest, parts }
                if manifest.key == chunk_manifest_key(logical_key) && !parts.is_empty() =>
            {
                let mut expected_upload_id = None;
                for (index, part) in parts.iter().enumerate() {
                    let prefix = format!("{logical_key}.part{index}.");
                    let upload_id = part.key.strip_prefix(&prefix).ok_or_else(|| {
                        CloudHomeError::Configuration(format!(
                            "CloudKit appended identity names an invalid part {index} for {logical_key:?}"
                        ))
                    })?;
                    if upload_id.is_empty() {
                        return Err(CloudHomeError::Configuration(format!(
                            "CloudKit appended identity names an empty upload id for {logical_key:?} part {index}"
                        )));
                    }
                    match expected_upload_id {
                        Some(expected) if expected != upload_id => {
                            return Err(CloudHomeError::Configuration(format!(
                                "CloudKit appended object {logical_key:?} mixes multipart upload ids"
                            )));
                        }
                        Some(_) => {}
                        None => expected_upload_id = Some(upload_id),
                    }
                }
                Ok(())
            }
            _ => Err(CloudHomeError::Configuration(format!(
                "CloudKit appended identity does not describe logical key {logical_key:?}"
            ))),
        }
    }

    fn records(self) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        match self {
            Self::Single { record } => Ok(vec![record.to_record()?]),
            Self::Chunked { manifest, parts } => {
                let mut records = Vec::with_capacity(parts.len() + 1);
                records.push(manifest.to_record()?);
                for part in parts {
                    records.push(part.to_record()?);
                }
                Ok(records)
            }
        }
    }
}

enum ExactCloudKitObjectLayout {
    Single(CloudKitRecordVersion),
    Chunked {
        manifest: ChunkManifest,
        parts: Vec<CloudKitRecordVersion>,
    },
}

impl ChunkManifest {
    fn new(total_len: usize, upload_id: String) -> Self {
        Self {
            part_count: total_len.div_ceil(CHUNK_SIZE),
            total_len,
            upload_id,
        }
    }
}

fn encode_chunk_manifest(manifest: ChunkManifest) -> Vec<u8> {
    let mut encoded = CHUNK_MANIFEST_MAGIC.to_vec();
    encoded.extend_from_slice(manifest.part_count.to_string().as_bytes());
    encoded.push(b'\n');
    encoded.extend_from_slice(manifest.total_len.to_string().as_bytes());
    encoded.push(b'\n');
    encoded.extend_from_slice(manifest.upload_id.as_bytes());
    encoded.push(b'\n');
    encoded
}

fn chunk_manifest_key(key: &str) -> String {
    format!("{key}{CHUNK_MANIFEST_SUFFIX}")
}

fn chunk_part_key(key: &str, upload_id: &str, index: usize) -> String {
    format!("{key}.part{index}.{upload_id}")
}

fn decode_chunk_manifest(data: &[u8]) -> Result<ChunkManifest, CloudHomeError> {
    let body = data.strip_prefix(CHUNK_MANIFEST_MAGIC).ok_or_else(|| {
        CloudHomeError::Transport("CloudKit chunk manifest missing magic".to_string())
    })?;
    let body = std::str::from_utf8(body).map_err(|e| {
        CloudHomeError::Transport(format!("CloudKit chunk manifest is not UTF-8: {e}"))
    })?;
    let mut lines = body.lines();
    let part_count = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit chunk manifest missing part count".to_string())
        })?
        .parse::<usize>()
        .map_err(|e| {
            CloudHomeError::Transport(format!(
                "CloudKit chunk manifest part count is invalid: {e}"
            ))
        })?;
    let total_len = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit chunk manifest missing total length".to_string())
        })?
        .parse::<usize>()
        .map_err(|e| {
            CloudHomeError::Transport(format!(
                "CloudKit chunk manifest total length is invalid: {e}"
            ))
        })?;
    let upload_id = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit chunk manifest missing upload id".to_string())
        })?
        .to_string();
    if lines.next().is_some() {
        return Err(CloudHomeError::Transport(
            "CloudKit chunk manifest has extra fields".to_string(),
        ));
    }
    if part_count == 0 || total_len == 0 {
        return Err(CloudHomeError::Transport(
            "CloudKit chunk manifest must describe a non-empty object".to_string(),
        ));
    }
    if upload_id.is_empty()
        || !upload_id
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
    {
        return Err(CloudHomeError::Transport(
            "CloudKit chunk manifest upload id is invalid".to_string(),
        ));
    }
    if ChunkManifest::new(total_len, upload_id.clone()).part_count != part_count {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit chunk manifest part count {part_count} does not match total length {total_len}"
        )));
    }
    Ok(ChunkManifest {
        part_count,
        total_len,
        upload_id,
    })
}

fn create_immutable_record(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
    data: Vec<u8>,
) -> Result<CloudKitRecordVersion, CloudHomeError> {
    let created = ops
        .create_record(scope, key, data)
        .map_err(|error| match error {
            CloudHeadCreateError::AlreadyExists => CloudHomeError::AlreadyExists(key.to_string()),
            CloudHeadCreateError::Storage(error) => error,
        })?;
    Ok(CloudKitRecordVersion {
        key: key.to_string(),
        version: created.version,
    })
}

fn read_immutable_record(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    expected: &CloudKitRecordVersion,
) -> Result<Vec<u8>, CloudHomeError> {
    let read = ops.read_versioned_record(scope, &expected.key)?;
    if read.version != expected.version {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit record {:?} changed from version {:?} to {:?}",
            expected.key,
            expected.version.as_provider(),
            read.version.as_provider()
        )));
    }
    Ok(read.bytes)
}

struct CloudKitStagingCleanup {
    ops: Arc<dyn CloudKitOps>,
    scope: CloudKitScope,
    batch: CloudKitAtomicCreateBatch,
    armed: std::sync::atomic::AtomicBool,
}

impl CloudKitStagingCleanup {
    fn disarm(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn cleanup_failure(&self, operation: CloudHomeError) -> CloudHomeError {
        self.disarm();
        match self.ops.discard_atomic_create(&self.scope, &self.batch) {
            Ok(()) => operation,
            Err(cleanup) => CloudHomeError::CleanupFailed {
                operation: Box::new(operation),
                cleanup: Box::new(CloudHomeError::Transport(format!(
                    "discard CloudKit atomic-create batch {:?}: {cleanup}",
                    self.batch.as_provider()
                ))),
            },
        }
    }
}

impl Drop for CloudKitStagingCleanup {
    fn drop(&mut self) {
        if !*self.armed.get_mut() {
            return;
        }
        if let Err(error) = self.ops.discard_atomic_create(&self.scope, &self.batch) {
            tracing::error!(
                batch = self.batch.as_provider(),
                %error,
                "CloudKit cancellation failed to discard atomic-create batch"
            );
            std::process::abort();
        }
    }
}

async fn begin_atomic_create(
    ops: Arc<dyn CloudKitOps>,
    scope: CloudKitScope,
) -> Result<Arc<CloudKitStagingCleanup>, CloudHomeError> {
    tokio::task::spawn_blocking(move || {
        let batch = ops.begin_atomic_create(&scope)?;
        Ok(Arc::new(CloudKitStagingCleanup {
            ops,
            scope,
            batch,
            armed: std::sync::atomic::AtomicBool::new(true),
        }))
    })
    .await
    .map_err(|error| {
        CloudHomeError::Transport(format!(
            "CloudKit atomic-create staging task failed: {error}"
        ))
    })?
}

async fn stage_atomic_create_record(
    staging: Arc<CloudKitStagingCleanup>,
    record: CloudKitRecordCreate,
) -> Result<(), CloudHomeError> {
    if record.data.len() > CHUNK_SIZE {
        return Err(CloudHomeError::Configuration(format!(
            "CloudKit staged record {:?} has {} bytes, above the {CHUNK_SIZE}-byte bound",
            record.key,
            record.data.len()
        )));
    }
    tokio::task::spawn_blocking(move || {
        staging
            .ops
            .stage_atomic_create_record(&staging.scope, &staging.batch, record)
    })
    .await
    .map_err(|error| {
        CloudHomeError::Transport(format!(
            "CloudKit atomic-create staging task failed: {error}"
        ))
    })?
}

async fn commit_atomic_create(
    staging: Arc<CloudKitStagingCleanup>,
) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
    tokio::task::spawn_blocking(move || {
        let created = staging
            .ops
            .commit_atomic_create(&staging.scope, &staging.batch)?;
        staging.disarm();
        Ok(created)
    })
    .await
    .map_err(|error| {
        CloudHomeError::Transport(format!(
            "CloudKit atomic-create commit task failed: {error}"
        ))
    })?
}

async fn authoritative_created_records(
    ops: Arc<dyn CloudKitOps>,
    scope: CloudKitScope,
    keys: Vec<String>,
) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
    blocking(move || {
        keys.into_iter()
            .map(|key| {
                let record = ops.read_versioned_record(&scope, &key).map_err(|error| {
                    CloudHomeError::Transport(format!(
                        "read committed CloudKit atomic-create record {key:?}: {error}"
                    ))
                })?;
                Ok(CloudKitRecordVersion {
                    key,
                    version: record.version,
                })
            })
            .collect()
    })
    .await
}

fn live_record_versions(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
) -> Result<BTreeMap<String, CloudKitRecordVersion>, CloudHomeError> {
    let mut live = BTreeMap::new();
    let mut after = None;
    let mut seen_tokens = BTreeSet::new();

    loop {
        let page = ops.record_changes(scope, after.as_ref())?;
        for change in page.changes {
            match change {
                CloudKitRecordChange::Present(record) => {
                    live.insert(record.key.clone(), record);
                }
                CloudKitRecordChange::Deleted { key } => {
                    live.remove(&key);
                }
            }
        }
        match page.continuation {
            CloudKitRecordChangesContinuation::More(token) => {
                if !seen_tokens.insert(token.as_provider().to_string()) {
                    return Err(CloudHomeError::Transport(format!(
                        "CloudKit repeated zone change token {:?} before completing the scan",
                        token.as_provider()
                    )));
                }
                after = Some(token);
            }
            CloudKitRecordChangesContinuation::Complete(_) => return Ok(live),
        }
    }
}

fn appended_listing_from_zone(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    prefix: &str,
) -> Result<AppendedListing, CloudHomeError> {
    let live = live_record_versions(ops, scope)?;
    let mut objects = Vec::new();

    for (key, record) in live.iter().filter(|(key, _)| key.starts_with(prefix)) {
        let Some(logical_key) = key.strip_suffix(CHUNK_MANIFEST_SUFFIX) else {
            if strip_part_suffix(key) == key {
                if live.contains_key(&chunk_manifest_key(key)) {
                    return Err(CloudHomeError::Transport(format!(
                        "CloudKit has both bounded and multipart records for {key:?}"
                    )));
                }
                let identity = CloudKitAppendedId::Single {
                    record: CloudKitStoredRecordId::from_record(record),
                };
                objects.push(AppendedObject::from_provider(
                    key.clone(),
                    identity.encode()?,
                )?);
            }
            continue;
        };

        if !logical_key.starts_with(prefix) {
            continue;
        }
        if live.contains_key(logical_key) {
            return Err(CloudHomeError::Transport(format!(
                "CloudKit has both bounded and multipart records for {logical_key:?}"
            )));
        }
        let manifest_data = read_immutable_record(ops, scope, record)?;
        let manifest = decode_chunk_manifest(&manifest_data)?;
        let mut parts = Vec::with_capacity(manifest.part_count);
        for index in 0..manifest.part_count {
            let part_key = chunk_part_key(logical_key, &manifest.upload_id, index);
            let part = live.get(&part_key).ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "CloudKit object {logical_key:?} is missing exact part {index}"
                ))
            })?;
            parts.push(CloudKitStoredRecordId::from_record(part));
        }
        let identity = CloudKitAppendedId::Chunked {
            manifest: CloudKitStoredRecordId::from_record(record),
            parts,
        };
        objects.push(AppendedObject::from_provider(
            logical_key.to_string(),
            identity.encode()?,
        )?);
    }

    objects.sort_by(|left, right| left.logical_key().cmp(right.logical_key()));
    Ok(AppendedListing {
        objects,
        coverage: ListingCoverage::CompleteAtScan,
    })
}

fn exact_object_layout(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    object: &AppendedObject,
) -> Result<ExactCloudKitObjectLayout, CloudHomeError> {
    match CloudKitAppendedId::decode(object)? {
        CloudKitAppendedId::Single { record } => {
            Ok(ExactCloudKitObjectLayout::Single(record.to_record()?))
        }
        CloudKitAppendedId::Chunked { manifest, parts } => {
            let manifest_record = manifest.to_record()?;
            let manifest_data = read_immutable_record(ops, scope, &manifest_record)?;
            let decoded = decode_chunk_manifest(&manifest_data)?;
            if decoded.part_count != parts.len() {
                return Err(CloudHomeError::Configuration(format!(
                    "CloudKit appended identity for {:?} has {} parts, manifest requires {}",
                    object.logical_key(),
                    parts.len(),
                    decoded.part_count
                )));
            }
            let mut exact_parts = Vec::with_capacity(parts.len());
            for (index, part) in parts.iter().enumerate() {
                let expected_key = chunk_part_key(object.logical_key(), &decoded.upload_id, index);
                if part.key != expected_key {
                    return Err(CloudHomeError::Configuration(format!(
                        "CloudKit appended identity for {:?} names part {index} as {:?}, expected {:?}",
                        object.logical_key(), part.key, expected_key
                    )));
                }
                exact_parts.push(part.to_record()?);
            }
            Ok(ExactCloudKitObjectLayout::Chunked {
                manifest: decoded,
                parts: exact_parts,
            })
        }
    }
}

fn read_exact_part(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
    manifest: &ChunkManifest,
    index: usize,
    part: &CloudKitRecordVersion,
) -> Result<Vec<u8>, CloudHomeError> {
    let bytes = read_immutable_record(ops, scope, part)?;
    let expected_len = if index + 1 == manifest.part_count {
        manifest.total_len - (CHUNK_SIZE * index)
    } else {
        CHUNK_SIZE
    };
    if bytes.len() != expected_len {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit object {logical_key:?} exact part {index} has {} bytes, expected {expected_len}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn parse_chunk_key(key: &str, upload_id: &str) -> Result<Option<usize>, CloudHomeError> {
    let Some((part_key, token)) = key.rsplit_once('.') else {
        return Ok(None);
    };
    if token != upload_id {
        return Ok(None);
    }
    let index = part_key
        .rsplit_once(".part")
        .and_then(|(_, suffix)| suffix.parse::<usize>().ok())
        .ok_or_else(|| {
            CloudHomeError::Transport(format!("chunk key {key:?} missing .part suffix"))
        })?;
    Ok(Some(index))
}

fn list_numbered_chunks(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
    manifest: &ChunkManifest,
) -> Result<Vec<(usize, String)>, CloudHomeError> {
    let chunk_prefix = format!("{key}.part");
    let mut numbered = Vec::new();
    for chunk_key in ops.list_records(scope, &chunk_prefix)? {
        let Some(index) = parse_chunk_key(&chunk_key, &manifest.upload_id)? else {
            continue;
        };
        numbered.push((index, chunk_key));
    }
    numbered.sort_by_key(|(index, _)| *index);
    Ok(numbered)
}

fn verify_chunk_manifest(
    key: &str,
    manifest: &ChunkManifest,
    chunks: &[(usize, String)],
) -> Result<(), CloudHomeError> {
    if chunks.len() != manifest.part_count {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit object {key} is incomplete: manifest expects {} parts, found {}",
            manifest.part_count,
            chunks.len()
        )));
    }
    for (expected, (actual, _)) in chunks.iter().enumerate() {
        if *actual != expected {
            return Err(CloudHomeError::Transport(format!(
                "CloudKit object {key} is incomplete: missing part {expected}"
            )));
        }
    }
    Ok(())
}

fn chunk_manifest_has_all_parts(manifest: &ChunkManifest, chunks: &[(usize, String)]) -> bool {
    chunks.len() == manifest.part_count
        && chunks
            .iter()
            .enumerate()
            .all(|(expected, (actual, _))| *actual == expected)
}

fn read_chunk(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
    manifest: &ChunkManifest,
    index: usize,
    chunk_key: &str,
) -> Result<Vec<u8>, CloudHomeError> {
    let chunk = ops.read_record(scope, chunk_key)?;
    let expected_len = if index + 1 == manifest.part_count {
        manifest.total_len - (CHUNK_SIZE * index)
    } else {
        CHUNK_SIZE
    };
    if chunk.len() != expected_len {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit object {key} part {index} has {} bytes, expected {expected_len}",
            chunk.len()
        )));
    }
    Ok(chunk)
}

fn read_chunked_object(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
    manifest: ChunkManifest,
) -> Result<Vec<u8>, CloudHomeError> {
    let chunks = list_numbered_chunks(ops, scope, key, &manifest)?;
    verify_chunk_manifest(key, &manifest, &chunks)?;

    let mut result = Vec::with_capacity(manifest.total_len);
    for (index, chunk_key) in &chunks {
        let chunk = read_chunk(ops, scope, key, &manifest, *index, chunk_key)?;
        result.extend_from_slice(&chunk);
    }
    if result.len() != manifest.total_len {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit object {key} assembled to {} bytes, expected {}",
            result.len(),
            manifest.total_len
        )));
    }
    Ok(result)
}

fn delete_chunk_layout(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
) -> Result<(), CloudHomeError> {
    match ops.delete_record(scope, &chunk_manifest_key(key)) {
        Ok(()) | Err(CloudHomeError::NotFound(_)) => {}
        Err(e) => return Err(e),
    }

    let chunk_prefix = format!("{key}.part");
    let chunks = ops.list_records(scope, &chunk_prefix)?;
    for chunk_key in chunks {
        match ops.delete_record(scope, &chunk_key) {
            Ok(()) | Err(CloudHomeError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

fn delete_stale_chunk_records(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
    upload_id: &str,
) -> Result<(), CloudHomeError> {
    let chunk_prefix = format!("{key}.part");
    let chunks = ops.list_records(scope, &chunk_prefix)?;
    for chunk_key in chunks {
        if parse_chunk_key(&chunk_key, upload_id)?.is_some() {
            continue;
        }
        match ops.delete_record(scope, &chunk_key) {
            Ok(()) | Err(CloudHomeError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn delete_single_record(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
) -> Result<(), CloudHomeError> {
    match ops.delete_record(scope, key) {
        Ok(()) | Err(CloudHomeError::NotFound(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Delete old single record and chunk records for a key.
fn delete_all_variants(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
) -> Result<(), CloudHomeError> {
    delete_single_record(ops, scope, key)?;
    delete_chunk_layout(ops, scope, key)
}

/// A [`PartSink`] over CloudKit's chunked record layout: each `send_part` writes
/// one tokened part record (CKAsset caps at 50 MB, so a large blob is split),
/// `finish` writes the `{key}.manifest` record that makes the object readable.
/// Existing records stay readable until the manifest points at the new token.
struct CloudKitPartSink {
    ops: Arc<dyn CloudKitOps>,
    scope: CloudKitScope,
    key: String,
    upload_id: String,
    index: usize,
    total_len: usize,
    written_len: usize,
}

impl CloudKitPartSink {
    /// Best-effort deletion of the part records this upload wrote, called when a
    /// part write or the manifest write fails — before any manifest is published.
    /// Without it a failed upload leaves tokened part records with no manifest: an
    /// orphan `list` would report (as a base key) but `read` cannot assemble. Only
    /// this upload's own token is deleted, so a prior object at the same key (which
    /// stays readable until its manifest is replaced) is untouched.
    async fn abort(&self) {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let written_parts = self.index;
        let result = blocking(move || {
            for i in 0..written_parts {
                delete_single_record(&*ops, &scope, &chunk_part_key(&key, &upload_id, i))?;
            }
            Ok(())
        })
        .await;
        if let Err(e) = result {
            warn!(
                "Failed to abort CloudKit multipart upload for {}: {e}",
                self.key
            );
        }
    }

    /// Write one record of this upload, aborting the whole upload on failure —
    /// the shape both the part writes and the manifest write share.
    async fn write_record_or_abort(
        &self,
        record_key: String,
        data: Vec<u8>,
    ) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        if let Err(e) = blocking(move || ops.write_record(&scope, &record_key, data)).await {
            self.abort().await;
            return Err(e);
        }
        Ok(())
    }
}

#[async_trait]
impl super::PartSink for CloudKitPartSink {
    fn part_size(&self) -> usize {
        CHUNK_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let i = self.index;
        self.index += 1;
        self.written_len += part.len();
        let chunk_key = chunk_part_key(&self.key, &self.upload_id, i);
        self.write_record_or_abort(chunk_key, part.to_vec()).await
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        let manifest = ChunkManifest::new(self.total_len, self.upload_id.clone());
        if self.index != manifest.part_count || self.written_len != manifest.total_len {
            self.abort().await;
            return Err(CloudHomeError::Transport(format!(
                "CloudKit multipart {} wrote {} parts/{} bytes, expected {} parts/{} bytes",
                self.key, self.index, self.written_len, manifest.part_count, manifest.total_len
            )));
        }
        let manifest_key = chunk_manifest_key(&self.key);
        let manifest_data = encode_chunk_manifest(manifest);
        self.write_record_or_abort(manifest_key, manifest_data)
            .await?;

        // The manifest is published, so the object is now readable. The remaining
        // steps only clean up records the previous object left; their failure fails
        // loud but must not touch the parts just published.
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = self.key.clone();
        blocking(move || delete_single_record(&*ops, &scope, &key)).await?;

        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        blocking(move || delete_stale_chunk_records(&*ops, &scope, &key, &upload_id)).await
    }
}

#[async_trait]
impl CloudHome for CloudKitCloudHome {
    fn immutable_copy_storage(self: Arc<Self>) -> Option<Arc<dyn ImmutableCopyStorage>> {
        Some(self)
    }

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
        Ok(Box::new(CloudKitPartSink {
            ops: self.ops.clone(),
            scope: self.scope.clone(),
            key: key.to_string(),
            upload_id: self.ids.new_id(),
            index: 0,
            total_len,
            written_len: 0,
        }))
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

            let chunks = ops.list_records(&scope, &format!("{key}.part"))?;
            if chunks.is_empty() {
                return Err(CloudHomeError::NotFound(key));
            }
            Err(CloudHomeError::Transport(format!(
                "CloudKit object {key} has chunk records but no manifest"
            )))
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

            let chunks = ops.list_records(&scope, &format!("{key}.part"))?;
            if chunks.is_empty() {
                return Err(CloudHomeError::NotFound(key));
            }
            Err(CloudHomeError::Transport(format!(
                "CloudKit object {key} has chunk records but no manifest"
            )))
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
            Ok(chunk_manifest_has_all_parts(&manifest, &chunks))
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

#[async_trait]
impl ImmutableCopyStorage for CloudKitCloudHome {
    async fn append_object(
        &self,
        full_logical_key: &str,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        let total_len = usize::try_from(body.len()).map_err(|_| {
            CloudHomeError::Transport(format!(
                "CloudKit object {full_logical_key:?} is too large for this platform"
            ))
        })?;

        if total_len <= CHUNK_SIZE {
            let data = body.collect().await?;
            let ops = self.ops.clone();
            let scope = self.scope.clone();
            let key = full_logical_key.to_string();
            let record =
                blocking(move || create_immutable_record(&*ops, &scope, &key, data)).await?;
            let identity = CloudKitAppendedId::Single {
                record: CloudKitStoredRecordId::from_record(&record),
            }
            .encode()?;
            progress(total_len as u64);
            return AppendedObject::from_provider(full_logical_key.to_string(), identity);
        }

        let upload_id = self.ids.new_id();
        let manifest = ChunkManifest::new(total_len, upload_id.clone());
        let staging = begin_atomic_create(self.ops.clone(), self.scope.clone()).await?;
        let mut requested_keys = Vec::with_capacity(manifest.part_count + 1);
        let mut written_len = 0usize;
        for index in 0..manifest.part_count {
            let part = match body.next_part(CHUNK_SIZE).await {
                Ok(Some(part)) => part,
                Ok(None) => {
                    let error = CloudHomeError::Transport(format!(
                        "CloudKit object {full_logical_key:?} ended after {written_len} of {total_len} bytes"
                    ));
                    return Err(staging.cleanup_failure(error));
                }
                Err(error) => return Err(staging.cleanup_failure(error)),
            };
            written_len += part.len();
            let key = chunk_part_key(full_logical_key, &upload_id, index);
            if let Err(error) = stage_atomic_create_record(
                staging.clone(),
                CloudKitRecordCreate {
                    key: key.clone(),
                    data: part.to_vec(),
                },
            )
            .await
            {
                return Err(staging.cleanup_failure(error));
            }
            requested_keys.push(key);
        }
        match body.next_part(CHUNK_SIZE).await {
            Ok(None) if written_len == total_len => {}
            Ok(None) => return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {full_logical_key:?} yielded {written_len} bytes, expected {total_len}"
                )))),
            Ok(Some(extra)) => return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {full_logical_key:?} yielded at least {} bytes, expected {total_len}",
                    written_len + extra.len()
                )))),
            Err(error) => return Err(staging.cleanup_failure(error)),
        }

        let manifest_key = chunk_manifest_key(full_logical_key);
        if let Err(error) = stage_atomic_create_record(
            staging.clone(),
            CloudKitRecordCreate {
                key: manifest_key.clone(),
                data: encode_chunk_manifest(manifest),
            },
        )
        .await
        {
            return Err(staging.cleanup_failure(error));
        }
        requested_keys.push(manifest_key);
        let mut created = match commit_atomic_create(staging.clone()).await {
            Ok(created) => created,
            Err(error) => return Err(staging.cleanup_failure(error)),
        };
        if created.len() != requested_keys.len()
            || created
                .iter()
                .zip(&requested_keys)
                .any(|(record, requested)| &record.key != requested)
        {
            created = authoritative_created_records(
                self.ops.clone(),
                self.scope.clone(),
                requested_keys.clone(),
            )
            .await?;
        }
        let (manifest_record, parts) = created.split_last().ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "CloudKit atomic create returned no records for {full_logical_key:?}"
            ))
        })?;
        let identity = CloudKitAppendedId::Chunked {
            manifest: CloudKitStoredRecordId::from_record(manifest_record),
            parts: parts
                .iter()
                .map(CloudKitStoredRecordId::from_record)
                .collect(),
        }
        .encode()?;
        progress(total_len as u64);
        AppendedObject::from_provider(full_logical_key.to_string(), identity)
    }

    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let prefix = prefix.to_string();
        blocking(move || appended_listing_from_zone(&*ops, &scope, &prefix)).await
    }

    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let object = object.clone();
        blocking(move || match exact_object_layout(&*ops, &scope, &object)? {
            ExactCloudKitObjectLayout::Single(record) => {
                read_immutable_record(&*ops, &scope, &record)
            }
            ExactCloudKitObjectLayout::Chunked { manifest, parts } => {
                let mut bytes = Vec::with_capacity(manifest.total_len);
                for (index, part) in parts.iter().enumerate() {
                    bytes.extend_from_slice(&read_exact_part(
                        &*ops,
                        &scope,
                        object.logical_key(),
                        &manifest,
                        index,
                        part,
                    )?);
                }
                Ok(bytes)
            }
        })
        .await
    }

    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let object_for_layout = object.clone();
        let layout =
            blocking(move || exact_object_layout(&*ops, &scope, &object_for_layout)).await?;
        let logical_key = object.logical_key().to_string();
        let stream: super::CloudObjectStream = match layout {
            ExactCloudKitObjectLayout::Single(record) => {
                let ops = self.ops.clone();
                let scope = self.scope.clone();
                Box::pin(futures_util::stream::once(async move {
                    blocking(move || read_immutable_record(&*ops, &scope, &record))
                        .await
                        .map(Bytes::from)
                }))
            }
            ExactCloudKitObjectLayout::Chunked { manifest, parts } => {
                let ops = self.ops.clone();
                let scope = self.scope.clone();
                Box::pin(
                    futures_util::stream::iter(parts.into_iter().enumerate()).then(
                        move |(index, part)| {
                            let ops = ops.clone();
                            let scope = scope.clone();
                            let logical_key = logical_key.clone();
                            let manifest = manifest.clone();
                            async move {
                                blocking(move || {
                                    read_exact_part(
                                        &*ops,
                                        &scope,
                                        &logical_key,
                                        &manifest,
                                        index,
                                        &part,
                                    )
                                    .map(Bytes::from)
                                })
                                .await
                            }
                        },
                    ),
                )
            }
        };
        super::write_cloud_object_stream(destination, stream).await?;
        Ok(())
    }

    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        let identity = CloudKitAppendedId::decode(object)?;
        let records = identity.records()?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        blocking(move || ops.delete_record_versions(&scope, &records)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_provider::SequentialIdProvider;
    use crate::storage::cloud::{no_progress, BlobBody};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum MockCall {
        Write(String),
        Read(String),
        List(String),
        Delete(String),
        Exists(String),
        Create(String),
        BeginBatch(String),
        Stage(String),
        CommitBatch(String),
        DiscardBatch(String),
        Changes(Option<String>),
        DeleteVersions(Vec<String>),
    }

    struct MockCloudKitOps {
        store: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
        versions: Mutex<HashMap<(CloudKitScope, String), u64>>,
        calls: Mutex<Vec<MockCall>>,
        fail_deletes: Mutex<HashSet<String>>,
        fail_writes: Mutex<HashSet<String>>,
        staged_batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
        next_batch: AtomicUsize,
        max_stage_payload: AtomicUsize,
        fail_discards: AtomicBool,
        lose_commit_response: AtomicBool,
        return_wrong_commit_keys: AtomicBool,
        scripted_change_pages: Mutex<Option<VecDeque<CloudKitRecordChangesPage>>>,
        record_exists_calls: AtomicUsize,
        grant_share_calls: AtomicUsize,
        revoke_share_calls: AtomicUsize,
        shares: Mutex<HashMap<String, CloudKitShare>>,
    }

    impl MockCloudKitOps {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
                versions: Mutex::new(HashMap::new()),
                calls: Mutex::new(Vec::new()),
                fail_deletes: Mutex::new(HashSet::new()),
                fail_writes: Mutex::new(HashSet::new()),
                staged_batches: Mutex::new(HashMap::new()),
                next_batch: AtomicUsize::new(0),
                max_stage_payload: AtomicUsize::new(0),
                fail_discards: AtomicBool::new(false),
                lose_commit_response: AtomicBool::new(false),
                return_wrong_commit_keys: AtomicBool::new(false),
                scripted_change_pages: Mutex::new(None),
                record_exists_calls: AtomicUsize::new(0),
                grant_share_calls: AtomicUsize::new(0),
                revoke_share_calls: AtomicUsize::new(0),
                shares: Mutex::new(HashMap::new()),
            }
        }

        fn calls(&self) -> Vec<MockCall> {
            self.calls.lock().unwrap().clone()
        }

        fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
        }

        fn fail_delete(&self, key: &str) {
            self.fail_deletes.lock().unwrap().insert(key.to_string());
        }

        fn fail_write(&self, key: &str) {
            self.fail_writes.lock().unwrap().insert(key.to_string());
        }

        fn fail_discard(&self) {
            self.fail_discards.store(true, Ordering::SeqCst);
        }

        fn lose_commit_response(&self) {
            self.lose_commit_response.store(true, Ordering::SeqCst);
        }

        fn return_wrong_commit_keys(&self) {
            self.return_wrong_commit_keys.store(true, Ordering::SeqCst);
        }

        fn script_change_pages(&self, pages: Vec<CloudKitRecordChangesPage>) {
            *self.scripted_change_pages.lock().unwrap() = Some(pages.into());
        }
    }

    impl CloudKitOps for MockCloudKitOps {
        fn write_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
            data: Vec<u8>,
        ) -> Result<(), CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Write(key.to_string()));
            if self.fail_writes.lock().unwrap().contains(key) {
                return Err(CloudHomeError::Transport(format!("write {key} failed")));
            }
            let record = (scope.clone(), key.to_string());
            self.store.lock().unwrap().insert(record.clone(), data);
            let mut versions = self.versions.lock().unwrap();
            let next = versions.get(&record).copied().unwrap_or(0) + 1;
            versions.insert(record, next);
            Ok(())
        }

        fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Read(key.to_string()));
            self.store
                .lock()
                .unwrap()
                .get(&(scope.clone(), key.to_string()))
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
        }

        fn list_records(
            &self,
            scope: &CloudKitScope,
            prefix: &str,
        ) -> Result<Vec<String>, CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::List(prefix.to_string()));
            let store = self.store.lock().unwrap();
            let mut keys: Vec<String> = store
                .keys()
                .filter(|(record_scope, key)| record_scope == scope && key.starts_with(prefix))
                .map(|(_, key)| key.clone())
                .collect();
            keys.sort();
            Ok(keys)
        }

        fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Delete(key.to_string()));
            if self.fail_deletes.lock().unwrap().contains(key) {
                return Err(CloudHomeError::Transport(format!("delete {key} failed")));
            }
            self.store
                .lock()
                .unwrap()
                .remove(&(scope.clone(), key.to_string()));
            self.versions
                .lock()
                .unwrap()
                .remove(&(scope.clone(), key.to_string()));
            Ok(())
        }

        fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError> {
            self.record_exists_calls.fetch_add(1, Ordering::Relaxed);
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Exists(key.to_string()));
            Ok(self
                .store
                .lock()
                .unwrap()
                .contains_key(&(scope.clone(), key.to_string())))
        }

        fn read_versioned_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
        ) -> Result<CloudVersionedHead, CloudHomeError> {
            let record = (scope.clone(), key.to_string());
            let store = self.store.lock().unwrap();
            let versions = self.versions.lock().unwrap();
            Ok(CloudVersionedHead {
                bytes: store
                    .get(&record)
                    .cloned()
                    .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?,
                version: CloudHeadVersion::from_provider(
                    versions
                        .get(&record)
                        .copied()
                        .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?
                        .to_string(),
                )?,
            })
        }

        fn create_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
            data: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Create(key.to_string()));
            if self.fail_writes.lock().unwrap().contains(key) {
                return Err(CloudHeadCreateError::Storage(CloudHomeError::Transport(
                    format!("create {key} failed"),
                )));
            }
            let record = (scope.clone(), key.to_string());
            let mut store = self.store.lock().unwrap();
            let mut versions = self.versions.lock().unwrap();
            if store.contains_key(&record) {
                return Err(CloudHeadCreateError::AlreadyExists);
            }
            store.insert(record.clone(), data.clone());
            versions.insert(record, 1);
            Ok(CloudVersionedHead {
                bytes: data,
                version: CloudHeadVersion::from_provider("1".to_string())?,
            })
        }

        fn begin_atomic_create(
            &self,
            _scope: &CloudKitScope,
        ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
            let batch = CloudKitAtomicCreateBatch::from_provider(format!(
                "batch-{}",
                self.next_batch.fetch_add(1, Ordering::SeqCst)
            ))?;
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::BeginBatch(batch.as_provider().to_string()));
            self.staged_batches
                .lock()
                .unwrap()
                .insert(batch.as_provider().to_string(), Vec::new());
            Ok(batch)
        }

        fn stage_atomic_create_record(
            &self,
            _scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
            record: CloudKitRecordCreate,
        ) -> Result<(), CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Stage(record.key.clone()));
            self.max_stage_payload
                .fetch_max(record.data.len(), Ordering::SeqCst);
            self.staged_batches
                .lock()
                .unwrap()
                .get_mut(batch.as_provider())
                .ok_or_else(|| {
                    CloudHomeError::NotFound(format!(
                        "CloudKit staging batch {:?}",
                        batch.as_provider()
                    ))
                })?
                .push(record);
            Ok(())
        }

        fn commit_atomic_create(
            &self,
            scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
        ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::CommitBatch(batch.as_provider().to_string()));
            let mut batches = self.staged_batches.lock().unwrap();
            let records = batches.get(batch.as_provider()).ok_or_else(|| {
                CloudHomeError::NotFound(format!(
                    "CloudKit staging batch {:?}",
                    batch.as_provider()
                ))
            })?;
            let fail_writes = self.fail_writes.lock().unwrap();
            let mut store = self.store.lock().unwrap();
            let mut versions = self.versions.lock().unwrap();
            for record in records {
                if fail_writes.contains(&record.key) {
                    return Err(CloudHomeError::Transport(format!(
                        "atomic create {:?} failed",
                        record.key
                    )));
                }
                if store.contains_key(&(scope.clone(), record.key.clone())) {
                    return Err(CloudHomeError::AlreadyExists(record.key.clone()));
                }
            }
            let records = batches
                .remove(batch.as_provider())
                .expect("validated CloudKit staging batch disappeared");
            let mut created = Vec::with_capacity(records.len());
            for record in records {
                let coordinate = (scope.clone(), record.key.clone());
                store.insert(coordinate.clone(), record.data);
                versions.insert(coordinate, 1);
                created.push(CloudKitRecordVersion {
                    key: record.key,
                    version: CloudHeadVersion::from_provider("1".to_string())?,
                });
            }
            if self.lose_commit_response.load(Ordering::SeqCst) {
                return Err(CloudHomeError::Transport(
                    "CloudKit commit response was lost".to_string(),
                ));
            }
            if self.return_wrong_commit_keys.load(Ordering::SeqCst) {
                for (index, record) in created.iter_mut().enumerate() {
                    record.key = format!("unexpected-returned-record-{index}");
                }
            }
            Ok(created)
        }

        fn discard_atomic_create(
            &self,
            _scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
        ) -> Result<(), CloudHomeError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::DiscardBatch(batch.as_provider().to_string()));
            if self.fail_discards.load(Ordering::SeqCst) {
                return Err(CloudHomeError::Transport(format!(
                    "discard staging batch {:?} failed",
                    batch.as_provider()
                )));
            }
            self.staged_batches
                .lock()
                .unwrap()
                .remove(batch.as_provider());
            Ok(())
        }

        fn record_changes(
            &self,
            scope: &CloudKitScope,
            after: Option<&CloudKitChangeToken>,
        ) -> Result<CloudKitRecordChangesPage, CloudHomeError> {
            self.calls.lock().unwrap().push(MockCall::Changes(
                after.map(|token| token.as_provider().to_string()),
            ));
            let mut scripted = self.scripted_change_pages.lock().unwrap();
            if let Some(pages) = scripted.as_mut() {
                return pages.pop_front().ok_or_else(|| {
                    CloudHomeError::Transport(
                        "CloudKit mock change-page script is exhausted".to_string(),
                    )
                });
            }
            if after.is_some() {
                return Err(CloudHomeError::Transport(
                    "CloudKit mock live scan received an unexpected continuation token".to_string(),
                ));
            }
            let store = self.store.lock().unwrap();
            let versions = self.versions.lock().unwrap();
            let mut changes = Vec::new();
            for ((record_scope, key), _) in store.iter() {
                if record_scope != scope {
                    continue;
                }
                let version = versions
                    .get(&(record_scope.clone(), key.clone()))
                    .ok_or_else(|| {
                        CloudHomeError::Transport(format!(
                            "CloudKit mock record {key:?} has no version"
                        ))
                    })?;
                changes.push(CloudKitRecordChange::Present(CloudKitRecordVersion {
                    key: key.clone(),
                    version: CloudHeadVersion::from_provider(version.to_string())?,
                }));
            }
            changes.sort_by(|left, right| match (left, right) {
                (CloudKitRecordChange::Present(left), CloudKitRecordChange::Present(right)) => {
                    left.key.cmp(&right.key)
                }
                _ => std::cmp::Ordering::Equal,
            });
            Ok(CloudKitRecordChangesPage {
                changes,
                continuation: CloudKitRecordChangesContinuation::Complete(
                    CloudKitChangeToken::from_provider("live".to_string())?,
                ),
            })
        }

        fn delete_record_versions(
            &self,
            scope: &CloudKitScope,
            records: &[CloudKitRecordVersion],
        ) -> Result<(), CloudHomeError> {
            self.calls.lock().unwrap().push(MockCall::DeleteVersions(
                records.iter().map(|record| record.key.clone()).collect(),
            ));
            let fail_deletes = self.fail_deletes.lock().unwrap();
            let mut store = self.store.lock().unwrap();
            let mut versions = self.versions.lock().unwrap();
            for record in records {
                if fail_deletes.contains(&record.key) {
                    return Err(CloudHomeError::Transport(format!(
                        "delete {:?} failed",
                        record.key
                    )));
                }
                let storage_key = (scope.clone(), record.key.clone());
                let current = versions
                    .get(&storage_key)
                    .ok_or_else(|| CloudHomeError::NotFound(record.key.clone()))?;
                if current.to_string() != record.version.as_provider() {
                    return Err(CloudHomeError::Transport(format!(
                        "CloudKit record {:?} changed before exact deletion",
                        record.key
                    )));
                }
                if !store.contains_key(&storage_key) {
                    return Err(CloudHomeError::NotFound(record.key.clone()));
                }
            }
            for record in records {
                let storage_key = (scope.clone(), record.key.clone());
                store.remove(&storage_key);
                versions.remove(&storage_key);
            }
            Ok(())
        }

        fn replace_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
            expected: &CloudHeadVersion,
            data: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
            let record = (scope.clone(), key.to_string());
            let mut store = self.store.lock().unwrap();
            let mut versions = self.versions.lock().unwrap();
            let current = versions
                .get(&record)
                .copied()
                .ok_or(CloudHeadReplaceError::VersionMismatch)?;
            if expected.as_provider() != current.to_string() {
                return Err(CloudHeadReplaceError::VersionMismatch);
            }
            let next = current + 1;
            store.insert(record.clone(), data.clone());
            versions.insert(record, next);
            Ok(CloudVersionedHead {
                bytes: data,
                version: CloudHeadVersion::from_provider(next.to_string())?,
            })
        }

        fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
            self.grant_share_calls.fetch_add(1, Ordering::Relaxed);
            let share = CloudKitShare {
                share_url: format!("https://share.example/{member_pubkey}"),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-store".to_string(),
            };
            self.shares
                .lock()
                .unwrap()
                .insert(member_pubkey.to_string(), share.clone());
            Ok(share)
        }

        fn share_for_member(
            &self,
            member_pubkey: &str,
        ) -> Result<Option<CloudKitShare>, CloudHomeError> {
            Ok(self.shares.lock().unwrap().get(member_pubkey).cloned())
        }

        fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError> {
            self.revoke_share_calls.fetch_add(1, Ordering::Relaxed);
            self.shares.lock().unwrap().remove(member_pubkey);
            Ok(())
        }

        fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
            Ok(CloudKitShare {
                share_url: share_url.to_string(),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-store".to_string(),
            })
        }
    }

    fn make_cloud_home() -> CloudKitCloudHome {
        CloudKitCloudHome::new_private_with_ids(
            Arc::new(MockCloudKitOps::new()),
            Arc::new(SequentialIdProvider::new("cloudkit-upload")),
        )
    }

    fn make_cloud_home_with_ops() -> (CloudKitCloudHome, Arc<MockCloudKitOps>) {
        let ops = Arc::new(MockCloudKitOps::new());
        (
            CloudKitCloudHome::new_private_with_ids(
                ops.clone(),
                Arc::new(SequentialIdProvider::new("cloudkit-upload")),
            ),
            ops,
        )
    }

    fn write_chunk_manifest(ops: &MockCloudKitOps, key: &str, total_len: usize) {
        write_chunk_manifest_with_upload_id(
            ops,
            key,
            total_len,
            "0123456789abcdef0123456789abcdef",
        );
    }

    fn write_chunk_manifest_with_upload_id(
        ops: &MockCloudKitOps,
        key: &str,
        total_len: usize,
        upload_id: &str,
    ) {
        ops.write_record(
            &CloudKitScope::Private,
            &chunk_manifest_key(key),
            encode_chunk_manifest(ChunkManifest::new(total_len, upload_id.to_string())),
        )
        .unwrap();
    }

    fn write_chunk_part(ops: &MockCloudKitOps, key: &str, index: usize, data: Vec<u8>) {
        ops.write_record(
            &CloudKitScope::Private,
            &chunk_part_key(key, "0123456789abcdef0123456789abcdef", index),
            data,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn write_reports_progress_per_chunk_record() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let ch = make_cloud_home();
        // 25 MB spans three records (10 + 10 + 5) so progress fires three
        // times, the last equalling the total.
        let total = 25 * 1024 * 1024u64;
        let data: Vec<u8> = vec![0u8; total as usize];
        let last = Arc::new(AtomicU64::new(0));
        let ticks = Arc::new(AtomicU64::new(0));
        let last2 = last.clone();
        let ticks2 = ticks.clone();
        let sink = move |n: u64| {
            last2.store(n, Ordering::Relaxed);
            ticks2.fetch_add(1, Ordering::Relaxed);
        };
        ch.write("chunked.bin", BlobBody::from_bytes(data), &sink)
            .await
            .unwrap();
        assert_eq!(last.load(Ordering::Relaxed), total);
        assert_eq!(ticks.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_small_file_roundtrip() {
        let ch = make_cloud_home();
        let data = b"hello world".to_vec();
        ch.write(
            "small.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();
        let read = ch.read("small.bin").await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn test_large_file_roundtrip() {
        let ch = make_cloud_home();
        // 25MB of data -- spans 3 chunks (10 + 10 + 5)
        let data: Vec<u8> = (0..25 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        ch.write(
            "large.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();
        let read = ch.read("large.bin").await.unwrap();
        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn test_read_range_single() {
        let ch = make_cloud_home();
        ch.write(
            "range.bin",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        let slice = ch.read_range("range.bin", 3, 7).await.unwrap();
        assert_eq!(slice, b"3456");
    }

    #[tokio::test]
    async fn read_single_record_does_not_probe_existence() {
        let (ch, ops) = make_cloud_home_with_ops();
        let data = b"hello world".to_vec();
        ch.write(
            "single.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read("single.bin").await.unwrap();

        assert_eq!(read, data);
        assert_eq!(ops.record_exists_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn read_range_single_record_does_not_probe_existence() {
        let (ch, ops) = make_cloud_home_with_ops();
        ch.write(
            "single-range.bin",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read_range("single-range.bin", 2, 6).await.unwrap();

        assert_eq!(read, b"2345");
        assert_eq!(ops.record_exists_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn read_chunked_record_without_manifest_errors() {
        let (ch, ops) = make_cloud_home_with_ops();
        let first = vec![1u8; CHUNK_SIZE];
        let second = b"tail".to_vec();
        write_chunk_part(&ops, "chunked.bin", 0, first.clone());
        write_chunk_part(&ops, "chunked.bin", 1, second.clone());

        let err = ch
            .read("chunked.bin")
            .await
            .expect_err("chunks without manifest must fail");
        let msg = err.to_string();

        assert!(
            msg.contains("chunked.bin") && msg.contains("no manifest"),
            "unexpected error: {msg}"
        );
        assert!(!ch.exists("chunked.bin").await.unwrap());
    }

    #[tokio::test]
    async fn list_omits_base_key_whose_manifest_is_absent() {
        let (ch, ops) = make_cloud_home_with_ops();
        // Part records with no manifest — an interrupted upload that never
        // published. `read` cannot assemble them, so `list` must not report them.
        write_chunk_part(&ops, "files/orphan.bin", 0, vec![1u8; CHUNK_SIZE]);
        write_chunk_part(&ops, "files/orphan.bin", 1, b"tail".to_vec());
        ch.write(
            "files/ok.bin",
            BlobBody::from_bytes(b"hi".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let keys = ch.list("files/").await.unwrap();

        assert_eq!(keys, vec!["files/ok.bin".to_string()]);
    }

    #[tokio::test]
    async fn multipart_part_failure_leaves_no_orphan_records_or_visibility() {
        let (ch, ops) = make_cloud_home_with_ops();
        // 25 MB spans three parts; fail the second part write mid-upload. The
        // upload id is the first id the sequential provider hands out.
        ops.fail_write(&chunk_part_key("orphan.bin", "cloudkit-upload-0", 1));
        let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];

        let err = ch
            .write("orphan.bin", BlobBody::from_bytes(data), &no_progress())
            .await
            .expect_err("injected part write failure must fail the upload");
        assert!(err.to_string().contains("write"), "unexpected error: {err}");

        assert!(!ch.exists("orphan.bin").await.unwrap());
        assert!(!ch
            .list("")
            .await
            .unwrap()
            .contains(&"orphan.bin".to_string()));
        assert!(
            ops.list_records(&CloudKitScope::Private, "orphan.bin.part")
                .unwrap()
                .is_empty(),
            "aborted upload must leave no part records"
        );
        assert!(!ops
            .record_exists(&CloudKitScope::Private, &chunk_manifest_key("orphan.bin"))
            .unwrap());
    }

    #[tokio::test]
    async fn read_chunked_record_with_missing_manifest_part_errors() {
        let (ch, ops) = make_cloud_home_with_ops();
        let first = vec![1u8; CHUNK_SIZE];
        let second = vec![2u8; CHUNK_SIZE];
        let total_len = (CHUNK_SIZE * 2) + 4;
        write_chunk_manifest(&ops, "chunked.bin", total_len);
        write_chunk_part(&ops, "chunked.bin", 0, first);
        write_chunk_part(&ops, "chunked.bin", 1, second);

        let err = ch
            .read("chunked.bin")
            .await
            .expect_err("missing manifest part must fail");
        let msg = err.to_string();

        assert!(
            msg.contains("expects 3 parts") && msg.contains("found 2"),
            "unexpected error: {msg}"
        );
        assert!(!ch.exists("chunked.bin").await.unwrap());
    }

    #[tokio::test]
    async fn read_range_chunked_rejects_range_past_manifest_length() {
        let ch = make_cloud_home();
        let data: Vec<u8> = vec![7u8; 15 * 1024 * 1024];
        ch.write(
            "range-limit.bin",
            BlobBody::from_bytes(data),
            &no_progress(),
        )
        .await
        .unwrap();

        let err = ch
            .read_range("range-limit.bin", 0, (16 * 1024 * 1024) as u64)
            .await
            .expect_err("range past manifest length must fail");
        let msg = err.to_string();

        assert!(msg.contains("exceeds file size"), "unexpected error: {msg}");
    }

    #[tokio::test]
    async fn read_range_chunked_short_chunk_errors_instead_of_panicking() {
        let (ch, ops) = make_cloud_home_with_ops();
        let total_len = CHUNK_SIZE + 8;
        write_chunk_manifest(&ops, "short-tail.bin", total_len);
        write_chunk_part(&ops, "short-tail.bin", 0, vec![1u8; CHUNK_SIZE]);
        write_chunk_part(&ops, "short-tail.bin", 1, vec![2u8; 4]);

        let err = ch
            .read_range("short-tail.bin", CHUNK_SIZE as u64, (CHUNK_SIZE + 8) as u64)
            .await
            .expect_err("short tail chunk must fail");
        let msg = err.to_string();

        assert!(
            msg.contains("part 1") && msg.contains("expected 8"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_read_range_chunked() {
        let ch = make_cloud_home();
        // Create data that spans 2 chunks: 15MB
        let data: Vec<u8> = (0..15 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        ch.write(
            "big.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        // Read a range that crosses the chunk boundary (last byte of chunk 0, first byte of chunk 1)
        let boundary = CHUNK_SIZE;
        let start = (boundary - 2) as u64;
        let end = (boundary + 3) as u64;
        let slice = ch.read_range("big.bin", start, end).await.unwrap();
        assert_eq!(slice.len(), 5);
        assert_eq!(slice, &data[start as usize..end as usize]);
    }

    #[tokio::test]
    async fn test_list_deduplicates_chunks() {
        let ch = make_cloud_home();
        // Write a chunked file
        let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write(
            "files/album.flac",
            BlobBody::from_bytes(data),
            &no_progress(),
        )
        .await
        .unwrap();

        // Also write a small file
        ch.write(
            "files/cover.jpg",
            BlobBody::from_bytes(b"img".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let keys = ch.list("files/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"files/album.flac".to_string()));
        assert!(keys.contains(&"files/cover.jpg".to_string()));
    }

    #[tokio::test]
    async fn test_delete_removes_all_chunks() {
        let ch = make_cloud_home();
        let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("to-delete.bin", BlobBody::from_bytes(data), &no_progress())
            .await
            .unwrap();

        assert!(ch.exists("to-delete.bin").await.unwrap());

        ch.delete("to-delete.bin").await.unwrap();

        assert!(!ch.exists("to-delete.bin").await.unwrap());

        // Verify the underlying ops store is empty of related keys
        let ops = &ch.ops;
        let keys = ops
            .list_records(&CloudKitScope::Private, "to-delete.bin")
            .unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_overwrite_chunked_with_single() {
        let ch = make_cloud_home();
        // Write large file (chunked)
        let large_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
            .await
            .unwrap();

        // Overwrite with small file (single record)
        let small_data = b"small".to_vec();
        ch.write(
            "file.bin",
            BlobBody::from_bytes(small_data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read("file.bin").await.unwrap();
        assert_eq!(read, small_data);

        // Verify no chunk records remain
        let chunks = ch
            .ops
            .list_records(&CloudKitScope::Private, "file.bin.part")
            .unwrap();
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn put_object_over_single_writes_without_deleting_base_first() {
        let (ch, ops) = make_cloud_home_with_ops();
        ch.write(
            "file.bin",
            BlobBody::from_bytes(b"old".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        ops.clear_calls();

        ch.write(
            "file.bin",
            BlobBody::from_bytes(b"new".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let calls = ops.calls();
        assert_eq!(
            calls.first(),
            Some(&MockCall::Write("file.bin".to_string()))
        );
        assert!(
            !calls.contains(&MockCall::Delete("file.bin".to_string())),
            "single-record overwrite must not delete the base record: {calls:?}"
        );
        assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
    }

    #[tokio::test]
    async fn put_object_over_chunked_publishes_single_before_cleanup() {
        let (ch, ops) = make_cloud_home_with_ops();
        let large_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
            .await
            .unwrap();
        ops.clear_calls();

        ch.write(
            "file.bin",
            BlobBody::from_bytes(b"new".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let calls = ops.calls();
        assert_eq!(
            calls.first(),
            Some(&MockCall::Write("file.bin".to_string()))
        );
        assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
        assert!(ch
            .ops
            .list_records(&CloudKitScope::Private, "file.bin.part")
            .unwrap()
            .is_empty());
        assert!(!ch
            .ops
            .record_exists(&CloudKitScope::Private, &chunk_manifest_key("file.bin"))
            .unwrap());
    }

    #[tokio::test]
    async fn put_object_cleanup_failure_leaves_new_single_readable() {
        let (ch, ops) = make_cloud_home_with_ops();
        let large_data: Vec<u8> = vec![0u8; 15 * 1024 * 1024];
        ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
            .await
            .unwrap();
        let stale_chunk = ch
            .ops
            .list_records(&CloudKitScope::Private, "file.bin.part")
            .unwrap()
            .into_iter()
            .next()
            .expect("chunked setup writes a chunk");
        ops.fail_delete(&stale_chunk);

        let err = ch
            .write(
                "file.bin",
                BlobBody::from_bytes(b"new".to_vec()),
                &no_progress(),
            )
            .await
            .expect_err("stale chunk cleanup failure must fail loud");
        let msg = err.to_string();

        assert!(msg.contains("delete"), "unexpected error: {msg}");
        assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
    }

    #[tokio::test]
    async fn test_overwrite_single_with_chunked() {
        let (ch, ops) = make_cloud_home_with_ops();
        // Write small file
        ch.write(
            "file.bin",
            BlobBody::from_bytes(b"small".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        ops.clear_calls();

        // Overwrite with large file (chunked)
        let large_data: Vec<u8> = vec![1u8; 25 * 1024 * 1024];
        ch.write(
            "file.bin",
            BlobBody::from_bytes(large_data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read("file.bin").await.unwrap();
        assert_eq!(read, large_data);

        let calls = ops.calls();
        let manifest_write = calls
            .iter()
            .position(|call| *call == MockCall::Write(chunk_manifest_key("file.bin")))
            .expect("chunked write publishes manifest");
        let base_delete = calls
            .iter()
            .position(|call| *call == MockCall::Delete("file.bin".to_string()))
            .expect("chunked write removes stale single base");
        assert!(
            manifest_write < base_delete,
            "chunk manifest must publish before stale base cleanup: {calls:?}"
        );

        // The single-record base is replaced by the chunk layout.
        assert!(!ch
            .ops
            .record_exists(&CloudKitScope::Private, "file.bin")
            .unwrap());
        assert!(ch
            .ops
            .record_exists(&CloudKitScope::Private, &chunk_manifest_key("file.bin"))
            .unwrap());
    }

    #[tokio::test]
    async fn chunked_over_longer_chunked_uses_new_token_before_stale_cleanup() {
        let (ch, ops) = make_cloud_home_with_ops();
        let old_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("file.bin", BlobBody::from_bytes(old_data), &no_progress())
            .await
            .unwrap();
        let old_chunks = ops
            .list_records(&CloudKitScope::Private, "file.bin.part")
            .unwrap();
        ops.clear_calls();

        let new_data: Vec<u8> = vec![1u8; 15 * 1024 * 1024];
        ch.write(
            "file.bin",
            BlobBody::from_bytes(new_data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        assert_eq!(ch.read("file.bin").await.unwrap(), new_data);
        let remaining_chunks = ch
            .ops
            .list_records(&CloudKitScope::Private, "file.bin.part")
            .unwrap();
        assert_eq!(remaining_chunks.len(), 2);
        assert!(
            old_chunks
                .iter()
                .all(|old| !remaining_chunks.iter().any(|new| new == old)),
            "old token chunks must be cleaned after new manifest publishes"
        );
    }

    #[tokio::test]
    async fn test_exists() {
        let ch = make_cloud_home();

        assert!(!ch.exists("nope.bin").await.unwrap());

        ch.write(
            "yep.bin",
            BlobBody::from_bytes(b"data".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        assert!(ch.exists("yep.bin").await.unwrap());

        // Chunked file
        let data: Vec<u8> = vec![0u8; 15 * 1024 * 1024];
        ch.write("chunked.bin", BlobBody::from_bytes(data), &no_progress())
            .await
            .unwrap();
        assert!(ch.exists("chunked.bin").await.unwrap());
    }

    #[tokio::test]
    async fn test_read_range_empty_when_end_leq_start() {
        let ch = make_cloud_home();
        ch.write(
            "range.bin",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        // end == start returns empty
        let slice = ch.read_range("range.bin", 3, 3).await.unwrap();
        assert!(slice.is_empty());

        // end < start returns empty
        let slice = ch.read_range("range.bin", 5, 2).await.unwrap();
        assert!(slice.is_empty());

        // end == 0 returns empty (the underflow case)
        let slice = ch.read_range("range.bin", 0, 0).await.unwrap();
        assert!(slice.is_empty());
    }

    #[tokio::test]
    async fn immutable_bounded_records_are_create_only_and_read_by_exact_version() {
        let (home, ops) = make_cloud_home_with_ops();
        let appended = home
            .append_object(
                "copies/bounded",
                BlobBody::from_bytes(b"first".to_vec()),
                &no_progress(),
            )
            .await
            .unwrap();

        let collision = home
            .append_object(
                "copies/bounded",
                BlobBody::from_bytes(b"second".to_vec()),
                &no_progress(),
            )
            .await
            .expect_err("an immutable record must never overwrite an existing key");
        assert!(matches!(collision, CloudHomeError::AlreadyExists(key) if key == "copies/bounded"));
        assert_eq!(home.read_appended(&appended).await.unwrap(), b"first");

        ops.write_record(
            &CloudKitScope::Private,
            "copies/bounded",
            b"replacement".to_vec(),
        )
        .unwrap();
        let changed = home
            .read_appended(&appended)
            .await
            .expect_err("an exact read must reject a later record version");
        assert!(changed.to_string().contains("changed from version"));
    }

    #[tokio::test]
    async fn immutable_multipart_stages_one_bounded_part_at_a_time_and_manifest_last() {
        let (home, ops) = make_cloud_home_with_ops();
        let data = vec![7u8; CHUNK_SIZE + 13];

        let appended = home
            .append_object(
                "copies/chunked",
                BlobBody::from_bytes(data.clone()),
                &no_progress(),
            )
            .await
            .unwrap();

        assert_eq!(
            ops.calls(),
            vec![
                MockCall::BeginBatch("batch-0".to_string()),
                MockCall::Stage(chunk_part_key("copies/chunked", "cloudkit-upload-0", 0,)),
                MockCall::Stage(chunk_part_key("copies/chunked", "cloudkit-upload-0", 1,)),
                MockCall::Stage(chunk_manifest_key("copies/chunked")),
                MockCall::CommitBatch("batch-0".to_string()),
            ]
        );
        assert_eq!(ops.max_stage_payload.load(Ordering::SeqCst), CHUNK_SIZE);
        assert_eq!(home.read_appended(&appended).await.unwrap(), data);

        let CloudKitAppendedId::Chunked { manifest, parts } =
            CloudKitAppendedId::decode(&appended).unwrap()
        else {
            panic!("multipart append returned a bounded identity");
        };
        assert_eq!(parts.len(), 2);
        assert_eq!(manifest.key, chunk_manifest_key("copies/chunked"));
        let manifest_bytes = ops
            .read_record(&CloudKitScope::Private, &manifest.key)
            .unwrap();
        let decoded_manifest = decode_chunk_manifest(&manifest_bytes).unwrap();
        for (index, part) in parts.iter().enumerate() {
            assert_eq!(
                part.key,
                chunk_part_key("copies/chunked", &decoded_manifest.upload_id, index)
            );
        }

        ops.write_record(
            &CloudKitScope::Private,
            &parts[0].key,
            vec![6u8; CHUNK_SIZE],
        )
        .unwrap();
        let mismatch = home
            .read_appended(&appended)
            .await
            .expect_err("multipart reads must reject a changed exact part tag");
        assert!(mismatch.to_string().contains("changed from version"));
    }

    #[tokio::test]
    async fn lost_atomic_commit_response_preserves_records_for_authoritative_listing() {
        let (home, ops) = make_cloud_home_with_ops();
        ops.lose_commit_response();
        let data = vec![2u8; CHUNK_SIZE + 1];

        let error = home
            .append_object(
                "copies/ambiguous",
                BlobBody::from_bytes(data.clone()),
                &no_progress(),
            )
            .await
            .expect_err("lost commit response must remain an unknown outcome");

        assert!(error.to_string().contains("response was lost"), "{error}");
        let listing = home.list_appended("copies/").await.unwrap();
        assert_eq!(listing.objects.len(), 1);
        assert_eq!(listing.objects[0].logical_key(), "copies/ambiguous");
        assert_eq!(home.read_appended(&listing.objects[0]).await.unwrap(), data);
    }

    #[tokio::test]
    async fn mismatched_commit_locators_are_replaced_by_authoritative_versions() {
        let (home, ops) = make_cloud_home_with_ops();
        ops.return_wrong_commit_keys();
        let data = vec![3u8; CHUNK_SIZE + 1];

        let appended = home
            .append_object(
                "copies/locator-mismatch",
                BlobBody::from_bytes(data.clone()),
                &no_progress(),
            )
            .await
            .expect("authoritative reads recover exact committed locators");

        assert_eq!(home.read_appended(&appended).await.unwrap(), data);
        let records = CloudKitAppendedId::decode(&appended)
            .unwrap()
            .records()
            .unwrap();
        assert!(records
            .iter()
            .all(|record| !record.key.starts_with("unexpected-returned-record-")));
    }

    #[tokio::test]
    async fn immutable_atomic_multipart_failure_and_collision_create_no_partial_layout() {
        let (home, ops) = make_cloud_home_with_ops();
        let first_part = chunk_part_key("copies/failed", "cloudkit-upload-0", 0);
        let second_part = chunk_part_key("copies/failed", "cloudkit-upload-0", 1);
        ops.fail_write(&second_part);
        let data = vec![3u8; CHUNK_SIZE + 1];

        let error = home
            .append_object("copies/failed", BlobBody::from_bytes(data), &no_progress())
            .await
            .expect_err("part creation failure must abort the append");
        assert!(matches!(error, CloudHomeError::Transport(_)));
        assert!(!ops
            .record_exists(&CloudKitScope::Private, &first_part)
            .unwrap());
        assert!(!ops
            .record_exists(
                &CloudKitScope::Private,
                &chunk_manifest_key("copies/failed")
            )
            .unwrap());
        assert!(ops.staged_batches.lock().unwrap().is_empty());

        let (home, ops) = make_cloud_home_with_ops();
        let first_part = chunk_part_key("copies/collision", "cloudkit-upload-0", 0);
        let second_part = chunk_part_key("copies/collision", "cloudkit-upload-0", 1);
        ops.create_record(&CloudKitScope::Private, &second_part, b"existing".to_vec())
            .unwrap();
        let error = home
            .append_object(
                "copies/collision",
                BlobBody::from_bytes(vec![3u8; CHUNK_SIZE + 1]),
                &no_progress(),
            )
            .await
            .expect_err("a batch collision must reject the whole append");
        assert!(matches!(error, CloudHomeError::AlreadyExists(key) if key == second_part));
        assert!(!ops
            .record_exists(&CloudKitScope::Private, &first_part)
            .unwrap());
        assert!(!ops
            .record_exists(
                &CloudKitScope::Private,
                &chunk_manifest_key("copies/collision")
            )
            .unwrap());
        assert!(ops
            .record_exists(&CloudKitScope::Private, &second_part)
            .unwrap());
        assert!(ops.staged_batches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn immutable_staging_cleanup_failure_is_typed_and_remote_state_stays_empty() {
        let (home, ops) = make_cloud_home_with_ops();
        let second_part = chunk_part_key("copies/discard", "cloudkit-upload-0", 1);
        ops.fail_write(&second_part);
        ops.fail_discard();

        let error = home
            .append_object(
                "copies/discard",
                BlobBody::from_bytes(vec![4u8; CHUNK_SIZE + 1]),
                &no_progress(),
            )
            .await
            .expect_err("failed staging discard must be returned with the commit error");

        assert!(matches!(error, CloudHomeError::CleanupFailed { .. }));
        assert!(error.to_string().contains("batch-0"), "{error}");
        assert!(ops.store.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_an_uncommitted_staging_batch_discards_host_local_payloads() {
        let (_home, ops) = make_cloud_home_with_ops();
        let staging = begin_atomic_create(ops.clone(), CloudKitScope::Private)
            .await
            .unwrap();
        stage_atomic_create_record(
            staging.clone(),
            CloudKitRecordCreate {
                key: "copies/cancelled.part0.upload".to_string(),
                data: vec![8u8; CHUNK_SIZE],
            },
        )
        .await
        .unwrap();

        drop(staging);

        assert!(ops.staged_batches.lock().unwrap().is_empty());
        assert!(ops.store.lock().unwrap().is_empty());
        assert!(ops
            .calls()
            .contains(&MockCall::DiscardBatch("batch-0".to_string())));
    }

    #[test]
    fn cancellation_discard_failure_terminates_the_process() {
        const CHILD: &str = "COVEN_CLOUDKIT_CANCEL_DISCARD_ABORT_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let ops = Arc::new(MockCloudKitOps::new());
                let staging = begin_atomic_create(ops.clone(), CloudKitScope::Private)
                    .await
                    .unwrap();
                stage_atomic_create_record(
                    staging.clone(),
                    CloudKitRecordCreate {
                        key: "copies/cancelled.part0.upload".to_string(),
                        data: vec![8u8; CHUNK_SIZE],
                    },
                )
                .await
                .unwrap();
                ops.fail_discard();
                let started = Arc::new(std::sync::Barrier::new(2));
                let release = Arc::new(std::sync::Barrier::new(2));
                let worker_started = started.clone();
                let worker_release = release.clone();
                let owner = tokio::spawn(async move {
                    tokio::task::spawn_blocking(move || {
                        worker_started.wait();
                        worker_release.wait();
                        drop(staging);
                    })
                    .await
                    .unwrap();
                });
                started.wait();
                owner.abort();
                release.wait();
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            });
            panic!("failed cancellation discard did not abort the process");
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("cancellation_discard_failure_terminates_the_process")
            .arg("--nocapture")
            .env(CHILD, "1")
            .status()
            .expect("run CloudKit cancellation sabotage subprocess");
        assert!(
            !status.success(),
            "sabotage subprocess unexpectedly survived"
        );
    }

    #[tokio::test]
    async fn immutable_listing_consumes_change_pages_through_the_terminal_token() {
        let (home, ops) = make_cloud_home_with_ops();
        let appended = home
            .append_object(
                "copies/paged",
                BlobBody::from_bytes(vec![9u8; CHUNK_SIZE + 1]),
                &no_progress(),
            )
            .await
            .unwrap();
        let records = CloudKitAppendedId::decode(&appended)
            .unwrap()
            .records()
            .unwrap();
        let deleted = CloudKitRecordVersion {
            key: "copies/deleted".to_string(),
            version: CloudHeadVersion::from_provider("deleted-version".to_string()).unwrap(),
        };
        ops.clear_calls();
        ops.script_change_pages(vec![
            CloudKitRecordChangesPage {
                changes: vec![
                    CloudKitRecordChange::Present(records[1].clone()),
                    CloudKitRecordChange::Present(deleted),
                ],
                continuation: CloudKitRecordChangesContinuation::More(
                    CloudKitChangeToken::from_provider("page-one".to_string()).unwrap(),
                ),
            },
            CloudKitRecordChangesPage {
                changes: vec![
                    CloudKitRecordChange::Present(records[2].clone()),
                    CloudKitRecordChange::Present(records[0].clone()),
                    CloudKitRecordChange::Deleted {
                        key: "copies/deleted".to_string(),
                    },
                ],
                continuation: CloudKitRecordChangesContinuation::Complete(
                    CloudKitChangeToken::from_provider("terminal".to_string()).unwrap(),
                ),
            },
        ]);

        let listing = home.list_appended("copies/").await.unwrap();

        assert_eq!(listing.coverage, ListingCoverage::CompleteAtScan);
        assert_eq!(listing.objects, vec![appended]);
        assert_eq!(
            ops.calls(),
            vec![
                MockCall::Changes(None),
                MockCall::Changes(Some("page-one".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn immutable_delete_is_atomic_over_exact_multipart_versions() {
        let (home, ops) = make_cloud_home_with_ops();
        let appended = home
            .append_object(
                "copies/delete",
                BlobBody::from_bytes(vec![5u8; CHUNK_SIZE + 1]),
                &no_progress(),
            )
            .await
            .unwrap();
        let records = CloudKitAppendedId::decode(&appended)
            .unwrap()
            .records()
            .unwrap();
        ops.write_record(
            &CloudKitScope::Private,
            &records[1].key,
            vec![6u8; CHUNK_SIZE],
        )
        .unwrap();

        home.delete_appended(&appended)
            .await
            .expect_err("a changed part must reject the whole exact deletion");

        for record in records {
            assert!(ops
                .record_exists(&CloudKitScope::Private, &record.key)
                .unwrap());
        }
    }

    #[tokio::test]
    async fn grant_access_returns_share_join_info_without_email() {
        let ch = make_cloud_home();
        // CloudKit shares bind identity at URL-accept time, so no invitee email
        // is supplied and the grant still succeeds.
        let join_info = ch
            .set_access(CloudAccessState::Present {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await
            .unwrap();
        assert_eq!(
            join_info,
            CloudAccessOutcome::Present(CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example/member-pubkey".to_string(),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-store".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn revoke_access_unshares_and_reports_revoked() {
        let ch = make_cloud_home();
        let outcome = ch
            .set_access(CloudAccessState::Absent {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await
            .unwrap();
        // CloudKit removes the member's share participation, so it reports the
        // credential actually withdrawn rather than Unsupported.
        assert_eq!(outcome, CloudAccessOutcome::Absent(RevokeOutcome::Revoked));
    }

    #[tokio::test]
    async fn repeated_present_access_reuses_the_verified_share() {
        let (home, ops) = make_cloud_home_with_ops();
        let desired = CloudAccessState::Present {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        };

        let first = home.set_access(desired.clone()).await.unwrap();
        let second = home.set_access(desired).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(ops.grant_share_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn repeated_absent_access_does_not_revoke_twice() {
        let (home, ops) = make_cloud_home_with_ops();
        home.set_access(CloudAccessState::Present {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        })
        .await
        .unwrap();
        let desired = CloudAccessState::Absent {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        };

        home.set_access(desired.clone()).await.unwrap();
        home.set_access(desired).await.unwrap();

        assert_eq!(ops.revoke_share_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_strip_part_suffix() {
        assert_eq!(strip_part_suffix("file.bin.part0"), "file.bin");
        assert_eq!(strip_part_suffix("file.bin.part123"), "file.bin");
        assert_eq!(
            strip_part_suffix("file.bin.part123.0123456789abcdef0123456789abcdef"),
            "file.bin"
        );
        assert_eq!(strip_part_suffix("file.bin.manifest"), "file.bin");
        assert_eq!(strip_part_suffix("file.bin"), "file.bin");
        assert_eq!(strip_part_suffix("file.partition"), "file.partition");
        assert_eq!(strip_part_suffix("file.part"), "file.part"); // no digits after .part
    }
}
