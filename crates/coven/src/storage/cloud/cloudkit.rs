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

use crate::id_provider::{IdRef, UuidProvider};

use super::{
    combine_cleanup_failure, BlobBody, CloudAccessOutcome, CloudAccessState, CloudHome,
    CloudHomeError, CloudHomeJoinInfo, CloudObjectVersion, CloudVersionedObject, ExactSlotStorage,
    RevokeOutcome, UploadProgress,
};
use crate::protocol::objects::ObjectSlot;

const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
const CHUNK_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-chunk-manifest-v1\0";
const CHUNK_MANIFEST_SUFFIX: &str = ".manifest";

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
    pub environment: crate::protocol::objects::CloudKitEnvironment,
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
pub(crate) struct CloudKitCloudHome {
    ops: Arc<dyn CloudKitOps>,
    ids: IdRef,
    scope: CloudKitScope,
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
        Self {
            ops,
            ids: Arc::new(UuidProvider),
            scope: CloudKitScope::Shared {
                owner_name,
                zone_name,
            },
        }
    }

    async fn begin_atomic_create(&self) -> Result<Arc<CloudKitStagingCleanup>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
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

    async fn authoritative_created_records(
        &self,
        keys: Vec<String>,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
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

    async fn settle_atomic_create_response_loss(
        &self,
        keys: Vec<String>,
    ) -> Result<AtomicCreateReadback, CloudHomeError> {
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        blocking(move || {
            let mut created = Vec::with_capacity(keys.len());
            let mut missing = 0usize;
            for key in keys {
                match ops.read_versioned_record(&scope, &key) {
                    Ok(record) => created.push(CloudKitRecordVersion {
                        key,
                        version: record.version,
                    }),
                    Err(CloudHomeError::NotFound(_)) => missing += 1,
                    Err(error) => return Err(error),
                }
            }
            match (created.is_empty(), missing) {
                (true, _) => Ok(AtomicCreateReadback::Absent),
                (false, 0) => Ok(AtomicCreateReadback::Created(created)),
                (false, _) => Err(CloudHomeError::Transport(
                    "CloudKit atomic create exposed only part of its record batch".to_string(),
                )),
            }
        })
        .await
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

    async fn stage_record(
        self: Arc<Self>,
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
            self.ops
                .stage_atomic_create_record(&self.scope, &self.batch, record)
        })
        .await
        .map_err(|error| {
            CloudHomeError::Transport(format!(
                "CloudKit atomic-create staging task failed: {error}"
            ))
        })?
    }

    async fn commit(self: Arc<Self>) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        tokio::task::spawn_blocking(move || {
            let created = self.ops.commit_atomic_create(&self.scope, &self.batch)?;
            self.disarm();
            Ok(created)
        })
        .await
        .map_err(|error| {
            CloudHomeError::Transport(format!(
                "CloudKit atomic-create commit task failed: {error}"
            ))
        })?
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

enum AtomicCreateReadback {
    Created(Vec<CloudKitRecordVersion>),
    Absent,
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

/// After a manifest read came back NotFound: distinguish a truly absent object
/// from one whose chunk parts exist without their manifest — a torn write,
/// surfaced as transport corruption rather than a clean miss.
fn missing_or_unassembled(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: String,
) -> CloudHomeError {
    match ops.list_records(scope, &format!("{key}.part")) {
        Ok(chunks) if chunks.is_empty() => CloudHomeError::NotFound(key),
        Ok(_) => CloudHomeError::Transport(format!(
            "CloudKit object {key} has chunk records but no manifest"
        )),
        Err(error) => error,
    }
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
    settled: Arc<std::sync::atomic::AtomicBool>,
}

impl CloudKitPartSink {
    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        if self.settled.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let written_parts = self.index;
        cancellation_safe_blocking(move || {
            for i in 0..written_parts {
                delete_single_record(&*ops, &scope, &chunk_part_key(&key, &upload_id, i))?;
            }
            Ok::<(), CloudHomeError>(())
        })
        .await??;
        self.settled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for CloudKitPartSink {
    fn drop(&mut self) {
        if self.settled.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        for index in 0..self.index {
            let part_key = chunk_part_key(&self.key, &self.upload_id, index);
            if let Err(error) = delete_single_record(&*self.ops, &self.scope, &part_key) {
                tracing::error!(
                    %error,
                    key = %self.key,
                    upload_id = %self.upload_id,
                    "CloudKit cancellation failed to discard multipart part"
                );
                std::process::abort();
            }
        }
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
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        cancellation_safe_blocking(move || ops.write_record(&scope, &chunk_key, part.to_vec()))
            .await?
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        CloudKitPartSink::abort(self).await
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        let manifest = ChunkManifest::new(self.total_len, self.upload_id.clone());
        if self.index != manifest.part_count || self.written_len != manifest.total_len {
            let operation = CloudHomeError::Transport(format!(
                "CloudKit multipart {} wrote {} parts/{} bytes, expected {} parts/{} bytes",
                self.key, self.index, self.written_len, manifest.part_count, manifest.total_len
            ));
            let cleanup = self.abort().await;
            return Err(combine_cleanup_failure(operation, cleanup));
        }
        let manifest_key = chunk_manifest_key(&self.key);
        let manifest_data = encode_chunk_manifest(manifest);
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let settled = self.settled.clone();
        if let Err(operation) = cancellation_safe_blocking(move || {
            let result = ops.write_record(&scope, &manifest_key, manifest_data);
            if result.is_ok() {
                settled.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            result
        })
        .await?
        {
            let cleanup = self.abort().await;
            return Err(combine_cleanup_failure(operation, cleanup));
        }

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
    fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
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
            settled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

const EXACT_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-exact-manifest-v1\0";

fn exact_part_key(logical_key: &str, index: usize) -> String {
    format!("{logical_key}.exact-part{index}")
}

fn encode_exact_manifest(part_count: usize, total_len: usize) -> Vec<u8> {
    let mut bytes = EXACT_MANIFEST_MAGIC.to_vec();
    bytes.extend_from_slice(part_count.to_string().as_bytes());
    bytes.push(b'\n');
    bytes.extend_from_slice(total_len.to_string().as_bytes());
    bytes.push(b'\n');
    bytes
}

fn decode_exact_manifest(bytes: &[u8]) -> Result<(usize, usize), CloudHomeError> {
    let text = std::str::from_utf8(bytes.strip_prefix(EXACT_MANIFEST_MAGIC).ok_or_else(|| {
        CloudHomeError::Transport("CloudKit exact object has an invalid manifest".to_string())
    })?)
    .map_err(|error| CloudHomeError::Transport(format!("CloudKit exact manifest: {error}")))?;
    let mut lines = text.lines();
    let part_count = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit exact manifest omitted part count".to_string())
        })?
        .parse::<usize>()
        .map_err(|error| {
            CloudHomeError::Transport(format!("CloudKit exact manifest part count: {error}"))
        })?;
    let total_len = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit exact manifest omitted length".to_string())
        })?
        .parse::<usize>()
        .map_err(|error| {
            CloudHomeError::Transport(format!("CloudKit exact manifest length: {error}"))
        })?;
    if lines.next().is_some() || part_count != total_len.div_ceil(CHUNK_SIZE) {
        return Err(CloudHomeError::Transport(
            "CloudKit exact manifest shape does not match its length".to_string(),
        ));
    }
    Ok((part_count, total_len))
}

fn read_exact_cloudkit_object(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
) -> Result<(Vec<u8>, Vec<CloudKitRecordVersion>), CloudHomeError> {
    let manifest = ops.read_versioned_record(scope, logical_key)?;
    let (part_count, total_len) = decode_exact_manifest(&manifest.bytes)?;
    let mut bytes = Vec::with_capacity(total_len);
    let mut records = Vec::with_capacity(part_count + 1);
    records.push(CloudKitRecordVersion {
        key: logical_key.to_string(),
        version: manifest.version,
    });
    for index in 0..part_count {
        let key = exact_part_key(logical_key, index);
        let part = read_exact_part(ops, scope, logical_key, part_count, total_len, index, &key)?;
        bytes.extend_from_slice(&part.bytes);
        records.push(CloudKitRecordVersion {
            key,
            version: part.version,
        });
    }
    Ok((bytes, records))
}

/// The plaintext length part `index` of an exact object carries: a full chunk
/// for every part but the last, which holds the remainder.
fn exact_part_len(part_count: usize, total_len: usize, index: usize) -> usize {
    if index + 1 == part_count {
        total_len - index * CHUNK_SIZE
    } else {
        CHUNK_SIZE
    }
}

/// Read one part record and refuse a length its manifest does not assign it. A
/// part that is short is not the part the manifest describes, so splicing it
/// would silently serve the wrong bytes at every later offset.
fn read_exact_part(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
    part_count: usize,
    total_len: usize,
    index: usize,
    key: &str,
) -> Result<CloudVersionedObject, CloudHomeError> {
    let part = ops.read_versioned_record(scope, key)?;
    let expected_len = exact_part_len(part_count, total_len, index);
    if part.bytes.len() != expected_len {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit exact object {logical_key:?} part {index} has {} bytes, expected {expected_len}",
            part.bytes.len()
        )));
    }
    Ok(part)
}

/// Read one byte range of an exact CloudKit object, fetching only the part
/// records that cover it.
///
/// The whole-object sibling is [`read_exact_cloudkit_object`]. Both read the
/// same manifest, but this one never touches a part the range does not reach —
/// which is what makes a ranged read of a blob cost the range. Reading the whole
/// object and slicing would answer correctly and cost the object, so the
/// caller's O(range) guarantee lives or dies here.
fn read_exact_cloudkit_range(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
    start: usize,
    end: usize,
) -> Result<Vec<u8>, CloudHomeError> {
    let manifest = ops.read_versioned_record(scope, logical_key)?;
    let (part_count, total_len) = decode_exact_manifest(&manifest.bytes)?;
    if end > total_len {
        return Err(CloudHomeError::Transport(format!(
            "range {start}..{end} exceeds CloudKit exact object {logical_key:?} size {total_len}"
        )));
    }
    let first = start / CHUNK_SIZE;
    let last = (end - 1) / CHUNK_SIZE;
    if last >= part_count {
        return Err(CloudHomeError::Transport(format!(
            "range {start}..{end} needs part {last} of CloudKit exact object {logical_key:?}, which has {part_count}"
        )));
    }
    let mut bytes = Vec::with_capacity(end - start);
    for index in first..=last {
        let key = exact_part_key(logical_key, index);
        let part = read_exact_part(ops, scope, logical_key, part_count, total_len, index, &key)?;
        let part_start = index * CHUNK_SIZE;
        let from = start.saturating_sub(part_start);
        let to = (end - part_start).min(part.bytes.len());
        bytes.extend_from_slice(&part.bytes[from..to]);
    }
    Ok(bytes)
}

#[async_trait]
impl ExactSlotStorage for CloudKitCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use crate::protocol::objects::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let identity = blocking(move || ops.provider_identity(&scope)).await?;
        if identity.container_id.is_empty()
            || identity.owner_name.is_empty()
            || identity.zone_name.is_empty()
            || identity.current_user_record_name.is_empty()
        {
            return Err(CloudHomeError::Configuration(
                "CloudKit provider identity contains an empty stable identifier".to_string(),
            ));
        }
        if let CloudKitScope::Shared {
            owner_name,
            zone_name,
        } = &self.scope
        {
            if owner_name != &identity.owner_name || zone_name != &identity.zone_name {
                return Err(CloudHomeError::Configuration(format!(
                    "CloudKit provider identity resolved zone {}/{}, expected {owner_name}/{zone_name}",
                    identity.owner_name, identity.zone_name
                )));
            }
        }
        let principal = match &self.scope {
            CloudKitScope::Private => ProviderPrincipalId::CloudKitPrivateZoneOwner {
                record_name: identity.current_user_record_name,
            },
            CloudKitScope::Shared { .. } => ProviderPrincipalId::CloudKitSharedZoneParticipant {
                record_name: identity.current_user_record_name,
            },
        };
        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::CloudKit {
                container_id: identity.container_id,
                environment: identity.environment,
                owner_name: identity.owner_name,
                zone_name: identity.zone_name,
            },
            device: ProviderDeviceBinding { principal },
        })
    }

    async fn cross_principal_evidence(
        &self,
    ) -> Result<crate::protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        use crate::protocol::provider::{CloudKitAcceptedShare, CrossPrincipalProviderEvidence};
        use crate::protocol::store_commit::ObjectHash;

        let CloudKitScope::Shared {
            owner_name,
            zone_name,
        } = &self.scope
        else {
            return Err(CloudHomeError::Configuration(
                "CloudKit cross-principal evidence requires an accepted shared zone".to_string(),
            ));
        };
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let accepted = blocking(move || ops.accepted_read_write_share(&scope)).await?;
        let binding = self.provider_binding().await?;
        let crate::protocol::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant {
            record_name,
        } = binding.device.principal
        else {
            return Err(CloudHomeError::Configuration(
                "CloudKit adapter returned a non-CloudKit principal".to_string(),
            ));
        };
        if accepted.share_record_name.is_empty()
            || accepted.owner_name != *owner_name
            || accepted.zone_name != *zone_name
            || accepted.participant_record_name != record_name
            || accepted.permission != CloudKitSharePermission::ReadWrite
            || accepted.acceptance != CloudKitShareAcceptance::Accepted
            || accepted.canonical_record.is_empty()
        {
            return Err(CloudHomeError::Configuration(
                "CloudKit accepted share does not prove read-write participation in the selected zone"
                    .to_string(),
            ));
        }
        let share_slot = ObjectSlot::logical(format!(
            "__coven_cloudkit_share__/{}",
            hex::encode(ObjectHash::digest(accepted.share_record_name.as_bytes()).as_bytes())
        ))?;
        Ok(CrossPrincipalProviderEvidence::CloudKit(
            CloudKitAcceptedShare {
                share: crate::protocol::objects::ExactObjectRef::new(
                    share_slot,
                    accepted.canonical_record.len() as u64,
                    ObjectHash::digest(&accepted.canonical_record),
                ),
                share_record_name: accepted.share_record_name,
                owner_name: accepted.owner_name,
                zone_name: accepted.zone_name,
                participant_record_name: accepted.participant_record_name,
            },
        ))
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let total_len = usize::try_from(body.len()).map_err(|_| {
            CloudHomeError::Transport(format!(
                "CloudKit object {:?} is too large for this platform",
                slot.logical_key()
            ))
        })?;
        let part_count = total_len.div_ceil(CHUNK_SIZE);
        let staging = self.begin_atomic_create().await?;
        let mut requested_keys = Vec::with_capacity(part_count + 1);
        let mut written_len = 0usize;
        for index in 0..part_count {
            let part = match body.next_part(CHUNK_SIZE).await {
                Ok(Some(part)) => part,
                Ok(None) => {
                    return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                        "CloudKit object {:?} ended after {written_len} of {total_len} bytes",
                        slot.logical_key()
                    ))))
                }
                Err(error) => return Err(staging.cleanup_failure(error)),
            };
            written_len += part.len();
            let key = exact_part_key(slot.logical_key(), index);
            if let Err(error) = staging
                .clone()
                .stage_record(CloudKitRecordCreate {
                    key: key.clone(),
                    data: part.to_vec(),
                })
                .await
            {
                return Err(staging.cleanup_failure(error));
            }
            requested_keys.push(key);
        }
        match body.next_part(CHUNK_SIZE).await {
            Ok(None) if written_len == total_len => {}
            Ok(None) => {
                return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {:?} yielded {written_len} bytes, expected {total_len}",
                    slot.logical_key()
                ))))
            }
            Ok(Some(extra)) => {
                return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {:?} yielded at least {} bytes, expected {total_len}",
                    slot.logical_key(),
                    written_len + extra.len()
                ))))
            }
            Err(error) => return Err(staging.cleanup_failure(error)),
        }
        if let Err(error) = staging
            .clone()
            .stage_record(CloudKitRecordCreate {
                key: slot.logical_key().to_string(),
                data: encode_exact_manifest(part_count, total_len),
            })
            .await
        {
            return Err(staging.cleanup_failure(error));
        }
        requested_keys.push(slot.logical_key().to_string());
        let created = match staging.clone().commit().await {
            Ok(created) => created,
            Err(CloudHomeError::AlreadyExists(_)) => {
                return Err(staging.cleanup_failure(CloudHomeError::AlreadyExists(
                    slot.logical_key().to_string(),
                )));
            }
            Err(operation) => {
                match self
                    .settle_atomic_create_response_loss(requested_keys.clone())
                    .await
                {
                    Ok(AtomicCreateReadback::Created(created)) => {
                        staging.disarm();
                        created
                    }
                    Ok(AtomicCreateReadback::Absent) => {
                        return Err(staging.cleanup_failure(operation))
                    }
                    Err(readback) => {
                        staging.disarm();
                        return Err(CloudHomeError::UnresolvedOutcome {
                            operation: Box::new(operation),
                            readback: Box::new(readback),
                        });
                    }
                }
            }
        };
        if created.len() != requested_keys.len()
            || created
                .iter()
                .zip(&requested_keys)
                .any(|(record, requested)| &record.key != requested)
        {
            self.authoritative_created_records(requested_keys).await?;
        }
        progress(total_len as u64);
        Ok(())
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || {
            read_exact_cloudkit_object(&*ops, &scope, &logical_key).map(|value| value.0)
        })
        .await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let start = usize::try_from(start)
            .map_err(|_| CloudHomeError::Configuration("range start is too large".to_string()))?;
        let end = usize::try_from(end)
            .map_err(|_| CloudHomeError::Configuration("range end is too large".to_string()))?;
        if end < start {
            return Err(CloudHomeError::Configuration(format!(
                "invalid range {start}..{end}"
            )));
        }
        if end == start {
            return Ok(Vec::new());
        }
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || read_exact_cloudkit_range(&*ops, &scope, &logical_key, start, end)).await
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        let bytes = self.read_at(slot).await?;
        let stream: super::CloudObjectStream =
            Box::pin(futures_util::stream::once(
                async move { Ok(Bytes::from(bytes)) },
            ));
        super::write_cloud_object_stream(destination, stream)
            .await
            .map(drop)
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || {
            let records = match read_exact_cloudkit_object(&*ops, &scope, &logical_key) {
                Ok((_, records)) => records,
                Err(CloudHomeError::NotFound(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            ops.delete_record_versions(&scope, &records)
        })
        .await
    }
}

#[cfg(test)]
#[path = "cloudkit_tests.rs"]
mod tests;
