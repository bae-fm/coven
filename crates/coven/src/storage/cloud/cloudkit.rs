//! CloudKit-backed `CloudHome` implementation.
//!
//! CloudKit's CKAsset has a 50MB limit, so large files are split into 10MB
//! chunks stored as tokened part records plus a manifest record.
//!
//! The `CloudKitOps` trait defines synchronous record operations that are
//! implemented in Swift via a UniFFI callback interface. `CloudKitCloudHome`
//! wraps these ops, adds chunking logic, and implements `CloudHome`.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::id_provider::{IdRef, UuidProvider};

use super::{
    CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    RevokeOutcome,
};

const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
const CHUNK_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-chunk-manifest-v1\0";
const CHUNK_MANIFEST_SUFFIX: &str = ".manifest";

/// Synchronous interface for raw CloudKit record operations.
/// Implemented in Swift via UniFFI callback interface.
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
    fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError>;
    fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError>;
    fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError>;
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
pub struct CloudKitCloudHome {
    ops: Arc<dyn CloudKitOps>,
    ids: IdRef,
    scope: CloudKitScope,
}

impl CloudKitCloudHome {
    pub fn new_private(ops: Arc<dyn CloudKitOps>) -> Self {
        Self::new_private_with_ids(ops, Arc::new(UuidProvider))
    }

    pub fn new_private_with_ids(ops: Arc<dyn CloudKitOps>, ids: IdRef) -> Self {
        Self {
            ops,
            ids,
            scope: CloudKitScope::Private,
        }
    }

    pub fn new_shared(ops: Arc<dyn CloudKitOps>, owner_name: String, zone_name: String) -> Self {
        Self::new_shared_with_ids(ops, Arc::new(UuidProvider), owner_name, zone_name)
    }

    pub fn new_shared_with_ids(
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

    async fn grant_access(
        &self,
        grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        // provider_account_email is ignored for CloudKit: shares bind the joiner's
        // identity at URL-accept time, so no invitee email is required.
        let ops = self.ops.clone();
        let member_pubkey = grant.member_pubkey;
        let share = blocking(move || ops.grant_share(&member_pubkey)).await?;
        Ok(CloudHomeJoinInfo::CloudKitShare {
            share_url: share.share_url,
            owner_name: share.owner_name,
            zone_name: share.zone_name,
        })
    }

    async fn revoke_access(
        &self,
        revoke: CloudAccessRevoke,
    ) -> Result<RevokeOutcome, CloudHomeError> {
        let ops = self.ops.clone();
        let member_pubkey = revoke.member_pubkey;
        blocking(move || ops.revoke_share(&member_pubkey)).await?;
        Ok(RevokeOutcome::Revoked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_provider::SequentialIdProvider;
    use crate::storage::cloud::{no_progress, BlobBody};
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum MockCall {
        Write(String),
        Read(String),
        List(String),
        Delete(String),
        Exists(String),
    }

    struct MockCloudKitOps {
        store: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
        calls: Mutex<Vec<MockCall>>,
        fail_deletes: Mutex<HashSet<String>>,
        fail_writes: Mutex<HashSet<String>>,
        record_exists_calls: AtomicUsize,
    }

    impl MockCloudKitOps {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
                calls: Mutex::new(Vec::new()),
                fail_deletes: Mutex::new(HashSet::new()),
                fail_writes: Mutex::new(HashSet::new()),
                record_exists_calls: AtomicUsize::new(0),
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
            self.store
                .lock()
                .unwrap()
                .insert((scope.clone(), key.to_string()), data);
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

        fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
            Ok(CloudKitShare {
                share_url: format!("https://share.example/{member_pubkey}"),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-library".to_string(),
            })
        }

        fn revoke_share(&self, _member_pubkey: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }

        fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
            Ok(CloudKitShare {
                share_url: share_url.to_string(),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-library".to_string(),
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
    async fn grant_access_returns_share_join_info_without_email() {
        let ch = make_cloud_home();
        // CloudKit shares bind identity at URL-accept time, so no invitee email
        // is supplied and the grant still succeeds.
        let join_info = ch
            .grant_access(CloudAccessGrant {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await
            .unwrap();
        assert_eq!(
            join_info,
            CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example/member-pubkey".to_string(),
                owner_name: "owner-name".to_string(),
                zone_name: "bae-library".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn revoke_access_unshares_and_reports_revoked() {
        let ch = make_cloud_home();
        let outcome = ch
            .revoke_access(CloudAccessRevoke {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await
            .unwrap();
        // CloudKit removes the member's share participation, so it reports the
        // credential actually withdrawn rather than Unsupported.
        assert_eq!(outcome, RevokeOutcome::Revoked);
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
