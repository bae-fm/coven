use super::*;

/// If a key ends with CloudKit layout metadata, strip that suffix to get the
/// base object key.
pub(crate) fn strip_part_suffix(key: &str) -> &str {
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
pub(crate) struct ChunkManifest {
    pub part_count: usize,
    pub total_len: usize,
    pub upload_id: String,
}

impl ChunkManifest {
    pub(crate) fn new(total_len: usize, upload_id: String) -> Self {
        Self {
            part_count: total_len.div_ceil(CHUNK_SIZE),
            total_len,
            upload_id,
        }
    }
}

pub(crate) fn encode_chunk_manifest(manifest: ChunkManifest) -> Vec<u8> {
    let mut encoded = CHUNK_MANIFEST_MAGIC.to_vec();
    encoded.extend_from_slice(manifest.part_count.to_string().as_bytes());
    encoded.push(b'\n');
    encoded.extend_from_slice(manifest.total_len.to_string().as_bytes());
    encoded.push(b'\n');
    encoded.extend_from_slice(manifest.upload_id.as_bytes());
    encoded.push(b'\n');
    encoded
}

pub(crate) fn chunk_manifest_key(key: &str) -> String {
    format!("{key}{CHUNK_MANIFEST_SUFFIX}")
}

pub(crate) fn chunk_part_key(key: &str, upload_id: &str, index: usize) -> String {
    format!("{key}.part{index}.{upload_id}")
}

pub(crate) fn decode_chunk_manifest(data: &[u8]) -> Result<ChunkManifest, CloudHomeError> {
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

pub(crate) struct CloudKitStagingCleanup {
    pub ops: Arc<dyn CloudKitOps>,
    pub scope: CloudKitScope,
    pub batch: CloudKitAtomicCreateBatch,
    pub armed: std::sync::atomic::AtomicBool,
}

impl CloudKitStagingCleanup {
    pub(crate) fn disarm(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn cleanup_failure(&self, operation: CloudHomeError) -> CloudHomeError {
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

    pub(crate) async fn stage_record(
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

    pub(crate) async fn commit(
        self: Arc<Self>,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
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

pub(crate) enum AtomicCreateReadback {
    Created,
    Absent,
}

pub(crate) fn parse_chunk_key(key: &str, upload_id: &str) -> Result<Option<usize>, CloudHomeError> {
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

pub(crate) fn list_numbered_chunks(
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

pub(crate) fn verify_chunk_manifest(
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
pub(crate) fn missing_or_unassembled(
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

pub(crate) fn read_chunk(
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

pub(crate) fn read_chunked_object(
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

pub(crate) fn delete_chunk_layout(
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

pub(crate) fn delete_stale_chunk_records(
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

pub(crate) fn delete_single_record(
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
pub(crate) fn delete_all_variants(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    key: &str,
) -> Result<(), CloudHomeError> {
    delete_single_record(ops, scope, key)?;
    delete_chunk_layout(ops, scope, key)
}
