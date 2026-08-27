use super::chunking::*;
use super::*;

/// A [`PartSink`] over CloudKit's chunked record layout: each `send_part` writes
/// one tokened part record (CKAsset caps at 50 MB, so a large blob is split),
/// `finish` writes the `{key}.manifest` record that makes the object readable.
/// Existing records stay readable until the manifest points at the new token.
pub(crate) struct CloudKitPartSink {
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
    pub(crate) fn new(
        ops: Arc<dyn CloudKitOps>,
        scope: CloudKitScope,
        key: String,
        upload_id: String,
        total_len: usize,
    ) -> Self {
        Self {
            ops,
            scope,
            key,
            upload_id,
            index: 0,
            total_len,
            written_len: 0,
            settled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

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
            }
        }
    }
}

#[async_trait]
impl crate::cloud::PartSink for CloudKitPartSink {
    fn part_size(&self) -> usize {
        CHUNK_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
        control: &crate::cloud::UploadControl,
    ) -> Result<(), CloudHomeError> {
        control.wait_until_resumed().await;
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
