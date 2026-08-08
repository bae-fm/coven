use super::blob_io::*;
use super::cipher::*;
use super::*;

#[async_trait]
impl CloudSyncObjectStorage for CloudSyncConnection {
    fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_path_scheme()
    }

    async fn probe_provider(&self) -> Result<(), StorageError> {
        self.home.probe().await.map_err(Into::into)
    }

    async fn set_member_access(
        &self,
        state: crate::cloud::CloudAccessState,
    ) -> Result<crate::cloud::CloudAccessOutcome, StorageError> {
        self.home.set_access(state).await.map_err(Into::into)
    }

    async fn read_provider_object(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.home.read(key).await.map_err(Into::into)
    }

    async fn write_provider_object(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.home
            .write(
                key,
                BlobBody::from_bytes(stored_bytes),
                &crate::cloud::no_progress(),
            )
            .await
            .map_err(Into::into)
    }

    async fn list_provider_objects(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.home.list(prefix).await.map_err(Into::into)
    }

    async fn provider_object_exists(&self, key: &str) -> Result<bool, StorageError> {
        self.home.exists(key).await.map_err(Into::into)
    }

    async fn delete_provider_object(&self, key: &str) -> Result<(), StorageError> {
        self.home.delete(key).await.map_err(Into::into)
    }

    fn provider_probes(&self) -> &crate::provider_probe::ProviderProbeStorage {
        &self.provider_probes
    }

    async fn observe_exact_slot(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<ExactObjectRef>, StorageError> {
        self.exact.observe_at(slot).await.map_err(Into::into)
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &ObjectSlot,
    ) -> Result<(), StorageError> {
        self.exact
            .delete_and_verify_absent(slot)
            .await
            .map_err(Into::into)
    }

    fn store_blob_protection(
        &self,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, StorageError> {
        Ok(match self.cipher_for_seal()? {
            CloudCipher::Encrypted(encryption) => {
                coven_protocol::objects::BlobSpoolProtection::Opaque(encryption)
            }
            CloudCipher::Plaintext => coven_protocol::objects::BlobSpoolProtection::Browsable,
        })
    }

    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, StorageError> {
        self.exact.provider_binding().await.map_err(Into::into)
    }

    async fn allocate_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<ObjectSlot, StorageError> {
        context.validate_path(semantic_prefix)?;
        context.validate_extension(extension)?;
        Ok(self
            .exact
            .allocate_slot(&format!("{semantic_prefix}{extension}"))
            .await?)
    }

    fn prepare_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<PreparedExactObject, StorageError> {
        context.validate_slot(&slot, semantic_prefix)?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let stored = self.protocol_cipher_for_seal(context)?.seal(data, &aad);
        let reference = ExactObjectRef::new(
            slot,
            stored.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(&stored),
        );
        PreparedExactObject::new(reference, stored)
    }

    async fn open_prepared_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        context.validate_reference(prepared.reference(), semantic_prefix)?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let cipher = self.protocol_cipher_for_open(context);
        let object = prepared.reference().clone();
        let stored = prepared.stored_bytes().to_vec();
        run_storage_cpu(
            "verify and open prepared protocol object",
            Box::new(move || {
                object.verify(&stored)?;
                cipher.open(stored, &aad).map_err(|error| {
                    StorageError::Decryption(format!(
                        "protocol object {}: {error}",
                        object.slot().logical_key()
                    ))
                })
            }),
        )
        .await
    }

    async fn create_protocol_object(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError> {
        let upload =
            crate::cloud::ExactUpload::from_bytes(prepared.reference(), prepared.stored_bytes())?;
        self.exact
            .create_at(&upload, &crate::cloud::no_progress())
            .await
            .map(drop)
            .map_err(Into::into)
    }

    async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        context.validate_reference(object, semantic_prefix)?;
        let stored = self.exact.read_at(object.slot()).await?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let cipher = self.protocol_cipher_for_open(context);
        let object = object.clone();
        run_storage_cpu(
            "verify and open protocol object",
            Box::new(move || {
                object.verify(&stored)?;
                cipher.open(stored, &aad).map_err(|error| {
                    StorageError::Decryption(format!(
                        "protocol object {}: {error}",
                        object.slot().logical_key()
                    ))
                })
            }),
        )
        .await
    }

    async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError> {
        let (opened, prepared) = self
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await?;
        Ok((opened, prepared.reference().clone()))
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError> {
        context.validate_slot(slot, semantic_prefix)?;
        let stored = self.exact.read_at(slot).await?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let cipher = self.protocol_cipher_for_open(context);
        let slot = slot.clone();
        run_storage_cpu(
            "identify and open protocol slot",
            Box::new(move || {
                let object = ExactObjectRef::new(
                    slot.clone(),
                    stored.len() as u64,
                    coven_protocol::store_commit::ObjectHash::digest(&stored),
                );
                let prepared = PreparedExactObject::new(object, stored.clone())?;
                let opened = cipher.open(stored, &aad).map_err(|error| {
                    StorageError::Decryption(format!(
                        "protocol object {}: {error}",
                        slot.logical_key()
                    ))
                })?;
                Ok((opened, prepared))
            }),
        )
        .await
    }

    async fn delete_protocol_object(&self, object: &ExactObjectRef) -> Result<(), StorageError> {
        match self.exact.read_at(object.slot()).await {
            Err(crate::cloud::CloudHomeError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(stored)
                if stored.len() as u64 != object.stored_size()
                    || coven_protocol::store_commit::ObjectHash::digest(&stored)
                        != object.stored_hash() =>
            {
                return Err(StorageError::SlotCollision(format!(
                    "exact delete target {} contains different bytes",
                    object.slot().logical_key()
                )));
            }
            Ok(_) => {}
        }
        let delete_error = self.exact.delete_at(object.slot()).await.err();
        if delete_error
            .as_ref()
            .is_some_and(|error| !error.is_retryable())
        {
            return Err(delete_error.expect("delete error exists").into());
        }
        match self.exact.read_at(object.slot()).await {
            Err(crate::cloud::CloudHomeError::NotFound(_)) => Ok(()),
            Err(readback) => match delete_error {
                Some(operation) => Err(StorageError::UnresolvedOutcome {
                    operation: Box::new(operation.into()),
                    settlement: Box::new(readback.into()),
                }),
                None => Err(readback.into()),
            },
            Ok(_) => match delete_error {
                Some(error) => Err(error.into()),
                None => Err(StorageError::Storage(format!(
                    "exact object remains after delete: {}",
                    object.slot().logical_key()
                ))),
            },
        }
    }

    async fn allocate_blob_slot(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        Ok(self.exact.allocate_slot(&locator.semantic_key()).await?)
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        protection: coven_protocol::objects::BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<coven_protocol::objects::BlobSpoolWrite, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let (plaintext_size, plaintext_hash) = crate::local_file::exact_file_facts(plaintext_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        if plaintext_size != locator.plaintext_size() || plaintext_hash != locator.plaintext_hash()
        {
            return Err(StorageError::InvalidContent(format!(
                "blob plaintext {}/{} does not match its locator size/hash",
                locator.namespace(),
                locator.blob_id()
            )));
        }

        match tokio::fs::metadata(spool_file).await {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(StorageError::LocalFilesystem(format!(
                        "blob spool path is not a file: {}",
                        spool_file.display()
                    )));
                }
                let (stored_size, stored_hash) = crate::local_file::exact_file_facts(spool_file)
                    .await
                    .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob =
                    coven_protocol::blob::locator::StoredBlobRef::new(locator.clone(), object)
                        .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                let mut reader =
                    ExactBlobPlaintextReader::new(spool_file, &self.store_id, &blob, protection)
                        .await?;
                loop {
                    let chunk = coven_foundation::local_file::PlaintextChunkReader::next_chunk(
                        &mut reader,
                        1 << 20,
                    )
                    .await
                    .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                return Ok(coven_protocol::objects::BlobSpoolWrite::Reused);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::LocalFilesystem(format!(
                    "inspect blob spool {}: {error}",
                    spool_file.display()
                )));
            }
        }

        let retry_protection = protection.clone();
        let body = match (locator, protection) {
            (
                coven_protocol::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                coven_protocol::objects::BlobSpoolProtection::Opaque(encryption),
            ) => {
                if encryption.seal_key_fingerprint() != *key_fingerprint {
                    return Err(StorageError::InvalidContent(format!(
                        "blob locator key fingerprint {key_fingerprint} differs from the supplied audience key {}",
                        encryption.seal_key_fingerprint()
                    )));
                }
                let aad = cloud_aad_context(&self.store_id, &locator.semantic_key());
                CloudCipher::Encrypted(encryption)
                    .open_body(
                        scope.clone(),
                        plaintext_file,
                        &aad,
                        self.blob_chunking.chunk(),
                    )
                    .await
                    .map_err(StorageError::LocalFilesystem)?
            }
            (
                coven_protocol::blob::locator::BlobLocator::Browsable { .. },
                coven_protocol::objects::BlobSpoolProtection::Browsable,
            ) => BlobBody::from_file(plaintext_file)
                .await
                .map_err(StorageError::LocalFilesystem)?,
            (coven_protocol::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (coven_protocol::blob::locator::BlobLocator::Browsable { .. }, _) => {
                return Err(StorageError::Configuration(
                    "browsable blob locator cannot use audience encryption".to_string(),
                ));
            }
        };
        let expected_size = body.len();
        let stream = futures_util::stream::try_unfold(body, |mut body| async move {
            match body.next_part(1 << 20).await? {
                Some(chunk) => Ok::<_, crate::cloud::CloudHomeError>(Some((chunk, body))),
                None => Ok::<_, crate::cloud::CloudHomeError>(None),
            }
        });
        let staged = coven_foundation::local_file::AtomicStagedFile::create(spool_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let (staged, written) =
            staged
                .write_byte_stream(Box::pin(stream))
                .await
                .map_err(|error| match error {
                    coven_foundation::local_file::ByteStreamWriteError::Source(error) => {
                        error.into()
                    }
                    coven_foundation::local_file::ByteStreamWriteError::SourceCleanup {
                        source,
                        cleanup,
                    } => StorageError::CleanupFailed {
                        operation: Box::new(source.into()),
                        cleanup: Box::new(StorageError::LocalFilesystem(cleanup)),
                    },
                    coven_foundation::local_file::ByteStreamWriteError::Local(error) => {
                        StorageError::LocalFilesystem(error)
                    }
                })?;
        if written != expected_size {
            return Err(StorageError::InvalidContent(format!(
                "blob spool {} contains {written} stored bytes, expected {expected_size}",
                spool_file.display()
            )));
        }
        match staged.commit_new().await {
            Ok(()) => Ok(coven_protocol::objects::BlobSpoolWrite::Created),
            Err(coven_foundation::local_file::CommitNewFileError::DestinationExists(_)) => {
                let (stored_size, stored_hash) = crate::local_file::exact_file_facts(spool_file)
                    .await
                    .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob =
                    coven_protocol::blob::locator::StoredBlobRef::new(locator.clone(), object)
                        .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                let mut reader = ExactBlobPlaintextReader::new(
                    spool_file,
                    &self.store_id,
                    &blob,
                    retry_protection,
                )
                .await?;
                loop {
                    let chunk = coven_foundation::local_file::PlaintextChunkReader::next_chunk(
                        &mut reader,
                        1 << 20,
                    )
                    .await
                    .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                Ok(coven_protocol::objects::BlobSpoolWrite::Reused)
            }
            Err(error) => Err(StorageError::LocalFilesystem(error.to_string())),
        }
    }

    async fn prepare_blob_object(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<coven_protocol::blob::locator::StoredBlobRef, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let expected = locator.semantic_key();
        if slot.logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob slot {:?} does not match locator key {expected:?}",
                slot.logical_key()
            )));
        }
        let (stored_size, stored_hash) = crate::local_file::exact_file_facts(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        coven_protocol::blob::locator::StoredBlobRef::new(
            locator.clone(),
            ExactObjectRef::new(slot, stored_size, stored_hash),
        )
        .map_err(|error| StorageError::InvalidContent(error.to_string()))
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &Path,
        progress: &crate::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        let upload = crate::cloud::ExactUpload::from_file(object, stored_file).await?;
        self.exact
            .create_at(&upload, progress)
            .await
            .map(drop)
            .map_err(Into::into)
    }

    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        self.validate_blob_locator_home(blob.locator())?;
        let expected = blob.locator().semantic_key();
        if blob.object().slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                blob.object().slot().logical_key()
            )));
        }
        let stored = self.exact.read_at(blob.object().slot()).await?;
        blob.object().verify(&stored)
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
        dest: &Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError> {
        let stored_destination = dest.with_extension("coven-stored-download");
        let stored = self
            .stage_exact_blob_download(blob, &stored_destination)
            .await?;
        let mut plaintext = coven_foundation::local_file::AtomicStagedFile::create(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let mut reader =
            ExactBlobPlaintextReader::new(stored.path(), &self.store_id, blob, protection).await?;
        let written =
            plaintext
                .write_plaintext(&mut reader)
                .await
                .map_err(|error| match error {
                    coven_foundation::local_file::StreamWriteError::Source(
                        crate::local_file::PlaintextChunkError::Remote(error),
                    ) => error,
                    coven_foundation::local_file::StreamWriteError::Source(
                        crate::local_file::PlaintextChunkError::InvalidContent(error),
                    ) => StorageError::InvalidContent(error),
                    coven_foundation::local_file::StreamWriteError::Source(
                        crate::local_file::PlaintextChunkError::Local(error),
                    )
                    | coven_foundation::local_file::StreamWriteError::Local(error) => {
                        StorageError::LocalFilesystem(error)
                    }
                })?;
        if written != blob.locator().plaintext_size() {
            return Err(StorageError::InvalidContent(format!(
                "blob {} plaintext stage contains {written} bytes, expected {}",
                blob.locator().locator_hash(),
                blob.locator().plaintext_size()
            )));
        }
        Ok(plaintext)
    }

    async fn open_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
    ) -> Result<BlobRangeReader, StorageError> {
        let locator = blob.locator();
        self.validate_blob_locator_home(locator)?;
        let slot = blob.object().slot().clone();
        let (scope, key_fingerprint) = match locator {
            coven_protocol::blob::locator::BlobLocator::Opaque {
                scope,
                key_fingerprint,
                ..
            } => (scope, key_fingerprint),
            // A browsable home stores the plaintext in the clear, so its objects
            // carry no tags and a range read has nothing to check the provider's
            // answer against. Ranged reading is refused rather than served
            // unverified; the caller materializes the whole blob, where the row's
            // content hash can refuse it.
            coven_protocol::blob::locator::BlobLocator::Browsable { .. } => {
                return Err(StorageError::Configuration(format!(
                    "blob {} is stored in the clear, which has no per-range verification",
                    locator.locator_hash()
                )));
            }
        };
        let coven_protocol::objects::BlobSpoolProtection::Opaque(master) = protection else {
            return Err(StorageError::Configuration(
                "opaque blob locator requires audience encryption".to_string(),
            ));
        };
        // One ranged read of the prefix names the key and the chunk size; every
        // later range is arithmetic over the header it carries, so this is the
        // only request a range does not pay for.
        let prefix = self
            .exact
            .read_range_at(&slot, 0, (KeyTag::LEN + SEALED_BLOB_HEADER_LEN) as u64)
            .await
            .map_err(StorageError::from)?;
        let opener = verified_sealed_blob_opener(
            &prefix,
            blob,
            key_fingerprint,
            scope,
            &master,
            &cloud_aad_context(&self.store_id, &locator.semantic_key()),
        )?;
        Ok(BlobRangeReader {
            exact: self.exact.clone(),
            slot,
            opener,
            plaintext_size: locator.plaintext_size(),
            window: self.blob_chunking.window(),
        })
    }

    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        self.delete_protocol_object(object).await?;
        Ok(())
    }
}

/// Reading a stored blob body into an unpublished sibling is a step of this
/// adapter's own verified download, not a capability the storage surface
/// offers: every caller reaches it through `stage_verified_blob_plaintext`.
impl CloudSyncConnection {
    async fn stage_exact_blob_download(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        dest: &Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        let mut staged = coven_foundation::local_file::AtomicStagedFile::create(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        self.exact
            .read_at_to_file(object.slot(), staged.path_for_atomic_replacement())
            .await
            .map_err(|error| match error {
                CloudFileReadError::Source(error) => StorageError::from(error),
                CloudFileReadError::SourceCleanup { source, cleanup } => {
                    StorageError::CleanupFailed {
                        operation: Box::new(StorageError::from(source)),
                        cleanup: Box::new(StorageError::LocalFilesystem(cleanup)),
                    }
                }
                CloudFileReadError::Local(error) => StorageError::LocalFilesystem(error),
            })?;
        {
            let (size, digest) = coven_foundation::local_file::file_facts(staged.path())
                .await
                .map_err(|error| StorageError::LocalFilesystem(error.to_string()))?;
            object.verify_stored_facts(
                staged.path(),
                size,
                coven_protocol::store_commit::ObjectHash::from_digest(digest),
            )?;
        }
        Ok(staged)
    }
}
