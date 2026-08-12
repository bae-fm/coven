use super::*;

struct BlobDownload {
    authority: coven_protocol::blob::RowBlobAuthority,
    stored: coven_protocol::blob::locator::StoredBlobRef,
}

impl BlobDownload {
    fn from_row(
        reference: coven_protocol::blob::RowBlobRef,
    ) -> Result<Self, crate::sync::store::snapshots::SnapshotBlobReconcileError> {
        let stored = reference.stored().cloned().ok_or(
            crate::sync::store::snapshots::SnapshotBlobReconcileError::MissingStoredReference,
        )?;
        Ok(Self {
            authority: reference.authority().clone(),
            stored,
        })
    }
}

impl<'storage> RestoringStore<'storage> {
    pub async fn begin_device_join(
        self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::JoiningStore<'storage>, crate::sync::store::DeviceJoinError>
    {
        crate::sync::store::JoiningStore::begin_from_restored_history(
            self.history,
            self.identity,
            pending,
            offer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        history: AuthorizedStoreHistory<'storage>,
        database: StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
        root: StoreRootRef,
        protocol: StoreProtocolRoot,
        membership: coven_protocol::membership::MembershipChain,
        identity: UserKeypair,
    ) -> Self {
        Self {
            history,
            database,
            storage,
            root,
            protocol,
            membership,
            identity,
        }
    }

    pub async fn pull(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<pull::StorePullResult, pull::StorePullError> {
        let execution = self
            .history
            .pull(&self.membership, Some(&self.identity), routing_encryption)
            .await?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub async fn install_activated_device_continuation(
        &self,
        continuation: coven_protocol::recovery::ActivatedContinuation,
    ) -> Result<(), StoreRegistrationError> {
        let registration = coven_protocol::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &self.root,
            continuation.registration.device_id,
        )
        .map_err(StoreRegistrationError::from)?;
        let device_signer = registration
            .device_signer(&self.identity)
            .map_err(StoreRegistrationError::from)?;
        let history = self.history.restore_history();
        let latest = history
            .load_store_ack(&continuation.latest_ack, &registration)
            .await?;
        let chain = history
            .load_acknowledgement_proof_chain(
                continuation.latest_ack.clone(),
                latest.value,
                &registration,
            )
            .await
            .map_err(crate::sync::store::StorePullError::from)
            .map_err(StoreRegistrationError::from)?
            .into_iter()
            .rev()
            .map(|(_, value)| value)
            .collect();
        let latest_snapshot = match &continuation.latest_snapshot {
            Some(reference) => Some(
                history
                    .load_store_snapshot(&continuation.registration, &registration, reference)
                    .await?,
            ),
            None => None,
        };
        self.database
            .install_activated_device_continuation(
                continuation,
                &self.identity,
                &device_signer,
                chain,
                latest_snapshot,
            )
            .await
            .map_err(StoreRegistrationError::from)
    }

    pub async fn reconcile_snapshot_blobs(
        &self,
        cancel: &watch::Receiver<bool>,
    ) -> Result<
        crate::sync::store::snapshots::SnapshotBlobReconcile,
        crate::sync::store::snapshots::SnapshotBlobReconcileError,
    > {
        let blobs = self
            .database
            .eager_row_blob_refs()
            .await?
            .into_iter()
            .map(BlobDownload::from_row)
            .collect::<Result<Vec<_>, _>>()?;

        if blobs.is_empty() {
            return Ok(crate::sync::store::snapshots::SnapshotBlobReconcile::Complete);
        }

        let total = blobs.len();
        for blob in blobs {
            if *cancel.borrow() {
                info!(total, "snapshot blob reconciliation cancelled");
                return Ok(crate::sync::store::snapshots::SnapshotBlobReconcile::Cancelled);
            }
            self.download_blob(blob).await.map_err(|failure| {
                crate::sync::store::snapshots::SnapshotBlobReconcileError::Download(
                    pull::BlobDownloadFailures::new(vec![failure]),
                )
            })?;
        }
        info!(total, "snapshot blob reconciliation complete");
        Ok(crate::sync::store::snapshots::SnapshotBlobReconcile::Complete)
    }

    async fn download_blob(&self, download: BlobDownload) -> Result<(), pull::BlobDownloadFailure> {
        let BlobDownload { authority, stored } = download;
        let namespace = stored.locator().namespace();
        let id = stored.locator().blob_id();
        self.history
            .verify_blob_plaintext(&authority, &stored, true)
            .await
            .map_err(|cause| {
                warn!(id, namespace, error = %cause, "failed to verify snapshot blob");
                pull::BlobDownloadFailure {
                    namespace: namespace.to_string(),
                    id: id.to_string(),
                    cause,
                }
            })
    }
}
