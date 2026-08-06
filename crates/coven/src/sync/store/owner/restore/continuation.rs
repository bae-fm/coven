use super::*;

impl<'storage> RestoringStore<'storage> {
    pub(crate) async fn begin_device_join(
        self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
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
        storage: &'storage dyn SyncStorage,
        root: StoreRootRef,
        protocol: StoreProtocolRoot,
        membership: crate::protocol::membership::MembershipChain,
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

    pub(crate) async fn pull(
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

    pub(crate) async fn install_activated_device_continuation(
        &self,
        continuation: crate::protocol::recovery::ActivatedContinuation,
    ) -> Result<(), StoreRegistrationError> {
        let registration = crate::protocol::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &self.root,
            continuation.registration.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let device_signer = registration
            .device_signer(&self.identity)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
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
            .map_err(|error| match error {
                crate::sync::store::owner::verified_history::registration::RegistrationLoadError::Object(error) => {
                    StoreRegistrationError::Object(error)
                }
                crate::sync::store::owner::verified_history::registration::RegistrationLoadError::Invalid(error) => {
                    StoreRegistrationError::Invalid(error)
                }
            })?
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
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))
    }

    pub(crate) async fn reconcile_snapshot_blobs(
        &self,
        cancel: &watch::Receiver<bool>,
    ) -> Result<
        crate::sync::store::owner::writer::snapshot::SnapshotBlobReconcile,
        crate::database::DbError,
    > {
        let blobs = self
            .database
            .eager_row_blob_refs()
            .await?
            .into_iter()
            .map(BlobDownload::from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::database::DbError::Message)?;

        if blobs.is_empty() {
            return Ok(
                crate::sync::store::owner::writer::snapshot::SnapshotBlobReconcile::Complete,
            );
        }

        let total = blobs.len();
        let mut all_ok = true;
        for blob in blobs {
            if *cancel.borrow() {
                info!(total, "snapshot blob reconciliation cancelled");
                return Ok(
                    crate::sync::store::owner::writer::snapshot::SnapshotBlobReconcile::Cancelled,
                );
            }
            if self.download_blob(blob).await.is_err() {
                all_ok = false;
            }
        }
        if all_ok {
            info!(total, "snapshot blob reconciliation complete");
            Ok(crate::sync::store::owner::writer::snapshot::SnapshotBlobReconcile::Complete)
        } else {
            warn!(total, "some snapshot blob files are not local");
            Ok(crate::sync::store::owner::writer::snapshot::SnapshotBlobReconcile::Incomplete)
        }
    }

    pub(super) async fn download_blob(
        &self,
        download: BlobDownload,
    ) -> Result<(), pull::BlobDownloadFailure> {
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
