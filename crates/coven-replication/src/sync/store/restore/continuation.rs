use super::*;
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
        // The code's cursors are the device's streams as they stood when the
        // code was exported; the device kept publishing after that. The pulled
        // history already holds the device's latest activated acknowledgement,
        // so the ack stream resumes from that head when it is past the code's.
        let latest_ack_ref = match self
            .database
            .activated_store_ack(&continuation.registration)
            .await
            .map_err(StoreRegistrationError::from)?
        {
            Some(activated) if activated.reference.sequence > continuation.latest_ack.sequence => {
                activated.reference
            }
            _ => continuation.latest_ack.clone(),
        };
        let latest = history
            .load_store_ack(&latest_ack_ref, &registration)
            .await?;
        let chain = history
            .load_acknowledgement_proof_chain(latest_ack_ref, latest, &registration)
            .await
            .map_err(crate::sync::store::StorePullError::from)
            .map_err(StoreRegistrationError::from)?
            .into_iter()
            .rev()
            .map(|(_, value)| value)
            .collect();
        self.database
            .install_activated_device_continuation(
                continuation,
                &self.identity,
                &device_signer,
                chain,
            )
            .await
            .map_err(StoreRegistrationError::from)
    }
}
