use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'storage> {
        self.history_verifier.membership_objects()
    }

    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        blob_cache: crate::sync::store::blob::StoreBlobCache,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: crate::sync::store::authorization::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            store_dir,
            blob_cache,
            history_verifier,
            blob_source,
            keyrings: Arc::new(keyrings),
        }
    }

    pub(crate) async fn finish_initialization(
        mut self,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let database = self.database.clone();
        let mut device_id = database
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?;
        let identity_is_founder = self
            .history_verifier
            .verified_root()
            .protocol()
            .descriptor
            .founder_pubkey
            == coven_keys::keys::public_key_hex(identity);
        if device_id.is_none() && !identity_is_founder {
            return Err(StoreInitializationError::NonFounderDeviceMissing);
        }
        let founder_pubkey = self
            .history_verifier
            .verified_root()
            .protocol()
            .descriptor
            .founder_pubkey
            .clone();
        self.load_and_install_owner_membership(&founder_pubkey)
            .await?;
        if device_id.is_none() && identity_is_founder {
            self.install_existing_founder_device(identity).await?;
            device_id = database
                .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?;
        }
        let device_id = device_id.ok_or(StoreInitializationError::LocalDeviceMissing)?;
        let store = Store::new(
            database,
            Arc::clone(self.storage),
            self.store_dir.clone(),
            identity.clone(),
            Some(device_id.clone()),
            self.history_verifier.verified_root().clone(),
        );
        Ok(InitializedStore::new(store, device_id))
    }

    pub(crate) async fn install_existing_founder_device(
        &self,
        signer: &UserKeypair,
    ) -> Result<(), crate::sync::store::authorization::registration::StoreRegistrationError> {
        use coven_protocol::objects::ProtocolObjectDomain;
        use coven_protocol::store_commit::{
            ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckRef,
            StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
        };

        let storage = self.storage;
        let root = self.history_verifier.verified_root().reference();
        let founder = self.history_verifier.load_founder_registration().await?;
        if founder.value.author_pubkey != coven_keys::keys::public_key_hex(signer) {
            return Err(
                crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                    "Store founder registration belongs to another identity".to_string(),
                ),
            );
        }
        if founder.value.provider
            != storage
                .provider_binding()
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?
                .device
        {
            return Err(
                crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                    "Store founder registration belongs to another provider principal".to_string(),
                ),
            );
        }
        founder.value.device_signer(signer)?;

        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_prefix =
            coven_protocol::store_commit::founder_registration_semantic_prefix(
                match founder.value.origin {
                    StoreDeviceRegistrationOrigin::Founder { creation_id } => creation_id,
                    _ => return Err(
                        crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                            "Store founder registration has a non-founder origin".to_string(),
                        ),
                    ),
                },
            );
        let (registration_bytes, registration_prepared) = storage
            .read_prepared_protocol_slot(
                &registration_context,
                founder.object.slot(),
                &registration_prefix,
            )
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        if registration_bytes != founder.bytes
            || registration_prepared.reference() != &founder.object
        {
            return Err(
                crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                    "prepared founder registration differs from its verified exact object"
                        .to_string(),
                ),
            );
        }
        let registration_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &founder.value.acknowledgements
        else {
            return Err(
                crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                    "Store founder registration has no acknowledgement anchor".to_string(),
                ),
            );
        };
        let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let ack_prefix = ack_slot_prefix(&founder.value.device_id.to_string(), 1);
        let (ack_bytes, ack_prepared) = storage
            .read_prepared_protocol_slot(&ack_context, first_slot, &ack_prefix)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let unverified_ack: StoreAck = serde_json::from_slice(&ack_bytes)?;
        let ack_ref = StoreAckRef {
            registration: registration_ref.clone(),
            sequence: unverified_ack.sequence,
            ack_hash: unverified_ack.ack_hash(),
            object: ack_prepared.reference().clone(),
        };
        let ack = StoreAck::parse_at(&ack_bytes, root, &ack_ref, &founder.value)?;
        if ack.registration != registration_ref {
            return Err(
                crate::sync::store::authorization::registration::StoreRegistrationError::Invalid(
                    "Store founder acknowledgement names another registration".to_string(),
                ),
            );
        }
        self.database
            .install_existing_local_founder_device(
                coven_protocol::objects::ExactProtocolObject {
                    value: founder.value,
                    bytes: registration_bytes,
                    prepared: registration_prepared,
                },
                ack_ref,
                coven_protocol::objects::ExactProtocolObject {
                    value: ack,
                    bytes: ack_bytes,
                    prepared: ack_prepared,
                },
            )
            .await
            .map_err(|error| {
                crate::sync::store::authorization::registration::StoreRegistrationError::Database(
                    error,
                )
            })
    }

    pub(crate) async fn authorize_store(
        mut self,
        identity: &'storage UserKeypair,
        device_id: Option<&str>,
    ) -> Result<AuthorizedStore<'storage>, SyncCycleFailure> {
        let owner = self
            .database
            .validated_store_owner(self.history_verifier.verified_root().reference())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("validate Store owner authority", error)
            })?;
        let membership = self
            .load_current_membership(&owner)
            .await
            .map_err(|error| SyncCycleFailure::operation("load membership chain", error))?;
        let local_device = match device_id {
            Some(device_id) => Some(
                LocalStoreDevice::load(
                    &self.database,
                    self.history_verifier.verified_root().reference(),
                    device_id,
                )
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("load local Store device authority", error)
                })?,
            ),
            None => None,
        };
        Ok(AuthorizedStore::new(
            self,
            identity,
            local_device,
            membership,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn authorize_writer(
        self,
        membership: coven_protocol::membership::MembershipChain,
        identity: &'storage UserKeypair,
        registration: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> crate::sync::store::commit_publication::AuthorizedWriterOperation<'storage> {
        let database = self.database.clone();
        let storage = self.storage;
        let store_dir = self.store_dir;
        let keyrings = Arc::clone(&self.keyrings);
        let writer = Arc::new(
            crate::sync::store::commit_publication::LocalStoreWriter::from_verified_parts(
                identity.clone(),
                registration,
                device_signer,
            ),
        );
        let keyrings = crate::sync::store::commit_publication::LocalWriterKeyrings::new(
            Arc::clone(&writer),
            keyrings,
        );
        crate::sync::store::commit_publication::AuthorizedWriterOperation::from_parts(
            database, self, storage, store_dir, membership, writer, keyrings,
        )
    }

    pub(crate) fn from_pending_device_join(
        _authority: crate::sync::store::device_join::PendingDeviceJoinHistoryConstruction,
        database: StoreDatabase,
        storage: &'storage Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        blob_cache: crate::sync::store::blob::StoreBlobCache,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: crate::sync::store::authorization::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self::new(
            database,
            storage,
            store_dir,
            blob_cache,
            history_verifier,
            blob_source,
            keyrings,
        )
    }

    pub(crate) fn from_snapshot(
        _authority: crate::sync::store::commit_publication::SnapshotHistoryConstruction,
        database: StoreDatabase,
        storage: &'storage Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        blob_cache: crate::sync::store::blob::StoreBlobCache,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: crate::sync::store::authorization::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self::new(
            database,
            storage,
            store_dir,
            blob_cache,
            history_verifier,
            blob_source,
            keyrings,
        )
    }
}
