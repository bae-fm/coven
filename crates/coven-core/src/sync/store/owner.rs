use super::*;

#[cfg(test)]
pub(in crate::sync::store) async fn prepare_acknowledgement_activation_for_test(
    db: &Database,
    acknowledgement: crate::sync::store_commit::StoreAckRef,
    candidate: crate::sync::store::operations::PreparedStoreOperationCommit,
) -> Result<(), crate::database::DbError> {
    StoreDatabase::new(db)
        .prepare_acknowledgement_activation(acknowledgement, candidate)
        .await
}

#[doc(hidden)]
pub struct Store {
    database: StoreDatabase,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

#[doc(hidden)]
pub struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: crate::join_code::MembershipFloor,
}

pub(crate) struct InitializedStore {
    pub(crate) store: Store,
    pub(crate) device_id: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreInitializationError {
    #[error("Store protocol root failed: {0}")]
    ProtocolRoot(String),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
}

#[derive(Clone)]
struct StoreAccess<'a> {
    database: StoreDatabase,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

pub(crate) struct AuthorizedStore<'a> {
    database: StoreDatabase,
    history_verifier: crate::sync::store::pull::MergeHistoryVerifier<'a>,
    membership: crate::sync::membership::MembershipChain,
}

pub(super) struct AuthorizedStoreAuthority<'operation, 'storage> {
    pub(super) database: &'operation StoreDatabase,
    pub(super) history_verifier:
        &'operation mut crate::sync::store::pull::MergeHistoryVerifier<'storage>,
    pub(super) membership: &'operation mut crate::sync::membership::MembershipChain,
}

impl Store {
    pub(crate) async fn create(
        database: StoreDatabase,
        storage: Arc<CloudSyncStorage>,
        founder_timestamp: &str,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let protocol_root = crate::sync::store::protocol_root::create_store(
            &database,
            &storage,
            founder_timestamp,
            identity,
        )
        .await
        .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        Self::finish_initialization(database, storage, protocol_root, identity).await
    }

    pub(crate) async fn open(
        database: StoreDatabase,
        storage: Arc<CloudSyncStorage>,
        expected_root: &StoreRootRef,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let protocol_root =
            crate::sync::store::protocol_root::open_store(&database, &*storage, expected_root)
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        Self::finish_initialization(database, storage, protocol_root, identity).await
    }

    async fn finish_initialization(
        database: StoreDatabase,
        storage: Arc<CloudSyncStorage>,
        protocol_root: StoreProtocolRoot,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let store_root = database
            .local_store_root_ref()
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
            .ok_or_else(|| {
                StoreInitializationError::ProtocolRoot(
                    "opened Store root has no durable exact reference".to_string(),
                )
            })?;
        anchor_owner_membership(&*storage, &database, &store_root, &protocol_root, identity)
            .await
            .map_err(StoreInitializationError::MembershipAnchor)?;

        let mut device_id = database
            .sqlite()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        if device_id.is_none()
            && protocol_root.descriptor.founder_pubkey == crate::keys::public_key_hex(identity)
        {
            registration::install_existing_founder_device(
                &database,
                &*storage,
                &store_root,
                identity,
            )
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
            device_id = database
                .sqlite()
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        }
        let device_id = device_id.ok_or_else(|| {
            StoreInitializationError::ProtocolRoot(
                "initialized Store has no local device registration id".to_string(),
            )
        })?;
        let store = Self::new(database, storage, store_root, &protocol_root)
            .map_err(StoreInitializationError::ProtocolRoot)?;
        Ok(InitializedStore { store, device_id })
    }

    #[doc(hidden)]
    pub async fn load(
        database: StoreDatabase,
        storage: Arc<CloudSyncStorage>,
    ) -> Result<Self, StoreError> {
        let store_root =
            database
                .local_store_root_ref()
                .await?
                .ok_or(StoreError::MissingState {
                    key: operations::STORE_ROOT_AUTHORITY,
                })?;
        let verified_root =
            crate::sync::store_objects::load_store_protocol_root(&*storage, &store_root)
                .await?
                .value;
        Self::new(database, storage, store_root, &verified_root)
            .map_err(StoreError::InvalidOutbound)
    }

    fn new(
        database: StoreDatabase,
        storage: Arc<CloudSyncStorage>,
        store_root: StoreRootRef,
        verified_root: &StoreProtocolRoot,
    ) -> Result<Self, String> {
        if store_root.store_root_hash != verified_root.object_hash() {
            return Err(
                "local Store root reference differs from the verified Store root".to_string(),
            );
        }
        Ok(Self {
            database,
            storage,
            store_root,
        })
    }
    pub(crate) fn storage(&self) -> &Arc<CloudSyncStorage> {
        &self.storage
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        &self.store_root
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        &self.database
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.storage().blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.storage().self_uploader()
    }

    pub(crate) fn cloud_home(&self) -> &dyn CloudHome {
        self.storage().cloud_home()
    }

    fn access(&self) -> StoreAccess<'_> {
        StoreAccess {
            database: self.database().clone(),
            storage: &**self.storage(),
            store_root: self.store_root().clone(),
        }
    }

    #[doc(hidden)]
    pub async fn discard_blocked_write(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        if let BlockedWriteDiscard::Discarded(discarded) =
            self.database().discard_blocked_write(&write_id).await?
        {
            return Ok(discarded);
        }

        match abandonment::abandon_merge_candidate(
            self.database(),
            &**self.storage(),
            device_id,
            identity,
            write_id.clone(),
        )
        .await?
        {
            abandonment::MergeCandidateAbandonment::NotRequired => {
                return Err(StoreError::InvalidOutbound(
                    "blocked Merge candidate has no abandonment authority".to_string(),
                ));
            }
            abandonment::MergeCandidateAbandonment::Abandoned => {}
            abandonment::MergeCandidateAbandonment::CandidateActivated => {
                return Err(StoreError::InvalidOutbound(
                    "Merge candidate activated before abandonment and cannot be discarded"
                        .to_string(),
                ));
            }
        }

        match self.database().discard_blocked_write(&write_id).await? {
            BlockedWriteDiscard::Discarded(discarded) => Ok(discarded),
            BlockedWriteDiscard::RemoteResolutionRequired => Err(StoreError::InvalidOutbound(
                "Merge candidate remains unresolved after abandonment".to_string(),
            )),
        }
    }

    #[doc(hidden)]
    pub async fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<Vec<crate::sync::membership::MemberInfo>, membership::MembershipOpsError> {
        membership::get_members(self.storage().as_ref(), user_pubkey, self.database()).await
    }

    #[doc(hidden)]
    pub async fn membership_conflict(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<
        Option<crate::sync::membership::MembershipConflictInfo>,
        membership::MembershipOpsError,
    > {
        membership::get_membership_conflict(self.storage().as_ref(), user_pubkey, self.database())
            .await
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        identity: &UserKeypair,
        device_id: &str,
        choice: &crate::sync::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        crate::sync::membership::StoreMembershipConflictResolutionRef,
        membership::MembershipOpsError,
    > {
        let mut chain =
            membership::load_current_membership_chain(&**self.storage(), self.database()).await?;
        let valid_choice = match (chain.status(), choice.selection()) {
            (
                crate::sync::membership::MembershipStatus::Conflict(
                    crate::sync::membership::MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        conflicting_grants,
                        ..
                    },
                ),
                crate::sync::membership::MembershipConflictSelection::MemberAssignment { grant },
            ) => conflict_hash == &choice.conflict_hash() && conflicting_grants.contains_key(grant),
            (
                crate::sync::membership::MembershipStatus::Conflict(
                    crate::sync::membership::MembershipConflict::RevocationCycle {
                        conflict_hash,
                        maximal_valid_branches,
                        ..
                    },
                ),
                crate::sync::membership::MembershipConflictSelection::RevocationBranch { heads },
            ) => {
                conflict_hash == &choice.conflict_hash()
                    && maximal_valid_branches
                        .iter()
                        .any(|branch| branch.heads == *heads)
            }
            _ => false,
        };
        if !valid_choice {
            return Err(membership::InviteError::Membership(
                crate::sync::membership::MembershipError::InvalidConflictResolution,
            )
            .into());
        }
        let result = membership::resolve_membership_conflict(
            &**self.storage(),
            &mut chain,
            identity,
            device_id,
            choice.conflict_hash(),
            choice.selection().clone(),
            created_at,
            self.database(),
        )
        .await?;
        Ok(result)
    }

    #[doc(hidden)]
    pub async fn restore_membership(
        &self,
    ) -> Result<StoreRestoreMembership, membership::MembershipOpsError> {
        let founder_pubkey = self
            .database()
            .local_store_founder_pubkey()
            .await
            .map_err(|error| membership::MembershipOpsError::Database(error.to_string()))?
            .ok_or(membership::MembershipOpsError::NoFounderChain)?;
        let membership_floor = membership::current_membership_floor(
            self.storage().as_ref(),
            self.store_root(),
            Some(&founder_pubkey),
            Some(self.database()),
        )
        .await?;
        Ok(StoreRestoreMembership {
            store_root: self.store_root().clone(),
            founder_pubkey,
            membership_floor: crate::join_code::MembershipFloor(membership_floor),
        })
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
        authorize(self.access()).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn authorize_borrowed<'a>(
        storage: &'a dyn SyncStorage,
        db: &'a Database,
    ) -> Result<AuthorizedStore<'a>, SyncCycleFailure> {
        let database = StoreDatabase::new(db);
        let store_root = database
            .local_store_root_ref()
            .await
            .map_err(|error| format!("read Store root reference: {error}"))?
            .ok_or_else(|| "Store root reference is absent".to_string())?;
        authorize(StoreAccess {
            database,
            storage,
            store_root,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        identity: &UserKeypair,
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::sync::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, crate::sync::store::membership::MembershipOpsError>
    {
        crate::sync::store::membership::invite_member(
            &**self.storage(),
            self.storage().cloud_home(),
            identity,
            hlc,
            public_key_hex,
            invitee_email,
            role,
            encryption,
            store_id,
            store_name,
            self.database(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &crate::sync::cloud_storage::CloudCipherState,
        pending_rotation: &crate::sync::cloud_storage::PendingRotation,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        crate::sync::store::membership::remove_member(
            &**self.storage(),
            self.storage().cloud_home(),
            identity,
            hlc,
            public_key_hex,
            encryption,
            custody,
            cipher,
            pending_rotation,
            self.database(),
        )
        .await
    }
}

impl<'storage> AuthorizedStore<'storage> {
    pub(crate) fn db(&self) -> &Database {
        self.database.sqlite()
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        &self.database
    }

    pub(super) fn membership(&self) -> &crate::sync::membership::MembershipChain {
        &self.membership
    }

    pub(crate) fn storage(&self) -> &dyn SyncStorage {
        self.history_verifier.storage()
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        self.history_verifier.root()
    }

    pub(super) fn operation_authority<'operation>(
        &'operation mut self,
    ) -> AuthorizedStoreAuthority<'operation, 'storage> {
        AuthorizedStoreAuthority {
            database: &self.database,
            history_verifier: &mut self.history_verifier,
            membership: &mut self.membership,
        }
    }

    pub(crate) async fn resume_operations(
        &mut self,
        identity: &UserKeypair,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), SyncCycleFailure> {
        self.resume_device_exclusion(identity)
            .await
            .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        let routing_key = routing_encryption
            .map(|encryption| {
                crate::sync::circle::derive_row_routing_key(
                    encryption,
                    self.store_root().store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| {
                SyncCycleFailure::operation("derive Circle operation routing key", error)
            })?;
        self.resume_circle_operations(identity, routing_key.as_ref())
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &crate::sync::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        StoreDatabase::validate_store_write_routing(
            self.db().gates().as_ref(),
            routing_encryption,
        )?;
        let (registration_ref, registration) = self.database().local_blob_write_authority().await?;
        let authority =
            crate::sync::storage::BlobWriteAuthority::new(&registration_ref, &registration)
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        crate::blob::upload::drain_uploads(
            self.database(),
            self.storage(),
            authority,
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
    }

    pub(crate) async fn gc_tombstones(
        &self,
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        store_id: &str,
        self_pubkey: &str,
        clock: &dyn crate::clock::Clock,
        grace: chrono::Duration,
    ) -> Result<usize, String> {
        let activated_uploaders = self
            .database()
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect();
        crate::blob::delete::gc_tombstones(
            self.db(),
            cloud_home,
            self.storage(),
            cipher,
            store_id,
            self_pubkey,
            &activated_uploaders,
            &self.membership,
            clock,
            grace,
        )
        .await
    }

    pub(crate) async fn drain_store_writes(
        &mut self,
    ) -> Result<u64, crate::sync::store::StoreError> {
        let authority = self.operation_authority();
        publication::drain_store_writes_with_verifier(
            authority.database,
            authority.history_verifier.commit_verifier(),
        )
        .await
    }

    pub(crate) async fn prepare_pending_store_write(
        &mut self,
        device_id: &str,
        timestamp: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        let authority = self.operation_authority();
        preparation::prepare_store_write_with_history(
            authority.database,
            authority.history_verifier,
            device_id,
            timestamp,
            identity,
            store_dir,
            &*authority.membership,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))
    }
}

pub(crate) async fn anchor_owner_membership(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    root: &StoreRootRef,
    protocol_root: &StoreProtocolRoot,
    owner: &UserKeypair,
) -> Result<(), String> {
    if root.store_root_hash != protocol_root.object_hash() {
        return Err("local Store root reference differs from the opened Store root".to_string());
    }
    let chain = membership::load_and_persist_owner_anchor(
        storage,
        root,
        &crate::keys::public_key_hex(owner),
        database,
    )
    .await
    .map_err(|error| error.to_string())?;
    let founder = chain
        .founder_entry()
        .ok_or_else(|| "membership founder is absent from Store membership chain".to_string())?;
    if protocol_root
        .descriptor
        .validate_merge_founder_entry(founder)
        .is_err()
    {
        return Err("membership founder does not match Store protocol root".to_string());
    }
    Ok(())
}

async fn authorize(access: StoreAccess<'_>) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(access.storage, &access.store_root)
            .await
            .map_err(|error| SyncCycleFailure::operation("open Store history authority", error))?;
    let membership = load_authorized_membership(&mut history_verifier, &access.database).await?;
    Ok(AuthorizedStore {
        database: access.database,
        history_verifier,
        membership,
    })
}

async fn load_authorized_membership(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &StoreDatabase,
) -> Result<crate::sync::membership::MembershipChain, SyncCycleFailure> {
    crate::sync::store::pull::load_cycle_membership_with_history(history_verifier, database)
        .await
        .map_err(|error| SyncCycleFailure::operation("load membership chain", error))
}
