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
    context: StoreContext,
}

#[doc(hidden)]
pub struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: crate::join_code::MembershipFloor,
}

#[derive(Clone)]
struct StoreAccess<'a> {
    database: StoreDatabase,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

pub(crate) struct AuthorizedStore<'a> {
    access: StoreAccess<'a>,
    membership: crate::sync::membership::MembershipChain,
}

impl Store {
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

    pub(crate) fn new(
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
            context: StoreContext {
                database,
                storage,
                store_root,
            },
        })
    }
    pub(crate) fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        self.context.database()
    }

    pub(crate) fn cloud_storage(&self) -> &Arc<CloudSyncStorage> {
        self.storage()
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

    pub(crate) async fn resume_operations(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), SyncCycleFailure> {
        self.resume_device_exclusion(identity)
            .await
            .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        crate::sync::store::circle_controls::resume_circle_operations(
            self.database(),
            &**self.storage(),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    #[doc(hidden)]
    pub async fn abandon_candidate(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        write_id: crate::WriteId,
    ) -> Result<abandonment::MergeCandidateAbandonment, crate::sync::store::StoreError> {
        abandonment::abandon_merge_candidate(
            self.database(),
            &**self.storage(),
            device_id,
            identity,
            write_id,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<Vec<crate::sync::membership::MemberInfo>, membership::MembershipOpsError> {
        membership::get_members(self.storage().as_ref(), user_pubkey, self.database()).await
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
        let verified_root =
            crate::sync::store_objects::load_store_protocol_root(storage, &store_root)
                .await
                .map_err(|error| SyncCycleFailure::operation("load Store protocol root", error))?;
        if verified_root.value.object_hash() != store_root.store_root_hash {
            return Err("verified Store root differs from local authority"
                .to_string()
                .into());
        }
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

    pub(crate) async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<crate::sync::circle::CircleId, super::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::CircleOperationError::BrowsableStorage);
        }
        crate::sync::store::circle_controls::create_circle(
            self.database(),
            &**self.storage(),
            device_id,
            timestamp,
            name,
            identity,
        )
        .await
    }

    pub(crate) async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: crate::sync::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), super::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::CircleOperationError::BrowsableStorage);
        }
        crate::sync::store::circle_controls::rename_circle(
            self.database(),
            &**self.storage(),
            device_id,
            timestamp,
            circle_id,
            name,
            identity,
        )
        .await
    }
}

impl AuthorizedStore<'_> {
    pub(crate) fn db(&self) -> &Database {
        self.access.database.sqlite()
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        &self.access.database
    }

    pub(super) fn membership(&self) -> &crate::sync::membership::MembershipChain {
        &self.membership
    }

    pub(crate) fn storage(&self) -> &dyn SyncStorage {
        self.access.storage
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        &self.access.store_root
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

    pub(crate) fn wrapped_keys(
        &self,
        recipient: &str,
    ) -> Result<Vec<crate::sync::wrapped_store_key::WrappedStoreKeyRef>, SyncCycleFailure> {
        self.membership
            .wrapped_key_authority_for(recipient)
            .map_err(|error| error.to_string().into())
    }

    pub(crate) async fn after_pull(&self) -> Result<&Self, SyncCycleFailure> {
        Ok(self)
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

    pub(crate) async fn drain_store_writes(&self) -> Result<u64, crate::sync::store::StoreError> {
        publication::drain_store_writes(self.database(), self.storage()).await
    }

    pub(crate) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        timestamp: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        preparation::prepare_store_write(
            self.database(),
            self.storage(),
            device_id,
            timestamp,
            identity,
            store_dir,
            &self.membership,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))
    }

    pub(crate) async fn snapshot_position(
        &self,
        snapshot: &crate::database::PublishedStoreSnapshot,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<u64, SyncCycleFailure> {
        let (root, registration, _, _) =
            crate::sync::store::operations::load_local_store_authority(
                self.database(),
                device_id,
                identity,
            )
            .await
            .map_err(|error| {
                SyncCycleFailure::from(format!(
                    "load local Store snapshot cadence authority: {error}"
                ))
            })?;
        let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
        .to_string();
        Ok(super::snapshot_position_for_stream(snapshot, &stream_id))
    }

    pub(crate) async fn push_snapshot(
        &self,
        snapshot: crate::sync::store::snapshot::CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        identity: &UserKeypair,
        created_at: String,
    ) -> Result<crate::sync::store_commit::SnapshotMeta, SyncCycleFailure> {
        snapshot::push_store_snapshot(
            self.storage(),
            self.store_root().store_root_hash,
            snapshot,
            coverage,
            schema_version,
            identity,
            created_at,
            &self.membership,
            self.database(),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))
    }

    pub(crate) async fn drain_snapshot(&self) -> Result<bool, SyncCycleFailure> {
        snapshot::drain_outbound_store_snapshot(self.storage(), self.database())
            .await
            .map(|snapshot| snapshot.is_some())
            .map_err(|error| SyncCycleFailure::operation("publish pending Store snapshot", error))
    }

    pub(crate) fn may_author_snapshot(&self, author_pubkey: &str) -> Result<(), String> {
        crate::sync::store::membership::authorize_loaded_membership_author(
            Some(&self.membership),
            author_pubkey,
            crate::sync::store::membership::MembershipAuthorRequirement::Owner,
        )
        .map_err(|error| error.to_string())
    }
}

async fn authorize(access: StoreAccess<'_>) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
    let crate::sync::store::pull::CycleMembership {
        chain,
        pinned_owner,
    } = crate::sync::store::pull::load_cycle_membership(access.storage, &access.database)
        .await
        .map_err(|error| SyncCycleFailure::operation("load membership chain", error))?;
    let membership = match (pinned_owner, chain) {
        (Some(_), Some(chain)) => chain,
        (None, _) => {
            return Err("authorized cycle has no pinned membership founder"
                .to_string()
                .into());
        }
        (Some(owner), None) => {
            return Err(
                format!("owner {owner} is pinned but the cycle has no membership chain").into(),
            );
        }
    };
    Ok(AuthorizedStore { access, membership })
}
