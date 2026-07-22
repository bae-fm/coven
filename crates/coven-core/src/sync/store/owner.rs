use super::*;

use database::StoreDatabase;

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

pub(crate) struct Store {
    context: StoreContext,
}

#[derive(Clone)]
struct StoreAccess<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

pub(crate) struct AuthorizedStore<'a> {
    access: StoreAccess<'a>,
    membership: crate::sync::membership::MembershipChain,
    discovery_proof: MembershipDiscoveryProof,
}

impl Store {
    pub(crate) fn new(
        db: Database,
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
                db,
                storage,
                store_root,
            },
        })
    }
    pub(crate) fn db(&self) -> &Database {
        self.context.db()
    }

    pub(crate) fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    pub(crate) fn database(&self) -> &Database {
        self.db()
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

    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &crate::sync::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        crate::blob::upload::drain_uploads(
            self.db(),
            &**self.storage(),
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
    }

    fn access(&self) -> StoreAccess<'_> {
        StoreAccess {
            db: self.db(),
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
        crate::sync::circle_ops::resume_circle_operations(self.db(), &**self.storage(), identity)
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(crate) async fn abandon_candidate(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        write_id: crate::WriteId,
    ) -> Result<abandonment::MergeCandidateAbandonment, crate::sync::store::StoreError> {
        abandonment::abandon_merge_candidate(
            self.db(),
            &**self.storage(),
            device_id,
            identity,
            write_id,
        )
        .await
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
        authorize(self.access()).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn authorize_borrowed<'a>(
        storage: &'a dyn SyncStorage,
        db: &'a Database,
    ) -> Result<AuthorizedStore<'a>, SyncCycleFailure> {
        let store_root = db
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
        authorize_borrowed(db, storage, store_root).await
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
    ) -> Result<crate::join_code::InviteCode, crate::sync::membership_ops::MembershipOpsError> {
        crate::sync::membership_ops::invite_member(
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
            self.db(),
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
    ) -> Result<String, crate::sync::membership_ops::MembershipOpsError> {
        crate::sync::membership_ops::remove_member(
            &**self.storage(),
            self.storage().cloud_home(),
            identity,
            hlc,
            public_key_hex,
            encryption,
            custody,
            cipher,
            pending_rotation,
            self.db(),
        )
        .await
    }

    pub(crate) async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<crate::sync::circle::CircleId, crate::sync::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(crate::sync::circle_ops::CircleOperationError::BrowsableStorage);
        }
        crate::sync::circle_ops::create_circle(
            self.db(),
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
    ) -> Result<(), crate::sync::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(crate::sync::circle_ops::CircleOperationError::BrowsableStorage);
        }
        crate::sync::circle_ops::rename_circle(
            self.db(),
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
        self.access.db
    }

    pub(super) fn database(&self) -> StoreDatabase<'_> {
        StoreDatabase::new(self.db())
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
        crate::blob::delete::gc_tombstones(
            self.db(),
            cloud_home,
            self.storage(),
            cipher,
            store_id,
            self_pubkey,
            Some(&self.membership),
            clock,
            grace,
        )
        .await
    }

    pub(crate) async fn drain_store_writes(&self) -> Result<u64, crate::sync::store::StoreError> {
        publication::drain_store_writes(self.db(), self.storage()).await
    }

    pub(crate) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        timestamp: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        preparation::prepare_store_write(
            self.db(),
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
                self.db(),
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
        snapshot: crate::sync::snapshot::CreatedSnapshot,
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
            self.db(),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))
    }

    pub(crate) async fn drain_snapshot(&self) -> Result<bool, SyncCycleFailure> {
        snapshot::drain_outbound_store_snapshot(self.storage(), self.db())
            .await
            .map(|snapshot| snapshot.is_some())
            .map_err(|error| SyncCycleFailure::operation("publish pending Store snapshot", error))
    }

    pub(crate) fn may_author_snapshot(&self, author_pubkey: &str) -> Result<(), String> {
        crate::sync::membership_ops::authorize_loaded_membership_author(
            Some(&self.membership),
            author_pubkey,
            crate::sync::membership_ops::MembershipAuthorRequirement::Owner,
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn reclaim_packages(
        &self,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<
        crate::sync::store_reclaim::StoreReclaimResult,
        crate::sync::store_reclaim::StoreReclaimError,
    > {
        crate::sync::store_reclaim::reclaim_store_packages(
            self.db(),
            self.storage(),
            device_id,
            identity,
            self.store_root().store_root_hash,
            crate::sync::store_reclaim::ReclaimMembership {
                membership: &self.membership,
                discovery_proof: self.discovery_proof,
            },
        )
        .await
    }
}

async fn authorize(access: StoreAccess<'_>) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
    let crate::sync::pull::CycleMembership {
        chain,
        pinned_owner,
        listed_entries: _,
        discovery_proof,
    } = crate::sync::pull::load_cycle_membership(access.storage, access.db)
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
    Ok(AuthorizedStore {
        access,
        membership,
        discovery_proof,
    })
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn authorize_borrowed<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
) -> Result<AuthorizedStore<'a>, SyncCycleFailure> {
    authorize(StoreAccess {
        db,
        storage,
        store_root,
    })
    .await
}
