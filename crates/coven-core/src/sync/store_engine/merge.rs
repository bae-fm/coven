use super::*;

pub(crate) mod abandonment;
mod acknowledgements;
pub(crate) mod preparation;
pub(crate) mod publication;
pub(super) mod pull;

pub(super) struct MergeStoreEngine {
    context: StoreEngineContext,
}

#[derive(Clone)]
struct MergeStoreAccess<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

pub(super) struct AuthorizedMergeStoreEngine<'a> {
    access: MergeStoreAccess<'a>,
    membership: crate::sync::membership::MembershipChain,
    discovery_proof: MembershipDiscoveryProof,
}

impl MergeStoreEngine {
    pub(super) fn new(context: StoreEngineContext) -> Self {
        Self { context }
    }
    pub(super) fn db(&self) -> &Database {
        self.context.db()
    }

    pub(super) fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    pub(super) fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    fn access(&self) -> MergeStoreAccess<'_> {
        MergeStoreAccess {
            db: self.db(),
            storage: &**self.storage(),
            store_root: self.store_root().clone(),
        }
    }

    pub(super) async fn resume_operations(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), SyncCycleFailure> {
        crate::sync::store_device_exclusion::resume_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        crate::sync::circle_ops::resume_circle_operations(
            self.db(),
            &**self.storage(),
            None,
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(super) async fn abandon_candidate(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        write_id: crate::WriteId,
    ) -> Result<
        abandonment::MergeCandidateAbandonment,
        crate::sync::store_outbound::StoreOutboundError,
    > {
        abandonment::abandon_merge_candidate(
            self.db(),
            &**self.storage(),
            device_id,
            identity,
            write_id,
        )
        .await
    }

    pub(super) async fn authorize(
        &self,
    ) -> Result<AuthorizedMergeStoreEngine<'_>, SyncCycleFailure> {
        authorize_merge(self.access()).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn invite_member(
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
    pub(super) async fn remove_member(
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

    pub(super) async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<crate::sync::circle::CircleId, crate::sync::circle_ops::CircleOperationError> {
        crate::sync::circle_ops::create_circle(
            self.db(),
            &**self.storage(),
            None,
            device_id,
            timestamp,
            name,
            identity,
        )
        .await
    }

    pub(super) async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: crate::sync::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), crate::sync::circle_ops::CircleOperationError> {
        crate::sync::circle_ops::rename_circle(
            self.db(),
            &**self.storage(),
            None,
            device_id,
            timestamp,
            circle_id,
            name,
            identity,
        )
        .await
    }

    pub(super) async fn propose_device_exclusion(
        &self,
        identity: &UserKeypair,
        target: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::sync::store_device_exclusion::StoreDeviceExclusionResult,
        crate::sync::store_device_exclusion::StoreDeviceExclusionError,
    > {
        crate::sync::store_device_exclusion::propose_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            target,
        )
        .await
    }

    pub(super) async fn cancel_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &crate::sync::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        crate::sync::store_device_exclusion::StoreDeviceExclusionResult,
        crate::sync::store_device_exclusion::StoreDeviceExclusionError,
    > {
        crate::sync::store_device_exclusion::cancel_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            proposal,
        )
        .await
    }

    pub(super) async fn finalize_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &crate::sync::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        crate::sync::store_device_exclusion::StoreDeviceExclusionResult,
        crate::sync::store_device_exclusion::StoreDeviceExclusionError,
    > {
        crate::sync::store_device_exclusion::finalize_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            proposal,
        )
        .await
    }
}

impl AuthorizedMergeStoreEngine<'_> {
    pub(super) fn db(&self) -> &Database {
        self.access.db
    }

    pub(super) fn storage(&self) -> &dyn SyncStorage {
        self.access.storage
    }

    pub(super) fn store_root(&self) -> &StoreRootRef {
        &self.access.store_root
    }

    pub(super) fn wrapped_keys(
        &self,
        recipient: &str,
    ) -> Result<Vec<crate::sync::wrapped_store_key::WrappedStoreKeyRef>, SyncCycleFailure> {
        self.membership
            .wrapped_key_authority_for(recipient)
            .map_err(|error| error.to_string().into())
    }

    pub(super) async fn gc_tombstones(
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
            None,
            clock,
            grace,
        )
        .await
    }

    pub(super) async fn drain_store_writes(
        &self,
    ) -> Result<u64, crate::sync::store_outbound::StoreOutboundError> {
        publication::drain_store_writes(self.db(), self.storage()).await
    }

    pub(super) async fn prepare_pending_store_write(
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

    pub(super) async fn snapshot_position(
        &self,
        snapshot: &crate::database::PublishedStoreSnapshot,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<u64, SyncCycleFailure> {
        let (root, registration, _, _) =
            crate::sync::store_outbound::load_local_store_authority(self.db(), device_id, identity)
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
        super::snapshot_position_for_stream(
            snapshot,
            crate::WritePolicy::MergeConcurrent,
            &stream_id,
        )
    }

    pub(super) async fn push_snapshot(
        &self,
        snapshot: crate::sync::snapshot::CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        identity: &UserKeypair,
        created_at: String,
    ) -> Result<crate::sync::store_commit::SnapshotMeta, SyncCycleFailure> {
        crate::sync::store_snapshot::push_store_snapshot(
            self.storage(),
            self.store_root().store_root_hash,
            snapshot,
            coverage,
            schema_version,
            identity,
            created_at,
            Some(&self.membership),
            self.db(),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))
    }

    pub(super) fn may_author_snapshot(&self, author_pubkey: &str) -> Result<(), String> {
        crate::sync::membership_ops::authorize_loaded_membership_author(
            Some(&self.membership),
            author_pubkey,
            crate::sync::membership_ops::MembershipAuthorRequirement::Owner,
        )
        .map_err(|error| error.to_string())
    }

    pub(super) async fn reclaim_packages(
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
            None,
            device_id,
            identity,
            self.store_root().store_root_hash,
            crate::sync::store_reclaim::ReclaimMembership::MergeConcurrent {
                membership: &self.membership,
                discovery_proof: self.discovery_proof,
            },
        )
        .await
    }
}

async fn authorize_merge(
    access: MergeStoreAccess<'_>,
) -> Result<AuthorizedMergeStoreEngine<'_>, SyncCycleFailure> {
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
            return Err("MergeConcurrent cycle has no pinned membership founder"
                .to_string()
                .into());
        }
        (Some(owner), None) => {
            return Err(
                format!("owner {owner} is pinned but the cycle has no membership chain").into(),
            );
        }
    };
    Ok(AuthorizedMergeStoreEngine {
        access,
        membership,
        discovery_proof,
    })
}

#[cfg(test)]
pub(super) async fn authorize_borrowed<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
) -> Result<AuthorizedMergeStoreEngine<'a>, SyncCycleFailure> {
    authorize_merge(MergeStoreAccess {
        db,
        storage,
        store_root,
    })
    .await
}
