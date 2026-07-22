use super::*;

pub(crate) mod abandonment;
mod acknowledgements;
mod database;
pub(crate) mod operations;
pub(crate) mod publication;
pub(crate) mod pull;

use database::SerialDatabase;

pub(super) struct SerialStoreEngine {
    context: StoreEngineContext,
}

#[derive(Clone)]
struct SerialStoreAccess<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root: StoreRootRef,
}

pub(super) struct AuthorizedSerialStoreEngine<'a> {
    access: SerialStoreAccess<'a>,
    authorization: SerialCycleAuthorization,
}

impl SerialStoreEngine {
    pub(super) fn new(context: StoreEngineContext) -> Self {
        Self { context }
    }
    pub(super) fn db(&self) -> &Database {
        self.context.db()
    }

    fn database(&self) -> SerialDatabase<'_> {
        SerialDatabase::new(self.db())
    }

    pub(super) fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    pub(super) fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    pub(super) fn coordination(&self) -> &dyn CoordinationStorage {
        &**self.storage()
    }

    fn access(&self) -> SerialStoreAccess<'_> {
        SerialStoreAccess {
            db: self.db(),
            storage: &**self.storage(),
            coordination: self.coordination(),
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
            Some(self.coordination()),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        crate::sync::circle_ops::resume_circle_operations(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(super) async fn abandon_branch(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
        branch_id: crate::PendingBranchId,
    ) -> Result<abandonment::SerialBranchAbandonment, crate::sync::store_outbound::StoreOutboundError>
    {
        abandonment::abandon_serial_branch(
            self.db(),
            &**self.storage(),
            self.coordination(),
            device_id,
            identity,
            store_dir,
            branch_id,
        )
        .await
    }

    pub(super) async fn prepare_resolution(
        &self,
        store_dir: &StoreDir,
        branch_base: Option<crate::sync::store_commit::StoreBatchCommitRef>,
        identity: &UserKeypair,
    ) -> Result<pull::SerialResolutionPlan, crate::sync::store_pull::StorePullError> {
        pull::prepare_serial_resolution(
            self.db(),
            &**self.storage(),
            self.coordination(),
            self.store_root().store_root_hash,
            store_dir,
            branch_base,
            identity,
        )
        .await
    }

    pub(super) async fn cleanup_resolution_candidates(
        &self,
        branch_id: crate::PendingBranchId,
        plan: &pull::SerialResolutionPlan,
    ) -> Result<(), crate::sync::store_pull::StorePullError> {
        pull::cleanup_serial_candidates(self.db(), &**self.storage(), branch_id, plan).await
    }

    pub(super) async fn authorize(
        &self,
    ) -> Result<AuthorizedSerialStoreEngine<'_>, SyncCycleFailure> {
        authorize_serial(self.access()).await
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
        let device_id = self.database().required_device_id().await?;
        crate::sync::membership_ops::invite_serial_member(
            &**self.storage(),
            self.storage().cloud_home(),
            self.coordination(),
            &device_id,
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
        let device_id = self.database().required_device_id().await?;
        crate::sync::membership_ops::remove_serial_member_and_adopt(
            &**self.storage(),
            self.storage().cloud_home(),
            self.coordination(),
            &device_id,
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
            Some(self.coordination()),
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
            Some(self.coordination()),
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
            Some(self.coordination()),
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
            Some(self.coordination()),
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
            Some(self.coordination()),
            identity,
            proposal,
        )
        .await
    }
}

impl AuthorizedSerialStoreEngine<'_> {
    pub(super) fn db(&self) -> &Database {
        self.access.db
    }

    fn database(&self) -> SerialDatabase<'_> {
        SerialDatabase::new(self.db())
    }

    pub(super) fn storage(&self) -> &dyn SyncStorage {
        self.access.storage
    }

    pub(super) fn coordination(&self) -> &dyn CoordinationStorage {
        self.access.coordination
    }

    pub(super) fn store_root(&self) -> &StoreRootRef {
        &self.access.store_root
    }

    pub(super) fn wrapped_keys(
        &self,
        recipient: &str,
    ) -> Result<Vec<crate::sync::wrapped_store_key::WrappedStoreKeyRef>, SyncCycleFailure> {
        Ok(self
            .authorization
            .authorization
            .active_wrapped_keys_for(recipient))
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
            None,
            Some(&self.authorization.authorization.membership),
            clock,
            grace,
        )
        .await
    }

    pub(super) async fn drain_store_writes(
        &self,
    ) -> Result<u64, crate::sync::store_outbound::StoreOutboundError> {
        publication::drain_store_writes(self.db(), self.storage(), self.coordination()).await
    }

    pub(super) async fn snapshot_position(
        &self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<u64, SyncCycleFailure> {
        super::snapshot_position_for_stream(
            snapshot,
            crate::WritePolicy::Serial,
            crate::sync::store_commit::SERIAL_STREAM_ID,
        )
    }

    pub(super) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        let authoritative_head = pull::load_serial_cycle_authorization(
            self.storage(),
            self.coordination(),
            self.store_root(),
        )
        .await
        .map_err(|error| {
            SyncCycleFailure::operation("reload Serial authorization after publication", error)
        })?
        .head;
        self.database()
            .should_stop_before_pull(authoritative_head)
            .await
            .map_err(|error| format!("inspect Serial branch before pull: {error}").into())
    }

    pub(super) async fn required_membership(
        &self,
    ) -> Result<crate::sync::membership::SerialMembershipState, SyncCycleFailure> {
        self.database()
            .required_membership()
            .await
            .map_err(|error| format!("read materialized Serial membership: {error}").into())
    }

    pub(super) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        publication::prepare_serial_store_branch(
            self.db(),
            self.storage(),
            self.coordination(),
            device_id,
            identity,
            store_dir,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))
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
            None,
            self.db(),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))
    }

    pub(super) fn may_author_snapshot(
        &self,
        membership: &crate::sync::membership::SerialMembershipState,
        author_pubkey: &str,
    ) -> Result<(), String> {
        membership
            .is_owner(author_pubkey)
            .then_some(())
            .ok_or_else(|| "not a current Serial Owner".to_string())
    }

    pub(super) async fn reclaim_packages(
        &self,
        membership: &crate::sync::membership::SerialMembershipState,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<
        crate::sync::store_reclaim::StoreReclaimResult,
        crate::sync::store_reclaim::StoreReclaimError,
    > {
        crate::sync::store_reclaim::reclaim_store_packages(
            self.db(),
            self.storage(),
            Some(self.coordination()),
            device_id,
            identity,
            self.store_root().store_root_hash,
            crate::sync::store_reclaim::ReclaimMembership::Serial(membership),
        )
        .await
    }
}

async fn authorize_serial(
    access: SerialStoreAccess<'_>,
) -> Result<AuthorizedSerialStoreEngine<'_>, SyncCycleFailure> {
    let authorization = pull::load_serial_cycle_authorization(
        access.storage,
        access.coordination,
        &access.store_root,
    )
    .await
    .map_err(|error| SyncCycleFailure::operation("load Serial authorization", error))?;
    Ok(AuthorizedSerialStoreEngine {
        access,
        authorization,
    })
}

#[cfg(any(test, feature = "test-utils"))]
pub(super) async fn authorize_borrowed<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root: StoreRootRef,
) -> Result<AuthorizedSerialStoreEngine<'a>, SyncCycleFailure> {
    authorize_serial(SerialStoreAccess {
        db,
        storage,
        coordination,
        store_root,
    })
    .await
}
