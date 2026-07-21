use super::*;

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

    pub(super) async fn authorize(
        &self,
    ) -> Result<AuthorizedSerialStoreEngine<'_>, SyncCycleFailure> {
        authorize_serial(self.access()).await
    }

    async fn membership_context(
        &self,
    ) -> Result<
        crate::sync::membership_ops::SerialMembershipContext<'_>,
        crate::sync::membership_ops::MembershipOpsError,
    > {
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| {
                crate::sync::membership_ops::MembershipOpsError::Database(error.to_string())
            })?
            .ok_or(
                crate::sync::store_outbound::StoreOutboundError::MissingState {
                    key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                },
            )?;
        Ok(crate::sync::membership_ops::SerialMembershipContext {
            coordination: self.coordination(),
            device_id,
        })
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
        let context = self.membership_context().await?;
        crate::sync::membership_ops::invite_member_with_coordination(
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
            Some(context),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        store_id: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &crate::sync::cloud_storage::CloudCipherState,
        pending_rotation: &crate::sync::cloud_storage::PendingRotation,
    ) -> Result<String, crate::sync::membership_ops::MembershipOpsError> {
        let context = self.membership_context().await?;
        crate::sync::membership_ops::remove_member_with_coordination(
            &**self.storage(),
            self.storage().cloud_home(),
            identity,
            hlc,
            public_key_hex,
            store_id,
            encryption,
            custody,
            cipher,
            pending_rotation,
            self.db(),
            Some(context),
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
        crate::sync::store_outbound::drain_serial_store_writes(
            self.db(),
            self.storage(),
            self.coordination(),
        )
        .await
    }

    pub(super) async fn pull(
        &self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        crate::sync::store_pull::pull_serial_store_commits_with_identity(
            self.db(),
            self.db().synced_tables(),
            self.storage(),
            self.coordination(),
            self.store_root().store_root_hash,
            store_dir,
            Some(identity),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))
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
        let authoritative_head = crate::sync::store_pull::load_serial_cycle_authorization(
            self.storage(),
            self.coordination(),
            self.store_root(),
        )
        .await
        .map_err(|error| {
            SyncCycleFailure::operation("reload Serial authorization after publication", error)
        })?
        .head;
        let Some(branch) = self
            .db()
            .unresolved_serial_branch()
            .await
            .map_err(|error| format!("read unresolved Serial branch: {error}"))?
        else {
            return Ok(false);
        };
        let stale = branch.base != authoritative_head;
        if !branch.conflicted && stale {
            let authoritative_predecessor = self
                .db()
                .exact_serial_predecessor(authoritative_head)
                .await
                .map_err(|error| format!("resolve exact Serial head: {error}"))?;
            self.db()
                .mark_serial_branch_conflict(
                    branch.branch_id,
                    branch.base,
                    authoritative_predecessor,
                )
                .await
                .map_err(|error| format!("record Serial branch conflict: {error}"))?;
        }
        Ok(branch.conflicted || stale)
    }

    pub(super) async fn required_membership(
        &self,
    ) -> Result<crate::sync::membership::SerialMembershipState, SyncCycleFailure> {
        self.db()
            .serial_authorization_state()
            .await
            .map_err(|error| format!("read materialized Serial membership: {error}"))?
            .ok_or_else(|| {
                "materialized Serial authorization is absent"
                    .to_string()
                    .into()
            })
            .map(|state| state.membership)
    }

    pub(super) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        crate::sync::store_outbound::prepare_pending_serial_store_write(
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

    pub(super) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(super::stage_and_publish_ack(
            self.db(),
            self.storage(),
            AckAuthority::Serial(self.coordination()),
            identity,
            sync_time,
        ))
        .await
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
    let authorization = crate::sync::store_pull::load_serial_cycle_authorization(
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

#[cfg(test)]
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
