use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudCipherAccess, CloudSyncStorage};
use super::cycle::SyncCycleFailure;
use super::pull::{CycleMembership, MembershipDiscoveryProof};
use super::storage::{CoordinationStorage, SyncStorage};
use super::store_commit::{CommitFrontier, StoreProtocolRoot, StoreRootRef};
use super::store_pull::{SerialCycleAuthorization, StorePullResult};

pub(crate) enum StoreEngine {
    Merge(MergeStoreEngine),
    Serial(SerialStoreEngine),
}

struct StoreEngineContext {
    db: Database,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

pub(crate) struct MergeStoreEngine {
    context: StoreEngineContext,
}

pub(crate) struct SerialStoreEngine {
    context: StoreEngineContext,
}

#[derive(Clone)]
struct MergeStoreAccess<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

#[derive(Clone)]
struct SerialStoreAccess<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root: StoreRootRef,
}

pub(crate) enum AuthorizedStoreEngine<'a> {
    Merge(AuthorizedMergeStoreEngine<'a>),
    Serial(AuthorizedSerialStoreEngine<'a>),
}

pub(crate) struct AuthorizedMergeStoreEngine<'a> {
    access: MergeStoreAccess<'a>,
    membership: super::membership::MembershipChain,
    discovery_proof: MembershipDiscoveryProof,
}

pub(crate) struct AuthorizedSerialStoreEngine<'a> {
    access: SerialStoreAccess<'a>,
    authorization: SerialCycleAuthorization,
}

pub(crate) enum PostPullStoreEngine<'cycle, 'engine> {
    Merge(&'cycle AuthorizedMergeStoreEngine<'engine>),
    Serial {
        engine: &'cycle AuthorizedSerialStoreEngine<'engine>,
        membership: super::membership::SerialMembershipState,
    },
}

impl StoreEngine {
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
        let root_policy = verified_root.descriptor.write_policy;
        if root_policy != db.write_policy() {
            return Err(format!(
                "verified Store root write policy {root_policy:?} differs from local database write policy {:?}",
                db.write_policy()
            ));
        }
        let context = StoreEngineContext {
            db,
            storage,
            store_root,
        };
        match root_policy {
            crate::WritePolicy::MergeConcurrent => Ok(Self::Merge(MergeStoreEngine { context })),
            crate::WritePolicy::Serial => {
                context
                    .storage
                    .serial_coordination()
                    .map_err(|error| format!("Serial coordination capability: {error}"))?;
                Ok(Self::Serial(SerialStoreEngine { context }))
            }
        }
    }

    pub(crate) fn database(&self) -> &Database {
        match self {
            Self::Merge(engine) => engine.db(),
            Self::Serial(engine) => engine.db(),
        }
    }

    pub(crate) fn cloud_storage(&self) -> &Arc<CloudSyncStorage> {
        match self {
            Self::Merge(engine) => engine.storage(),
            Self::Serial(engine) => engine.storage(),
        }
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.cloud_storage().blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.cloud_storage().self_uploader()
    }

    pub(crate) fn cloud_home(&self) -> &dyn CloudHome {
        self.cloud_storage().cloud_home()
    }

    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &super::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        crate::blob::upload::drain_uploads(
            self.database(),
            &**self.cloud_storage(),
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        match self {
            Self::Merge(engine) => {
                engine
                    .invite_member(
                        identity,
                        hlc,
                        public_key_hex,
                        invitee_email,
                        role,
                        encryption,
                        store_id,
                        store_name,
                    )
                    .await
            }
            Self::Serial(engine) => {
                engine
                    .invite_member(
                        identity,
                        hlc,
                        public_key_hex,
                        invitee_email,
                        role,
                        encryption,
                        store_id,
                        store_name,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        store_id: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &super::cloud_storage::CloudCipherState,
        pending_rotation: &super::cloud_storage::PendingRotation,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        match self {
            Self::Merge(engine) => {
                engine
                    .remove_member(
                        identity,
                        hlc,
                        public_key_hex,
                        store_id,
                        encryption,
                        custody,
                        cipher,
                        pending_rotation,
                    )
                    .await
            }
            Self::Serial(engine) => {
                engine
                    .remove_member(
                        identity,
                        hlc,
                        public_key_hex,
                        store_id,
                        encryption,
                        custody,
                        cipher,
                        pending_rotation,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<super::circle::CircleId, super::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::circle_ops::CircleOperationError::BrowsableStorage);
        }
        match self {
            Self::Merge(engine) => {
                engine
                    .create_circle(device_id, timestamp, name, identity)
                    .await
            }
            Self::Serial(engine) => {
                engine
                    .create_circle(device_id, timestamp, name, identity)
                    .await
            }
        }
    }

    pub(crate) async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: super::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), super::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::circle_ops::CircleOperationError::BrowsableStorage);
        }
        match self {
            Self::Merge(engine) => {
                engine
                    .rename_circle(device_id, timestamp, circle_id, name, identity)
                    .await
            }
            Self::Serial(engine) => {
                engine
                    .rename_circle(device_id, timestamp, circle_id, name, identity)
                    .await
            }
        }
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        identity: &UserKeypair,
        target: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match self {
            Self::Merge(engine) => engine.propose_device_exclusion(identity, target).await,
            Self::Serial(engine) => engine.propose_device_exclusion(identity, target).await,
        }
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match self {
            Self::Merge(engine) => engine.cancel_device_exclusion(identity, proposal).await,
            Self::Serial(engine) => engine.cancel_device_exclusion(identity, proposal).await,
        }
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match self {
            Self::Merge(engine) => engine.finalize_device_exclusion(identity, proposal).await,
            Self::Serial(engine) => engine.finalize_device_exclusion(identity, proposal).await,
        }
    }

    pub(crate) async fn device_exclusion_operations(
        &self,
    ) -> Result<
        Vec<super::store_device_exclusion::StoreDeviceExclusionOperationInfo>,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::get_device_exclusion_operations(self.database()).await
    }

    pub(crate) async fn resume_operations(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), SyncCycleFailure> {
        match self {
            Self::Merge(engine) => engine.resume_operations(identity).await,
            Self::Serial(engine) => engine.resume_operations(identity).await,
        }
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStoreEngine<'_>, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => engine.authorize().await.map(AuthorizedStoreEngine::Merge),
            Self::Serial(engine) => engine.authorize().await.map(AuthorizedStoreEngine::Serial),
        }
    }

    #[cfg(test)]
    pub(crate) async fn authorize_borrowed<'a>(
        storage: &'a dyn SyncStorage,
        coordination: Option<&'a dyn CoordinationStorage>,
        db: &'a Database,
    ) -> Result<AuthorizedStoreEngine<'a>, SyncCycleFailure> {
        let serial_coordination = match db.write_policy() {
            crate::WritePolicy::MergeConcurrent => None,
            crate::WritePolicy::Serial => Some(
                coordination
                    .ok_or_else(|| "Serial coordination capability is absent".to_string())?,
            ),
        };
        let store_root = db
            .local_store_root_ref()
            .await
            .map_err(|error| format!("read Store root reference: {error}"))?
            .ok_or_else(|| "Store root reference is absent".to_string())?;
        let verified_root = super::store_objects::load_store_protocol_root(storage, &store_root)
            .await
            .map_err(|error| SyncCycleFailure::operation("load Store protocol root", error))?
            .value;
        let root_policy = verified_root.descriptor.write_policy;
        if root_policy != db.write_policy() {
            return Err(format!(
                "verified Store root write policy {root_policy:?} differs from local database write policy {:?}",
                db.write_policy()
            )
            .into());
        }
        match (root_policy, serial_coordination) {
            (crate::WritePolicy::MergeConcurrent, None) => authorize_merge(MergeStoreAccess {
                db,
                storage,
                store_root,
            })
            .await
            .map(AuthorizedStoreEngine::Merge),
            (crate::WritePolicy::Serial, Some(coordination)) => {
                authorize_serial(SerialStoreAccess {
                    db,
                    storage,
                    coordination,
                    store_root,
                })
                .await
                .map(AuthorizedStoreEngine::Serial)
            }
            _ => Err("Store engine capability does not match its verified policy"
                .to_string()
                .into()),
        }
    }
}

impl StoreEngineContext {
    fn db(&self) -> &Database {
        &self.db
    }

    fn storage(&self) -> &Arc<CloudSyncStorage> {
        &self.storage
    }

    fn store_root(&self) -> &StoreRootRef {
        &self.store_root
    }
}

impl MergeStoreEngine {
    fn db(&self) -> &Database {
        self.context.db()
    }

    fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    fn access(&self) -> MergeStoreAccess<'_> {
        MergeStoreAccess {
            db: self.db(),
            storage: &**self.storage(),
            store_root: self.store_root().clone(),
        }
    }

    async fn resume_operations(&self, identity: &UserKeypair) -> Result<(), SyncCycleFailure> {
        super::store_device_exclusion::resume_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        super::circle_ops::resume_circle_operations(self.db(), &**self.storage(), None, identity)
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    async fn authorize(&self) -> Result<AuthorizedMergeStoreEngine<'_>, SyncCycleFailure> {
        authorize_merge(self.access()).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn invite_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        super::membership_ops::invite_member_with_coordination(
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
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        store_id: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &super::cloud_storage::CloudCipherState,
        pending_rotation: &super::cloud_storage::PendingRotation,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        super::membership_ops::remove_member_with_coordination(
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
            None,
        )
        .await
    }

    async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<super::circle::CircleId, super::circle_ops::CircleOperationError> {
        super::circle_ops::create_circle(
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

    async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: super::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), super::circle_ops::CircleOperationError> {
        super::circle_ops::rename_circle(
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

    async fn propose_device_exclusion(
        &self,
        identity: &UserKeypair,
        target: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::propose_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            target,
        )
        .await
    }

    async fn cancel_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::cancel_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            proposal,
        )
        .await
    }

    async fn finalize_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::finalize_device_exclusion(
            self.db(),
            &**self.storage(),
            None,
            identity,
            proposal,
        )
        .await
    }
}

async fn authorize_merge(
    access: MergeStoreAccess<'_>,
) -> Result<AuthorizedMergeStoreEngine<'_>, SyncCycleFailure> {
    let CycleMembership {
        chain,
        pinned_owner,
        listed_entries: _,
        discovery_proof,
    } = super::pull::load_cycle_membership(access.storage, access.db)
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

impl SerialStoreEngine {
    fn db(&self) -> &Database {
        self.context.db()
    }

    fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.context.storage()
    }

    fn store_root(&self) -> &StoreRootRef {
        self.context.store_root()
    }

    fn coordination(&self) -> &dyn CoordinationStorage {
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

    async fn resume_operations(&self, identity: &UserKeypair) -> Result<(), SyncCycleFailure> {
        super::store_device_exclusion::resume_device_exclusion(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        super::circle_ops::resume_circle_operations(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }
    async fn authorize(&self) -> Result<AuthorizedSerialStoreEngine<'_>, SyncCycleFailure> {
        authorize_serial(self.access()).await
    }

    async fn membership_context(
        &self,
    ) -> Result<
        super::membership_ops::SerialMembershipContext<'_>,
        super::membership_ops::MembershipOpsError,
    > {
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| {
                super::membership_ops::MembershipOpsError::Database(error.to_string())
            })?
            .ok_or(super::store_outbound::StoreOutboundError::MissingState {
                key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            })?;
        Ok(super::membership_ops::SerialMembershipContext {
            coordination: self.coordination(),
            device_id,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn invite_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        let context = self.membership_context().await?;
        super::membership_ops::invite_member_with_coordination(
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
    async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        store_id: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &super::cloud_storage::CloudCipherState,
        pending_rotation: &super::cloud_storage::PendingRotation,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        let context = self.membership_context().await?;
        super::membership_ops::remove_member_with_coordination(
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

    async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<super::circle::CircleId, super::circle_ops::CircleOperationError> {
        super::circle_ops::create_circle(
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

    async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: super::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), super::circle_ops::CircleOperationError> {
        super::circle_ops::rename_circle(
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

    async fn propose_device_exclusion(
        &self,
        identity: &UserKeypair,
        target: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::propose_device_exclusion(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
            target,
        )
        .await
    }

    async fn cancel_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::cancel_device_exclusion(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
            proposal,
        )
        .await
    }

    async fn finalize_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::finalize_device_exclusion(
            self.db(),
            &**self.storage(),
            Some(self.coordination()),
            identity,
            proposal,
        )
        .await
    }
}

async fn authorize_serial(
    access: SerialStoreAccess<'_>,
) -> Result<AuthorizedSerialStoreEngine<'_>, SyncCycleFailure> {
    let authorization = super::store_pull::load_serial_cycle_authorization(
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

impl<'engine> AuthorizedStoreEngine<'engine> {
    pub(crate) fn db(&self) -> &Database {
        match self {
            Self::Merge(engine) => engine.access.db,
            Self::Serial(engine) => engine.access.db,
        }
    }

    pub(crate) fn storage(&self) -> &dyn SyncStorage {
        match self {
            Self::Merge(engine) => engine.access.storage,
            Self::Serial(engine) => engine.access.storage,
        }
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        match self {
            Self::Merge(engine) => &engine.access.store_root,
            Self::Serial(engine) => &engine.access.store_root,
        }
    }

    pub(crate) fn wrapped_keys(
        &self,
        recipient: &str,
    ) -> Result<Vec<super::wrapped_store_key::WrappedStoreKeyRef>, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => engine
                .membership
                .wrapped_key_authority_for(recipient)
                .map_err(|error| error.to_string().into()),
            Self::Serial(engine) => Ok(engine
                .authorization
                .authorization
                .active_wrapped_keys_for(recipient)),
        }
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
        match self {
            Self::Merge(engine) => {
                crate::blob::delete::gc_tombstones(
                    engine.access.db,
                    cloud_home,
                    engine.access.storage,
                    cipher,
                    store_id,
                    self_pubkey,
                    Some(&engine.membership),
                    None,
                    clock,
                    grace,
                )
                .await
            }
            Self::Serial(engine) => {
                crate::blob::delete::gc_tombstones(
                    engine.access.db,
                    cloud_home,
                    engine.access.storage,
                    cipher,
                    store_id,
                    self_pubkey,
                    None,
                    Some(&engine.authorization.authorization.membership),
                    clock,
                    grace,
                )
                .await
            }
        }
    }

    pub(crate) async fn drain_store_writes(
        &self,
    ) -> Result<u64, super::store_outbound::StoreOutboundError> {
        match self {
            Self::Merge(engine) => {
                super::store_outbound::drain_merge_store_writes(
                    engine.access.db,
                    engine.access.storage,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_outbound::drain_serial_store_writes(
                    engine.access.db,
                    engine.access.storage,
                    engine.access.coordination,
                )
                .await
            }
        }
    }

    pub(crate) async fn pull(
        &self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => {
                super::store_pull::pull_merge_store_commits_with_identity(
                    engine.access.db,
                    engine.access.db.synced_tables(),
                    engine.access.storage,
                    engine.access.store_root.store_root_hash,
                    store_dir,
                    &engine.membership,
                    Some(identity),
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_pull::pull_serial_store_commits_with_identity(
                    engine.access.db,
                    engine.access.db.synced_tables(),
                    engine.access.storage,
                    engine.access.coordination,
                    engine.access.store_root.store_root_hash,
                    store_dir,
                    Some(identity),
                )
                .await
            }
        }
        .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))
    }

    pub(crate) async fn snapshot_position(
        &self,
        snapshot: &crate::database::PublishedStoreSnapshot,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<u64, SyncCycleFailure> {
        let local_stream_id = match self {
            Self::Merge(engine) => {
                require_snapshot_policy(snapshot, crate::WritePolicy::MergeConcurrent)?;
                let (root, registration, _, _) = super::store_outbound::load_local_store_authority(
                    engine.access.db,
                    device_id,
                    identity,
                )
                .await
                .map_err(|error| {
                    SyncCycleFailure::from(format!(
                        "load local Store snapshot cadence authority: {error}"
                    ))
                })?;
                super::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &registration,
                    super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                )
                .to_string()
            }
            Self::Serial(_) => {
                require_snapshot_policy(snapshot, crate::WritePolicy::Serial)?;
                super::store_commit::SERIAL_STREAM_ID.to_string()
            }
        };
        Ok(snapshot
            .meta
            .coverage
            .clone()
            .into_refs()
            .remove(&local_stream_id)
            // Missing local-stream coverage is an exact genesis position.
            .map(|reference| reference.coord.sequence())
            .unwrap_or(0))
    }

    pub(crate) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        let Self::Serial(engine) = self else {
            return Ok(false);
        };
        let authoritative_head = super::store_pull::load_serial_cycle_authorization(
            engine.access.storage,
            engine.access.coordination,
            &engine.access.store_root,
        )
        .await
        .map_err(|error| {
            SyncCycleFailure::operation("reload Serial authorization after publication", error)
        })?
        .head;
        let Some(branch) = engine
            .access
            .db
            .unresolved_serial_branch()
            .await
            .map_err(|error| format!("read unresolved Serial branch: {error}"))?
        else {
            return Ok(false);
        };
        let stale = branch.base != authoritative_head;
        if !branch.conflicted && stale {
            let authoritative_predecessor = engine
                .access
                .db
                .exact_serial_predecessor(authoritative_head)
                .await
                .map_err(|error| format!("resolve exact Serial head: {error}"))?;
            engine
                .access
                .db
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

    pub(crate) async fn after_pull(
        &self,
    ) -> Result<PostPullStoreEngine<'_, 'engine>, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => Ok(PostPullStoreEngine::Merge(engine)),
            Self::Serial(engine) => Ok(PostPullStoreEngine::Serial {
                engine,
                membership: required_serial_membership(engine).await?,
            }),
        }
    }

    pub(crate) async fn ensure_active_registration(&self) -> Result<(), SyncCycleFailure> {
        super::store_registration::ensure_active_registration(self.db(), self.storage())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish Store device registration", error)
            })
    }

    pub(crate) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        timestamp: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => {
                super::store_outbound::prepare_pending_merge_store_write(
                    engine.access.db,
                    engine.access.storage,
                    device_id,
                    timestamp,
                    identity,
                    store_dir,
                    &engine.membership,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_outbound::prepare_pending_serial_store_write(
                    engine.access.db,
                    engine.access.storage,
                    engine.access.coordination,
                    device_id,
                    identity,
                    store_dir,
                )
                .await
            }
        }
        .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))
    }

    pub(crate) async fn push_snapshot(
        &self,
        snapshot: super::snapshot::CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        identity: &UserKeypair,
        created_at: String,
    ) -> Result<super::store_commit::SnapshotMeta, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => {
                super::store_snapshot::push_store_snapshot(
                    engine.access.storage,
                    engine.access.store_root.store_root_hash,
                    snapshot,
                    coverage,
                    schema_version,
                    identity,
                    created_at,
                    Some(&engine.membership),
                    engine.access.db,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_snapshot::push_store_snapshot(
                    engine.access.storage,
                    engine.access.store_root.store_root_hash,
                    snapshot,
                    coverage,
                    schema_version,
                    identity,
                    created_at,
                    None,
                    engine.access.db,
                )
                .await
            }
        }
        .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))
    }

    pub(crate) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        match self {
            Self::Merge(engine) => {
                Box::pin(stage_and_publish_ack(
                    engine.access.db,
                    engine.access.storage,
                    AckAuthority::Merge(&engine.membership),
                    identity,
                    sync_time,
                ))
                .await
            }
            Self::Serial(engine) => {
                Box::pin(stage_and_publish_ack(
                    engine.access.db,
                    engine.access.storage,
                    AckAuthority::Serial(engine.access.coordination),
                    identity,
                    sync_time,
                ))
                .await
            }
        }
    }
}

impl PostPullStoreEngine<'_, '_> {
    pub(crate) fn may_author_snapshot(&self, author_pubkey: &str) -> Result<(), String> {
        match self {
            Self::Merge(engine) => super::membership_ops::authorize_loaded_membership_author(
                Some(&engine.membership),
                author_pubkey,
                super::membership_ops::MembershipAuthorRequirement::Owner,
            )
            .map_err(|error| error.to_string()),
            Self::Serial { membership, .. } => membership
                .is_owner(author_pubkey)
                .then_some(())
                .ok_or_else(|| "not a current Serial Owner".to_string()),
        }
    }

    pub(crate) async fn reclaim_packages(
        &self,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<super::store_reclaim::StoreReclaimResult, super::store_reclaim::StoreReclaimError>
    {
        match self {
            Self::Merge(engine) => {
                super::store_reclaim::reclaim_store_packages(
                    engine.access.db,
                    engine.access.storage,
                    None,
                    device_id,
                    identity,
                    engine.access.store_root.store_root_hash,
                    super::store_reclaim::ReclaimMembership::MergeConcurrent {
                        membership: &engine.membership,
                        discovery_proof: engine.discovery_proof,
                    },
                )
                .await
            }
            Self::Serial { engine, membership } => {
                super::store_reclaim::reclaim_store_packages(
                    engine.access.db,
                    engine.access.storage,
                    Some(engine.access.coordination),
                    device_id,
                    identity,
                    engine.access.store_root.store_root_hash,
                    super::store_reclaim::ReclaimMembership::Serial(membership),
                )
                .await
            }
        }
    }
}

fn require_snapshot_policy(
    snapshot: &crate::database::PublishedStoreSnapshot,
    policy: crate::WritePolicy,
) -> Result<(), SyncCycleFailure> {
    if snapshot.meta.coverage.policy() == policy {
        Ok(())
    } else {
        Err(
            "latest local Store snapshot coverage has the wrong write policy"
                .to_string()
                .into(),
        )
    }
}

async fn required_serial_membership(
    engine: &AuthorizedSerialStoreEngine<'_>,
) -> Result<super::membership::SerialMembershipState, SyncCycleFailure> {
    engine
        .access
        .db
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

async fn stage_and_publish_ack(
    db: &Database,
    storage: &dyn SyncStorage,
    authority: AckAuthority<'_>,
    identity: &UserKeypair,
    sync_time: &str,
) -> Result<(), SyncCycleFailure> {
    let (coordination, membership, policy) = match authority {
        AckAuthority::Merge(membership) => {
            (None, Some(membership), crate::WritePolicy::MergeConcurrent)
        }
        AckAuthority::Serial(coordination) => {
            (Some(coordination), None, crate::WritePolicy::Serial)
        }
    };
    super::store_ack::drain_outbound_store_acks(db, storage, coordination, identity, membership)
        .await
        .map_err(|error| {
            SyncCycleFailure::operation("publish queued Store acknowledgement", error)
        })?;
    let frontier = db
        .materialized_frontier()
        .await
        .map_err(|error| format!("read Store acknowledgement frontier: {error}"))?;
    let frontier = CommitFrontier::from_refs(policy, frontier)
        .map_err(|error| format!("shape Store acknowledgement frontier: {error}"))?;
    super::store_ack::stage_store_ack(
        db,
        storage,
        coordination,
        frontier,
        sync_time.to_owned(),
        identity,
    )
    .await
    .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
    super::store_ack::drain_outbound_store_acks(db, storage, coordination, identity, membership)
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
    Ok(())
}

enum AckAuthority<'a> {
    Merge(&'a super::membership::MembershipChain),
    Serial(&'a dyn CoordinationStorage),
}
