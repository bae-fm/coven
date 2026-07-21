use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::CloudCipherAccess;
use super::cycle::SyncCycleFailure;
use super::pull::{CycleMembership, MembershipDiscoveryProof};
use super::storage::{CoordinationStorage, SyncStorage};
use super::store_commit::{CommitFrontier, StoreRootRef};
use super::store_pull::{SerialCycleAuthorization, StorePullResult};

pub(crate) enum CycleEngine<'a> {
    Merge(MergeCycleEngine<'a>),
    Serial(SerialCycleEngine<'a>),
}

pub(crate) struct MergeCycleEngine<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    store_root: StoreRootRef,
}

pub(crate) struct SerialCycleEngine<'a> {
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root: StoreRootRef,
}

pub(crate) enum AuthorizedCycleEngine<'a> {
    Merge(AuthorizedMergeCycleEngine<'a>),
    Serial(AuthorizedSerialCycleEngine<'a>),
}

pub(crate) struct AuthorizedMergeCycleEngine<'a> {
    engine: MergeCycleEngine<'a>,
    membership: super::membership::MembershipChain,
    discovery_proof: MembershipDiscoveryProof,
}

pub(crate) struct AuthorizedSerialCycleEngine<'a> {
    engine: SerialCycleEngine<'a>,
    authorization: SerialCycleAuthorization,
}

pub(crate) enum PostPullCycleEngine<'cycle, 'engine> {
    Merge(&'cycle AuthorizedMergeCycleEngine<'engine>),
    Serial {
        engine: &'cycle AuthorizedSerialCycleEngine<'engine>,
        membership: super::membership::SerialMembershipState,
    },
}

impl<'a> CycleEngine<'a> {
    pub(crate) async fn load(
        storage: &'a dyn SyncStorage,
        coordination: Option<&'a dyn CoordinationStorage>,
        db: &'a Database,
    ) -> Result<Self, SyncCycleFailure> {
        let local_capability = match db.write_policy() {
            crate::WritePolicy::MergeConcurrent => LocalCycleCapability::Merge,
            crate::WritePolicy::Serial => LocalCycleCapability::Serial(
                coordination
                    .ok_or_else(|| "Serial coordination capability is absent".to_string())?,
            ),
        };
        let store_root = required_store_root(db).await?;
        let verified_root = super::store_objects::load_store_protocol_root(storage, &store_root)
            .await
            .map_err(|error| SyncCycleFailure::operation("load Store protocol root", error))?
            .value;
        let root_policy = verified_root.descriptor.write_policy;
        match (root_policy, local_capability) {
            (crate::WritePolicy::MergeConcurrent, LocalCycleCapability::Merge) => {
                Ok(Self::Merge(MergeCycleEngine {
                    db,
                    storage,
                    store_root,
                }))
            }
            (crate::WritePolicy::Serial, LocalCycleCapability::Serial(coordination)) => {
                Ok(Self::Serial(SerialCycleEngine {
                    db,
                    storage,
                    coordination,
                    store_root,
                }))
            }
            (root_policy, local_capability) => Err(format!(
                "verified Store root write policy {root_policy:?} differs from local database write policy {:?}",
                local_capability.policy()
            )
            .into()),
        }
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

    pub(crate) async fn authorize(self) -> Result<AuthorizedCycleEngine<'a>, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => engine.authorize().await.map(AuthorizedCycleEngine::Merge),
            Self::Serial(engine) => engine.authorize().await.map(AuthorizedCycleEngine::Serial),
        }
    }
}

impl MergeCycleEngine<'_> {
    async fn resume_operations(&self, identity: &UserKeypair) -> Result<(), SyncCycleFailure> {
        super::store_device_exclusion::resume_device_exclusion(
            self.db,
            self.storage,
            None,
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        super::circle_ops::resume_circle_operations(self.db, self.storage, None, identity)
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }
}

impl<'a> MergeCycleEngine<'a> {
    async fn authorize(self) -> Result<AuthorizedMergeCycleEngine<'a>, SyncCycleFailure> {
        let CycleMembership {
            chain,
            pinned_owner,
            listed_entries: _,
            discovery_proof,
        } = super::pull::load_cycle_membership(self.storage, self.db)
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
                return Err(format!(
                    "owner {owner} is pinned but the cycle has no membership chain"
                )
                .into());
            }
        };
        Ok(AuthorizedMergeCycleEngine {
            engine: self,
            membership,
            discovery_proof,
        })
    }
}

impl SerialCycleEngine<'_> {
    async fn resume_operations(&self, identity: &UserKeypair) -> Result<(), SyncCycleFailure> {
        super::store_device_exclusion::resume_device_exclusion(
            self.db,
            self.storage,
            Some(self.coordination),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        super::circle_ops::resume_circle_operations(
            self.db,
            self.storage,
            Some(self.coordination),
            identity,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }
}

impl<'a> SerialCycleEngine<'a> {
    async fn authorize(self) -> Result<AuthorizedSerialCycleEngine<'a>, SyncCycleFailure> {
        let authorization = super::store_pull::load_serial_cycle_authorization(
            self.storage,
            self.coordination,
            &self.store_root,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("load Serial authorization", error))?;
        Ok(AuthorizedSerialCycleEngine {
            engine: self,
            authorization,
        })
    }
}

impl<'engine> AuthorizedCycleEngine<'engine> {
    pub(crate) fn db(&self) -> &Database {
        match self {
            Self::Merge(engine) => engine.engine.db,
            Self::Serial(engine) => engine.engine.db,
        }
    }

    pub(crate) fn storage(&self) -> &dyn SyncStorage {
        match self {
            Self::Merge(engine) => engine.engine.storage,
            Self::Serial(engine) => engine.engine.storage,
        }
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        match self {
            Self::Merge(engine) => &engine.engine.store_root,
            Self::Serial(engine) => &engine.engine.store_root,
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
                    engine.engine.db,
                    cloud_home,
                    engine.engine.storage,
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
                    engine.engine.db,
                    cloud_home,
                    engine.engine.storage,
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
                super::store_outbound::drain_store_writes_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    None,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_outbound::drain_store_writes_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    Some(engine.engine.coordination),
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
                super::store_pull::pull_store_commits_with_identity(
                    engine.engine.db,
                    engine.engine.db.synced_tables(),
                    engine.engine.storage,
                    None,
                    engine.engine.store_root.store_root_hash,
                    store_dir,
                    Some(&engine.membership),
                    Some(identity),
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_pull::pull_store_commits_with_identity(
                    engine.engine.db,
                    engine.engine.db.synced_tables(),
                    engine.engine.storage,
                    Some(engine.engine.coordination),
                    engine.engine.store_root.store_root_hash,
                    store_dir,
                    None,
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
                    engine.engine.db,
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
            engine.engine.storage,
            engine.engine.coordination,
            &engine.engine.store_root,
        )
        .await
        .map_err(|error| {
            SyncCycleFailure::operation("reload Serial authorization after publication", error)
        })?
        .head;
        let Some(branch) = engine
            .engine
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
                .engine
                .db
                .exact_serial_predecessor(authoritative_head)
                .await
                .map_err(|error| format!("resolve exact Serial head: {error}"))?;
            engine
                .engine
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
    ) -> Result<PostPullCycleEngine<'_, 'engine>, SyncCycleFailure> {
        match self {
            Self::Merge(engine) => Ok(PostPullCycleEngine::Merge(engine)),
            Self::Serial(engine) => Ok(PostPullCycleEngine::Serial {
                engine,
                membership: required_serial_membership(engine).await?,
            }),
        }
    }

    pub(crate) async fn ensure_active_registration(
        &self,
        identity: &UserKeypair,
        published_at: &str,
    ) -> Result<(), SyncCycleFailure> {
        match self {
            Self::Merge(engine) => {
                super::store_registration::ensure_active_registration_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    None,
                    identity,
                    Some(&engine.membership),
                    published_at,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_registration::ensure_active_registration_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    Some(engine.engine.coordination),
                    identity,
                    None,
                    published_at,
                )
                .await
            }
        }
        .map_err(|error| SyncCycleFailure::operation("publish Store device registration", error))
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
                super::store_outbound::prepare_pending_store_write_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    None,
                    device_id,
                    timestamp,
                    identity,
                    store_dir,
                    Some(&engine.membership),
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_outbound::prepare_pending_store_write_with_coordination(
                    engine.engine.db,
                    engine.engine.storage,
                    Some(engine.engine.coordination),
                    device_id,
                    timestamp,
                    identity,
                    store_dir,
                    None,
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
                    engine.engine.storage,
                    engine.engine.store_root.store_root_hash,
                    snapshot,
                    coverage,
                    schema_version,
                    identity,
                    created_at,
                    Some(&engine.membership),
                    engine.engine.db,
                )
                .await
            }
            Self::Serial(engine) => {
                super::store_snapshot::push_store_snapshot(
                    engine.engine.storage,
                    engine.engine.store_root.store_root_hash,
                    snapshot,
                    coverage,
                    schema_version,
                    identity,
                    created_at,
                    None,
                    engine.engine.db,
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
                    engine.engine.db,
                    engine.engine.storage,
                    AckAuthority::Merge(&engine.membership),
                    identity,
                    sync_time,
                ))
                .await
            }
            Self::Serial(engine) => {
                Box::pin(stage_and_publish_ack(
                    engine.engine.db,
                    engine.engine.storage,
                    AckAuthority::Serial(engine.engine.coordination),
                    identity,
                    sync_time,
                ))
                .await
            }
        }
    }
}

impl PostPullCycleEngine<'_, '_> {
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
                    engine.engine.db,
                    engine.engine.storage,
                    None,
                    device_id,
                    identity,
                    engine.engine.store_root.store_root_hash,
                    super::store_reclaim::ReclaimMembership::MergeConcurrent {
                        membership: &engine.membership,
                        discovery_proof: engine.discovery_proof,
                    },
                )
                .await
            }
            Self::Serial { engine, membership } => {
                super::store_reclaim::reclaim_store_packages(
                    engine.engine.db,
                    engine.engine.storage,
                    Some(engine.engine.coordination),
                    device_id,
                    identity,
                    engine.engine.store_root.store_root_hash,
                    super::store_reclaim::ReclaimMembership::Serial(membership),
                )
                .await
            }
        }
    }
}

async fn required_store_root(db: &Database) -> Result<StoreRootRef, SyncCycleFailure> {
    db.local_store_root_ref()
        .await
        .map_err(|error| format!("read Store root reference: {error}"))?
        .ok_or_else(|| "Store root reference is absent".to_string().into())
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
    engine: &AuthorizedSerialCycleEngine<'_>,
) -> Result<super::membership::SerialMembershipState, SyncCycleFailure> {
    engine
        .engine
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

enum LocalCycleCapability<'a> {
    Merge,
    Serial(&'a dyn CoordinationStorage),
}

impl LocalCycleCapability<'_> {
    fn policy(&self) -> crate::WritePolicy {
        match self {
            Self::Merge => crate::WritePolicy::MergeConcurrent,
            Self::Serial(_) => crate::WritePolicy::Serial,
        }
    }
}
