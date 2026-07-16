//! Durable creation and activation of circles through the Store commit stream.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::circle::{
    circle_semantic_prefix, CircleCreation, CircleCreationPolicyObjects, CircleId,
    CircleMetadataHeadRef, CircleOperationState, CircleRosterHeadRef, CircleSemanticSlot,
    StoreMembershipStateRef,
};
use super::membership::SerialAuthorizationState;
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, CommitPosition, ObjectHash, StoreBatchCommit,
    StoreCommitOrder, StoreDeviceHead, StoreSerialHead,
};
use super::store_objects::append_and_verify;
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

pub(crate) use super::circle_activation::{
    load_circle_activations, load_exact_slot_bytes, verify_control_context,
    verify_local_circle_activation, verify_preceding_merge_registration, VerifiedCircleReference,
};
#[cfg(test)]
pub(crate) use super::circle_activation::{VerifiedCircleAccess, VerifiedCircleActive};

#[cfg(test)]
use super::circle::{CircleAccessDisposition, CircleRole};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationPolicy {
    MergeConcurrent {
        head: StoreDeviceHead,
    },
    Serial {
        head: StoreSerialHead,
        base: Option<CommitPosition>,
        base_head_bytes: Option<Vec<u8>>,
        authorization: SerialAuthorizationState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationJournal {
    pub operation_id: String,
    pub status: CircleOperationState,
    pub creation: CircleCreation,
    pub commit_bytes: Vec<u8>,
    pub policy: CircleOperationPolicy,
    pub uploaded: BTreeSet<String>,
}

impl CircleOperationJournal {
    pub(crate) fn circle_id(&self) -> CircleId {
        self.creation.circle_id
    }

    pub(crate) fn commit(&self) -> Result<StoreBatchCommit, CircleOperationError> {
        serde_json::from_slice(&self.commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("parse Store commit: {error}")))
    }
}

fn signed_circle_commit(
    store_root_hash: ObjectHash,
    operation_id: crate::WriteId,
    device_id: &str,
    order: StoreCommitOrder,
    membership_authority: Option<super::membership::MembershipGrantCreationAuthority>,
    creation: &CircleCreation,
    signer: &UserKeypair,
) -> Result<StoreBatchCommit, CircleOperationError> {
    StoreBatchCommit::signed_batch(
        store_root_hash,
        operation_id,
        device_id.to_string(),
        order,
        membership_authority,
        None,
        Vec::new(),
        vec![creation.control_ref()],
        None,
        &[],
        signer,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum CircleOperationError {
    #[error("database: {0}")]
    Database(String),
    #[error("circle protocol state is absent: {0}")]
    MissingState(&'static str),
    #[error("circle protocol state is invalid: {0}")]
    InvalidState(String),
    #[error("circle construction: {0}")]
    Construction(#[from] super::circle::CircleCreateError),
    #[error("circle object: {0}")]
    Object(#[from] super::store_objects::StoreObjectError),
    #[error("Store publication: {0}")]
    StoreOutbound(#[from] super::store_outbound::StoreOutboundError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] super::store_registration::StoreRegistrationError),
    #[error("circles require opaque cloud storage")]
    BrowsableStorage,
    #[error("circle operation journal: {0}")]
    Journal(String),
    #[error("circle operation {circle_id} is blocked: {reason}")]
    Blocked { circle_id: CircleId, reason: String },
    #[error("circle command channel is closed")]
    CommandChannelClosed,
    #[error("circle command ended without a reply")]
    ReplyChannelClosed,
}

impl From<crate::database::DbError> for CircleOperationError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

pub(crate) async fn create_circle(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleId, CircleOperationError> {
    super::store_registration::ensure_active_registration_with_coordination(
        db,
        storage,
        coordination,
        signer,
        None,
        metadata_stamp,
    )
    .await?;
    let journal = prepare_circle_operation(
        db,
        storage,
        coordination,
        device_id,
        metadata_stamp,
        name,
        signer,
    )
    .await?;
    let circle_id = journal.circle_id();
    db.insert_circle_operation(journal).await?;
    publish_circle_operation(db, storage, coordination, circle_id).await?;
    Ok(circle_id)
}

pub(crate) async fn resume_circle_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
) -> Result<(), CircleOperationError> {
    while let Some(journal) = db.oldest_pending_circle_operation().await? {
        if !matches!(journal.status, CircleOperationState::Pending) {
            return Err(CircleOperationError::Journal(format!(
                "pending circle operation {} contains a blocked payload",
                journal.circle_id()
            )));
        }
        match publish_circle_operation(db, storage, coordination, journal.circle_id()).await {
            Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn prepare_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let store_root_hash = required_store_root_hash(db).await?;
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let author_pubkey = keys::public_key_hex(signer);
    let operation_id = db.new_write_id();
    let (creation, commit, policy) = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let entries = super::membership_ops::list_membership_entries(storage, store_root_hash)
                .await
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let current = super::membership_ops::load_anchored_chain(
                storage,
                store_root_hash,
                &entries,
                Some(&founder),
                Some(db),
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let heads = current.head_refs().to_vec();
            let resolutions = current.resolution_refs().to_vec();
            let exact = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                store_root_hash,
                &founder,
                &heads,
                &resolutions,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let members = exact.current_members();
            let state_hash = match exact.status() {
                super::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
                super::membership::MembershipStatus::Conflict(_) => {
                    return Err(CircleOperationError::InvalidState(
                        "circle creation requires resolved Store membership".to_string(),
                    ));
                }
            };
            let membership_authority =
                exact.write_grant_authority(&author_pubkey).ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "circle creator is not a current Store writer".to_string(),
                    )
                })?;
            let creation = CircleCreation::founder(
                store_root_hash,
                device_id,
                name,
                metadata_stamp,
                StoreMembershipStateRef::merge_concurrent(heads, resolutions, state_hash),
                Some(membership_authority.clone()),
                members,
                db.id_provider(),
                signer,
            )?;
            let base = db.latest_local_store_position().await?;
            let seq = base.as_ref().map_or(1, |position| position.seq + 1);
            let mut dependencies = db.materialized_frontier().await?;
            dependencies.remove(device_id);
            let commit = signed_circle_commit(
                store_root_hash,
                operation_id.clone(),
                device_id,
                StoreCommitOrder::MergeConcurrent {
                    seq,
                    previous_commit_hash: base.map(|position| position.commit_hash),
                    dependencies,
                },
                Some(membership_authority),
                &creation,
                signer,
            )?;
            let head = StoreDeviceHead::signed(
                store_root_hash,
                device_id.to_string(),
                Some(commit.position()),
                metadata_stamp.to_string(),
                signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            (
                creation,
                commit,
                CircleOperationPolicy::MergeConcurrent { head },
            )
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle creation requires coordination storage".to_string(),
                )
            })?;
            let snapshot = super::store_outbound::current_serial_authorization_snapshot(
                db,
                storage,
                coordination,
            )
            .await?;
            if !snapshot.authorization.membership.can_write(&author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "circle creator is not a current Store writer".to_string(),
                ));
            }
            let base = snapshot.base;
            let members = snapshot.authorization.membership.current_members();
            let creation = CircleCreation::founder(
                store_root_hash,
                device_id,
                name,
                metadata_stamp,
                StoreMembershipStateRef::serial(base.clone(), &snapshot.authorization.membership),
                None,
                members,
                db.id_provider(),
                signer,
            )?;
            let commit = signed_circle_commit(
                store_root_hash,
                operation_id.clone(),
                device_id,
                StoreCommitOrder::Serial {
                    seq: base.as_ref().map_or(1, |position| position.seq + 1),
                    previous_commit_hash: base.as_ref().map(|position| position.commit_hash),
                },
                None,
                &creation,
                signer,
            )?;
            let head = StoreSerialHead::signed(
                store_root_hash,
                Some(commit.position()),
                Some(commit.write_id.clone()),
                signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            (
                creation,
                commit,
                CircleOperationPolicy::Serial {
                    head,
                    base,
                    base_head_bytes: snapshot.base_head_bytes,
                    authorization: snapshot.authorization,
                },
            )
        }
    };
    Ok(CircleOperationJournal {
        operation_id: operation_id.as_str().to_string(),
        status: CircleOperationState::Pending,
        creation,
        commit_bytes: commit.to_bytes(),
        policy,
        uploaded: BTreeSet::new(),
    })
}

async fn publish_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    circle_id: CircleId,
) -> Result<(), CircleOperationError> {
    let mut journal = db
        .circle_operation(circle_id)
        .await?
        .ok_or_else(|| CircleOperationError::Journal(format!("circle {circle_id} is absent")))?;
    if let CircleOperationState::Blocked { reason } = &journal.status {
        return Err(CircleOperationError::Blocked {
            circle_id,
            reason: reason.clone(),
        });
    }
    let creation = journal.creation.clone();
    let store_root_hash = creation.control.value.store_root_hash;
    let circle_encryption = EncryptionService::from(
        MasterKeyring::from_serialized(&creation.keyring)
            .map_err(|error| CircleOperationError::Journal(format!("circle keyring: {error}")))?,
    );
    let commit = journal.commit()?;
    verify_control_context(&creation.control_ref(), &creation.control, &commit)?;
    if creation.access.iter().any(|access| {
        !access
            .leaf
            .verify_envelope(&creation.control, &access.envelope)
    }) {
        return Err(CircleOperationError::InvalidState(
            "prepared Circle access bytes, plaintext hash, ciphertext hash, or envelope differ"
                .to_string(),
        ));
    }
    verify_preceding_merge_registration(storage, &commit).await?;
    if commit.policy() == crate::WritePolicy::MergeConcurrent
        && !has_current_merge_authority(db, storage, &commit).await?
    {
        let reason = "circle operation author is not a current Store writer under its exact grant"
            .to_string();
        db.block_circle_operation(circle_id, reason.clone()).await?;
        return Err(CircleOperationError::Blocked { circle_id, reason });
    }

    let metadata_encryption = circle_encryption
        .service_for_fingerprint(creation.metadata.key_fingerprint.as_bytes())
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "Circle metadata key fingerprint is absent from its keyring: {error}"
            ))
        })?;
    append_step(
        db,
        storage,
        &mut journal,
        "metadata",
        &ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            metadata_encryption,
        ),
        &circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
            circle_id: creation.circle_id,
            coord: &creation.metadata.coord(),
        }),
        ".json",
        &serde_json::to_vec(&creation.metadata).expect("circle metadata serialization cannot fail"),
    )
    .await?;
    if let CircleCreationPolicyObjects::MergeConcurrent {
        roster_entry,
        roster_head,
        metadata_head,
        ..
    } = &creation.policy_objects
    {
        let roster_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            circle_encryption.clone(),
        );
        append_step(
            db,
            storage,
            &mut journal,
            "metadata-head",
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                circle_id: creation.circle_id,
                head: &CircleMetadataHeadRef::from_head(metadata_head),
            }),
            ".json",
            &serde_json::to_vec(metadata_head)
                .expect("circle metadata head serialization cannot fail"),
        )
        .await?;
        append_step(
            db,
            storage,
            &mut journal,
            "roster-entry",
            &roster_context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id: creation.circle_id,
                coord: &roster_entry.coord(),
            }),
            ".json",
            &serde_json::to_vec(roster_entry)
                .expect("circle roster entry serialization cannot fail"),
        )
        .await?;
        append_step(
            db,
            storage,
            &mut journal,
            "roster-head",
            &roster_context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id: creation.circle_id,
                head: &CircleRosterHeadRef::from_head(roster_head),
            }),
            ".json",
            &serde_json::to_vec(roster_head).expect("circle roster head serialization cannot fail"),
        )
        .await?;
    }
    for (index, access) in creation.access.iter().enumerate() {
        append_step(
            db,
            storage,
            &mut journal,
            &format!("access-leaf-{index}"),
            &ProtocolObjectContext::recipient_sealed(store_root_hash),
            &circle_semantic_prefix(CircleSemanticSlot::AccessLeaf {
                circle_id: access.leaf.value.circle_id,
                owner_pubkey: &access.leaf.value.owner_pubkey,
                epoch_id: access.leaf.value.epoch_id,
                recipient_slot: &access.leaf.value.recipient_slot,
                leaf_id: access.leaf.value.leaf_id,
            }),
            "",
            &access.leaf.bytes,
        )
        .await?;
    }
    append_step(
        db,
        storage,
        &mut journal,
        "control",
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::CircleControl),
        &circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: creation.circle_id,
            control: &creation.control.coord,
        }),
        ".json",
        &creation.control.bytes,
    )
    .await?;
    if let CircleCreationPolicyObjects::MergeConcurrent { control_head, .. } =
        &creation.policy_objects
    {
        append_step(
            db,
            storage,
            &mut journal,
            "control-head",
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::CircleControl),
            &circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: creation.circle_id,
                control: &control_head.control,
                head_hash: control_head.head_hash(),
            }),
            ".json",
            &serde_json::to_vec(control_head)
                .expect("circle control head serialization cannot fail"),
        )
        .await?;
    }
    for (index, access) in creation.access.iter().enumerate() {
        append_step(
            db,
            storage,
            &mut journal,
            &format!("access-envelope-{index}"),
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &circle_semantic_prefix(CircleSemanticSlot::AccessEnvelope {
                circle_id: access.envelope.circle_id,
                owner_pubkey: &access.envelope.owner_pubkey,
                recipient_slot: &access.envelope.recipient_slot,
                control_hash: access.envelope.control_hash,
            }),
            ".json",
            &serde_json::to_vec(&access.envelope)
                .expect("access envelope serialization cannot fail"),
        )
        .await?;
    }
    let policy = journal.policy.clone();
    match policy {
        CircleOperationPolicy::MergeConcurrent { head } => {
            let commit_bytes = journal.commit_bytes.clone();
            append_step(
                db,
                storage,
                &mut journal,
                "store-commit",
                &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreCommit),
                &commit_semantic_prefix(&commit.device_id, commit.seq(), commit.commit_hash()),
                ".json",
                &commit_bytes,
            )
            .await?;
            append_step(
                db,
                storage,
                &mut journal,
                "store-head",
                &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreHead),
                &head_semantic_prefix(&commit.device_id, commit.seq(), head.head_hash()),
                ".json",
                &head.to_bytes(),
            )
            .await?;
        }
        CircleOperationPolicy::Serial {
            head,
            base,
            base_head_bytes,
            ..
        } => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle activation requires coordination storage".to_string(),
                )
            })?;
            if let Err(error) = super::store_outbound::activate_serial_commit_head(
                db,
                storage,
                coordination,
                base,
                base_head_bytes.as_deref(),
                &commit,
                &head,
            )
            .await
            {
                if matches!(
                    error,
                    super::store_outbound::StoreOutboundError::SerialControlConflict { .. }
                ) {
                    let reason = error.to_string();
                    db.block_circle_operation(circle_id, reason.clone()).await?;
                    return Err(CircleOperationError::Blocked { circle_id, reason });
                }
                return Err(error.into());
            }
        }
    }
    db.activate_circle_operation(journal).await?;
    Ok(())
}

async fn has_current_merge_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
) -> Result<bool, CircleOperationError> {
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let entries = super::membership_ops::list_membership_entries(storage, commit.store_root_hash)
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let current = super::membership_ops::load_anchored_chain(
        storage,
        commit.store_root_hash,
        &entries,
        Some(&founder),
        Some(db),
    )
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    Ok(commit
        .membership_authority
        .as_ref()
        .is_some_and(|authority| {
            current.authorizes_write_authority(authority, &commit.author_pubkey)
        }))
}

#[allow(clippy::too_many_arguments)]
async fn append_step(
    db: &Database,
    storage: &dyn SyncStorage,
    journal: &mut CircleOperationJournal,
    step: &str,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<(), CircleOperationError> {
    if journal.uploaded.contains(step) {
        let persisted = load_exact_slot_bytes(storage, context, semantic_prefix).await?;
        if persisted != bytes {
            return Err(CircleOperationError::InvalidState(format!(
                "circle upload step {step:?} differs from its durable journal bytes"
            )));
        }
        return Ok(());
    }
    append_and_verify(storage, context, semantic_prefix, extension, bytes).await?;
    journal.uploaded.insert(step.to_string());
    db.update_circle_operation(journal.clone()).await?;
    Ok(())
}

async fn required_store_root_hash(db: &Database) -> Result<ObjectHash, CircleOperationError> {
    db.get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState(
            "Store protocol root hash",
        ))?
        .parse()
        .map_err(|error| CircleOperationError::InvalidState(format!("Store root hash: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::database::DbError;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::{founder_entry, AuthorStreamId, MemberRole, MembershipChain};
    use crate::sync::storage::{
        CoordinationError, CoordinationStorage, CreateHeadError, ProtocolObjectContext,
        ProtocolObjectDomain, ReplaceHeadError, VersionToken, VersionedObject,
    };
    use crate::sync::store_commit::{
        serial_head_key, StoreBatchCommit, StoreCommitOrder, StoreControl,
    };
    use crate::sync::test_helpers::{
        open_serial_test_db, open_test_db, publish_test_serial_store_protocol_root,
        publish_test_store_protocol_root, temp_store_dir, test_migrations, test_synced_tables,
    };

    fn merge_storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        name: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            name,
            signer.clone(),
            Arc::new(SequentialCopyIdGenerator::new(name)),
        )
        .expect("test cloud storage supports immutable copies")
    }

    fn serial_storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        name: &str,
    ) -> CloudSyncStorage {
        merge_storage(home, signer, name).with_test_serial_coordination(Arc::new(home.clone()))
    }

    struct HeadChangesAfterAuthorization<'a> {
        inner: &'a dyn CoordinationStorage,
        authorization_head: Vec<u8>,
        reads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CoordinationStorage for HeadChangesAfterAuthorization<'_> {
        async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError> {
            let mut current = self.inner.read_head(key).await?;
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                current.bytes.clone_from(&self.authorization_head);
            }
            Ok(current)
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<VersionedObject, CreateHeadError> {
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &VersionToken,
            bytes: &[u8],
        ) -> Result<VersionedObject, ReplaceHeadError> {
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_probe_head(&self, key: &str) -> Result<(), CoordinationError> {
            self.inner.delete_probe_head(key).await
        }
    }

    async fn persist_merge_operation(
        db: &Database,
        name: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        UserKeypair,
        CircleOperationJournal,
    ) {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = merge_storage(&home, &signer, name);
        let root = publish_test_store_protocol_root(db, &storage, name, "creator", &signer).await;
        let founder = founder_entry(name, &signer, "0000000000001-0000-test-store-protocol-root");
        let chain = MembershipChain::from_entries(vec![founder.clone()])
            .expect("build founder membership chain");
        super::super::store_objects::append_membership_entry_object(
            &storage,
            root,
            &founder.coord(),
            &founder,
        )
        .await
        .expect("publish founder membership entry");
        super::super::store_objects::append_membership_head_object(
            &storage,
            root,
            &chain
                .signed_head(&signer)
                .expect("sign founder membership head"),
        )
        .await
        .expect("publish founder membership head");
        db.set_protocol_state(
            super::super::membership_ops::OWNER_PUBKEY_STATE_KEY,
            &keys::public_key_hex(&signer),
        )
        .await
        .expect("pin Store founder");
        super::super::store_registration::ensure_active_registration_with_coordination(
            db,
            &storage,
            None,
            &signer,
            Some(&chain),
            "0000000000999-0000-creator",
        )
        .await
        .expect("publish Circle creator registration");
        let journal = prepare_circle_operation(
            db,
            &storage,
            None,
            "creator",
            "0000000001000-0000-creator",
            "Household",
            &signer,
        )
        .await
        .expect("prepare circle operation");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist circle operation");
        (home, storage, signer, journal)
    }

    async fn activation_count(db: &Database, circle_id: CircleId) -> i64 {
        let circle_id = circle_id.to_string();
        db.call(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("count circle activations")
    }

    fn assert_exact_operation(expected: &CircleOperationJournal, actual: &CircleOperationJournal) {
        assert_eq!(actual.operation_id, expected.operation_id);
        assert_eq!(actual.creation, expected.creation);
        assert_eq!(actual.commit_bytes, expected.commit_bytes);
        assert_eq!(actual.policy, expected.policy);
    }

    #[tokio::test]
    async fn merge_publication_replays_exact_bytes_across_every_append_failure() {
        for after_visible_write in [false, true] {
            for call in 1..=7 {
                let db = open_test_db();
                let name = format!(
                    "circle-replay-{}-{call}",
                    if after_visible_write {
                        "after"
                    } else {
                        "before"
                    }
                );
                let (home, storage, signer, expected) = persist_merge_operation(&db, &name).await;
                assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
                assert!(db
                    .get_circles(&keys::public_key_hex(&signer))
                    .await
                    .expect("read active circles")
                    .is_empty());
                assert_eq!(
                    db.get_circle_operations()
                        .await
                        .expect("read pending circle operations"),
                    vec![crate::sync::circle::CircleOperationInfo {
                        circle_id: expected.circle_id(),
                        name: "Household".to_string(),
                        state: crate::sync::circle::CircleOperationState::Pending,
                    }]
                );
                if after_visible_write {
                    home.fail_append_after_call(call);
                } else {
                    home.fail_append_before_call(call);
                }

                let error = resume_circle_operations(&db, &storage, None)
                    .await
                    .expect_err("injected append failure must interrupt activation");
                assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
                let persisted = db
                    .circle_operation(expected.circle_id())
                    .await
                    .expect("read interrupted operation")
                    .expect("interrupted operation remains durable");
                assert_exact_operation(&expected, &persisted);
                assert_eq!(persisted.status, CircleOperationState::Pending);
                assert_eq!(activation_count(&db, expected.circle_id()).await, 0);

                resume_circle_operations(&db, &storage, None)
                    .await
                    .expect("resume exact circle operation");
                assert!(db
                    .circle_operation(expected.circle_id())
                    .await
                    .expect("read completed operation")
                    .is_none());
                assert_eq!(activation_count(&db, expected.circle_id()).await, 1);
                assert_eq!(
                    db.get_circles(&keys::public_key_hex(&signer))
                        .await
                        .expect("read activated circle"),
                    vec![crate::sync::circle::CircleInfo {
                        id: expected.circle_id(),
                        name: "Household".to_string(),
                        role: crate::sync::circle::CircleRole::Owner,
                    }]
                );
                assert!(db
                    .get_circle_operations()
                    .await
                    .expect("read completed circle operations")
                    .is_empty());
            }
        }
    }

    #[tokio::test]
    async fn pending_circle_operation_reopens_with_identical_signed_state() {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join("circle-restart.sqlite3");
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("open circle database");
        let (_home, storage, _signer, expected) =
            persist_merge_operation(&db, "circle-restart").await;
        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("reopen circle database");
        let persisted = reopened
            .circle_operation(expected.circle_id())
            .await
            .expect("read reopened circle operation")
            .expect("circle operation survives restart");
        assert_exact_operation(&expected, &persisted);
        assert_eq!(persisted.status, CircleOperationState::Pending);

        resume_circle_operations(&reopened, &storage, None)
            .await
            .expect("resume reopened circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
    }

    #[tokio::test]
    async fn pending_serial_circle_operation_reopens_with_exact_policy_state() {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join("serial-circle-restart.sqlite3");
        let founder = UserKeypair::generate();
        let home = InMemoryCloudHome::new();
        let storage = serial_storage(&home, &founder, "serial-circle-restart");
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "founder-device".to_string(),
            &test_migrations(),
        )
        .expect("open Serial Circle database");
        publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "serial-circle-restart",
            "founder-device",
            &founder,
        )
        .await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let expected = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            "founder-device",
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare Serial Circle operation");
        assert!(matches!(
            expected.policy,
            CircleOperationPolicy::Serial { .. }
        ));
        db.insert_circle_operation(expected.clone())
            .await
            .expect("persist Serial Circle operation");
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close Serial Circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "founder-device".to_string(),
            &test_migrations(),
        )
        .expect("reopen Serial Circle database");
        let persisted = reopened
            .circle_operation(expected.circle_id())
            .await
            .expect("read reopened Serial Circle operation")
            .expect("Serial Circle operation survives restart");
        assert_exact_operation(&expected, &persisted);

        resume_circle_operations(&reopened, &storage, Some(coordination))
            .await
            .expect("resume reopened Serial Circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
    }

    #[tokio::test]
    async fn persisted_merge_circle_operation_rejects_serial_policy_state() {
        let db = open_test_db();
        let (_home, _storage, _signer, journal) =
            persist_merge_operation(&db, "circle-merge-serial-state").await;
        let mut payload = serde_json::to_value(&journal).expect("serialize Merge journal");
        let policy = payload
            .get_mut("policy")
            .and_then(|policy| policy.get_mut("merge_concurrent"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("Merge policy object");
        policy.insert("base".to_string(), serde_json::Value::Null);
        policy.insert(
            "base_head_bytes".to_string(),
            serde_json::json!([115, 101, 114, 105, 97, 108]),
        );
        let payload = serde_json::to_vec(&payload).expect("serialize invalid journal");
        let circle_id = journal.circle_id();
        let stored_circle_id = circle_id.to_string();
        db.call(move |conn| {
            conn.execute(
                "UPDATE circle_operations SET payload = ?2 WHERE circle_id = ?1",
                rusqlite::params![stored_circle_id, payload],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("install invalid durable payload");

        db.circle_operation(circle_id)
            .await
            .expect_err("Merge operation must reject Serial-only policy state");
    }

    #[tokio::test]
    async fn uploaded_circle_steps_are_read_back_after_restart_before_activation() {
        for corrupt in [false, true] {
            let temp = tempfile::tempdir().expect("create database directory");
            let path = temp.path().join(if corrupt {
                "circle-corrupt-upload.sqlite3"
            } else {
                "circle-missing-upload.sqlite3"
            });
            let (db, _stamper) = Database::open(
                &path,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("open circle database");
            let (home, storage, _signer, expected) =
                persist_merge_operation(&db, if corrupt { "corrupt" } else { "missing" }).await;
            home.fail_append_before_call(2);
            resume_circle_operations(&db, &storage, None)
                .await
                .expect_err("roster append failure interrupts publication");
            let persisted = db
                .circle_operation(expected.circle_id())
                .await
                .expect("read interrupted circle operation")
                .expect("interrupted circle operation remains durable");
            assert!(persisted.uploaded.contains("metadata"));

            let metadata_prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
                circle_id: expected.creation.circle_id,
                coord: &expected.creation.metadata.coord(),
            });
            let listing = storage
                .list_protocol_objects(&format!("{metadata_prefix}/copies/"))
                .await
                .expect("list uploaded metadata");
            assert_eq!(listing.objects.len(), 1);
            if corrupt {
                home.replace_appended_candidate(
                    listing.objects[0].physical(),
                    b"corrupt metadata bytes".to_vec(),
                );
            } else {
                home.remove_appended_candidate(listing.objects[0].physical());
            }
            std::thread::spawn(move || drop(db))
                .join()
                .expect("close circle database");

            let (reopened, _stamper) = Database::open(
                &path,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("reopen circle database");
            resume_circle_operations(&reopened, &storage, None)
                .await
                .expect_err("durable upload marker must not bypass readback");
            assert_eq!(activation_count(&reopened, expected.circle_id()).await, 0);
            assert!(reopened
                .circle_operation(expected.circle_id())
                .await
                .expect("read rejected circle operation")
                .is_some());
        }
    }

    #[tokio::test]
    async fn local_activation_rejects_a_tampered_leaf_disposition() {
        let db = open_test_db();
        let (_home, storage, signer, mut journal) =
            persist_merge_operation(&db, "circle-tampered-local-access").await;
        let author = keys::public_key_hex(&signer);
        let own_access = journal
            .creation
            .access
            .iter_mut()
            .find(|access| access.leaf.value.recipient_pubkey == author)
            .expect("founder access");
        assert!(matches!(
            own_access.leaf.value.disposition,
            CircleAccessDisposition::Active { .. }
        ));
        own_access.leaf.value.disposition = CircleAccessDisposition::Inactive;
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist tampered journal");

        resume_circle_operations(&db, &storage, None)
            .await
            .expect_err("local activation must verify journal access context");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .circle_operation(journal.circle_id())
            .await
            .expect("read rejected operation")
            .is_some());
    }

    #[tokio::test]
    async fn remote_activation_rejects_metadata_with_a_different_historical_roster() {
        let db = open_test_db();
        let (_home, storage, signer, mut journal) =
            persist_merge_operation(&db, "circle-remote-metadata-roster").await;
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let creation = &mut journal.creation;
        let store_root_hash = creation.control.value.store_root_hash;
        let circle_encryption = EncryptionService::from(
            MasterKeyring::from_serialized(&creation.keyring).expect("parse Circle keyring"),
        );
        let super::super::circle::CircleRosterStateRef::MergeConcurrent { state_hash, .. } =
            &mut creation.metadata.author_roster
        else {
            panic!("Merge creation metadata must name a Merge roster")
        };
        *state_hash = ObjectHash::digest(b"different historical roster state");
        creation.metadata.signature =
            keys::sign_hex(&signer, &creation.metadata.canonical_bytes()).1;
        let metadata_head =
            super::super::circle::CircleMetadataHead::signed(&creation.metadata, &signer);
        creation.control.value.metadata =
            super::super::circle::CircleMetadataStateRef::MergeConcurrent {
                heads: vec![super::super::circle::CircleMetadataHeadRef::from_head(
                    &metadata_head,
                )],
                selected: creation.metadata.coord(),
                state_hash: creation.metadata.metadata_hash(),
            };
        creation.control.value.signature =
            keys::sign_hex(&signer, &creation.control.value.canonical_bytes()).1;
        creation.control.coord = creation.control.value.coord();
        creation.control.bytes =
            serde_json::to_vec(&creation.control.value).expect("serialize forged Circle control");
        for access in &mut creation.access {
            access.envelope.control_hash = creation.control.coord.control_hash();
            access.envelope.signature =
                keys::sign_hex(&signer, &access.envelope.canonical_bytes()).1;
        }
        let (roster_entry, roster_head, stored_metadata_head, control_head) = {
            let CircleCreationPolicyObjects::MergeConcurrent {
                roster_entry,
                roster_head,
                metadata_head: stored_metadata_head,
                control_head,
            } = &mut creation.policy_objects
            else {
                panic!("Merge creation must carry Merge policy objects")
            };
            *stored_metadata_head = metadata_head;
            *control_head =
                super::super::circle::CircleControlHead::signed(&creation.control.value, &signer);
            (
                roster_entry.clone(),
                roster_head.clone(),
                stored_metadata_head.clone(),
                control_head.clone(),
            )
        };
        let commit = StoreBatchCommit::signed_batch(
            store_root_hash,
            old_commit.write_id,
            old_commit.device_id,
            old_commit.order,
            old_commit.membership_authority,
            None,
            Vec::new(),
            vec![creation.control_ref()],
            None,
            &[],
            &signer,
        )
        .expect("sign forged metadata activation commit");

        append_and_verify(
            &storage,
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
                circle_id: creation.circle_id,
                coord: &creation.metadata.coord(),
            }),
            ".json",
            &serde_json::to_vec(&creation.metadata).expect("serialize metadata"),
        )
        .await
        .expect("publish metadata");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                circle_id: creation.circle_id,
                head: &super::super::circle::CircleMetadataHeadRef::from_head(
                    &stored_metadata_head,
                ),
            }),
            ".json",
            &serde_json::to_vec(&stored_metadata_head).expect("serialize metadata head"),
        )
        .await
        .expect("publish metadata head");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRoster,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id: creation.circle_id,
                coord: &roster_entry.coord(),
            }),
            ".json",
            &serde_json::to_vec(&roster_entry).expect("serialize roster entry"),
        )
        .await
        .expect("publish roster entry");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRoster,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id: creation.circle_id,
                head: &super::super::circle::CircleRosterHeadRef::from_head(&roster_head),
            }),
            ".json",
            &serde_json::to_vec(&roster_head).expect("serialize roster head"),
        )
        .await
        .expect("publish roster head");
        for access in &creation.access {
            append_and_verify(
                &storage,
                &ProtocolObjectContext::recipient_sealed(store_root_hash),
                &circle_semantic_prefix(CircleSemanticSlot::AccessLeaf {
                    circle_id: creation.circle_id,
                    owner_pubkey: &access.leaf.value.owner_pubkey,
                    epoch_id: access.leaf.value.epoch_id,
                    recipient_slot: &access.leaf.value.recipient_slot,
                    leaf_id: access.leaf.value.leaf_id,
                }),
                "",
                &access.leaf.bytes,
            )
            .await
            .expect("publish access leaf");
        }
        append_and_verify(
            &storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::CircleControl),
            &circle_semantic_prefix(CircleSemanticSlot::Control {
                circle_id: creation.circle_id,
                control: &creation.control.coord,
            }),
            ".json",
            &creation.control.bytes,
        )
        .await
        .expect("publish Circle control");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::CircleControl),
            &circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: creation.circle_id,
                control: &control_head.control,
                head_hash: control_head.head_hash(),
            }),
            ".json",
            &serde_json::to_vec(&control_head).expect("serialize control head"),
        )
        .await
        .expect("publish control head");
        for access in &creation.access {
            append_and_verify(
                &storage,
                &ProtocolObjectContext::store(
                    store_root_hash,
                    ProtocolObjectDomain::CircleAccessEnvelope,
                ),
                &circle_semantic_prefix(CircleSemanticSlot::AccessEnvelope {
                    circle_id: creation.circle_id,
                    owner_pubkey: &access.envelope.owner_pubkey,
                    recipient_slot: &access.envelope.recipient_slot,
                    control_hash: access.envelope.control_hash,
                }),
                ".json",
                &serde_json::to_vec(&access.envelope).expect("serialize access envelope"),
            )
            .await
            .expect("publish access envelope");
        }

        let error =
            load_circle_activations(&storage, &commit, &signer, &keys::public_key_hex(&signer))
                .await
                .expect_err("metadata cannot borrow authority from a different roster state");
        assert!(
            error.to_string().contains("roster state hash differs"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn merge_resume_blocks_revoked_journals_without_stopping_later_operations() {
        let db = open_test_db();
        let (_home, storage, founder, journal) =
            persist_merge_operation(&db, "circle-merge-revoked-grant").await;
        let successor = UserKeypair::generate();
        let store_root_hash = journal.creation.control.value.store_root_hash;
        let founder_pubkey = keys::public_key_hex(&founder);
        let entries =
            super::super::membership_ops::list_membership_entries(&storage, store_root_hash)
                .await
                .expect("list founder membership");
        let mut chain = super::super::membership_ops::load_anchored_chain(
            &storage,
            store_root_hash,
            &entries,
            Some(&founder_pubkey),
            None,
        )
        .await
        .expect("load founder chain");
        let add_successor = chain
            .signed_set_member(
                &founder,
                keys::public_key_hex(&successor),
                None,
                MemberRole::Owner,
                "0000000001001-0000-founder".to_string(),
            )
            .expect("add successor owner");
        super::super::store_objects::append_membership_entry_object(
            &storage,
            store_root_hash,
            &add_successor.coord(),
            &add_successor,
        )
        .await
        .expect("publish successor grant");
        chain
            .add_entry(add_successor)
            .expect("apply successor grant");
        super::super::store_objects::append_membership_head_object(
            &storage,
            store_root_hash,
            &chain
                .signed_head(&founder)
                .expect("sign successor-grant membership head"),
        )
        .await
        .expect("publish successor-grant membership head");
        let remove_founder = chain
            .signed_remove_member_in_stream(
                &successor,
                AuthorStreamId::from_bytes([31; 16]),
                founder_pubkey.clone(),
                "0000000001002-0000-successor".to_string(),
            )
            .expect("remove founder");
        super::super::store_objects::append_membership_entry_object(
            &storage,
            store_root_hash,
            &remove_founder.coord(),
            &remove_founder,
        )
        .await
        .expect("publish founder removal");
        chain
            .add_entry(remove_founder)
            .expect("apply founder removal");
        super::super::store_objects::append_membership_head_object(
            &storage,
            store_root_hash,
            &chain
                .signed_head(&successor)
                .expect("sign successor membership head"),
        )
        .await
        .expect("publish successor membership head");

        let (successor_db, _stamper) = Database::open(
            std::path::Path::new(":memory:"),
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "successor-device".to_string(),
            &test_migrations(),
        )
        .expect("open successor database");
        assert_eq!(
            publish_test_store_protocol_root(
                &successor_db,
                &storage,
                "circle-merge-revoked-grant",
                "successor-device",
                &founder,
            )
            .await,
            store_root_hash
        );
        successor_db
            .set_protocol_state(
                super::super::membership_ops::OWNER_PUBKEY_STATE_KEY,
                &founder_pubkey,
            )
            .await
            .expect("pin founder on successor database");
        super::super::store_registration::ensure_active_registration_with_coordination(
            &successor_db,
            &storage,
            None,
            &successor,
            Some(&chain),
            "0000000001003-0000-successor",
        )
        .await
        .expect("publish successor device registration");
        let (_store_temp, store_dir) = temp_store_dir();
        super::super::store_pull::pull_store_commits_with_identity(
            &db,
            &test_synced_tables(),
            &storage,
            None,
            store_root_hash,
            "creator",
            &store_dir,
            Some(&chain),
            Some(&successor),
        )
        .await
        .expect("materialize successor registration before its Circle operation");
        let later = prepare_circle_operation(
            &successor_db,
            &storage,
            None,
            "successor-device",
            "0000000001004-0000-successor",
            "Later Circle",
            &successor,
        )
        .await
        .expect("prepare still-authorized operation");
        db.insert_circle_operation(later.clone())
            .await
            .expect("persist still-authorized operation");

        resume_circle_operations(&db, &storage, None)
            .await
            .expect("revoked journal is blocked without interrupting the resume loop");

        let blocked = db
            .circle_operation(journal.circle_id())
            .await
            .expect("read revoked journal")
            .expect("revoked journal remains durable");
        assert!(matches!(
            blocked.status,
            CircleOperationState::Blocked { .. }
        ));
        assert!(db
            .circle_operation(later.circle_id())
            .await
            .expect("read later journal")
            .is_none());
        assert_eq!(
            db.get_circles(&keys::public_key_hex(&successor))
                .await
                .expect("read successor circles"),
            vec![crate::sync::circle::CircleInfo {
                id: later.circle_id(),
                name: "Later Circle".to_string(),
                role: CircleRole::Owner,
            }]
        );
        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn serial_circle_cannot_activate_from_authorization_before_a_removal_head() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let successor = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-authority-race");
        let db = open_serial_test_db();
        publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "circle-serial-authority-race",
            "founder-device",
            &founder,
        )
        .await;
        let coordination = storage.serial_coordination().expect("Serial coordination");

        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("founder authorization");
        let add_successor = authorization
            .membership
            .signed_set_member(
                &founder,
                keys::public_key_hex(&successor),
                None,
                MemberRole::Owner,
                "0000000000001-0000-founder".to_string(),
            )
            .expect("add successor owner");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            "founder-device",
            StoreControl::SerialMembership {
                entry: add_successor,
            },
            &founder,
        )
        .await
        .expect("prepare successor addition");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate successor addition");
        let authorization_head = coordination
            .read_head(serial_head_key())
            .await
            .expect("read authorization head")
            .bytes;

        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("authorization before removal");
        let remove_founder = authorization
            .membership
            .signed_remove_member(
                &successor,
                keys::public_key_hex(&founder),
                "0000000000002-0000-successor".to_string(),
            )
            .expect("remove founder");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            "successor-device",
            StoreControl::SerialMembership {
                entry: remove_founder,
            },
            &successor,
        )
        .await
        .expect("prepare founder removal");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate founder removal");

        let changed = HeadChangesAfterAuthorization {
            inner: coordination,
            authorization_head,
            reads: AtomicUsize::new(0),
        };
        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(&changed),
            "founder-device",
            "0000000001000-0000-founder",
            "Removed founder circle",
            &founder,
        )
        .await
        .expect("reproduce mismatched authorization and base snapshot");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist raced operation");

        let error =
            publish_circle_operation(&db, &storage, Some(coordination), journal.circle_id())
                .await
                .expect_err("a removed writer must not activate a Circle commit");

        assert!(matches!(error, CircleOperationError::Blocked { .. }));
        assert!(db
            .get_circles(&keys::public_key_hex(&founder))
            .await
            .expect("read founder circles")
            .is_empty());
    }

    #[tokio::test]
    async fn serial_circle_rejects_different_head_bytes_at_the_same_commit_position_before_upload()
    {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let successor = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-head-bytes");
        let db = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "circle-serial-head-bytes",
            "founder-device",
            &founder,
        )
        .await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("founder authorization");
        let entry = authorization
            .membership
            .signed_set_member(
                &founder,
                keys::public_key_hex(&successor),
                None,
                MemberRole::Member,
                "0000000000001-0000-founder".to_string(),
            )
            .expect("sign membership entry");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            "founder-device",
            StoreControl::SerialMembership { entry },
            &founder,
        )
        .await
        .expect("prepare serial control");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate serial control");

        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            "founder-device",
            "0000000001000-0000-founder",
            "Same-position conflict",
            &founder,
        )
        .await
        .expect("prepare Circle operation at the first exact head");
        let CircleOperationPolicy::Serial {
            base: Some(base), ..
        } = &journal.policy
        else {
            panic!("expected Serial Circle operation with a base")
        };
        let competing = StoreSerialHead::signed(
            root,
            Some(base.clone()),
            Some(crate::WriteId::from_generated(
                "different-tip-at-same-position".to_string(),
            )),
            &founder,
        )
        .expect("sign different same-position head");
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read first head");
        coordination
            .replace_head(serial_head_key(), &current.version, &competing.to_bytes())
            .await
            .expect("install different same-position head");
        let commit = journal.commit().expect("parse Circle commit");
        let commit_prefix = commit_semantic_prefix(
            super::super::store_commit::SERIAL_STREAM_ID,
            commit.seq(),
            commit.commit_hash(),
        );
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist Circle operation");

        let error =
            publish_circle_operation(&db, &storage, Some(coordination), journal.circle_id())
                .await
                .expect_err("same-position head substitution must block activation");

        assert!(matches!(error, CircleOperationError::Blocked { .. }));
        assert!(storage
            .list_protocol_objects(&format!("{commit_prefix}/copies/"))
            .await
            .expect("list Circle commit slot")
            .objects
            .is_empty());
    }

    #[tokio::test]
    async fn serial_circle_matching_head_without_its_commit_does_not_activate_or_reappend() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-missing-commit");
        let db = open_serial_test_db();
        publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "circle-serial-missing-commit",
            "founder-device",
            &founder,
        )
        .await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            "founder-device",
            "0000000001000-0000-founder",
            "Missing commit",
            &founder,
        )
        .await
        .expect("prepare Circle operation");
        let CircleOperationPolicy::Serial { head, .. } = &journal.policy else {
            panic!("expected Serial Circle head")
        };
        coordination
            .create_head(serial_head_key(), &head.to_bytes())
            .await
            .expect("publish head without commit");
        let commit = journal.commit().expect("parse Circle commit");
        let commit_prefix = commit_semantic_prefix(
            super::super::store_commit::SERIAL_STREAM_ID,
            commit.seq(),
            commit.commit_hash(),
        );
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist Circle operation");

        publish_circle_operation(&db, &storage, Some(coordination), journal.circle_id())
            .await
            .expect_err("an activated head cannot repair or trust an absent commit");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .circle_operation(journal.circle_id())
            .await
            .expect("read Circle journal")
            .is_some());
        assert!(storage
            .list_protocol_objects(&format!("{commit_prefix}/copies/"))
            .await
            .expect("list absent commit slot")
            .objects
            .is_empty());
    }

    #[tokio::test]
    async fn stale_serial_head_blocks_the_exact_operation_without_activation() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = serial_storage(&home, &signer, "circle-stale-serial");
        let db = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "circle-stale-serial",
            "creator",
            &signer,
        )
        .await;
        let expected = prepare_circle_operation(
            &db,
            &storage,
            Some(storage.serial_coordination().expect("Serial coordination")),
            "creator",
            "0000000001000-0000-creator",
            "Household",
            &signer,
        )
        .await
        .expect("prepare Serial circle operation");
        db.insert_circle_operation(expected.clone())
            .await
            .expect("persist Serial circle operation");

        let competing = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("competing-serial-head".to_string()),
            "competitor".to_string(),
            StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            1,
            &[],
            &signer,
        )
        .expect("sign competing Serial commit");
        let package = competing
            .store_package
            .as_ref()
            .expect("competing commit carries Store package");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::store(root, ProtocolObjectDomain::StorePackage),
            &package.object_key,
            ".pkg",
            &[],
        )
        .await
        .expect("publish competing Store package");
        append_and_verify(
            &storage,
            &ProtocolObjectContext::store(root, ProtocolObjectDomain::StoreCommit),
            &commit_semantic_prefix(
                super::super::store_commit::SERIAL_STREAM_ID,
                competing.seq(),
                competing.commit_hash(),
            ),
            ".json",
            &competing.to_bytes(),
        )
        .await
        .expect("publish competing Serial commit");
        let competing_head = StoreSerialHead::signed(
            root,
            Some(competing.position()),
            Some(competing.write_id.clone()),
            &signer,
        )
        .expect("sign competing Serial head");
        storage
            .serial_coordination()
            .expect("Serial coordination")
            .create_head(serial_head_key(), &competing_head.to_bytes())
            .await
            .expect("publish competing Serial head");

        let error = publish_circle_operation(
            &db,
            &storage,
            Some(storage.serial_coordination().expect("Serial coordination")),
            expected.circle_id(),
        )
        .await
        .expect_err("stale Serial base must lose head activation");
        assert!(
            matches!(error, CircleOperationError::Blocked { .. }),
            "{error}"
        );
        let blocked = db
            .circle_operation(expected.circle_id())
            .await
            .expect("read blocked Serial operation")
            .expect("blocked Serial operation remains durable");
        assert_exact_operation(&expected, &blocked);
        assert!(matches!(
            blocked.status,
            CircleOperationState::Blocked { .. }
        ));
        let operations = db
            .get_circle_operations()
            .await
            .expect("read blocked circle operations");
        assert_eq!(operations.len(), 1);
        assert!(matches!(
            operations[0].state,
            crate::sync::circle::CircleOperationState::Blocked { .. }
        ));
        assert!(db
            .get_circles(&keys::public_key_hex(&signer))
            .await
            .expect("read inactive circles")
            .is_empty());
        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
        assert!(db
            .materialized_frontier()
            .await
            .expect("read frontier")
            .is_empty());

        resume_circle_operations(
            &db,
            &storage,
            Some(storage.serial_coordination().expect("Serial coordination")),
        )
        .await
        .expect("blocked circle operation remains inert during resume");
        db.discard_blocked_circle_operation(expected.circle_id())
            .await
            .expect("discard blocked circle operation");
        db.discard_blocked_circle_operation(expected.circle_id())
            .await
            .expect("repeated discard remains idempotent");
        assert!(db
            .circle_operation(expected.circle_id())
            .await
            .expect("read discarded circle operation")
            .is_none());
        assert!(db
            .get_circle_operations()
            .await
            .expect("read circle operations after discard")
            .is_empty());
    }
}
