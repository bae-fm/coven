//! Durable creation and activation of circles through the Store commit stream.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::circle::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    circle_control_semantic_prefix, circle_metadata_semantic_prefix, circle_roster_semantic_prefix,
    recipient_slot_with_peer, AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf,
    CircleControl, CircleControlOrder, CircleCreation, CircleId, CircleMetadata, CircleRoster,
    PreparedAccessLeaf, PreparedCircleControl, StoreMembershipStateRef,
};
use super::membership::SerialAuthorizationState;
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, CircleControlRef, CommitPosition, ObjectHash,
    StoreBatchCommit, StoreCommitOrder, StoreDeviceHead, StoreProtocolError, StoreSerialHead,
};
use super::store_objects::append_and_verify;
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationStatus {
    Pending,
    Blocked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleActivationHead {
    MergeConcurrent(StoreDeviceHead),
    Serial(StoreSerialHead),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationJournal {
    pub operation_id: String,
    pub status: CircleOperationStatus,
    pub creation: CircleCreation,
    pub store_base: Option<CommitPosition>,
    pub commit_bytes: Vec<u8>,
    pub head: CircleActivationHead,
    pub serial_authorization: Option<SerialAuthorizationState>,
    pub uploaded: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCircleActivation {
    pub circle_id: CircleId,
    pub control: PreparedCircleControl,
    pub access_bytes: Vec<u8>,
    pub disposition: CircleAccessDisposition,
    pub roster: Option<CircleRoster>,
    pub metadata: Option<CircleMetadata>,
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
    while let Some(journal) = db.oldest_circle_operation().await? {
        match journal.status {
            CircleOperationStatus::Pending => {
                publish_circle_operation(db, storage, coordination, journal.circle_id()).await?;
            }
            CircleOperationStatus::Blocked { ref reason } => {
                return Err(CircleOperationError::Blocked {
                    circle_id: journal.circle_id(),
                    reason: reason.clone(),
                });
            }
        }
    }
    Ok(())
}

pub(crate) async fn load_circle_activations(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    identity: &UserKeypair,
    founder_pubkey: &str,
) -> Result<Vec<VerifiedCircleActivation>, CircleOperationError> {
    let mut activations = Vec::with_capacity(commit.circle_controls.len());
    for reference in &commit.circle_controls {
        let control_prefix =
            circle_control_semantic_prefix(reference.circle_id, &reference.control);
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            &ProtocolObjectContext::store(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &control_prefix,
            reference.control.control_hash(),
            |bytes| {
                if ObjectHash::digest(bytes) != reference.control.control_hash() {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: reference.control.control_hash(),
                        actual: ObjectHash::digest(bytes),
                    });
                }
                let value: CircleControl = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                if !value.verify() || value.coord() != reference.control {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(value)
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "circle control {} is absent",
                reference.control.control_hash()
            ))
        })?;
        let control = PreparedCircleControl {
            coord: reference.control.clone(),
            bytes: loaded.bytes,
            value: loaded.value,
        };
        verify_control_membership(storage, &control, founder_pubkey).await?;
        let own_pubkey = keys::public_key_hex(identity);
        let owner_pubkey = control.value.owners.first().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "circle control has no Owner recipient slot".to_string(),
            )
        })?;
        let owner = (
            owner_pubkey.clone(),
            recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!(
                        "derive circle Owner recipient slot: {error}"
                    ))
                },
            )?,
        );
        let envelope_prefix = format!(
            "circles/{}/access-envelopes/{}/{}/{}",
            reference.circle_id,
            owner.0,
            owner.1,
            reference.control.control_hash()
        );
        let envelope_bytes = load_exact_slot_bytes(
            storage,
            &ProtocolObjectContext::store(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &envelope_prefix,
        )
        .await?;
        let envelope: AccessEnvelope =
            serde_json::from_slice(&envelope_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access envelope: {error}"))
            })?;
        if envelope.owner_pubkey != owner.0
            || envelope.recipient_slot != owner.1
            || !envelope.verify(&control)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access envelope failed verification".to_string(),
            ));
        }
        let leaf_prefix = format!(
            "circles/{}/access-leaves/{}/{}/{}/{}",
            reference.circle_id, owner.0, control.value.epoch_id, owner.1, envelope.leaf_id
        );
        let loaded_leaf = super::store_objects::load_semantic_copies(
            storage,
            &ProtocolObjectContext::recipient_sealed(commit.store_root_hash),
            &leaf_prefix,
            envelope.leaf_hash,
            |bytes| {
                if ObjectHash::digest(bytes) != envelope.leaf_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: envelope.leaf_hash,
                        actual: ObjectHash::digest(bytes),
                    });
                }
                Ok(bytes.to_vec())
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "circle access leaf {} is absent",
                envelope.leaf_hash
            ))
        })?;
        let plaintext =
            keys::seal_box_decrypt(&loaded_leaf.bytes, &identity.to_x25519_secret_key()).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
                },
            )?;
        let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
        })?;
        let prepared_leaf = PreparedAccessLeaf {
            bytes: loaded_leaf.bytes.clone(),
            value: leaf.clone(),
            leaf_hash: envelope.leaf_hash,
        };
        if leaf.owner_pubkey != owner.0
            || leaf.recipient_pubkey != own_pubkey
            || leaf.recipient_slot != owner.1
            || leaf.store_membership != control.value.store_membership
            || !prepared_leaf.verify_envelope(&control, &envelope)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access leaf failed context verification".to_string(),
            ));
        }
        let (roster, metadata) = match &leaf.disposition {
            CircleAccessDisposition::Active {
                keyring,
                key_fingerprint,
                roster_hash,
            } => {
                if *key_fingerprint != control.value.key_fingerprint
                    || *roster_hash != control.value.roster_hash
                {
                    return Err(CircleOperationError::InvalidState(
                        "circle Active access names a different key or roster".to_string(),
                    ));
                }
                let encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse circle access keyring: {error}"
                        ))
                    })?,
                );
                if encryption.seal_key_fingerprint() != *key_fingerprint {
                    return Err(CircleOperationError::InvalidState(
                        "circle access keyring fingerprint differs from control".to_string(),
                    ));
                }
                let roster = match &control.value.order {
                    CircleControlOrder::Serial { roster, .. } => roster.clone(),
                    CircleControlOrder::MergeConcurrent {
                        device_id,
                        author_owner_grant,
                        seq,
                        ..
                    } => {
                        let prefix = format!(
                            "circles/{}/roster/entries/{}/{}/{}/{}/{}",
                            reference.circle_id,
                            control.value.author_pubkey,
                            device_id,
                            author_owner_grant,
                            seq,
                            roster_hash
                        );
                        super::store_objects::load_semantic_copies(
                            storage,
                            &ProtocolObjectContext::circle(
                                commit.store_root_hash,
                                ProtocolObjectDomain::CircleRoster,
                                encryption.clone(),
                            ),
                            &prefix,
                            *roster_hash,
                            |bytes| {
                                let roster: CircleRoster =
                                    serde_json::from_slice(bytes).map_err(|error| {
                                        StoreProtocolError::Malformed(error.to_string())
                                    })?;
                                if roster.roster_hash() != *roster_hash || !roster.verify() {
                                    return Err(StoreProtocolError::InvalidSignature);
                                }
                                Ok(roster)
                            },
                        )
                        .await?
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "circle roster is absent".to_string(),
                            )
                        })?
                        .value
                    }
                };
                let roster_grant_matches_control = match &control.value.order {
                    CircleControlOrder::MergeConcurrent {
                        author_owner_grant, ..
                    } => &roster.owner_grant == author_owner_grant,
                    CircleControlOrder::Serial {
                        roster: embedded, ..
                    } => &roster == embedded,
                };
                if roster.roster_hash() != control.value.roster_hash
                    || roster.store_root_hash != control.value.store_root_hash
                    || roster.circle_id != control.value.circle_id
                    || !roster_grant_matches_control
                    || !roster.verify()
                {
                    return Err(CircleOperationError::InvalidState(
                        "circle roster failed verification".to_string(),
                    ));
                }
                let roster_owners = roster
                    .members
                    .iter()
                    .filter_map(|(pubkey, role)| {
                        (*role == super::circle::CircleRole::Owner).then_some(pubkey.clone())
                    })
                    .collect::<Vec<_>>();
                if roster_owners != control.value.owners
                    || roster.members.get(&control.value.author_pubkey)
                        != Some(&super::circle::CircleRole::Owner)
                {
                    return Err(CircleOperationError::InvalidState(
                        "circle control Owners differ from its roster".to_string(),
                    ));
                }
                let metadata_prefix = format!(
                    "circles/{}/metadata/{}/{}/{}",
                    reference.circle_id,
                    control.value.author_pubkey,
                    control.value.epoch_id,
                    control.value.metadata_hash
                );
                let metadata = super::store_objects::load_semantic_copies(
                    storage,
                    &ProtocolObjectContext::circle(
                        commit.store_root_hash,
                        ProtocolObjectDomain::CircleMetadata,
                        encryption,
                    ),
                    &metadata_prefix,
                    control.value.metadata_hash,
                    |bytes| {
                        let metadata: CircleMetadata = serde_json::from_slice(bytes)
                            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                        if metadata.metadata_hash() != control.value.metadata_hash
                            || !metadata.verify()
                        {
                            return Err(StoreProtocolError::InvalidSignature);
                        }
                        Ok(metadata)
                    },
                )
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState("circle metadata is absent".to_string())
                })?
                .value;
                if metadata.store_root_hash != control.value.store_root_hash
                    || metadata.circle_id != control.value.circle_id
                    || metadata.epoch_id != control.value.epoch_id
                    || metadata.owner_grant != roster.owner_grant
                    || roster.members.get(&metadata.author_pubkey)
                        != Some(&super::circle::CircleRole::Owner)
                {
                    return Err(CircleOperationError::InvalidState(
                        "circle metadata author is not an Owner in its roster".to_string(),
                    ));
                }
                (Some(roster), Some(metadata))
            }
            CircleAccessDisposition::Inactive => (None, None),
        };
        activations.push(VerifiedCircleActivation {
            circle_id: reference.circle_id,
            control,
            access_bytes: loaded_leaf.bytes,
            disposition: leaf.disposition,
            roster,
            metadata,
        });
    }
    Ok(activations)
}

async fn verify_control_membership(
    storage: &dyn SyncStorage,
    control: &PreparedCircleControl,
    founder_pubkey: &str,
) -> Result<(), CircleOperationError> {
    let members = match &control.value.store_membership {
        StoreMembershipStateRef::MergeConcurrent { heads, .. } => {
            let chain = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                control.value.store_root_hash,
                founder_pubkey,
                heads,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let grant = control.value.membership_grant.as_ref().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Merge circle control lacks Store membership grant".to_string(),
                )
            })?;
            if !chain.authorizes_write_at(grant, &control.value.author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "Store membership does not authorize circle control author".to_string(),
                ));
            }
            chain.current_members()
        }
        StoreMembershipStateRef::Serial { position, .. } => {
            let authorization = super::store_pull::load_serial_authorization_at_position(
                storage,
                control.value.store_root_hash,
                position.clone(),
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            if !authorization
                .membership
                .can_write(&control.value.author_pubkey)
            {
                return Err(CircleOperationError::InvalidState(
                    "Serial Store membership does not authorize circle control author".to_string(),
                ));
            }
            authorization.membership.current_members()
        }
    };
    if super::circle::store_membership_state_hash(&members)
        != control.value.store_membership.state_hash()
    {
        return Err(CircleOperationError::InvalidState(
            "circle control Store membership state hash is invalid".to_string(),
        ));
    }
    Ok(())
}

async fn load_exact_slot_bytes(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    let listing = storage
        .list_protocol_objects(&format!("{semantic_prefix}/copies/"))
        .await
        .map_err(super::store_objects::StoreObjectError::from)?;
    let mut canonical = None;
    for object in listing.objects {
        let bytes = storage
            .read_protocol_object(context, &object, semantic_prefix)
            .await
            .map_err(super::store_objects::StoreObjectError::from)?;
        if canonical.as_ref().is_some_and(|value| value != &bytes) {
            return Err(CircleOperationError::InvalidState(format!(
                "circle semantic slot {semantic_prefix:?} contains a fork"
            )));
        }
        canonical = Some(bytes);
    }
    canonical.ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "circle semantic slot {semantic_prefix:?} is absent"
        ))
    })
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
    let (creation, base, order, membership_grant, serial_authorization) = match db.write_policy() {
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
            let heads = current.author_heads();
            let exact = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                store_root_hash,
                &founder,
                &heads,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let members = exact.current_members();
            let membership_grant = exact.write_grant_coord(&author_pubkey).ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "circle creator is not a current Store writer".to_string(),
                )
            })?;
            let creation = CircleCreation::founder(
                store_root_hash,
                device_id,
                name,
                metadata_stamp,
                StoreMembershipStateRef::merge_concurrent(heads, &members),
                Some(membership_grant.clone()),
                members,
                db.id_provider(),
                signer,
            )?;
            let base = db.latest_local_store_position().await?;
            let seq = base.as_ref().map_or(1, |position| position.seq + 1);
            let mut dependencies = db.materialized_frontier().await?;
            dependencies.remove(device_id);
            (
                creation,
                base.clone(),
                StoreCommitOrder::MergeConcurrent {
                    seq,
                    previous_commit_hash: base.map(|position| position.commit_hash),
                    dependencies,
                },
                Some(membership_grant),
                None,
            )
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle creation requires coordination storage".to_string(),
                )
            })?;
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            if !authorization.membership.can_write(&author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "circle creator is not a current Store writer".to_string(),
                ));
            }
            let base =
                super::store_outbound::current_serial_head_position(db, coordination).await?;
            let members = authorization.membership.current_members();
            let creation = CircleCreation::founder(
                store_root_hash,
                device_id,
                name,
                metadata_stamp,
                StoreMembershipStateRef::serial(base.clone(), &members),
                None,
                members,
                db.id_provider(),
                signer,
            )?;
            (
                creation,
                base.clone(),
                StoreCommitOrder::Serial {
                    seq: base.as_ref().map_or(1, |position| position.seq + 1),
                    previous_commit_hash: base.as_ref().map(|position| position.commit_hash),
                },
                None,
                Some(authorization),
            )
        }
    };
    let commit = StoreBatchCommit::signed_batch(
        store_root_hash,
        operation_id.clone(),
        device_id.to_string(),
        order,
        membership_grant,
        None,
        Vec::new(),
        vec![CircleControlRef {
            circle_id: creation.circle_id,
            control: creation.control.coord.clone(),
        }],
        None,
        &[],
        signer,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let head = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => CircleActivationHead::MergeConcurrent(
            StoreDeviceHead::signed(
                store_root_hash,
                device_id.to_string(),
                Some(commit.position()),
                metadata_stamp.to_string(),
                signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
        ),
        crate::WritePolicy::Serial => CircleActivationHead::Serial(
            StoreSerialHead::signed(
                store_root_hash,
                Some(commit.position()),
                Some(commit.write_id.clone()),
                signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
        ),
    };
    Ok(CircleOperationJournal {
        operation_id: operation_id.as_str().to_string(),
        status: CircleOperationStatus::Pending,
        creation,
        store_base: base,
        commit_bytes: commit.to_bytes(),
        head,
        serial_authorization,
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
    if let CircleOperationStatus::Blocked { reason } = &journal.status {
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

    append_step(
        db,
        storage,
        &mut journal,
        "metadata",
        &ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            circle_encryption.clone(),
        ),
        &circle_metadata_semantic_prefix(&creation.metadata),
        ".json",
        &serde_json::to_vec(&creation.metadata).expect("circle metadata serialization cannot fail"),
    )
    .await?;
    if matches!(
        creation.control.value.order,
        super::circle::CircleControlOrder::MergeConcurrent { .. }
    ) {
        append_step(
            db,
            storage,
            &mut journal,
            "roster",
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRoster,
                circle_encryption,
            ),
            &circle_roster_semantic_prefix(&creation.roster),
            ".json",
            &serde_json::to_vec(&creation.roster).expect("circle roster serialization cannot fail"),
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
            &circle_access_leaf_semantic_prefix(&access.leaf.value),
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
        &circle_control_semantic_prefix(creation.circle_id, &creation.control.coord),
        ".json",
        &creation.control.bytes,
    )
    .await?;
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
            &circle_access_envelope_semantic_prefix(&access.envelope),
            ".json",
            &serde_json::to_vec(&access.envelope)
                .expect("access envelope serialization cannot fail"),
        )
        .await?;
    }
    let commit = journal.commit()?;
    let activation_head = journal.head.clone();
    match activation_head {
        CircleActivationHead::MergeConcurrent(head) => {
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
        CircleActivationHead::Serial(head) => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle activation requires coordination storage".to_string(),
                )
            })?;
            if let Err(error) = super::store_outbound::activate_serial_commit_head(
                db,
                storage,
                coordination,
                journal.store_base.clone(),
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
    use std::sync::Arc;

    use super::*;
    use crate::database::DbError;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::{founder_entry, MembershipChain};
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
    use crate::sync::store_commit::{serial_head_key, StoreBatchCommit, StoreCommitOrder};
    use crate::sync::test_helpers::{
        open_serial_test_db, open_test_db, publish_test_serial_store_protocol_root,
        publish_test_store_protocol_root, test_migrations, test_synced_tables,
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
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(name)))
    }

    fn serial_storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        name: &str,
    ) -> CloudSyncStorage {
        merge_storage(home, signer, name).with_test_serial_coordination(Arc::new(home.clone()))
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
        assert_eq!(actual.store_base, expected.store_base);
        assert_eq!(actual.commit_bytes, expected.commit_bytes);
        assert_eq!(actual.head, expected.head);
        assert_eq!(actual.serial_authorization, expected.serial_authorization);
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
                assert_eq!(persisted.status, CircleOperationStatus::Pending);
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
        assert_eq!(persisted.status, CircleOperationStatus::Pending);

        resume_circle_operations(&reopened, &storage, None)
            .await
            .expect("resume reopened circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
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
            CircleOperationStatus::Blocked { .. }
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
    }
}
