use std::collections::BTreeMap;

use super::authorization::history::AuthorizedStoreHistory;
use super::pull;
use super::*;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{PreparedExactObject, ProtocolObjectDomain};
use coven_protocol::store_commit::StoreRootRef;
use coven_protocol::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, head_slot_prefix, owner_recovery_semantic_prefix,
    registration_semantic_prefix, snapshot_slot_prefix, ActivatedStoreDeviceRegistrationRef,
    CommitFrontier, DeviceRecoveryId, DeviceRecoveryReadiness, DeviceStreamAnchor, ObjectHash,
    OwnerRecoveryNode, OwnerRecoveryNodeRef, OwnerRecoveryPosition, StoreAck,
    StoreAckExclusionState, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreCommitOrder, StoreDeviceHead, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, SuccessorLink,
};

/// A snapshot-installed Store that retains the exact remote authority used to
/// verify the image. Restore-only operations consume this authority directly
/// instead of reconstructing it from database rows and cloud objects.
mod continuation;
mod history;
use recovery_preparation::*;
mod recovery_preparation;
mod restore_test_support;

pub(crate) use history::RestoreHistory;

pub struct RestoringStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    database: StoreDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
    root: StoreRootRef,
    protocol: StoreProtocolRoot,
    membership: coven_protocol::membership::MembershipChain,
    identity: UserKeypair,
}

impl<'storage> RestoringStore<'storage> {
    async fn complete_owner_recovery_predecessor_history(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<StoreHistoryCut, StoreRegistrationError> {
        let target = self
            .history
            .current_merge_authority_cut(&self.membership)
            .await?;
        loop {
            let before = CommitFrontier::from_refs(
                self.database
                    .materialized_frontier()
                    .await
                    .map_err(StoreRegistrationError::from)?,
            )
            .map_err(StoreRegistrationError::from)?;
            if before.covers(&target.frontier()) {
                return Ok(StoreHistoryCut(before.0));
            }

            let pulled = self.pull(routing_encryption).await?;
            let after = CommitFrontier::from_refs(
                self.database
                    .materialized_frontier()
                    .await
                    .map_err(StoreRegistrationError::from)?,
            )
            .map_err(StoreRegistrationError::from)?;
            if after.covers(&target.frontier()) {
                return Ok(StoreHistoryCut(after.0));
            }
            if !pulled.held_positions.is_empty() {
                return Err(StoreRegistrationError::Invalid(format!(
                    "Owner recovery predecessor history is held at {:?}",
                    pulled.held_positions
                )));
            }
            if after == before {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery predecessor history made no progress".into(),
                ));
            }
        }
    }

    /// Record where an adopted registration's published streams stand: the
    /// acknowledgement head the pulled history activated for it (the initial
    /// acknowledgement when it never published another) and the snapshot its
    /// stream on the provider ends on.
    async fn resume_adopted_device_streams(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
    ) -> Result<(), StoreRegistrationError> {
        let registration = self
            .database
            .activated_store_device_registration_for_device(registration_ref.device_id)
            .await
            .map_err(StoreRegistrationError::from)?
            .ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "adopted Owner recovery registration is not activated".into(),
                )
            })?;
        let history = self.history.restore_history();
        let latest_ack_ref = match self
            .database
            .activated_store_ack(registration_ref)
            .await
            .map_err(StoreRegistrationError::from)?
        {
            Some(activated) => activated.reference,
            None => {
                let durable = self
                    .database
                    .latest_local_store_device_registration()
                    .await
                    .map_err(StoreRegistrationError::from)?
                    .ok_or_else(|| {
                        StoreRegistrationError::Invalid(
                            "adopted Owner recovery registration has no local journal".into(),
                        )
                    })?;
                durable.initial_ack_ref
            }
        };
        let latest_ack = history
            .load_store_ack(&latest_ack_ref, registration.value())
            .await?;
        let latest_snapshot = history
            .load_store_snapshot_stream(registration_ref, registration.value())
            .await
            .map_err(|error| StoreRegistrationError::SnapshotStream(Box::new(error)))?
            .into_iter()
            .last()
            .map(|snapshot| (snapshot.reference, snapshot.meta));
        self.database
            .resume_local_device_streams((latest_ack_ref, latest_ack), latest_snapshot)
            .await
            .map_err(StoreRegistrationError::from)
    }

    pub async fn recover_owner_device(
        &mut self,
        authority: &coven_protocol::recovery::OwnerRecoveryAuthority,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
        let database = self.database.clone();
        let storage = self.storage;
        let identity_signer = &self.identity;
        let membership = &self.membership;
        let root = self.root.clone();
        let protocol = self.protocol.clone();
        let owner_pubkey = coven_keys::keys::public_key_hex(identity_signer);
        if owner_pubkey != protocol.descriptor.founder_pubkey
            || authority.owner_grant != protocol.descriptor.founder_grant
            || membership.active_owner_grant(&owner_pubkey).as_ref() != Some(&authority.owner_grant)
            || authority.recovery.owner_grant != authority.owner_grant
        {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery authority differs from the active root founder grant".into(),
            ));
        }
        let coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
            &protocol.descriptor.founder_recovery
        else {
            return Err(StoreRegistrationError::Invalid(
                "Store root has no founder recovery stream".into(),
            ));
        };
        let (recovery_slot, predecessor, sequence) = match &authority.recovery.position {
            OwnerRecoveryPosition::BeforeFirst { activation } => {
                let expected = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
                    &root,
                    &owner_pubkey,
                    &authority.owner_grant,
                    &protocol.descriptor.founder_recovery,
                )
                .map_err(StoreRegistrationError::from)?;
                if activation != &expected {
                    return Err(StoreRegistrationError::Invalid(
                        "Owner recovery activation differs from the root anchor".into(),
                    ));
                }
                (first_slot.clone(), None, 1)
            }
            OwnerRecoveryPosition::At { node } => {
                let loaded = self
                    .history
                    .restore_history()
                    .load_owner_recovery_node(node)
                    .await?;
                if loaded.value.owner_pubkey != owner_pubkey
                    || loaded.value.owner_grant != authority.owner_grant
                {
                    return Err(StoreRegistrationError::Invalid(
                        "Owner recovery cursor belongs to another authority".into(),
                    ));
                }
                (
                    loaded.value.next_slot.clone(),
                    Some(node.clone()),
                    node.sequence.saturating_add(1),
                )
            }
        };
        let recovery_hash = ObjectHash::digest(
            &serde_json::to_vec(&(
                &root,
                &owner_pubkey,
                &authority.owner_grant,
                &authority.recovery,
            ))
            .map_err(StoreRegistrationError::from)?,
        );
        let recovery_id = DeviceRecoveryId::from_hash(recovery_hash);
        let origin = StoreDeviceRegistrationOrigin::Recovery {
            recovery_id,
            recovery_slot: recovery_slot.clone(),
            owner_grant: authority.owner_grant.clone(),
        };
        let device_id = coven_protocol::store_commit::StoreDeviceId::derive(&root, &origin);
        if let Some(registration) = self
            .install_activated_owner_recovery(
                &origin,
                device_id,
                recovery_id,
                &recovery_slot,
                &owner_pubkey,
                &authority.owner_grant,
                sequence,
                &predecessor,
            )
            .await?
        {
            // The device this authority derives already registered in an
            // earlier life and published since; resume its streams from the
            // heads the provider holds rather than the registration's first
            // slots.
            self.resume_adopted_device_streams(&registration).await?;
            return Ok(registration);
        }
        let context = |domain| {
            coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                domain,
            )
        };
        let commit_context = context(ProtocolObjectDomain::StoreCommit);
        let published_node = self
            .load_published_owner_recovery_node(
                &recovery_slot,
                &owner_pubkey,
                &authority.owner_grant,
                sequence,
                recovery_id,
                &predecessor,
            )
            .await?;
        let staged = database
            .latest_local_store_device_registration()
            .await
            .map_err(StoreRegistrationError::from)?
            .filter(|durable| durable.device_id == device_id);
        let readiness = if let Some(durable) = staged {
            let registration = StoreDeviceRegistration::parse_at(
                &durable.registration_bytes,
                &root,
                durable.device_id,
            )
            .map_err(StoreRegistrationError::from)?;
            if registration.origin != origin
                || registration.to_bytes() != durable.registration_bytes
                || registration.registration_hash() != durable.registration_hash
            {
                return Err(StoreRegistrationError::Invalid(
                    "staged Owner recovery registration differs from its exact authority".into(),
                ));
            }
            let registration_ref = StoreDeviceRegistrationRef::from_registration(
                &registration,
                durable.prepared.reference().clone(),
            );
            let initial_ack = StoreAck::parse_at(
                &durable.initial_ack.bytes,
                &root,
                &durable.initial_ack_ref,
                &registration,
            )
            .map_err(StoreRegistrationError::from)?;
            if initial_ack != durable.initial_ack.value {
                return Err(StoreRegistrationError::Invalid(
                    "staged Owner recovery acknowledgement differs from its exact authority".into(),
                ));
            }
            let registration_exists = self
                .prepared_protocol_object_exists(
                    &context(
                        coven_protocol::objects::ProtocolObjectDomain::StoreDeviceRegistration,
                    ),
                    &durable.prepared,
                    &registration_semantic_prefix(&device_id.to_string()),
                    &durable.registration_bytes,
                )
                .await?;
            let initial_ack_exists = self
                .prepared_protocol_object_exists(
                    &context(coven_protocol::objects::ProtocolObjectDomain::StoreAck),
                    &durable.initial_ack.prepared,
                    &ack_slot_prefix(&device_id.to_string(), 1),
                    &durable.initial_ack.bytes,
                )
                .await?;
            PreparedRecoveryReadiness {
                registration: RecoveryProtocolObject::from_remote_state(
                    coven_protocol::objects::ExactProtocolObject {
                        value: registration,
                        bytes: durable.registration_bytes,
                        prepared: durable.prepared,
                    },
                    registration_ref,
                    registration_exists,
                ),
                initial_ack: RecoveryProtocolObject::from_remote_state(
                    durable.initial_ack,
                    durable.initial_ack_ref,
                    initial_ack_exists,
                ),
            }
        } else if let Some(node) = published_node.as_ref() {
            self.load_published_recovery_readiness(&node.exact().value, &origin)
                .await?
        } else {
            let head_context = context(coven_protocol::objects::ProtocolObjectDomain::StoreHead);
            let ack_context = context(coven_protocol::objects::ProtocolObjectDomain::StoreAck);
            let snapshot_context =
                context(coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotMeta);
            let registration_context =
                context(coven_protocol::objects::ProtocolObjectDomain::StoreDeviceRegistration);
            let first_head = storage
                .allocate_protocol_slot(
                    &head_context,
                    &head_slot_prefix(&device_id.to_string(), 1),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let first_ack = storage
                .allocate_protocol_slot(
                    &ack_context,
                    &ack_slot_prefix(&device_id.to_string(), 1),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let first_snapshot = storage
                .allocate_protocol_slot(
                    &snapshot_context,
                    &snapshot_slot_prefix(&device_id.to_string(), 0),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let registration_prefix = registration_semantic_prefix(&device_id.to_string());
            let registration_slot = storage
                .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let provider = storage
                .provider_binding()
                .await
                .map_err(StoreObjectError::from)?
                .device;
            let expected_registration = StoreDeviceRegistration::signed(
                root.clone(),
                origin.clone(),
                provider,
                DeviceStreamAnchor::StoreAnnouncements {
                    first_slot: first_head,
                },
                DeviceStreamAnchor::StoreAcknowledgements {
                    first_slot: first_ack.clone(),
                },
                DeviceStreamAnchor::StoreSnapshots {
                    first_slot: first_snapshot,
                },
                identity_signer,
            )
            .map_err(StoreRegistrationError::from)?;
            let registration = self
                .prepare_or_load_recovery_registration(
                    expected_registration,
                    registration_slot,
                    &registration_prefix,
                )
                .await?;
            let dependencies = database
                .materialized_frontier()
                .await
                .map_err(StoreRegistrationError::from)?
                .into_iter()
                .map(|(stream, reference)| {
                    stream
                        .parse()
                        .map(|stream| (stream, reference))
                        .map_err(StoreRegistrationError::AuthorStreamId)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let device_signer = registration
                .exact()
                .value
                .device_signer(identity_signer)
                .map_err(StoreRegistrationError::from)?;
            let bootstrap_cut = StoreHistoryCut(dependencies);
            let (device_state, _) = database
                .store_device_state_for_history_cut(&bootstrap_cut)
                .await
                .map_err(StoreRegistrationError::from)?;
            let initial_ack = self
                .prepare_or_load_initial_recovery_ack(
                    &registration.exact().value,
                    registration.reference(),
                    first_ack,
                    bootstrap_cut,
                    device_state,
                    &authority.published_at,
                    &device_signer,
                )
                .await?;
            PreparedRecoveryReadiness {
                registration,
                initial_ack,
            }
        };
        let registration_ref = readiness.registration.reference().clone();
        let initial_ack_ref = readiness.initial_ack.reference().clone();
        let device_signer = readiness
            .registration
            .exact()
            .value
            .device_signer(identity_signer)
            .map_err(StoreRegistrationError::from)?;
        let bootstrap_cut = readiness.initial_ack.exact().value.store_cut.clone();
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery requires resolved membership".into(),
            ));
        };
        let node_membership_state =
            coven_protocol::circle_control::StoreMembershipStateRef::from_parts(
                membership.head_refs().to_vec(),
                membership.resolution_refs().to_vec(),
                vec![authority.recovery.clone()],
                resolved.state_hash,
            )
            .map_err(StoreRegistrationError::from)?;
        let recovery_readiness = DeviceRecoveryReadiness {
            registration: registration_ref.clone(),
            initial_ack: initial_ack_ref.clone(),
            bootstrap_cut: bootstrap_cut.clone(),
        };
        let node = match published_node {
            Some(node) => {
                if node.exact().value.readiness != recovery_readiness {
                    return Err(StoreRegistrationError::Invalid(
                        "published Owner recovery node differs from its exact readiness".into(),
                    ));
                }
                node
            }
            None => {
                self.prepare_owner_recovery_node(
                    &recovery_slot,
                    &owner_pubkey,
                    &authority.owner_grant,
                    sequence,
                    recovery_id,
                    &node_membership_state,
                    &predecessor,
                    &recovery_readiness,
                    identity_signer,
                )
                .await?
            }
        };
        self.history
            .verify_owner_recovery_node_authority(&node.exact().value, &self.membership)
            .await?;
        let node_ref = node.reference().clone();
        let registration_activation = StoreDeviceRegistrationActivation::Recovery {
            recovery_id,
            node: node_ref.clone(),
        };
        let registration = readiness.registration.exact().value.clone();
        let already_activated = database
            .stage_owner_recovery_registration(
                readiness.registration.exact().clone(),
                initial_ack_ref.clone(),
                readiness.initial_ack.exact().clone(),
                registration_activation.clone(),
            )
            .await
            .map_err(StoreRegistrationError::from)?;
        if already_activated {
            return Ok(registration_ref);
        }
        if let Some(prepared) = readiness.registration.prepared_for_creation() {
            let exact = readiness.registration.exact();
            let registration_context = context(ProtocolObjectDomain::StoreDeviceRegistration);
            let registration_prefix = registration_semantic_prefix(&device_id.to_string());
            storage
                .create_verified_protocol_object(
                    &registration_context,
                    prepared,
                    &registration_prefix,
                    &exact.bytes,
                )
                .await
                .map_err(StoreObjectError::from)?;
        }
        if let Some(prepared) = readiness.initial_ack.prepared_for_creation() {
            let exact = readiness.initial_ack.exact();
            let ack_context = context(ProtocolObjectDomain::StoreAck);
            let ack_prefix = ack_slot_prefix(&device_id.to_string(), 1);
            storage
                .create_verified_protocol_object(&ack_context, prepared, &ack_prefix, &exact.bytes)
                .await
                .map_err(StoreObjectError::from)?;
        }
        if let Some(prepared) = node.prepared_for_creation() {
            let exact = node.exact();
            let node_context = context(ProtocolObjectDomain::OwnerRecoveryNode);
            let node_prefix = owner_recovery_semantic_prefix(
                &owner_pubkey,
                authority.owner_grant.clone(),
                sequence,
            );
            storage
                .create_verified_protocol_object(
                    &node_context,
                    prepared,
                    &node_prefix,
                    &exact.bytes,
                )
                .await
                .map_err(StoreObjectError::from)?;
        }
        let PreparedRecoveryReadiness {
            registration: prepared_registration,
            initial_ack: prepared_initial_ack,
        } = readiness;
        let exact_registration = prepared_registration.into_exact();
        let exact_initial_ack = prepared_initial_ack.into_exact();
        database
            .mark_local_store_device_registration_published(
                exact_registration.clone(),
                initial_ack_ref.clone(),
                exact_initial_ack.clone(),
            )
            .await
            .map_err(StoreRegistrationError::from)?;
        database
            .mark_local_store_device_ack_published(
                exact_registration,
                initial_ack_ref,
                exact_initial_ack,
            )
            .await
            .map_err(StoreRegistrationError::from)?;

        let publication = match database
            .owner_recovery_publication()
            .await
            .map_err(StoreRegistrationError::from)?
        {
            Some(publication) => publication,
            None => {
                let predecessor_cut = self
                    .complete_owner_recovery_predecessor_history(routing_encryption)
                    .await?;
                if let Some(adopted) = self
                    .install_activated_owner_recovery(
                        &origin,
                        device_id,
                        recovery_id,
                        &recovery_slot,
                        &owner_pubkey,
                        &authority.owner_grant,
                        sequence,
                        &predecessor,
                    )
                    .await?
                {
                    self.resume_adopted_device_streams(&adopted).await?;
                    return Ok(adopted);
                }
                let stream_id =
                    coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &registration_ref,
                        coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                let order = StoreCommitOrder {
                    seq: 1,
                    predecessor: None,
                    dependencies: predecessor_cut.0,
                };
                let (device_state, predecessor_state) = database
                    .store_device_state_for_order(&order)
                    .await
                    .map_err(StoreRegistrationError::from)?;
                let membership_state = crate::sync::store::commit_verification::merge_history::merge_membership_state_ref(
                    &self.membership,
                    &predecessor_state,
                )?;
                let coord = StoreCommitCoord {
                    stream_id,
                    sequence: 1,
                };
                let activation_ref = ActivatedStoreDeviceRegistrationRef {
                    registration: registration_ref.clone(),
                    authority: StoreDeviceRegistrationActivationRef::Recovery {
                        recovery_id,
                        node: node_ref.clone(),
                    },
                };
                let commit = StoreBatchCommit::signed_operations(
                    root.store_root_hash,
                    coven_protocol::write::WriteId::from_generated(format!(
                        "owner-recovery-{recovery_hash}"
                    )),
                    coord.clone(),
                    registration_ref.clone(),
                    &registration,
                    order,
                    membership_state,
                    device_state,
                    StoreOperationMembershipAuthority {
                        predecessor: self
                            .membership
                            .active_grant(&authority.owner_grant)
                            .ok_or_else(|| {
                                StoreRegistrationError::Invalid(
                                    "Owner recovery grant is absent from active membership"
                                        .to_string(),
                                )
                            })?
                            .creation_authority
                            .clone(),
                    },
                    coven_protocol::store_commit::StoreCommitOperationsInput {
                        device_registrations: vec![activation_ref],
                        ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
                    },
                    &device_signer,
                )
                .map_err(StoreRegistrationError::from)?;
                let commit_prefix = commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    1,
                    commit.commit_hash(),
                );
                let commit_slot = storage
                    .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?;
                let commit_prepared = storage
                    .prepare_protocol_object(
                        &commit_context,
                        commit_slot,
                        &commit_prefix,
                        commit.to_bytes(),
                    )
                    .map_err(StoreObjectError::from)?;
                let commit_ref = StoreBatchCommitRef::from_commit(
                    &commit,
                    coord,
                    commit_prepared.reference().clone(),
                )
                .map_err(StoreRegistrationError::from)?;
                let verified_commit =
                    coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
                        &commit.to_bytes(),
                        root.store_root_hash,
                        &commit_ref,
                        &registration,
                    )
                    .map_err(StoreRegistrationError::from)?;
                let state_after = predecessor_state
                    .activate_registration(
                        registration_ref.clone(),
                        Some(coven_protocol::store_commit::OwnerRecoveryCursor {
                            owner_grant: authority.owner_grant.clone(),
                            position: OwnerRecoveryPosition::At {
                                node: node_ref.clone(),
                            },
                        }),
                    )
                    .map_err(StoreRegistrationError::from)?;
                let prepared_history = self
                    .history
                    .prepare_merge_history_successor(
                        &verified_commit,
                        &self.membership,
                        Some(&registration_ref),
                        state_after,
                        crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence {
                            registrations: vec![
                                coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                                    registration_ref.clone(),
                                    registration.clone(),
                                )
                                .map_err(StoreRegistrationError::from)?,
                            ],
                            acknowledgement: None,
                            membership_proof: None,
                        },
                    )
                    .await
                    .map_err(StoreRegistrationError::from)?;
                let head_context = context(ProtocolObjectDomain::StoreHead);
                let DeviceStreamAnchor::StoreAnnouncements { first_slot: _ } =
                    &registration.store_commits
                else {
                    return Err(StoreRegistrationError::Invalid(
                        "Owner recovery registration has no announcement stream anchor".into(),
                    ));
                };
                let next_head = storage
                    .allocate_protocol_slot(
                        &head_context,
                        &head_slot_prefix(&device_id.to_string(), 2),
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?;
                let head = StoreDeviceHead::signed(
                    root.store_root_hash,
                    registration_ref.clone(),
                    commit_ref.clone(),
                    SuccessorLink {
                        activation: registration
                            .store_announcement_activation(&registration_ref)
                            .map_err(StoreRegistrationError::from)?
                            .activation_id(),
                        predecessor: None,
                        next_slot: next_head,
                    },
                    &device_signer,
                )
                .map_err(StoreRegistrationError::from)?;
                let head_bytes = head.to_bytes();
                let head_prepared = storage
                    .prepare_protocol_object(
                        &head_context,
                        prepared_history.head_slot,
                        &head_slot_prefix(&device_id.to_string(), 1),
                        head_bytes.clone(),
                    )
                    .map_err(StoreObjectError::from)?;
                database
                    .stage_owner_recovery_publication(coven_database::OwnerRecoveryPublication {
                        commit: coven_protocol::objects::ExactProtocolObject {
                            value: verified_commit,
                            bytes: commit.to_bytes(),
                            prepared: commit_prepared,
                        },
                        head: coven_protocol::objects::ExactProtocolObject {
                            value: head,
                            bytes: head_bytes,
                            prepared: head_prepared,
                        },
                        history_evidence: prepared_history.history_evidence,
                    })
                    .await
                    .map_err(StoreRegistrationError::from)?
            }
        };
        let publication_commit = publication.commit.value.value();
        let publication_commit_prefix = commit_semantic_prefix(
            publication_commit.candidate_family(),
            &publication
                .commit
                .value
                .reference()
                .coord
                .stream_id
                .to_string(),
            publication_commit.seq(),
            publication_commit.commit_hash(),
        );
        storage
            .create_verified_protocol_object(
                &commit_context,
                &publication.commit.prepared,
                &publication_commit_prefix,
                &publication.commit.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        let head_context = context(ProtocolObjectDomain::StoreHead);
        let publication_head_prefix = head_slot_prefix(
            &device_id.to_string(),
            publication.head.value.slot_sequence(),
        );
        storage
            .create_verified_protocol_object(
                &head_context,
                &publication.head.prepared,
                &publication_head_prefix,
                &publication.head.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        let registration =
            coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                registration_ref.clone(),
                registration,
            )
            .and_then(|registration| {
                coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                    registration,
                    registration_activation,
                )
            })
            .map_err(StoreRegistrationError::from)?;
        let coven_database::OwnerRecoveryPublication {
            commit,
            head,
            history_evidence,
        } = publication;
        let head_object = head.prepared.reference().clone();
        database
            .complete_owner_recovery(
                commit.value,
                head.value,
                head_object,
                history_evidence,
                registration,
            )
            .await
            .map_err(StoreRegistrationError::from)?;
        Ok(registration_ref)
    }
}
