//! Durable creation and exact opening of the Store protocol root.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::membership::{AuthorHead, MembershipHeadRef};
use super::storage::{ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    ack_slot_prefix, membership_head_slot_prefix, owner_recovery_semantic_prefix, CommitFrontier,
    DeviceStreamAnchor, GrantStreamAnchor, ObjectHash, ResolvedStoreDeviceState, StoreAck,
    StoreAckExclusionState, StoreAckRef, StoreCreationId, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut, StoreProtocolRoot,
    StoreRootRef, SuccessorLink,
};
use super::store_objects::StoreObjectError;

pub(crate) const STORE_CREATION_ATTEMPT_STATE_KEY: &str = "store_creation_attempt_v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreCreationProbeIds {
    exact_slots: super::provider::ProviderProbeId,
}

impl StoreCreationProbeIds {
    pub(crate) fn exact_slots(&self) -> super::provider::ProviderProbeId {
        self.exact_slots
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreCreationAuthority {
    pub creation_id: StoreCreationId,
    pub founder_grant: super::membership::MembershipGrantId,
    pub provider_admin_grant: super::provider::ProviderAdminGrantId,
    pub probes: StoreCreationProbeIds,
    pub binding: super::storage::ResolvedProviderBinding,
    pub access: super::provider::ProviderAccessLocator,
    pub founder_pubkey: String,
    pub founder_timestamp: String,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreRootReservation {
    pub authority: StoreCreationAuthority,
    pub root_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderRegistrationReservation {
    pub root: StoreRootReservation,
    pub registration_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipReservation {
    pub founder: FounderRegistrationReservation,
    pub first_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DescriptorReservation {
    pub membership: MembershipReservation,
    pub recovery_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderMembershipPublicationReservation {
    pub next_head_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderGraphReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub next_ack_slot: crate::storage::cloud::ObjectSlot,
    pub membership: FounderMembershipPublicationReservation,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderStoreCommitsReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderAcknowledgementsReservation {
    pub store_commits: FounderStoreCommitsReservation,
    pub acknowledgements: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderSnapshotsReservation {
    pub acknowledgements: FounderAcknowledgementsReservation,
    pub snapshots: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderNextAckReservation {
    pub snapshots: FounderSnapshotsReservation,
    pub next_ack_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StoreCreationAttempt {
    Initialized(StoreCreationAuthority),
    RootReserved(StoreRootReservation),
    FounderRegistrationReserved(FounderRegistrationReservation),
    MembershipReserved(MembershipReservation),
    DescriptorReserved(DescriptorReservation),
    FounderStoreCommitsReserved(FounderStoreCommitsReservation),
    FounderAcknowledgementsReserved(FounderAcknowledgementsReservation),
    FounderSnapshotsReserved(FounderSnapshotsReservation),
    FounderNextAckReserved(FounderNextAckReservation),
    FounderGraphReserved(FounderGraphReservation),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreProtocolRootError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store protocol root database state: {0}")]
    Database(String),
    #[error("Store protocol root schema version {root_schema} is newer than local schema {local}")]
    SchemaTooNew { root_schema: u32, local: u32 },
    #[error("Store protocol root is missing at {0}")]
    Missing(ObjectHash),
    #[error("Store provider check failed: {0}")]
    Provider(String),
    #[error("{operation}; Store founder rollback failed: {rollback}")]
    Rollback {
        #[source]
        operation: Box<StoreProtocolRootError>,
        rollback: String,
    },
}

fn creation_authority(attempt: &StoreCreationAttempt) -> &StoreCreationAuthority {
    match attempt {
        StoreCreationAttempt::Initialized(authority) => authority,
        StoreCreationAttempt::RootReserved(reservation) => &reservation.authority,
        StoreCreationAttempt::FounderRegistrationReserved(reservation) => {
            &reservation.root.authority
        }
        StoreCreationAttempt::MembershipReserved(reservation) => {
            &reservation.founder.root.authority
        }
        StoreCreationAttempt::DescriptorReserved(reservation) => {
            &reservation.membership.founder.root.authority
        }
        StoreCreationAttempt::FounderStoreCommitsReserved(reservation) => {
            &reservation.descriptor.membership.founder.root.authority
        }
        StoreCreationAttempt::FounderAcknowledgementsReserved(reservation) => {
            &reservation
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderSnapshotsReserved(reservation) => {
            &reservation
                .acknowledgements
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderNextAckReserved(reservation) => {
            &reservation
                .snapshots
                .acknowledgements
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderGraphReserved(reservation) => {
            &reservation.descriptor.membership.founder.root.authority
        }
    }
}

async fn durable_descriptor_reservation(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<DescriptorReservation, StoreProtocolRootError> {
    let binding = super::storage::SyncStorage::provider_binding(storage)
        .await
        .map_err(|error| StoreProtocolRootError::Provider(error.to_string()))?;
    let probes = StoreCreationProbeIds {
        exact_slots: super::provider::ProviderProbeId::from_bytes(
            crate::encryption::generate_random_key(),
        ),
    };
    let access = super::provider::ProviderAccessLocator::for_current_administrator(&binding)
        .map_err(|error| StoreProtocolRootError::Provider(error.to_string()))?;
    let initialized = StoreCreationAttempt::Initialized(StoreCreationAuthority {
        creation_id: StoreCreationId::from_random_bytes(crate::encryption::generate_random_key()),
        founder_grant: super::membership::MembershipGrantId(ObjectHash::from_digest(
            crate::encryption::generate_random_key(),
        )),
        provider_admin_grant: super::provider::ProviderAdminGrantId::from_random_bytes(
            crate::encryption::generate_random_key(),
        ),
        probes,
        binding: binding.clone(),
        access,
        founder_pubkey: crate::keys::public_key_hex(signer),
        founder_timestamp: founder_timestamp.to_string(),
        schema_version: db.schema_version(),
        sync_routing_hash: db.sync_routing_hash(),
    });
    let mut attempt = db
        .begin_store_creation_attempt(initialized)
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let authority = creation_authority(&attempt);
    if authority.binding != binding
        || authority.founder_pubkey != crate::keys::public_key_hex(signer)
        || authority.founder_timestamp != founder_timestamp
        || authority.schema_version != db.schema_version()
        || authority.sync_routing_hash != db.sync_routing_hash()
    {
        return Err(StoreProtocolRootError::Database(
            "durable Store creation authority differs from this creation request".to_string(),
        ));
    }
    let allocation_context = super::storage::ProtocolObjectContext::signed_plaintext(
        ObjectHash::digest(authority.creation_id.to_string().as_bytes()),
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    if let StoreCreationAttempt::Initialized(authority) = &attempt {
        let root_slot = storage
            .allocate_protocol_slot(
                &allocation_context,
                super::store_commit::store_protocol_root_logical_key(),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let next = StoreCreationAttempt::RootReserved(StoreRootReservation {
            authority: authority.clone(),
            root_slot,
        });
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::RootReserved(root) = &attempt {
        let prefix =
            super::store_commit::founder_registration_semantic_prefix(root.authority.creation_id);
        let context = super::storage::ProtocolObjectContext::signed_plaintext(
            allocation_context.store_root_hash(),
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let next =
            StoreCreationAttempt::FounderRegistrationReserved(FounderRegistrationReservation {
                root: root.clone(),
                registration_slot,
            });
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::FounderRegistrationReserved(founder) = &attempt {
        let prefix = super::store_commit::founder_membership_head_semantic_prefix(
            founder.root.authority.creation_id,
        );
        let context = super::storage::ProtocolObjectContext::signed_plaintext(
            allocation_context.store_root_hash(),
            super::storage::ProtocolObjectDomain::StoreMembershipHead,
        );
        let first_slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let membership = MembershipReservation {
            founder: founder.clone(),
            first_slot,
        };
        let next = StoreCreationAttempt::MembershipReserved(membership);
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::MembershipReserved(membership) = &attempt {
        let authority = &membership.founder.root.authority;
        let prefix = owner_recovery_semantic_prefix(
            &authority.founder_pubkey,
            authority.founder_grant.clone(),
            1,
        );
        let context = super::storage::ProtocolObjectContext::signed_plaintext(
            allocation_context.store_root_hash(),
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let recovery_slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let next = StoreCreationAttempt::DescriptorReserved(DescriptorReservation {
            membership: membership.clone(),
            recovery_slot,
        });
        db.advance_store_creation_attempt(attempt, next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        return Ok(match next {
            StoreCreationAttempt::DescriptorReserved(reservation) => reservation,
            _ => unreachable!("constructed descriptor reservation variant"),
        });
    }
    match attempt {
        StoreCreationAttempt::DescriptorReserved(reservation) => Ok(reservation),
        StoreCreationAttempt::FounderStoreCommitsReserved(reservation) => {
            Ok(reservation.descriptor)
        }
        StoreCreationAttempt::FounderAcknowledgementsReserved(reservation) => {
            Ok(reservation.store_commits.descriptor)
        }
        StoreCreationAttempt::FounderSnapshotsReserved(reservation) => {
            Ok(reservation.acknowledgements.store_commits.descriptor)
        }
        StoreCreationAttempt::FounderNextAckReserved(reservation) => Ok(reservation
            .snapshots
            .acknowledgements
            .store_commits
            .descriptor),
        StoreCreationAttempt::FounderGraphReserved(reservation) => Ok(reservation.descriptor),
        _ => Err(StoreProtocolRootError::Database(
            "Store creation attempt did not reach descriptor reservation".to_string(),
        )),
    }
}

async fn durable_founder_graph_reservation(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    descriptor: &DescriptorReservation,
    root: &StoreRootRef,
) -> Result<FounderGraphReservation, StoreProtocolRootError> {
    let mut attempt = db
        .load_store_creation_attempt()
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
        .ok_or_else(|| {
            StoreProtocolRootError::Database("Store creation attempt is absent".to_string())
        })?;
    if creation_authority(&attempt) != &descriptor.membership.founder.root.authority {
        return Err(StoreProtocolRootError::Database(
            "founder graph reservation belongs to another descriptor".to_string(),
        ));
    }
    let authority = &descriptor.membership.founder.root.authority;
    let origin = StoreDeviceRegistrationOrigin::Founder {
        creation_id: authority.creation_id,
    };
    let device = super::store_commit::StoreDeviceId::derive(root, &origin).to_string();
    let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );

    if let StoreCreationAttempt::DescriptorReserved(current) = &attempt {
        if current != descriptor {
            return Err(StoreProtocolRootError::Database(
                "founder graph reservation belongs to another descriptor".to_string(),
            ));
        }
        let store_commits = DeviceStreamAnchor::StoreAnnouncements {
            first_slot: storage
                .allocate_protocol_slot(
                    &super::storage::ProtocolObjectContext::signed_plaintext(
                        root.store_root_hash,
                        ProtocolObjectDomain::StoreHead,
                    ),
                    &super::store_commit::head_slot_prefix(&device, 1),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?,
        };
        let next =
            StoreCreationAttempt::FounderStoreCommitsReserved(FounderStoreCommitsReservation {
                descriptor: descriptor.clone(),
                store_commits,
            });
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::FounderStoreCommitsReserved(current) = &attempt {
        let next = StoreCreationAttempt::FounderAcknowledgementsReserved(
            FounderAcknowledgementsReservation {
                store_commits: current.clone(),
                acknowledgements: DeviceStreamAnchor::StoreAcknowledgements {
                    first_slot: storage
                        .allocate_protocol_slot(&ack_context, &ack_slot_prefix(&device, 1), ".json")
                        .await
                        .map_err(StoreObjectError::from)?,
                },
            },
        );
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::FounderAcknowledgementsReserved(current) = &attempt {
        let next = StoreCreationAttempt::FounderSnapshotsReserved(FounderSnapshotsReservation {
            acknowledgements: current.clone(),
            snapshots: DeviceStreamAnchor::StoreSnapshots {
                first_slot: storage
                    .allocate_protocol_slot(
                        &super::storage::ProtocolObjectContext::signed_plaintext(
                            root.store_root_hash,
                            ProtocolObjectDomain::StoreSnapshotMeta,
                        ),
                        &super::store_commit::snapshot_slot_prefix(&device, 0),
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?,
            },
        });
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::FounderSnapshotsReserved(current) = &attempt {
        let next = StoreCreationAttempt::FounderNextAckReserved(FounderNextAckReservation {
            snapshots: current.clone(),
            next_ack_slot: storage
                .allocate_protocol_slot(&ack_context, &ack_slot_prefix(&device, 2), ".json")
                .await
                .map_err(StoreObjectError::from)?,
        });
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::FounderNextAckReserved(current) = &attempt {
        let founder_stream = super::membership::derive_founder_stream_id(
            &root.store_root_id.to_string(),
            &authority.founder_pubkey,
        );
        let prefix = membership_head_slot_prefix(
            &authority.founder_pubkey,
            &authority.founder_grant,
            founder_stream,
            2,
        );
        let membership = FounderMembershipPublicationReservation {
            next_head_slot: storage
                .allocate_protocol_slot(
                    &super::storage::ProtocolObjectContext::signed_plaintext(
                        root.store_root_hash,
                        super::storage::ProtocolObjectDomain::StoreMembershipHead,
                    ),
                    &prefix,
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?,
        };
        let reservation = FounderGraphReservation {
            descriptor: current
                .snapshots
                .acknowledgements
                .store_commits
                .descriptor
                .clone(),
            store_commits: current
                .snapshots
                .acknowledgements
                .store_commits
                .store_commits
                .clone(),
            acknowledgements: current.snapshots.acknowledgements.acknowledgements.clone(),
            snapshots: current.snapshots.snapshots.clone(),
            next_ack_slot: current.next_ack_slot.clone(),
            membership,
        };
        let next = StoreCreationAttempt::FounderGraphReserved(reservation.clone());
        db.advance_store_creation_attempt(attempt, next)
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        return Ok(reservation);
    }
    match attempt {
        StoreCreationAttempt::FounderGraphReserved(reservation) => Ok(reservation),
        _ => Err(StoreProtocolRootError::Database(
            "Store creation attempt regressed before founder graph reservation".to_string(),
        )),
    }
}

async fn prepare_founder_graph(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<Box<crate::database::DurableFounderGraph>, StoreProtocolRootError> {
    let reservation =
        durable_descriptor_reservation(db, storage, founder_timestamp, signer).await?;
    let authority = &reservation.membership.founder.root.authority;
    let (first_exact, second_exact) = storage.exact_slot_probe_clients();
    let exact_slots = super::provider::probe_exact_slots(
        first_exact,
        second_exact,
        db,
        authority.probes.exact_slots,
        &authority.binding,
    )
    .await
    .map_err(|error| StoreProtocolRootError::Provider(error.to_string()))?;
    let provider_admin = super::provider::FounderProviderAdminGrant {
        grant_id: authority.provider_admin_grant.clone(),
        provider: authority.binding.device.clone(),
        access: authority.access.clone(),
        capability: super::provider::ProviderCapabilityProof { exact_slots },
    };
    let founder_anchor = GrantStreamAnchor::StoreMembership {
        first_slot: reservation.membership.first_slot.clone(),
    };
    let founder_recovery = GrantStreamAnchor::OwnerRecovery {
        first_slot: reservation.recovery_slot.clone(),
    };
    let descriptor = super::store_commit::StoreCreationDescriptor {
        version: super::store_commit::STORE_PROTOCOL_VERSION,
        creation_id: authority.creation_id,
        provider: authority.binding.store.clone(),
        schema_version: authority.schema_version,
        sync_routing_hash: authority.sync_routing_hash,
        founder_pubkey: authority.founder_pubkey.clone(),
        founder_grant: authority.founder_grant.clone(),
        root_slot: reservation.membership.founder.root.root_slot.clone(),
        founder_registration: reservation.membership.founder.registration_slot.clone(),
        founder_provider_admin: provider_admin.clone(),
        founder_membership: founder_anchor.clone(),
        founder_recovery,
    };
    let root_value = StoreProtocolRoot::signed(descriptor, signer)
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let root_id = root_value.descriptor.store_root_id();
    let root_hash = root_value.object_hash();
    let root_prefix = super::store_commit::store_protocol_root_logical_key();
    let root_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root_hash,
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    let root_bytes = root_value.to_bytes();
    let root_prepared = storage
        .prepare_protocol_object(
            &root_context,
            root_value.descriptor.root_slot.clone(),
            root_prefix,
            root_bytes.clone(),
        )
        .map_err(StoreObjectError::from)?;
    let root_ref = StoreRootRef {
        store_root_id: root_id,
        store_root_hash: root_hash,
        object: root_prepared.reference().clone(),
    };
    let graph_reservation =
        durable_founder_graph_reservation(db, storage, &reservation, &root_ref).await?;
    let origin = StoreDeviceRegistrationOrigin::Founder {
        creation_id: authority.creation_id,
    };
    let (registration_value, registration_prepared) =
        super::store::prepare_registration_for_origin(
            storage,
            signer,
            root_ref.clone(),
            origin,
            root_value.descriptor.founder_registration.clone(),
            authority.binding.device.clone(),
            graph_reservation.store_commits.clone(),
            graph_reservation.acknowledgements.clone(),
            graph_reservation.snapshots.clone(),
        )
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let registration_bytes = registration_value.to_bytes();
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &registration_value,
        registration_prepared.reference().clone(),
    );
    let device_signer = registration_value
        .device_signer(signer)
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
        &registration_value.acknowledgements
    else {
        return Err(StoreProtocolRootError::Database(
            "founder registration has no acknowledgement anchor".to_string(),
        ));
    };
    let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let next_ack_slot = graph_reservation.next_ack_slot.clone();
    let frontier = StoreHistoryCut::from_commits(Default::default());
    let resolved_devices = ResolvedStoreDeviceState::founder(
        &root_ref,
        registration_ref.clone(),
        &root_value.descriptor.founder_pubkey,
        root_value.descriptor.founder_grant.clone(),
        &root_value.descriptor.founder_recovery,
    )
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let device_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.0.clone()), &resolved_devices)
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let exclusions = StoreAckExclusionState {
        proposal_freezes: Vec::new(),
    };
    let acknowledgement_activation = registration_value
        .store_acknowledgement_activation(&registration_ref)
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
        .activation_id();
    let initial_ack_value = StoreAck::signed(
        root_hash,
        registration_ref.clone(),
        1,
        frontier,
        device_state,
        None,
        exclusions,
        founder_timestamp.to_string(),
        SuccessorLink {
            activation: acknowledgement_activation,
            predecessor: None,
            next_slot: next_ack_slot,
        },
        &device_signer,
    )
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let initial_ack_bytes = initial_ack_value.to_bytes();
    let initial_ack_prepared = storage
        .prepare_protocol_object(
            &ack_context,
            first_slot.clone(),
            &ack_slot_prefix(&registration_value.device_id.to_string(), 1),
            initial_ack_bytes.clone(),
        )
        .map_err(StoreObjectError::from)?;
    let initial_ack_ref = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: 1,
        ack_hash: initial_ack_value.ack_hash(),
        object: initial_ack_prepared.reference().clone(),
    };
    let membership = {
        let founder_head_slot = reservation.membership.first_slot.clone();
        let founder_grant = authority.founder_grant.clone();
        let founder = super::membership::founder_entry_for_creation(
            &root_id.to_string(),
            root_value.descriptor.creation_id,
            signer,
            founder_grant.clone(),
            founder_timestamp,
            founder_anchor.clone(),
            provider_admin,
        );
        let (entry_prepared, entry_ref) =
            super::store_objects::prepare_membership_entry(storage, root_hash, &founder).await?;
        let head_context = super::storage::ProtocolObjectContext::signed_plaintext(
            root_hash,
            super::storage::ProtocolObjectDomain::StoreMembershipHead,
        );
        let next_head_slot = &graph_reservation.membership.next_head_slot;
        let head_value = AuthorHead::signed(
            root_id.to_string(),
            super::membership::MembershipHeadBody {
                author_registration: registration_ref.clone(),
                entry: entry_ref.clone(),
                predecessor: None,
                resolutions: Vec::new(),
                successor: SuccessorLink {
                    activation: super::store_commit::StreamActivation::grant_authorized(
                        root_ref.store_root_hash,
                        registration_ref.clone(),
                        founder_grant.clone(),
                        founder_anchor.clone(),
                    )
                    .activation_id(),
                    predecessor: None,
                    next_slot: next_head_slot.clone(),
                },
            },
            super::membership::MembershipHeadActivation::Direct,
            &device_signer,
        );
        let head_bytes = serde_json::to_vec(&head_value)
            .expect("founder membership head serialization cannot fail");
        let founder_head_prefix =
            super::store_commit::founder_membership_head_semantic_prefix(authority.creation_id);
        let head_prepared = storage
            .prepare_protocol_object(
                &head_context,
                founder_head_slot,
                &founder_head_prefix,
                head_bytes.clone(),
            )
            .map_err(StoreObjectError::from)?;
        let head_ref = MembershipHeadRef {
            coord: founder.coord(),
            head_hash: head_value.head_hash(),
            object: head_prepared.reference().clone(),
        };
        let founder_bytes = serde_json::to_vec(&founder)
            .expect("founder membership entry serialization cannot fail");
        crate::database::DurableFounderMembership {
            entry: crate::database::ExactProtocolObject {
                value: founder,
                bytes: founder_bytes,
                object: entry_prepared.reference().clone(),
                prepared: entry_prepared,
            },
            entry_ref,
            head: crate::database::ExactProtocolObject {
                value: head_value,
                bytes: head_bytes,
                object: head_prepared.reference().clone(),
                prepared: head_prepared,
            },
            head_ref,
        }
    };
    Ok(Box::new(crate::database::DurableFounderGraph {
        root: crate::database::ExactProtocolObject {
            value: root_value,
            bytes: root_bytes,
            object: root_prepared.reference().clone(),
            prepared: root_prepared,
        },
        registration: crate::database::ExactProtocolObject {
            value: registration_value,
            bytes: registration_bytes,
            object: registration_prepared.reference().clone(),
            prepared: registration_prepared,
        },
        initial_ack: crate::database::ExactProtocolObject {
            value: initial_ack_value,
            bytes: initial_ack_bytes,
            object: initial_ack_prepared.reference().clone(),
            prepared: initial_ack_prepared,
        },
        initial_ack_ref,
        membership,
        registration_state: crate::database::LocalDeviceRegistrationState::Prepared,
    }))
}

async fn rollback_founder_exact_objects(
    storage: &dyn SyncStorage,
    graph: &crate::database::DurableFounderGraph,
) -> Result<(), String> {
    let mut objects = vec![
        graph.membership.head.object.clone(),
        graph.membership.entry.object.clone(),
    ];
    objects.extend([
        graph.initial_ack.object.clone(),
        graph.registration.object.clone(),
        graph.root.object.clone(),
    ]);
    let mut failures = Vec::new();
    for object in objects {
        match super::store_objects::delete_exact_object(storage, &object).await {
            Ok(())
            | Err(StoreObjectError::Storage(super::storage::StorageError::SlotCollision(_))) => {}
            Err(error) => failures.push(format!("{}: {error}", object.slot().logical_key())),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

async fn rollback_founder_publication(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    graph: &crate::database::DurableFounderGraph,
) -> Result<(), String> {
    let mut failures = Vec::new();
    if let Err(error) = rollback_founder_exact_objects(storage, graph).await {
        failures.push(error);
    }
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }
    db.reset_store_founder_graph_publication(graph)
        .await
        .map_err(|error| error.to_string())
}

pub async fn create_store(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let _creation = db.lock_store_creation().await;
    let mut graph = match db
        .local_store_founder_graph()
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
    {
        Some(graph) => graph,
        None => {
            let graph = Box::pin(prepare_founder_graph(
                db,
                storage,
                founder_timestamp,
                signer,
            ))
            .await?;
            db.stage_store_founder_graph(graph)
                .await
                .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
            db.local_store_founder_graph()
                .await
                .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
                .ok_or_else(|| {
                    StoreProtocolRootError::Database(
                        "staged Store founder graph is absent".to_string(),
                    )
                })?
        }
    };
    let rollback_allowed = match &graph.registration_state {
        crate::database::LocalDeviceRegistrationState::Prepared
        | crate::database::LocalDeviceRegistrationState::Created => true,
        crate::database::LocalDeviceRegistrationState::Activated { .. } => false,
    };
    if rollback_allowed {
        Box::pin(rollback_founder_publication(db, storage, &graph))
            .await
            .map_err(|rollback| {
                StoreProtocolRootError::Database(format!(
                    "Store founder rollback before publication: {rollback}"
                ))
            })?;
        graph = db
            .local_store_founder_graph()
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
            .ok_or_else(|| {
                StoreProtocolRootError::Database(
                    "rolled-back Store founder graph is absent".to_string(),
                )
            })?;
    }
    match Box::pin(publish_store_founder_graph(
        db,
        storage,
        founder_timestamp,
        signer,
        &graph,
    ))
    .await
    {
        Ok(root) => Ok(root),
        Err(operation) if rollback_allowed => {
            match Box::pin(rollback_founder_publication(db, storage, &graph)).await {
                Ok(()) => Err(operation),
                Err(rollback) => Err(StoreProtocolRootError::Rollback {
                    operation: Box::new(operation),
                    rollback,
                }),
            }
        }
        Err(operation) => Err(operation),
    }
}

async fn publish_store_founder_graph(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    founder_timestamp: &str,
    signer: &UserKeypair,
    graph: &crate::database::DurableFounderGraph,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let root_ref = StoreRootRef {
        store_root_id: graph.root.value.descriptor.store_root_id(),
        store_root_hash: graph.root.value.object_hash(),
        object: graph.root.object.clone(),
    };
    if graph.initial_ack.value.last_sync != founder_timestamp {
        return Err(StoreProtocolRootError::Database(
            "durable Store founder timestamp differs from this creation request".to_string(),
        ));
    }
    let store_protocol_root =
        StoreProtocolRoot::parse_expected(&graph.root.bytes, &root_ref, db.sync_routing_hash())
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    if store_protocol_root.descriptor.founder_pubkey != crate::keys::public_key_hex(signer) {
        return Err(StoreProtocolRootError::Database(
            "durable Store founder differs from the creation signer".to_string(),
        ));
    }
    if store_protocol_root.descriptor.schema_version > db.schema_version() {
        return Err(StoreProtocolRootError::SchemaTooNew {
            root_schema: store_protocol_root.descriptor.schema_version,
            local: db.schema_version(),
        });
    }
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &graph.registration.value,
        graph.registration.object.clone(),
    );
    storage
        .create_protocol_object(&graph.root.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let opened_root = super::store_objects::load_store_protocol_root(storage, &root_ref)
        .await?
        .value;
    if opened_root != store_protocol_root {
        return Err(StoreProtocolRootError::Missing(root_ref.store_root_hash));
    }
    storage
        .create_protocol_object(&graph.registration.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let registration =
        super::store_objects::load_registration_ref(storage, &root_ref, &registration_ref)
            .await?
            .value;
    if registration != graph.registration.value {
        return Err(StoreProtocolRootError::Database(
            "founder registration readback differs from durable bytes".to_string(),
        ));
    }
    storage
        .create_protocol_object(&graph.initial_ack.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let initial_ack = super::store_objects::load_store_ack_ref(
        storage,
        &root_ref,
        &graph.initial_ack_ref,
        &registration,
    )
    .await?
    .value;
    if initial_ack != graph.initial_ack.value {
        return Err(StoreProtocolRootError::Database(
            "founder initial acknowledgement readback differs from durable bytes".to_string(),
        ));
    }
    if !matches!(
        &graph.registration_state,
        crate::database::LocalDeviceRegistrationState::Activated { .. }
    ) {
        db.mark_local_store_device_registration_created(
            graph.registration.clone(),
            graph.initial_ack_ref.clone(),
            graph.initial_ack.clone(),
        )
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    }
    let membership = &graph.membership;
    storage
        .create_protocol_object(&membership.entry.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let loaded_entry = super::store_objects::load_membership_entry_ref(
        storage,
        root_ref.store_root_hash,
        &membership.entry_ref,
    )
    .await?
    .value;
    if loaded_entry != membership.entry.value {
        return Err(StoreProtocolRootError::Database(
            "founder membership entry readback differs from durable bytes".to_string(),
        ));
    }
    storage
        .create_protocol_object(&membership.head.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let loaded_head = super::store_objects::load_membership_head_ref(
        storage,
        root_ref.store_root_hash,
        &membership.head_ref,
        &registration,
    )
    .await?
    .value;
    if loaded_head != membership.head.value {
        return Err(StoreProtocolRootError::Database(
            "founder membership head readback differs from durable bytes".to_string(),
        ));
    }
    let membership_refs = crate::database::FounderMembershipRefs {
        entry: membership.entry_ref.clone(),
        head: membership.head_ref.clone(),
    };
    db.complete_store_founder_graph(
        root_ref,
        registration_ref,
        graph.initial_ack_ref.clone(),
        membership_refs,
    )
    .await
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    Ok(store_protocol_root)
}

pub async fn open_store(
    db: &Database,
    storage: &dyn SyncStorage,
    expected: &StoreRootRef,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let context = super::storage::ProtocolObjectContext::signed_plaintext(
        expected.store_root_hash,
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    let bytes = storage
        .read_protocol_object(
            &context,
            &expected.object,
            super::store_commit::store_protocol_root_logical_key(),
        )
        .await
        .map_err(StoreObjectError::from)?;
    let verified = StoreProtocolRoot::parse_expected(&bytes, expected, db.sync_routing_hash())
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let live_binding = storage
        .provider_binding()
        .await
        .map_err(|error| StoreProtocolRootError::Provider(error.to_string()))?;
    if live_binding.store != verified.descriptor.provider {
        return Err(StoreProtocolRootError::Database(
            "live provider namespace differs from the signed Store root".to_string(),
        ));
    }
    if let Some(local) = crate::sync::store::database::StoreDatabase::new(db)
        .latest_local_store_device_registration()
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
        .filter(|registration| registration.is_activated())
    {
        let registration = super::store_commit::StoreDeviceRegistration::parse_at(
            &local.registration_bytes,
            expected,
            local.device_id,
        )
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        if registration.provider != live_binding.device {
            return Err(StoreProtocolRootError::Database(
                "live provider principal differs from the active Store registration".to_string(),
            ));
        }
    }
    if verified.descriptor.schema_version > db.schema_version() {
        return Err(StoreProtocolRootError::SchemaTooNew {
            root_schema: verified.descriptor.schema_version,
            local: db.schema_version(),
        });
    }
    db.install_store_root_authority(expected.clone(), bytes)
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::test_helpers::{open_test_db, test_migrations, test_synced_tables};

    #[tokio::test]
    async fn created_merge_store_immediately_has_its_exact_founder_chain() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "exact-founder-graph",
            founder.clone(),
        )
        .expect("construct exact founder storage");
        let db = open_test_db();

        let root = create_store(&db, &storage, "0000000000001-0000-founder", &founder)
            .await
            .expect("create Store with founder graph");
        let root_ref = db
            .local_store_root_ref()
            .await
            .expect("read exact Store root")
            .expect("created Store root exists");

        super::super::cycle::ensure_owner_anchored_chain(&storage, &db, &root_ref, &root, &founder)
            .await
            .expect("created Store founder chain is immediately readable");
    }

    #[tokio::test]
    async fn merge_store_creation_failure_removes_every_founder_object_before_returning() {
        for failing_create in 1..=5 {
            let home = InMemoryCloudHome::new();
            let founder = UserKeypair::generate();
            let storage = CloudSyncStorage::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                format!("founder-rollback-{failing_create}"),
                founder.clone(),
            )
            .expect("construct founder rollback storage");
            let db = open_test_db();
            let timestamp = "0000000000001-0000-founder";
            let graph = prepare_founder_graph(&db, &storage, timestamp, &founder)
                .await
                .expect("prepare exact founder graph");
            db.stage_store_founder_graph(graph.clone())
                .await
                .expect("stage exact founder graph");
            let mut exact_objects = vec![
                graph.root.object.clone(),
                graph.registration.object.clone(),
                graph.initial_ack.object.clone(),
            ];
            let entry = &graph.membership.entry;
            let head = &graph.membership.head;
            exact_objects.push(entry.object.clone());
            exact_objects.push(head.object.clone());
            home.fail_exact_create_before_call(failing_create);

            create_store(&db, &storage, timestamp, &founder)
                .await
                .expect_err("injected founder publication failure must abort creation");

            for object in &exact_objects {
                assert!(
                    home.get(object.slot().logical_key()).is_none(),
                    "founder object {} remains after create call {failing_create} failed",
                    object.slot().logical_key(),
                );
            }
            create_store(&db, &storage, timestamp, &founder)
                .await
                .expect("retry creates the Store after complete rollback");
        }
    }

    #[tokio::test]
    async fn failed_founder_rollback_is_resumed_before_publication_retry() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "founder-rollback-retry",
            founder.clone(),
        )
        .expect("construct founder rollback retry storage");
        let temp = tempfile::tempdir().expect("create founder rollback database directory");
        let path = temp.path().join("founder-rollback.sqlite");
        let open = || {
            Database::open(
                &path,
                test_synced_tables(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "founder-rollback-device".to_string(),
                &test_migrations(),
            )
            .expect("open founder rollback database")
            .0
        };
        let db = open();
        let timestamp = "0000000000001-0000-founder";
        let graph = prepare_founder_graph(&db, &storage, timestamp, &founder)
            .await
            .expect("prepare exact founder graph");
        db.stage_store_founder_graph(graph.clone())
            .await
            .expect("stage exact founder graph");
        home.fail_exact_create_before_call(3);
        home.fail_exact_delete_on_call(1);

        let failure = create_store(&db, &storage, timestamp, &founder)
            .await
            .expect_err("failed exact deletion must fail the creation call");
        assert!(matches!(failure, StoreProtocolRootError::Rollback { .. }));
        drop(db);
        let db = open();

        create_store(&db, &storage, timestamp, &founder)
            .await
            .expect("retry resumes rollback before publishing the founder graph");
    }

    #[tokio::test]
    async fn concurrent_store_creation_calls_do_not_rollback_each_other() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = Arc::new(
            CloudSyncStorage::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "concurrent-founder-publication",
                founder.clone(),
            )
            .expect("construct concurrent founder storage"),
        );
        let db = open_test_db();
        let timestamp = "0000000000001-0000-founder";
        let graph = prepare_founder_graph(&db, &storage, timestamp, &founder)
            .await
            .expect("prepare exact founder graph");
        db.stage_store_founder_graph(graph)
            .await
            .expect("stage exact founder graph");
        let deletes_before = home.deletes_seen();
        let (reached, release) = home.pause_after_exact_create_call(1);
        let first_db = db.clone();
        let first_storage = storage.clone();
        let first_founder = founder.clone();
        let first = tokio::spawn(async move {
            create_store(&first_db, &first_storage, timestamp, &first_founder).await
        });
        reached.notified().await;
        let second_db = db.clone();
        let second_storage = storage.clone();
        let second_founder = founder.clone();
        let second = tokio::spawn(async move {
            create_store(&second_db, &second_storage, timestamp, &second_founder).await
        });
        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "second creation call bypassed founder publication serialization",
        );
        release.notify_one();

        first
            .await
            .expect("first creation task joins")
            .expect("first creation call succeeds");
        second
            .await
            .expect("second creation task joins")
            .expect("second creation call observes the activated founder graph");
        assert_eq!(home.deletes_seen(), deletes_before);
    }

    #[tokio::test]
    async fn founder_rollback_preserves_a_different_object_in_the_reserved_slot() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "founder-rollback-slot-collision",
            founder.clone(),
        )
        .expect("construct founder collision storage");
        let db = open_test_db();
        let timestamp = "0000000000001-0000-founder";
        let graph = prepare_founder_graph(&db, &storage, timestamp, &founder)
            .await
            .expect("prepare exact founder graph");
        db.stage_store_founder_graph(graph.clone())
            .await
            .expect("stage exact founder graph");
        let competing = b"different Store root occupant".to_vec();
        home.insert_exact_object(graph.root.object.slot().logical_key(), competing.clone());

        create_store(&db, &storage, timestamp, &founder)
            .await
            .expect_err("different root occupant must prevent Store creation");

        assert_eq!(
            home.get(graph.root.object.slot().logical_key()),
            Some(competing),
            "founder rollback erased a different object in the reserved root slot",
        );
    }

    #[tokio::test]
    async fn opaque_store_reopens_exact_founder_root_registration_and_ack() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key([41; 32])),
            BlobPathScheme::Hashed,
            "opaque-founder-graph",
            founder.clone(),
        )
        .expect("construct opaque founder storage");
        let db = open_test_db();

        create_store(&db, &storage, "0000000000001-0000-opaque-founder", &founder)
            .await
            .expect("create opaque Store");
        let root_ref = db
            .local_store_root_ref()
            .await
            .expect("read Store root reference")
            .expect("Store root exists");
        let root = super::super::store_objects::load_store_protocol_root(&storage, &root_ref)
            .await
            .expect("open exact opaque root");
        let registration =
            super::super::store_objects::load_founder_registration(&storage, &root_ref)
                .await
                .expect("open exact opaque founder registration");
        let durable = crate::sync::store::database::StoreDatabase::new(&db)
            .latest_local_store_device_registration()
            .await
            .expect("read durable founder registration")
            .expect("founder registration exists");
        let ack = super::super::store_objects::load_store_ack_ref(
            &storage,
            &root_ref,
            &durable.initial_ack_ref,
            &registration.value,
        )
        .await
        .expect("open exact opaque founder acknowledgement");

        assert_eq!(root.value.object_hash(), root_ref.store_root_hash);
        assert_eq!(registration.value.device_id, durable.device_id);
        assert_eq!(ack.value, durable.initial_ack.value);
    }
}
