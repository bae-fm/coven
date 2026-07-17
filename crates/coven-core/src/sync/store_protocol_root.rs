//! Durable creation and exact opening of the Store protocol root.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::membership::{AuthorHead, MembershipHeadRef};
use super::storage::SyncStorage;
use super::store_commit::{
    ack_slot_prefix, membership_head_slot_prefix, owner_recovery_semantic_prefix,
    DeviceStreamAnchor, GrantStreamAnchor, ObjectHash, StoreAck, StoreAckRef, StoreCreationId,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreHistoryCut, StoreProtocolRoot,
    StoreRootRef, StoreSerialPredecessor, StreamActivationId, SuccessorLink,
};
use super::store_objects::StoreObjectError;
use crate::WritePolicy;

pub(crate) const STORE_CREATION_ATTEMPT_STATE_KEY: &str = "store_creation_attempt_v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StoreCreationProbeIds {
    MergeConcurrent {
        exact_slots: super::provider::ProviderProbeId,
    },
    Serial {
        exact_slots: super::provider::ProviderProbeId,
        serial_coordination: super::provider::ProviderProbeId,
    },
}

impl StoreCreationProbeIds {
    fn exact_slots(&self) -> super::provider::ProviderProbeId {
        match self {
            Self::MergeConcurrent { exact_slots } | Self::Serial { exact_slots, .. } => {
                *exact_slots
            }
        }
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
    pub write_policy: WritePolicy,
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
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MembershipReservation {
    MergeConcurrent {
        founder: FounderRegistrationReservation,
        first_slot: crate::storage::cloud::ObjectSlot,
    },
    Serial {
        founder: FounderRegistrationReservation,
    },
}

impl MembershipReservation {
    pub(crate) fn founder(&self) -> &FounderRegistrationReservation {
        match self {
            Self::MergeConcurrent { founder, .. } | Self::Serial { founder } => founder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DescriptorReservation {
    pub membership: MembershipReservation,
    pub recovery_slot: crate::storage::cloud::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FounderMembershipPublicationReservation {
    MergeConcurrent {
        next_head_slot: crate::storage::cloud::ObjectSlot,
    },
    Serial,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderGraphReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: super::store_commit::StoreCommitAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub next_ack_slot: crate::storage::cloud::ObjectSlot,
    pub membership: FounderMembershipPublicationReservation,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderStoreCommitsReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: super::store_commit::StoreCommitAnchor,
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
    #[error("Serial Store coordination check failed: {0}")]
    Coordination(String),
}

fn creation_authority(attempt: &StoreCreationAttempt) -> &StoreCreationAuthority {
    match attempt {
        StoreCreationAttempt::Initialized(authority) => authority,
        StoreCreationAttempt::RootReserved(reservation) => &reservation.authority,
        StoreCreationAttempt::FounderRegistrationReserved(reservation) => {
            &reservation.root.authority
        }
        StoreCreationAttempt::MembershipReserved(reservation) => {
            &reservation.founder().root.authority
        }
        StoreCreationAttempt::DescriptorReserved(reservation) => {
            &reservation.membership.founder().root.authority
        }
        StoreCreationAttempt::FounderStoreCommitsReserved(reservation) => {
            &reservation.descriptor.membership.founder().root.authority
        }
        StoreCreationAttempt::FounderAcknowledgementsReserved(reservation) => {
            &reservation
                .store_commits
                .descriptor
                .membership
                .founder()
                .root
                .authority
        }
        StoreCreationAttempt::FounderSnapshotsReserved(reservation) => {
            &reservation
                .acknowledgements
                .store_commits
                .descriptor
                .membership
                .founder()
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
                .founder()
                .root
                .authority
        }
        StoreCreationAttempt::FounderGraphReserved(reservation) => {
            &reservation.descriptor.membership.founder().root.authority
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
        .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
    let write_policy = db.write_policy();
    let probes = match write_policy {
        WritePolicy::MergeConcurrent => StoreCreationProbeIds::MergeConcurrent {
            exact_slots: super::provider::ProviderProbeId::from_bytes(
                crate::encryption::generate_random_key(),
            ),
        },
        WritePolicy::Serial => StoreCreationProbeIds::Serial {
            exact_slots: super::provider::ProviderProbeId::from_bytes(
                crate::encryption::generate_random_key(),
            ),
            serial_coordination: super::provider::ProviderProbeId::from_bytes(
                crate::encryption::generate_random_key(),
            ),
        },
    };
    let access = super::provider::ProviderAccessLocator::for_current_administrator(&binding)
        .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
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
        write_policy,
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
        || authority.write_policy != write_policy
        || authority.schema_version != db.schema_version()
        || authority.sync_routing_hash != db.sync_routing_hash()
    {
        return Err(StoreProtocolRootError::Database(
            "durable Store creation authority differs from this creation request".to_string(),
        ));
    }
    let allocation_context = super::storage::ProtocolObjectContext::store(
        ObjectHash::digest(authority.creation_id.to_string().as_bytes()),
        super::storage::ProtocolObjectDomain::StoreProtocolRoot,
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
        let context = super::storage::ProtocolObjectContext::store(
            allocation_context.store_root_hash(),
            super::storage::ProtocolObjectDomain::StoreDeviceRegistration,
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
        let membership = match founder.root.authority.write_policy {
            WritePolicy::MergeConcurrent => {
                let prefix = super::store_commit::founder_membership_head_semantic_prefix(
                    founder.root.authority.creation_id,
                );
                let context = super::storage::ProtocolObjectContext::store(
                    allocation_context.store_root_hash(),
                    super::storage::ProtocolObjectDomain::StoreMembershipHead,
                );
                let first_slot = storage
                    .allocate_protocol_slot(&context, &prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?;
                MembershipReservation::MergeConcurrent {
                    founder: founder.clone(),
                    first_slot,
                }
            }
            WritePolicy::Serial => MembershipReservation::Serial {
                founder: founder.clone(),
            },
        };
        let next = StoreCreationAttempt::MembershipReserved(membership);
        db.advance_store_creation_attempt(attempt.clone(), next.clone())
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        attempt = next;
    }
    if let StoreCreationAttempt::MembershipReserved(membership) = &attempt {
        let authority = &membership.founder().root.authority;
        let prefix = owner_recovery_semantic_prefix(
            &authority.founder_pubkey,
            authority.founder_grant.clone(),
            1,
        );
        let context = super::storage::ProtocolObjectContext::store(
            allocation_context.store_root_hash(),
            super::storage::ProtocolObjectDomain::OwnerRecoveryNode,
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
    if creation_authority(&attempt) != &descriptor.membership.founder().root.authority {
        return Err(StoreProtocolRootError::Database(
            "founder graph reservation belongs to another descriptor".to_string(),
        ));
    }
    let authority = &descriptor.membership.founder().root.authority;
    let origin = StoreDeviceRegistrationOrigin::Founder {
        creation_id: authority.creation_id,
    };
    let device = super::store_commit::StoreDeviceId::derive(root, &origin).to_string();
    let ack_context = super::storage::ProtocolObjectContext::store(
        root.store_root_hash,
        super::storage::ProtocolObjectDomain::StoreAck,
    );

    if let StoreCreationAttempt::DescriptorReserved(current) = &attempt {
        if current != descriptor {
            return Err(StoreProtocolRootError::Database(
                "founder graph reservation belongs to another descriptor".to_string(),
            ));
        }
        let store_commits = match authority.write_policy {
            WritePolicy::MergeConcurrent => {
                super::store_commit::StoreCommitAnchor::MergeConcurrent {
                    announcements: DeviceStreamAnchor::StoreAnnouncements {
                        first_slot: storage
                            .allocate_protocol_slot(
                                &super::storage::ProtocolObjectContext::store(
                                    root.store_root_hash,
                                    super::storage::ProtocolObjectDomain::StoreHead,
                                ),
                                &super::store_commit::head_slot_prefix(&device, 1),
                                ".json",
                            )
                            .await
                            .map_err(StoreObjectError::from)?,
                    },
                }
            }
            WritePolicy::Serial => super::store_commit::StoreCommitAnchor::Serial,
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
                        &super::storage::ProtocolObjectContext::store(
                            root.store_root_hash,
                            super::storage::ProtocolObjectDomain::StoreSnapshotMeta,
                        ),
                        &super::store_commit::snapshot_slot_prefix(&device, 1),
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
        let membership = match authority.write_policy {
            WritePolicy::MergeConcurrent => {
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
                FounderMembershipPublicationReservation::MergeConcurrent {
                    next_head_slot: storage
                        .allocate_protocol_slot(
                            &super::storage::ProtocolObjectContext::store(
                                root.store_root_hash,
                                super::storage::ProtocolObjectDomain::StoreMembershipHead,
                            ),
                            &prefix,
                            ".json",
                        )
                        .await
                        .map_err(StoreObjectError::from)?,
                }
            }
            WritePolicy::Serial => FounderMembershipPublicationReservation::Serial,
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
) -> Result<crate::database::DurableFounderGraph, StoreProtocolRootError> {
    let reservation =
        durable_descriptor_reservation(db, storage, founder_timestamp, signer).await?;
    let authority = &reservation.membership.founder().root.authority;
    let (first_exact, second_exact) = storage.exact_slot_probe_clients();
    let exact_slots = super::provider::probe_exact_slots(
        first_exact,
        second_exact,
        db,
        authority.probes.exact_slots(),
        &authority.binding,
    )
    .await
    .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
    let serial_coordination = match authority.probes {
        StoreCreationProbeIds::MergeConcurrent { .. } => None,
        StoreCreationProbeIds::Serial {
            serial_coordination,
            ..
        } => {
            let (first, second) = storage
                .serial_coordination_probe_clients()
                .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
            Some(
                super::provider::probe_serial_coordination_receipt(
                    &first,
                    &second,
                    db,
                    serial_coordination,
                    &authority.binding,
                )
                .await
                .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?,
            )
        }
    };
    let provider_admin = super::provider::FounderProviderAdminGrant {
        grant_id: authority.provider_admin_grant.clone(),
        provider: authority.binding.device.clone(),
        access: authority.access.clone(),
        capability: super::provider::ProviderCapabilityProof {
            exact_slots,
            serial_coordination,
        },
    };
    let (membership, founder_anchor, founder_head_slot) = match &reservation.membership {
        MembershipReservation::MergeConcurrent { first_slot, .. } => {
            let anchor = GrantStreamAnchor::StoreMembership {
                first_slot: first_slot.clone(),
            };
            (
                super::store_commit::StoreMembershipGenesis::MergeConcurrent {
                    founder_membership: anchor.clone(),
                },
                Some(anchor),
                Some(first_slot.clone()),
            )
        }
        MembershipReservation::Serial { .. } => (
            super::store_commit::StoreMembershipGenesis::Serial,
            None,
            None,
        ),
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
        write_policy: authority.write_policy,
        founder_pubkey: authority.founder_pubkey.clone(),
        founder_grant: authority.founder_grant.clone(),
        root_slot: reservation.membership.founder().root.root_slot.clone(),
        founder_registration: reservation.membership.founder().registration_slot.clone(),
        founder_provider_admin: provider_admin.clone(),
        membership,
        founder_recovery,
    };
    let root_value = StoreProtocolRoot::signed(descriptor, signer)
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let root_id = root_value.descriptor.store_root_id();
    let root_hash = root_value.object_hash();
    let root_prefix = super::store_commit::store_protocol_root_logical_key();
    let root_context = super::storage::ProtocolObjectContext::store(
        root_hash,
        super::storage::ProtocolObjectDomain::StoreProtocolRoot,
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
        super::store_registration::prepare_registration_for_origin(
            storage,
            signer,
            db.write_policy(),
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
    let ack_context = super::storage::ProtocolObjectContext::store(
        root_hash,
        super::storage::ProtocolObjectDomain::StoreAck,
    );
    let next_ack_slot = graph_reservation.next_ack_slot.clone();
    let frontier = match db.write_policy() {
        WritePolicy::MergeConcurrent => StoreHistoryCut::merge_concurrent(Default::default()),
        WritePolicy::Serial => StoreHistoryCut::serial(StoreSerialPredecessor::Genesis {
            root: root_ref.clone(),
            founder_registration: registration_ref.clone(),
        }),
    };
    let initial_ack_value = StoreAck::signed(
        root_hash,
        registration_ref.clone(),
        1,
        None,
        frontier,
        founder_timestamp.to_string(),
        SuccessorLink {
            activation: StreamActivationId::store_acknowledgements(&root_ref, &registration_ref),
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
        revision: 1,
        ack_hash: initial_ack_value.ack_hash(),
        object: initial_ack_prepared.reference().clone(),
    };
    let membership = match (founder_anchor, founder_head_slot) {
        (Some(founder_anchor), Some(founder_head_slot)) => {
            let founder_grant = authority.founder_grant.clone();
            let founder = super::membership::founder_entry(
                &root_id.to_string(),
                signer,
                founder_grant.clone(),
                founder_timestamp,
                founder_anchor.clone(),
                provider_admin,
            );
            let (entry_prepared, entry_ref) =
                super::store_objects::prepare_membership_entry(storage, root_hash, &founder)
                    .await?;
            let head_context = super::storage::ProtocolObjectContext::store(
                root_hash,
                super::storage::ProtocolObjectDomain::StoreMembershipHead,
            );
            let FounderMembershipPublicationReservation::MergeConcurrent { next_head_slot } =
                &graph_reservation.membership
            else {
                return Err(StoreProtocolRootError::Database(
                    "Merge founder graph has no next membership-head reservation".to_string(),
                ));
            };
            let head_value = AuthorHead::signed(
                root_id.to_string(),
                registration_ref.clone(),
                entry_ref.clone(),
                None,
                Vec::new(),
                SuccessorLink {
                    activation: StreamActivationId::store_membership(
                        &root_ref,
                        &registration_ref,
                        &founder_grant,
                        &founder_anchor,
                    ),
                    predecessor: None,
                    next_slot: next_head_slot.clone(),
                },
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
            crate::database::DurableFounderMembership::MergeConcurrent {
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
        }
        (None, None) => crate::database::DurableFounderMembership::Serial,
        _ => {
            return Err(StoreProtocolRootError::Database(
                "founder membership reservation is only partially policy-shaped".to_string(),
            ))
        }
    };
    Ok(crate::database::DurableFounderGraph {
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
    })
}

pub async fn create_store(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let write_policy = db.write_policy();
    let graph = match db
        .local_store_founder_graph()
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
    {
        Some(graph) => graph,
        None => {
            let graph = prepare_founder_graph(db, storage, founder_timestamp, signer).await?;
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
    let store_protocol_root = StoreProtocolRoot::parse_expected(
        &graph.root.bytes,
        &root_ref,
        write_policy,
        db.sync_routing_hash(),
    )
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
    db.mark_local_store_device_registration_created(
        graph.registration.clone(),
        graph.initial_ack_ref.clone(),
        graph.initial_ack.clone(),
    )
    .await
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let membership_refs = match &graph.membership {
        crate::database::DurableFounderMembership::MergeConcurrent {
            entry,
            entry_ref,
            head,
            head_ref,
        } => {
            storage
                .create_protocol_object(&entry.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let loaded_entry = super::store_objects::load_membership_entry_ref(
                storage,
                root_ref.store_root_hash,
                entry_ref,
            )
            .await?
            .value;
            if loaded_entry != entry.value {
                return Err(StoreProtocolRootError::Database(
                    "founder membership entry readback differs from durable bytes".to_string(),
                ));
            }
            storage
                .create_protocol_object(&head.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let loaded_head = super::store_objects::load_membership_head_ref(
                storage,
                root_ref.store_root_hash,
                head_ref,
                &registration,
            )
            .await?
            .value;
            if loaded_head != head.value {
                return Err(StoreProtocolRootError::Database(
                    "founder membership head readback differs from durable bytes".to_string(),
                ));
            }
            crate::database::FounderMembershipRefs::MergeConcurrent {
                entry: entry_ref.clone(),
                head: head_ref.clone(),
            }
        }
        crate::database::DurableFounderMembership::Serial => {
            crate::database::FounderMembershipRefs::Serial
        }
    };
    if write_policy == WritePolicy::Serial {
        let coordination = storage
            .serial_coordination()
            .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
        let device_signer = registration
            .device_signer(signer)
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        let genesis = super::store_commit::StoreSerialHead::signed(
            root_ref.store_root_hash,
            super::store_commit::StoreSerialHeadState::Genesis {
                root: root_ref.clone(),
                founder_registration: registration_ref.clone(),
            },
            &device_signer,
        )
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
        let genesis_bytes = genesis.to_bytes();
        match coordination
            .create_head(super::store_commit::serial_head_key(), &genesis_bytes)
            .await
        {
            Ok(created) if created.bytes == genesis_bytes => {}
            Ok(_) => {
                return Err(StoreProtocolRootError::Coordination(
                    "Serial genesis create returned different bytes".to_string(),
                ))
            }
            Err(_) => {
                let observed = coordination
                    .read_head(super::store_commit::serial_head_key())
                    .await
                    .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
                if observed.bytes != genesis_bytes {
                    return Err(StoreProtocolRootError::Coordination(
                        "Serial genesis slot contains different bytes".to_string(),
                    ));
                }
            }
        }
    }
    db.complete_store_founder_graph(
        root_ref,
        registration_ref,
        graph.initial_ack_ref,
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
    let write_policy = db.write_policy();
    let context = super::storage::ProtocolObjectContext::store(
        expected.store_root_hash,
        super::storage::ProtocolObjectDomain::StoreProtocolRoot,
    );
    let bytes = storage
        .read_protocol_object(
            &context,
            &expected.object,
            super::store_commit::store_protocol_root_logical_key(),
        )
        .await
        .map_err(StoreObjectError::from)?;
    let verified =
        StoreProtocolRoot::parse_expected(&bytes, expected, write_policy, db.sync_routing_hash())
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    let live_binding = storage
        .provider_binding()
        .await
        .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
    if live_binding.store != verified.descriptor.provider {
        return Err(StoreProtocolRootError::Database(
            "live provider namespace differs from the signed Store root".to_string(),
        ));
    }
    if let Some(local) = db
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
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        CloudHeadCreateError, CloudHeadReplaceError, CloudHeadStorage, CloudHeadVersion,
        CloudHomeError, CloudVersionedHead,
    };
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::test_helpers::{open_serial_test_db, open_test_db};

    struct RecordingHeadClient {
        id: usize,
        calls: Arc<Mutex<Vec<usize>>>,
        inner: InMemoryCloudHome,
    }

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
        let durable = db
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

    #[async_trait]
    impl CloudHeadStorage for RecordingHeadClient {
        async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.read_head(key).await
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &CloudHeadVersion,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.delete_probe_head(key).await
        }
    }

    #[tokio::test]
    async fn serial_store_creation_probes_two_independent_provider_clients() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "independent-probe-clients",
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies")
        .with_serial_coordination_clients(
            Arc::new(RecordingHeadClient {
                id: 1,
                calls: calls.clone(),
                inner: home.clone(),
            }),
            Arc::new(RecordingHeadClient {
                id: 2,
                calls: calls.clone(),
                inner: home,
            }),
        );
        let db = open_serial_test_db();

        create_store(&db, &storage, "0000000000001-0000-founder", &keypair)
            .await
            .expect("create Serial Store");

        let create_clients: BTreeSet<_> = calls.lock().unwrap().iter().copied().collect();
        assert_eq!(create_clients.len(), 2);
    }

    #[tokio::test]
    async fn created_serial_store_installs_the_complete_founder_authorization() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "serial-founder-authorization",
            founder.clone(),
        )
        .expect("construct Serial founder storage")
        .with_serial_coordination_clients(Arc::new(home.clone()), Arc::new(home));
        let db = open_serial_test_db();

        let root = create_store(&db, &storage, "0000000000001-0000-founder", &founder)
            .await
            .expect("create Serial Store");
        let root_ref = db
            .local_store_root_ref()
            .await
            .expect("read exact Store root")
            .expect("created Store root exists");
        let graph = db
            .local_store_founder_graph()
            .await
            .expect("read durable founder graph")
            .expect("durable founder graph exists");
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &graph.registration.value,
            graph.registration.object.clone(),
        );
        let expected = super::super::membership::SerialAuthorizationState::from_founder(
            &root_ref,
            &root,
            &registration_ref,
            &graph.registration.value,
        )
        .expect("derive exact Serial founder authorization");

        assert_eq!(
            db.get_protocol_state(super::super::membership_ops::OWNER_PUBKEY_STATE_KEY)
                .await
                .expect("read pinned founder")
                .as_deref(),
            Some(root.descriptor.founder_pubkey.as_str())
        );
        assert_eq!(
            db.serial_authorization_state()
                .await
                .expect("read Serial founder authorization"),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn failed_probe_cleanup_does_not_poison_store_root_creation_retry() {
        let home = InMemoryCloudHome::new();
        home.fail_coordination_probe_cleanup();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "probe-cleanup-retry",
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies")
        .with_serial_coordination_clients(Arc::new(home.clone()), Arc::new(home));
        let db = open_serial_test_db();

        let first = create_store(&db, &storage, "0000000000001-0000-founder", &keypair).await;
        assert!(matches!(
            first,
            Err(StoreProtocolRootError::Coordination(_))
        ));

        create_store(&db, &storage, "0000000000001-0000-founder", &keypair)
            .await
            .expect("retry creates the Store root after one failed probe cleanup");
    }
}
