//! Durable record of an in-flight Store creation: the probe identities and
//! outcome states persisted in `protocol_state` so an interrupted creation is
//! resumed or abandoned exactly.

use crate::store_commit::{DeviceStreamAnchor, ObjectHash, StoreCreationId};

pub const STORE_CREATION_ATTEMPT_STATE_KEY: &str = "store_creation_attempt_v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCreationAttempt {
    Initialized(StoreCreationAuthority),
    RootReserved(StoreRootReservation),
    FounderRegistrationReserved(FounderRegistrationReservation),
    MembershipReserved(MembershipReservation),
    CurrentPublicationReserved(CurrentPublicationReservation),
    DescriptorReserved(DescriptorReservation),
    FounderStoreCommitsReserved(FounderStoreCommitsReservation),
    FounderAcknowledgementsReserved(FounderAcknowledgementsReservation),
    FounderSnapshotsReserved(FounderSnapshotsReservation),
    FounderNextAckReserved(FounderNextAckReservation),
    FounderGraphReserved(FounderGraphReservation),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCreationProbeIds {
    exact_slots: crate::provider::ProviderProbeId,
}

impl StoreCreationProbeIds {
    pub fn new(exact_slots: crate::provider::ProviderProbeId) -> Self {
        Self { exact_slots }
    }

    pub fn exact_slots(&self) -> crate::provider::ProviderProbeId {
        self.exact_slots
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCreationAuthority {
    pub creation_id: StoreCreationId,
    pub founder_grant: crate::membership::MembershipGrantId,
    pub provider_admin_grant: crate::provider::ProviderAdminGrantId,
    pub probes: StoreCreationProbeIds,
    pub binding: crate::objects::ResolvedProviderBinding,
    pub access: crate::provider::ProviderAccessLocator,
    pub founder_pubkey: String,
    pub founder_timestamp: String,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreRootReservation {
    pub authority: StoreCreationAuthority,
    pub root_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderRegistrationReservation {
    pub root: StoreRootReservation,
    pub registration_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipReservation {
    pub founder: FounderRegistrationReservation,
    pub first_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescriptorReservation {
    pub membership: MembershipReservation,
    pub current_publication_slot: crate::objects::ObjectSlot,
    pub recovery_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentPublicationReservation {
    pub membership: MembershipReservation,
    pub current_publication_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderMembershipPublicationReservation {
    pub next_head_slot: crate::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderGraphReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub next_ack_slot: crate::objects::ObjectSlot,
    pub membership: FounderMembershipPublicationReservation,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderStoreCommitsReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderAcknowledgementsReservation {
    pub store_commits: FounderStoreCommitsReservation,
    pub acknowledgements: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderSnapshotsReservation {
    pub acknowledgements: FounderAcknowledgementsReservation,
    pub snapshots: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderNextAckReservation {
    pub snapshots: FounderSnapshotsReservation,
    pub next_ack_slot: crate::objects::ObjectSlot,
}
