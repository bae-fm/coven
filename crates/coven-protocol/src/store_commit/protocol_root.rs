use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "sealed")]
pub enum StoreKeyConfirmation {
    NotRequired,
    Opaque(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCreationDescriptor {
    pub creation_id: StoreCreationId,
    pub key_confirmation: StoreKeyConfirmation,
    pub provider: crate::objects::StoreProviderBinding,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
    pub founder_pubkey: String,
    pub founder_grant: MembershipGrantId,
    pub root_slot: ObjectSlot,
    pub current_publication_slot: ObjectSlot,
    pub founder_registration: ObjectSlot,
    pub founder_provider_admin: crate::provider::FounderProviderAdminGrant,
    pub founder_membership: GrantStreamAnchor,
    pub founder_recovery: GrantStreamAnchor,
}

impl StoreCreationDescriptor {
    pub fn store_root_id(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(b"coven.store-creation-descriptor.v1\0", self))
    }

    pub fn validate_merge_founder_entry(
        &self,
        founder: &MembershipEntry,
    ) -> Result<(), StoreProtocolError> {
        let MembershipChange::Founder {
            creation_id,
            owner_pubkey,
            owner_grant_id,
            membership,
            provider_admin,
        } = &founder.change
        else {
            return Err(StoreProtocolError::InvalidFounder);
        };
        if founder.store_id != self.store_root_id().to_string()
            || creation_id != &self.creation_id
            || founder.author_pubkey != self.founder_pubkey
            || founder.author_owner_grant != self.founder_grant
            || owner_pubkey != &self.founder_pubkey
            || owner_grant_id != &self.founder_grant
            || membership != &self.founder_membership
            || provider_admin != &self.founder_provider_admin
            || founder.seq != 1
            || founder.previous_hash.is_some()
            || !founder.dependencies.is_empty()
            || !founder.resolution_dependencies.is_empty()
            || founder.provider_admin.is_some()
            || !verify_membership_entry(founder)
        {
            return Err(StoreProtocolError::InvalidFounder);
        }
        Ok(())
    }
}

/// The wire body of a Store's protocol root. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreProtocolRootBody {
    pub descriptor: StoreCreationDescriptor,
}

impl SignedBody for StoreProtocolRootBody {
    const DOMAIN: &'static [u8] = STORE_PROTOCOL_ROOT_DOMAIN;
}

pub type StoreProtocolRoot = Signed<StoreProtocolRootBody>;

impl StoreProtocolRoot {
    pub fn signed(
        descriptor: StoreCreationDescriptor,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let body = StoreProtocolRootBody { descriptor };
        body.validate_descriptor()?;
        if keys::public_key_hex(signer) != body.descriptor.founder_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(Signed::sign(body, signer))
    }

    pub fn object_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, StoreProtocolError> {
        let store_protocol_root: Self = crate::objects::decode_protocol_object(bytes)?;
        store_protocol_root.body().validate_descriptor()?;
        let founder_pubkey = store_protocol_root.descriptor.founder_pubkey.clone();
        store_protocol_root.verify_by(&founder_pubkey)?;
        Ok(store_protocol_root)
    }

    pub fn parse_expected(
        bytes: &[u8],
        expected: &StoreRootRef,
        expected_sync_routing_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let store_protocol_root = Self::parse_pinned(bytes, expected)?;
        if store_protocol_root.descriptor.sync_routing_hash != expected_sync_routing_hash {
            return Err(StoreProtocolError::SyncRoutingMismatch {
                expected: expected_sync_routing_hash,
                actual: store_protocol_root.descriptor.sync_routing_hash,
            });
        }
        Ok(store_protocol_root)
    }

    pub fn parse_pinned(bytes: &[u8], expected: &StoreRootRef) -> Result<Self, StoreProtocolError> {
        let store_protocol_root = Self::parse(bytes)?;
        let actual_hash = store_protocol_root.object_hash();
        crate::objects::verify_store_root(expected.store_root_hash, actual_hash)?;
        let actual_root_id = store_protocol_root.descriptor.store_root_id();
        if actual_root_id != expected.store_root_id {
            return Err(StoreProtocolError::StoreRootIdMismatch {
                expected: expected.store_root_id,
                actual: actual_root_id,
            });
        }
        if expected.object.slot() != &store_protocol_root.descriptor.root_slot {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: serde_json::to_string(&store_protocol_root.descriptor.root_slot)
                    .expect("Store root slot serialization cannot fail"),
                actual: serde_json::to_string(expected.object.slot())
                    .expect("Store root slot serialization cannot fail"),
            });
        }
        Ok(store_protocol_root)
    }
}

impl StoreProtocolRootBody {
    fn validate_descriptor(&self) -> Result<(), StoreProtocolError> {
        let descriptor = &self.descriptor;
        descriptor.provider.validate()?;
        descriptor
            .founder_provider_admin
            .provider
            .validate_for(&descriptor.provider)?;
        descriptor.founder_provider_admin.capability.verify(
            &descriptor.provider,
            &descriptor.founder_provider_admin.provider,
        )?;
        if !matches!(
            descriptor.founder_recovery,
            GrantStreamAnchor::OwnerRecovery { .. }
        ) {
            return Err(StoreProtocolError::InvalidFounder);
        }
        if descriptor.founder_pubkey.is_empty()
            || descriptor.root_slot.logical_key() != "store-v1/store-protocol-root.json"
            || descriptor.current_publication_slot.logical_key()
                != store_current_publication_logical_key()
            || !matches!(
                descriptor.founder_membership,
                GrantStreamAnchor::StoreMembership { .. }
            )
        {
            return Err(StoreProtocolError::InvalidFounder);
        }
        Ok(())
    }
}
