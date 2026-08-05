use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleControl {
    pub coord: CircleControlCoord,
    pub bytes: Vec<u8>,
    pub value: CircleControl,
}

impl PreparedCircleControl {
    pub(crate) fn verify(&self) -> bool {
        self.bytes
            == serde_json::to_vec(&self.value).expect("circle control serialization cannot fail")
            && self.value.verify()
            && self.coord == self.value.coord()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedAccessLeaf {
    pub bytes: Vec<u8>,
    pub value: CircleAccessLeaf,
    pub leaf_hash: ObjectHash,
}

impl PreparedAccessLeaf {
    pub(crate) fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
    ) -> bool {
        self.value.verify_for_control(control, candidate_family)
            && ObjectHash::digest(&self.bytes) == self.leaf_hash
    }

    pub(crate) fn verify_envelope(
        &self,
        control: &PreparedCircleControl,
        envelope: &AccessEnvelope,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
    ) -> bool {
        self.verify(control, candidate_family)
            && envelope.verify(control, candidate_family)
            && self.leaf_hash == envelope.leaf_hash
            && envelope.value_hash
                == ObjectHash::digest(
                    &serde_json::to_vec(&self.value)
                        .expect("circle access leaf serialization cannot fail"),
                )
            && self.value.leaf_id == envelope.leaf_id
            && self.value.owner_pubkey == envelope.owner_pubkey
            && self.value.recipient_slot == envelope.recipient_slot
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleAccess {
    pub leaf: PreparedAccessLeaf,
    pub envelope: AccessEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterPolicyObjects {
    pub entry: CircleRosterEntry,
    pub head: CircleRosterHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleTransitionPolicyObjects {
    pub roster: Option<CircleRosterPolicyObjects>,
    pub metadata_head: Option<CircleMetadataHead>,
    pub control_head: CircleControlHead,
}

#[derive(Debug, Clone)]
pub(crate) enum CircleRosterDraftPolicy {
    Inherited,
    Founder {
        entry: CircleRosterEntry,
    },
    Successor {
        predecessor: CircleRosterChain,
        entry: CircleRosterEntry,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CircleTransitionDraftPolicy {
    pub roster: CircleRosterDraftPolicy,
    pub metadata_successor: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleTransitionDraft {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy: CircleTransitionDraftPolicy,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_finalization: Option<CircleEpochCloseFinalizationDraft>,
    pub close_cancellation: Option<CircleEpochCloseCancellationDraft>,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleEpochCloseFinalizationDraft {
    pub close_control: PreparedCircleControl,
    pub intent: CircleEpochCloseIntent,
    pub responses: Vec<CircleEpochCloseSettlement>,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleEpochCloseCancellationDraft {
    pub close_control: PreparedCircleControl,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
pub(super) struct FounderRosterObjects {
    pub(super) entry: CircleRosterEntry,
    pub(super) resolved: ResolvedCircleRoster,
}

pub(super) struct CircleAccessDraft<'identity> {
    store_root_hash: ObjectHash,
    candidate_family: crate::protocol::store_commit::CandidateFamilyId,
    circle_id: CircleId,
    access_root: ObjectHash,
    leaves: Vec<PreparedAccessLeaf>,
    proofs: Vec<Vec<MerkleStep>>,
    signer: &'identity dyn crate::keys::IdentityKeyAuthority,
}

impl<'identity> CircleAccessDraft<'identity> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare(
        store_root_hash: ObjectHash,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        keyring: &str,
        key_fingerprint: KeyFingerprint,
        roster_state: &CircleRosterStateRef,
        roster_members: &std::collections::BTreeMap<String, crate::protocol::circle::CircleRole>,
        store_membership: &StoreMembershipStateRef,
        store_members: &[(String, MemberRole)],
        bootstraps: &std::collections::BTreeMap<String, CircleBootstrapRef>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &'identity dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(signer);
        let leaves = store_members
            .iter()
            .map(|(recipient_pubkey, _)| {
                let recipient_slot = recipient_slot(signer, recipient_pubkey, circle_id)?;
                let disposition = if roster_members.contains_key(recipient_pubkey) {
                    CircleAccessDisposition::Active {
                        keyring: keyring.to_string(),
                        key_fingerprint,
                        roster: roster_state.clone(),
                        bootstrap: bootstraps.get(recipient_pubkey).cloned(),
                    }
                } else {
                    CircleAccessDisposition::Inactive
                };
                let mut value = CircleAccessLeaf {
                    version: STORE_PROTOCOL_VERSION,
                    store_root_hash,
                    candidate_family,
                    circle_id,
                    epoch_id,
                    leaf_id: AccessLeafId::generate(ids),
                    owner_pubkey: author_pubkey.clone(),
                    recipient_pubkey: recipient_pubkey.clone(),
                    recipient_slot,
                    disposition,
                    store_membership: store_membership.clone(),
                    signature: String::new(),
                };
                value.signature = keys::sign_hex(signer, &value.canonical_bytes()).1;
                let recipient_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] =
                    hex::decode(recipient_pubkey)
                        .map_err(|_| {
                            CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                        })?
                        .try_into()
                        .map_err(|_| {
                            CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                        })?;
                let recipient_x25519 = keys::ed25519_to_x25519_public_key(&recipient_ed25519)
                    .map_err(|_| {
                        CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                    })?;
                let plaintext =
                    serde_json::to_vec(&value).expect("circle access serialization cannot fail");
                let bytes = keys::seal_box_encrypt(&plaintext, &recipient_x25519);
                let leaf_hash = ObjectHash::digest(&bytes);
                Ok::<PreparedAccessLeaf, CircleTransitionError>(PreparedAccessLeaf {
                    bytes,
                    value,
                    leaf_hash,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let leaf_hashes = leaves.iter().map(|leaf| leaf.leaf_hash).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        Ok(Self {
            store_root_hash,
            candidate_family,
            circle_id,
            access_root,
            leaves,
            proofs,
            signer,
        })
    }

    pub(super) fn access_root(&self) -> ObjectHash {
        self.access_root
    }

    pub(super) fn finish(
        self,
        control: &PreparedCircleControl,
    ) -> Result<Vec<PreparedCircleAccess>, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(self.signer);
        if control.value.store_root_hash != self.store_root_hash
            || control.value.circle_id != self.circle_id
            || control.value.author_pubkey != author_pubkey
            || control.value.access_root() != self.access_root
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(self
            .leaves
            .into_iter()
            .zip(self.proofs)
            .map(|(leaf, proof)| {
                let mut envelope = AccessEnvelope {
                    version: STORE_PROTOCOL_VERSION,
                    store_root_hash: self.store_root_hash,
                    candidate_family: self.candidate_family,
                    circle_id: self.circle_id,
                    owner_pubkey: author_pubkey.clone(),
                    recipient_slot: leaf.value.recipient_slot.clone(),
                    control_hash: control.coord.control_hash(),
                    leaf_id: leaf.value.leaf_id,
                    leaf_hash: leaf.leaf_hash,
                    value_hash: ObjectHash::digest(
                        &serde_json::to_vec(&leaf.value)
                            .expect("circle access leaf serialization cannot fail"),
                    ),
                    proof,
                    signature: String::new(),
                };
                envelope.signature = keys::sign_hex(self.signer, &envelope.canonical_bytes()).1;
                PreparedCircleAccess { leaf, envelope }
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleTransition {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy_objects: CircleTransitionPolicyObjects,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_outcome: Option<CircleEpochCloseOutcome>,
    pub close_cancellation: Option<CircleEpochCloseCancellation>,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

impl PreparedCircleTransition {
    pub(crate) fn resolved_roster(&self) -> CircleMaterializedRoster {
        self.roster.clone()
    }

    pub(crate) fn control_ref(
        &self,
        objects: crate::protocol::store_commit::CircleActivationObjects,
        head_object: Option<ExactObjectRef>,
    ) -> crate::protocol::store_commit::CircleControlRef {
        let head_object =
            head_object.expect("prepared Circle transition must contain its stored head");
        crate::protocol::store_commit::CircleControlRef {
            circle_id: self.circle_id,
            control: self.control.coord.clone(),
            head_hash: self.policy_objects.control_head.head_hash(),
            head_object,
            objects,
        }
    }
}

pub(super) struct CircleSuccessorContext<'a> {
    pub(super) store_members: Vec<(String, MemberRole)>,
    pub(super) author_pubkey: String,
    pub(super) epoch: &'a MergeActiveCircleEpoch,
    pub(super) grant_id: MembershipGrantId,
    pub(super) author_authority: MergeCircleOwnerAuthorityRef,
    pub(super) key_fingerprint: KeyFingerprint,
}

/// The successor context for a command that publishes a new active epoch: the
/// current control must be `ActiveEpoch`, so a closing or deleted control is
/// refused.
pub(super) fn circle_successor_context<'a>(
    store_members: Vec<(String, MemberRole)>,
    current_control: &'a PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &dyn crate::keys::IdentityKeyAuthority,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    let epoch = current_control
        .value
        .active_epoch()
        .ok_or(CircleTransitionError::InvalidCurrentState)?;
    circle_authored_successor_context(
        store_members,
        current_control,
        current_roster,
        current_metadata,
        keyring,
        signer,
        epoch,
    )
}

/// The successor context for a terminal deletion, which supersedes an in-flight
/// close. It authors over the control's access epoch — the active epoch itself,
/// or a close's frozen epoch — so a `Closing` control resolves to the frozen
/// spine the deletion freezes, rather than being refused for lacking an active
/// epoch.
pub(super) fn circle_delete_successor_context<'a>(
    store_members: Vec<(String, MemberRole)>,
    current_control: &'a PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &dyn crate::keys::IdentityKeyAuthority,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    let epoch = current_control.value.access_epoch();
    circle_authored_successor_context(
        store_members,
        current_control,
        current_roster,
        current_metadata,
        keyring,
        signer,
        epoch,
    )
}

pub(super) fn circle_authored_successor_context<'a>(
    mut store_members: Vec<(String, MemberRole)>,
    current_control: &PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &dyn crate::keys::IdentityKeyAuthority,
    epoch: &'a MergeActiveCircleEpoch,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    if !current_control.verify()
        || !current_roster.verify()
        || !current_metadata.verify()
        || current_control.value.circle_id != current_metadata.circle_id
        || current_control.value.epoch_id() != current_metadata.epoch_id
    {
        return Err(CircleTransitionError::InvalidCurrentState);
    }
    let author_pubkey = keys::public_key_hex(signer);
    store_members.sort_by(|left, right| left.0.cmp(&right.0));
    store_members.dedup_by(|left, right| left.0 == right.0);
    if !store_members
        .iter()
        .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
    {
        return Err(CircleTransitionError::AuthorNotStoreWriter);
    }
    if current_roster.members().get(&author_pubkey)
        != Some(&crate::protocol::circle::CircleRole::Owner)
    {
        return Err(CircleTransitionError::AuthorNotCircleOwner);
    }
    let key_fingerprint = EncryptionService::from(
        MasterKeyring::from_serialized(keyring)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
    )
    .seal_key_fingerprint();
    if key_fingerprint != current_control.value.key_fingerprint()
        || current_metadata.key_fingerprint != key_fingerprint
    {
        return Err(CircleTransitionError::InvalidCurrentState);
    }
    let (grant_id, record) = current_roster
        .active_grants()
        .find(|(_, record)| {
            record.member_pubkey == author_pubkey
                && record.role == crate::protocol::circle::CircleRole::Owner
        })
        .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
    let author_authority = match &record.creation_authority {
        CircleGrantCreationAuthority::Entry(created_at) => MergeCircleOwnerAuthorityRef::Roster {
            roster: epoch.roster.clone(),
            grant_id: grant_id.clone(),
            created_at: created_at.clone(),
        },
        CircleGrantCreationAuthority::ConflictResolution(resolution) => {
            MergeCircleOwnerAuthorityRef::ConflictResolution {
                conflict_hash: resolution.conflict_hash,
                resolution_hash: resolution.resolution_hash,
            }
        }
    };
    Ok(CircleSuccessorContext {
        store_members,
        author_pubkey,
        epoch,
        grant_id: grant_id.clone(),
        author_authority,
        key_fingerprint,
    })
}
