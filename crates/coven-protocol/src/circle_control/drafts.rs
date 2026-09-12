use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCircleControl {
    pub coord: CircleControlCoord,
    pub bytes: Vec<u8>,
    pub value: CircleControl,
}

impl PreparedCircleControl {
    pub fn verify(&self) -> bool {
        self.bytes
            == serde_json::to_vec(&self.value).expect("circle control serialization cannot fail")
            && self.value.verify()
            && self.coord == self.value.coord()
    }
}

/// One recipient's sealed access leaf beside its plaintext: the sealed bytes
/// as they appear in the control's access map, and the signed leaf they carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedAccessLeaf {
    pub bytes: Vec<u8>,
    pub value: CircleAccessLeaf,
}

impl PreparedAccessLeaf {
    /// Seal a signed leaf to its own recipient.
    pub fn seal(value: CircleAccessLeaf) -> Result<Self, CircleTransitionError> {
        let recipient_x25519 = keys::ed25519_hex_to_x25519_public_key(&value.recipient_pubkey)
            .map_err(|_| CircleTransitionError::InvalidRecipient(value.recipient_pubkey.clone()))?;
        let plaintext =
            serde_json::to_vec(&value).expect("circle access leaf serialization cannot fail");
        Ok(Self {
            bytes: keys::seal_box_encrypt(&plaintext, &recipient_x25519),
            value,
        })
    }

    /// Open one control access entry with the recipient's own identity key. The
    /// caller checks the leaf's context against the control it came from.
    pub fn open(
        entry: &CircleAccessEntry,
        identity: &UserKeypair,
    ) -> Result<Self, crate::circle_activation::CircleStateError> {
        let bytes = hex::decode(&entry.sealed).map_err(|source| {
            crate::circle_activation::CircleStateError::Hex {
                subject: "Circle access entry",
                source,
            }
        })?;
        let plaintext = keys::seal_box_decrypt(&bytes, &identity.to_x25519_secret_key())?;
        let value = serde_json::from_slice(&plaintext).map_err(|source| {
            crate::circle_activation::CircleStateError::Json {
                operation: "parse sealed Circle access leaf",
                source,
            }
        })?;
        Ok(Self { bytes, value })
    }

    /// This leaf's entry as the signing control carries it.
    pub fn entry(&self) -> CircleAccessEntry {
        CircleAccessEntry {
            sealed: hex::encode(&self.bytes),
            value_hash: ObjectHash::digest(
                &serde_json::to_vec(&self.value)
                    .expect("circle access leaf serialization cannot fail"),
            ),
        }
    }

    pub fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: crate::store_commit::CandidateFamilyId,
    ) -> bool {
        self.value.verify_for_control(control, candidate_family)
            && control.value.value.access.entry(&self.value.recipient_slot) == Some(&self.entry())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterPolicyObjects {
    pub entry: CircleRosterEntry,
    pub head: CircleRosterHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleTransitionPolicyObjects {
    pub roster: Option<CircleRosterPolicyObjects>,
    pub metadata_head: Option<CircleMetadataHead>,
    pub control_head: CircleControlHead,
}

#[derive(Debug, Clone)]
pub enum CircleRosterDraftPolicy {
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
pub struct CircleTransitionDraftPolicy {
    pub roster: CircleRosterDraftPolicy,
    pub metadata_successor: bool,
}

#[derive(Debug, Clone)]
pub struct CircleTransitionDraft {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy: CircleTransitionDraftPolicy,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_finalization: Option<CircleEpochCloseFinalizationDraft>,
    pub close_cancellation: Option<CircleEpochCloseCancellationDraft>,
    pub access: Vec<PreparedAccessLeaf>,
    pub control: PreparedCircleControl,
}

#[derive(Debug, Clone)]
pub struct CircleEpochCloseFinalizationDraft {
    pub close_control: PreparedCircleControl,
    pub intent: CircleEpochCloseIntent,
    pub responses: Vec<CircleEpochCloseSettlement>,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
pub struct CircleEpochCloseCancellationDraft {
    pub close_control: PreparedCircleControl,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
pub(super) struct FounderRosterObjects {
    pub(super) entry: CircleRosterEntry,
    pub(super) resolved: ResolvedCircleRoster,
}

/// Every Store member's sealed access leaf for one control, and the map the
/// control signs over them.
pub(super) struct CircleAccessSet {
    pub(super) leaves: Vec<PreparedAccessLeaf>,
    pub(super) map: CircleAccessMap,
}

impl CircleAccessSet {
    /// Sign one leaf per Store member — Active for roster members, Inactive
    /// otherwise — seal each to its recipient, and build the control's map.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare(
        store_root_hash: ObjectHash,
        candidate_family: crate::store_commit::CandidateFamilyId,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        keyring: &str,
        key_fingerprint: KeyFingerprint,
        roster_state: &CircleRosterStateRef,
        roster_members: &std::collections::BTreeMap<String, crate::circle::CircleRole>,
        store_membership: &StoreMembershipStateRef,
        store_members: &[(String, MemberRole)],
        bootstraps: &std::collections::BTreeMap<String, CircleBootstrapRef>,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
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
                let value = CircleAccessLeafBody {
                    store_root_hash,
                    candidate_family,
                    circle_id,
                    epoch_id,
                    owner_pubkey: author_pubkey.clone(),
                    recipient_pubkey: recipient_pubkey.clone(),
                    recipient_slot,
                    disposition,
                    store_membership: store_membership.clone(),
                };
                PreparedAccessLeaf::seal(Signed::sign(value, signer))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let map = CircleAccessMap::from_leaves(&leaves)?;
        Ok(Self { leaves, map })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCircleTransition {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy_objects: CircleTransitionPolicyObjects,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_outcome: Option<CircleEpochCloseOutcome>,
    pub close_cancellation: Option<CircleEpochCloseCancellation>,
    pub access: Vec<PreparedAccessLeaf>,
    pub control: PreparedCircleControl,
}

impl PreparedCircleTransition {
    pub fn resolved_roster(&self) -> CircleMaterializedRoster {
        self.roster.clone()
    }

    pub fn control_ref(
        &self,
        objects: crate::store_commit::CircleActivationObjects,
        head_object: Option<ExactObjectRef>,
    ) -> crate::store_commit::CircleControlRef {
        let head_object =
            head_object.expect("prepared Circle transition must contain its stored head");
        crate::store_commit::CircleControlRef {
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
    signer: &dyn coven_keys::keys::IdentityKeyAuthority,
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
    signer: &dyn coven_keys::keys::IdentityKeyAuthority,
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
    signer: &dyn coven_keys::keys::IdentityKeyAuthority,
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
    if current_roster.members().get(&author_pubkey) != Some(&crate::circle::CircleRole::Owner) {
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
            record.member_pubkey == author_pubkey && record.role == crate::circle::CircleRole::Owner
        })
        .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
    let author_authority = MergeCircleOwnerAuthorityRef {
        roster: epoch.roster.clone(),
        grant_id: grant_id.clone(),
        created_at: record.creation_authority.clone(),
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
