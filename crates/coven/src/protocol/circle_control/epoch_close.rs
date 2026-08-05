use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveCircleEpochCore {
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub owners: Vec<String>,
    pub access_root: ObjectHash,
    pub origin: CircleEpochOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochOrigin {
    Founder,
    Closed {
        closed_epoch_id: CircleEpochId,
        close_control: CircleControlCoord,
        close_id: CircleEpochCloseId,
        outcome_hash: ObjectHash,
        cutoff: CommitFrontier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeActiveCircleEpoch {
    pub common: ActiveCircleEpochCore,
    pub metadata: MergeCircleMetadataStateRef,
    pub roster: MergeCircleRosterStateRef,
    pub store_membership: StoreMembershipStateRef,
    pub covered_control_heads: Vec<MergeCircleControlHeadRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseParticipant {
    pub registration: StoreDeviceRegistrationRef,
    pub response_slot: ObjectSlot,
}

/// The wire body of an Owner's intent to close an epoch. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseIntentBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub epoch_id: CircleEpochId,
    pub predecessor_roster: MergeCircleRosterStateRef,
    pub removal: CircleRosterEntry,
    pub remaining_roster_state_hash: ObjectHash,
    pub owner_pubkey: String,
}

impl SignedBody for CircleEpochCloseIntentBody {
    const DOMAIN: &'static [u8] = CLOSE_INTENT_DOMAIN;
}

pub(crate) type CircleEpochCloseIntent = Signed<CircleEpochCloseIntentBody>;

impl CircleEpochCloseIntentBody {
    pub(super) fn verify_shape(&self) -> bool {
        self.removal.verify()
            && self.removal.store_root_hash == self.store_root_hash
            && self.removal.circle_id == self.circle_id
            && self.removal.author_pubkey == self.owner_pubkey
            && matches!(
                self.removal.change,
                crate::protocol::circle_roster::CircleRosterChange::RemoveMember { .. }
            )
    }
}

impl CircleEpochCloseIntent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        close_id: CircleEpochCloseId,
        epoch_id: CircleEpochId,
        predecessor_roster: MergeCircleRosterStateRef,
        removal: CircleRosterEntry,
        remaining_roster_state_hash: ObjectHash,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let body = CircleEpochCloseIntentBody {
            store_root_hash,
            circle_id,
            close_id,
            epoch_id,
            predecessor_roster,
            removal,
            remaining_roster_state_hash,
            owner_pubkey: keys::public_key_hex(signer),
        };
        if !body.verify_shape() {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Signed::sign(body, signer))
    }

    pub(crate) fn verify(&self) -> bool {
        self.body().verify_shape() && self.verify_by(&self.owner_pubkey).is_ok()
    }

    pub(crate) fn intent_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseIntentRef {
    pub close_id: CircleEpochCloseId,
    pub intent_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseIntentRef {
    pub(crate) fn from_intent(
        intent: &CircleEpochCloseIntent,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        let reference = Self {
            close_id: intent.close_id,
            intent_hash: intent.intent_hash(),
            object,
        };
        if reference.object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_intent_semantic_prefix(
                    intent.circle_id,
                    intent.close_id,
                    intent.intent_hash(),
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(reference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochClose {
    pub close_id: CircleEpochCloseId,
    pub frozen_epoch: MergeActiveCircleEpoch,
    pub intent: CircleEpochCloseIntentRef,
    pub frozen_device_state: StoreDeviceStateRef,
    pub participants: Vec<CircleEpochCloseParticipant>,
    pub provisional_frontier: CommitFrontier,
    pub outcome_slot: ObjectSlot,
}

impl CircleEpochClose {
    pub(super) fn verify_shape(&self, circle_id: CircleId) -> bool {
        crate::protocol::store_commit::validate_commit_frontier(&self.provisional_frontier).is_ok()
            && self.intent.close_id == self.close_id
            && !self.participants.is_empty()
            && self
                .participants
                .windows(2)
                .all(|pair| pair[0].registration.device_id < pair[1].registration.device_id)
            && self.participants.iter().all(|participant| {
                participant.response_slot.logical_key()
                    == format!(
                        "{}.json",
                        circle_epoch_close_response_semantic_prefix(
                            circle_id,
                            self.close_id,
                            participant.registration.device_id,
                        )
                    )
            })
            && self.outcome_slot.logical_key()
                == format!(
                    "{}.json",
                    circle_epoch_close_outcome_semantic_prefix(circle_id, self.close_id)
                )
    }
}

/// The wire body of one participant device's close response. Every field here
/// is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseResponseBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub registration: StoreDeviceRegistrationRef,
    pub frontier: CommitFrontier,
}

impl SignedBody for CircleEpochCloseResponseBody {
    const DOMAIN: &'static [u8] = CLOSE_RESPONSE_DOMAIN;
}

pub(crate) type CircleEpochCloseResponse = Signed<CircleEpochCloseResponseBody>;

impl CircleEpochCloseResponse {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        registration: StoreDeviceRegistrationRef,
        frontier: CommitFrontier,
        author: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let response = Signed::sign(
            CircleEpochCloseResponseBody {
                store_root_hash: control.value.store_root_hash,
                circle_id: control.value.circle_id,
                close_id: close.close_id,
                close_control: control.coord.clone(),
                registration,
                frontier,
            },
            signer,
        );
        if !response.verify_for(control, author) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(response)
    }

    pub(crate) fn verify_for(
        &self,
        control: &PreparedCircleControl,
        author: &StoreDeviceRegistration,
    ) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && crate::protocol::store_commit::validate_commit_frontier(&self.frontier).is_ok()
            && self.frontier.covers(&close.provisional_frontier)
            && self.registration.verify_registration(author).is_ok()
            && author.store_root.store_root_hash == self.store_root_hash
            && close
                .participants
                .iter()
                .any(|participant| participant.registration == self.registration)
            && self.verify_by(&author.device_signing_pubkey).is_ok()
    }

    pub(crate) fn response_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseResponseRef {
    pub registration: StoreDeviceRegistrationRef,
    pub frontier: CommitFrontier,
    pub response_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseResponseRef {
    pub(crate) fn from_response(
        response: &CircleEpochCloseResponse,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_response_semantic_prefix(
                    response.circle_id,
                    response.close_id,
                    response.registration.device_id,
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            registration: response.registration.clone(),
            frontier: response.frontier.clone(),
            response_hash: response.response_hash(),
            object,
        })
    }

    pub(crate) fn verify_response(&self, response: &CircleEpochCloseResponse) -> bool {
        self.registration == response.registration
            && self.frontier == response.frontier
            && self.response_hash == response.response_hash()
    }
}

/// One Owner-signed exclusion of an unavailable participant device. It competes
/// at that device's create-once response slot; activating it excludes the device
/// from the close cutoff and forces it to reset from the successor bootstrap.
/// The wire body of an Owner's exclusion of an unavailable participant. Every
/// field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseExclusionBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub excluded: StoreDeviceRegistrationRef,
    pub owner_pubkey: String,
}

impl SignedBody for CircleEpochCloseExclusionBody {
    const DOMAIN: &'static [u8] = CLOSE_EXCLUSION_DOMAIN;
}

pub(crate) type CircleEpochCloseExclusion = Signed<CircleEpochCloseExclusionBody>;

impl CircleEpochCloseExclusion {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        excluded: StoreDeviceRegistrationRef,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let exclusion = Signed::sign(
            CircleEpochCloseExclusionBody {
                store_root_hash: control.value.store_root_hash,
                circle_id: control.value.circle_id,
                close_id: close.close_id,
                close_control: control.coord.clone(),
                excluded,
                owner_pubkey: keys::public_key_hex(signer),
            },
            signer,
        );
        if !exclusion.verify_shape(control) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(exclusion)
    }

    pub(super) fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && close
                .participants
                .iter()
                .any(|participant| participant.registration == self.excluded)
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    pub(crate) fn verify_for(&self, control: &PreparedCircleControl) -> bool {
        self.verify_shape(control) && self.verify_by(&self.owner_pubkey).is_ok()
    }

    pub(crate) fn exclusion_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseExclusionRef {
    pub registration: StoreDeviceRegistrationRef,
    pub exclusion_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseExclusionRef {
    pub(crate) fn from_exclusion(
        exclusion: &CircleEpochCloseExclusion,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_response_semantic_prefix(
                    exclusion.circle_id,
                    exclusion.close_id,
                    exclusion.excluded.device_id,
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            registration: exclusion.excluded.clone(),
            exclusion_hash: exclusion.exclusion_hash(),
            object,
        })
    }

    pub(crate) fn verify_exclusion(&self, exclusion: &CircleEpochCloseExclusion) -> bool {
        self.registration == exclusion.excluded && self.exclusion_hash == exclusion.exclusion_hash()
    }
}

/// The exactly-one value a participant's create-once close-response slot holds:
/// the device's own signed response, or an Owner-signed exclusion of that device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseResponseSlotValue {
    Response(CircleEpochCloseResponse),
    Exclusion(CircleEpochCloseExclusion),
}

impl CircleEpochCloseResponseSlotValue {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self)
            .expect("Circle epoch-close response slot value serialization cannot fail")
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, CircleTransitionError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        if value.to_bytes() != bytes {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(value)
    }
}

/// One participant's contribution to a close outcome: either its verified device
/// response (whose frontier joins the cutoff) or an Owner exclusion (which does
/// not). The outcome carries one per participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseSettlement {
    Response(CircleEpochCloseResponseRef),
    Exclusion(CircleEpochCloseExclusionRef),
}

impl CircleEpochCloseSettlement {
    pub(crate) fn registration(&self) -> &StoreDeviceRegistrationRef {
        match self {
            Self::Response(reference) => &reference.registration,
            Self::Exclusion(reference) => &reference.registration,
        }
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Response(reference) => &reference.object,
            Self::Exclusion(reference) => &reference.object,
        }
    }

    pub(crate) fn response_frontier(&self) -> Option<&CommitFrontier> {
        match self {
            Self::Response(reference) => Some(&reference.frontier),
            Self::Exclusion(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochSuccessor {
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub owners: Vec<String>,
    pub access_root: ObjectHash,
    pub metadata: MergeCircleMetadataStateRef,
    pub roster: MergeCircleRosterStateRef,
    pub store_membership: StoreMembershipStateRef,
}

/// The wire body of an epoch close's outcome. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseOutcomeBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub intent: CircleEpochCloseIntentRef,
    pub responses: Vec<CircleEpochCloseSettlement>,
    pub cutoff: CommitFrontier,
    pub successor: CircleEpochSuccessor,
    pub owner_pubkey: String,
}

impl SignedBody for CircleEpochCloseOutcomeBody {
    const DOMAIN: &'static [u8] = CLOSE_OUTCOME_DOMAIN;
}

pub(crate) type CircleEpochCloseOutcome = Signed<CircleEpochCloseOutcomeBody>;

impl CircleEpochCloseOutcome {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        intent: &CircleEpochCloseIntent,
        responses: Vec<CircleEpochCloseSettlement>,
        successor: CircleEpochSuccessor,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let cutoff = responses
            .iter()
            .filter_map(CircleEpochCloseSettlement::response_frontier)
            .try_fold(close.provisional_frontier.clone(), |cutoff, frontier| {
                cutoff.join(frontier.clone())
            })
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let outcome = Signed::sign(
            CircleEpochCloseOutcomeBody {
                store_root_hash: control.value.store_root_hash,
                circle_id: control.value.circle_id,
                close_id: close.close_id,
                close_control: control.coord.clone(),
                intent: close.intent.clone(),
                responses,
                cutoff,
                successor,
                owner_pubkey: keys::public_key_hex(signer),
            },
            signer,
        );
        if !outcome.verify_shape(control) || !outcome.verify_intent(intent) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(outcome)
    }

    pub(super) fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        let responses_are_canonical = self
            .responses
            .windows(2)
            .all(|pair| pair[0].registration().device_id < pair[1].registration().device_id);
        let responses_match_participants =
            self.responses.len() == close.participants.len()
                && self.responses.iter().zip(&close.participants).all(
                    |(settlement, participant)| {
                        settlement.registration() == &participant.registration
                            && settlement.object().slot() == &participant.response_slot
                            && settlement
                                .response_frontier()
                                .is_none_or(|frontier| frontier.covers(&close.provisional_frontier))
                    },
                );
        let expected_cutoff = self
            .responses
            .iter()
            .filter_map(CircleEpochCloseSettlement::response_frontier)
            .try_fold(close.provisional_frontier.clone(), |cutoff, frontier| {
                cutoff.join(frontier.clone())
            });
        control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && self.intent == close.intent
            && responses_are_canonical
            && responses_match_participants
            && expected_cutoff.is_ok_and(|cutoff| cutoff == self.cutoff)
            && crate::protocol::store_commit::validate_commit_frontier(&self.cutoff).is_ok()
            && self.successor.epoch_id != close.frozen_epoch.common.epoch_id
            && self.successor.key_fingerprint != close.frozen_epoch.common.key_fingerprint
            && !self.successor.owners.is_empty()
            && self
                .successor
                .owners
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    fn verify_intent(&self, intent: &CircleEpochCloseIntent) -> bool {
        intent.verify()
            && intent.close_id == self.close_id
            && intent.circle_id == self.circle_id
            && intent.store_root_hash == self.store_root_hash
            && intent.intent_hash() == self.intent.intent_hash
            && self.successor.roster.state_hash == intent.remaining_roster_state_hash
    }

    pub(crate) fn verify_for(
        &self,
        control: &PreparedCircleControl,
        intent: &CircleEpochCloseIntent,
        settlements: &[(
            CircleEpochCloseSettlement,
            CircleEpochCloseResponseSlotValue,
        )],
    ) -> bool {
        self.verify_shape(control)
            && self.verify_intent(intent)
            && self.responses.len() == settlements.len()
            && self
                .responses
                .iter()
                .zip(settlements)
                .all(|(expected, (settlement, slot_value))| {
                    expected == settlement
                        && match (settlement, slot_value) {
                            (
                                CircleEpochCloseSettlement::Response(reference),
                                CircleEpochCloseResponseSlotValue::Response(response),
                            ) => {
                                reference.verify_response(response)
                                    && response.close_control == self.close_control
                            }
                            (
                                CircleEpochCloseSettlement::Exclusion(reference),
                                CircleEpochCloseResponseSlotValue::Exclusion(exclusion),
                            ) => {
                                reference.verify_exclusion(exclusion)
                                    && exclusion.verify_for(control)
                                    && exclusion.close_control == self.close_control
                            }
                            _ => false,
                        }
                })
            && self.verify_signature()
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.verify_by(&self.owner_pubkey).is_ok()
    }

    pub(crate) fn outcome_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseOutcomeRef {
    pub close_id: CircleEpochCloseId,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseOutcomeRef {
    pub(crate) fn from_outcome(
        outcome: &CircleEpochCloseOutcome,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_outcome_semantic_prefix(outcome.circle_id, outcome.close_id)
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            close_id: outcome.close_id,
            outcome_hash: outcome.outcome_hash(),
            object,
        })
    }
}

/// One Owner-signed cancellation of an epoch close. It competes at the same
/// create-once outcome slot as the final outcome; activating it reopens the
/// frozen epoch instead of rotating to a successor epoch.
/// The wire body of an Owner's cancellation of an epoch close. Every field here
/// is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseCancellationBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub intent: CircleEpochCloseIntentRef,
    pub owner_pubkey: String,
}

impl SignedBody for CircleEpochCloseCancellationBody {
    const DOMAIN: &'static [u8] = CLOSE_CANCELLATION_DOMAIN;
}

pub(crate) type CircleEpochCloseCancellation = Signed<CircleEpochCloseCancellationBody>;

impl CircleEpochCloseCancellation {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let cancellation = Signed::sign(
            CircleEpochCloseCancellationBody {
                store_root_hash: control.value.store_root_hash,
                circle_id: control.value.circle_id,
                close_id: close.close_id,
                close_control: control.coord.clone(),
                intent: close.intent.clone(),
                owner_pubkey: keys::public_key_hex(signer),
            },
            signer,
        );
        if !cancellation.verify_shape(control) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(cancellation)
    }

    pub(super) fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && self.intent == close.intent
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    pub(crate) fn verify_for(&self, control: &PreparedCircleControl) -> bool {
        self.verify_shape(control) && self.verify_signature()
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.verify_by(&self.owner_pubkey).is_ok()
    }

    pub(crate) fn cancellation_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseCancellationRef {
    pub close_id: CircleEpochCloseId,
    pub cancellation_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseCancellationRef {
    pub(crate) fn from_cancellation(
        cancellation: &CircleEpochCloseCancellation,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_outcome_semantic_prefix(
                    cancellation.circle_id,
                    cancellation.close_id
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            close_id: cancellation.close_id,
            cancellation_hash: cancellation.cancellation_hash(),
            object,
        })
    }
}

/// The exactly-one value the create-once epoch-close outcome slot holds. Readers
/// parse this tagged form and dispatch on the settled arm: a final outcome
/// rotates to a successor epoch, a cancellation reopens the frozen epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseSlotValue {
    Outcome(CircleEpochCloseOutcome),
    Cancellation(CircleEpochCloseCancellation),
}

impl CircleEpochCloseSlotValue {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Circle epoch-close slot value serialization cannot fail")
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, CircleTransitionError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        if value.to_bytes() != bytes {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(value)
    }
}
