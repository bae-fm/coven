use super::access::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedStreamActivations {
    activating_commit: StoreBatchCommitRef,
    activations: Vec<StreamActivation>,
}

impl VerifiedStreamActivations {
    pub(crate) fn none(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::protocol::store_commit::StoreProtocolError> {
        if !commit.stream_activations().is_empty() {
            return Err(
                crate::protocol::store_commit::StoreProtocolError::Malformed(
                    "Store commit stream activations have not been verified".to_string(),
                ),
            );
        }
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: Vec::new(),
        })
    }

    pub(crate) fn from_verified_circle_commit(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::protocol::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub(crate) fn from_verified_store_control(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::protocol::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        if commit.control().is_none() {
            return Err(
                crate::protocol::store_commit::StoreProtocolError::Malformed(
                    "verified Store membership activations carry another control".to_string(),
                ),
            );
        }
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub(crate) fn as_slice(&self) -> &[StreamActivation] {
        &self.activations
    }

    pub(crate) fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedStreamActivationPrefix {
    by_activation: BTreeMap<StreamActivationId, (StreamActivation, StoreBatchCommitRef)>,
}

impl VerifiedStreamActivationPrefix {
    pub(crate) fn empty() -> Self {
        Self {
            by_activation: BTreeMap::new(),
        }
    }

    pub(crate) fn activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Option<&(StreamActivation, StoreBatchCommitRef)> {
        self.by_activation.get(&activation_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleActivations {
    pub(super) circles: Vec<VerifiedCircleReference>,
    pub(super) stream_activations: VerifiedStreamActivations,
    pub(super) bootstraps: Vec<VerifiedCircleImage>,
    /// Transient: the local device's exclusions detected from the verified
    /// outcomes this activation carries. Never serialized into the retained
    /// form — a reset is dispatched from the durable `circle_close_exclusions`
    /// row this records, not from replayed activations.
    pub(super) local_exclusions: Vec<LocalCircleExclusion>,
    /// Transient: exclusions whose successor bootstrap could not be read this
    /// pull. The pull records the exclusion and holds the successor; a later
    /// pull that reads the bootstrap completes the reset.
    pub(super) bootstrap_pending_exclusions: Vec<LocalCircleExclusion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleActivations {
    activating_commit: StoreBatchCommitRef,
    circles: Vec<RetainedCircleReference>,
    bootstraps: Vec<VerifiedCircleImage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleReference {
    reference: CircleControlRef,
    circle_id: CircleId,
    control: PreparedCircleControl,
    local_access: Option<RetainedCircleAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleAccess {
    access: PreparedCircleAccess,
    state: RetainedCircleAccessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RetainedCircleAccessState {
    Active {
        roster: CircleMaterializedRoster,
        metadata: CircleMetadata,
    },
    Inactive,
}

impl VerifiedCircleActivations {
    pub(crate) fn from_verified_parts(
        circles: Vec<VerifiedCircleReference>,
        stream_activations: VerifiedStreamActivations,
        bootstraps: Vec<VerifiedCircleImage>,
        local_exclusions: Vec<LocalCircleExclusion>,
        bootstrap_pending_exclusions: Vec<LocalCircleExclusion>,
    ) -> Self {
        Self {
            circles,
            stream_activations,
            bootstraps,
            local_exclusions,
            bootstrap_pending_exclusions,
        }
    }

    pub(crate) fn none(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, crate::protocol::store_commit::StoreProtocolError> {
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::none(commit, commit_ref)?,
            bootstraps: Vec::new(),
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
    }

    pub(crate) fn membership_control(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, crate::protocol::store_commit::StoreProtocolError> {
        if !commit.circle_controls().is_empty() {
            return Err(
                crate::protocol::store_commit::StoreProtocolError::Malformed(
                    "Store membership control also carries Circle controls".to_string(),
                ),
            );
        }
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::from_verified_store_control(
                commit, commit_ref,
            )?,
            bootstraps: Vec::new(),
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
    }

    pub(crate) fn circles(&self) -> &[VerifiedCircleReference] {
        &self.circles
    }

    pub(crate) fn stream_activations(&self) -> &VerifiedStreamActivations {
        &self.stream_activations
    }

    pub(crate) fn bootstraps(&self) -> &[VerifiedCircleImage] {
        &self.bootstraps
    }

    pub(crate) fn local_exclusions(&self) -> &[LocalCircleExclusion] {
        &self.local_exclusions
    }

    pub(crate) fn bootstrap_pending_exclusions(&self) -> &[LocalCircleExclusion] {
        &self.bootstrap_pending_exclusions
    }

    pub(crate) fn to_retained(&self) -> Result<Vec<u8>, CircleStateError> {
        let retained = RetainedCircleActivations {
            activating_commit: self.stream_activations.activating_commit.clone(),
            circles: self
                .circles
                .iter()
                .map(RetainedCircleReference::from_verified)
                .collect(),
            bootstraps: self.bootstraps.clone(),
        };
        serde_json::to_vec(&retained).map_err(|error| {
            CircleStateError(format!("serialize retained Circle activations: {error}"))
        })
    }

    pub(crate) fn parse_retained_for_verified_commit(
        bytes: &[u8],
        verified: &VerifiedStoreBatchCommit,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleStateError> {
        let commit = verified.value();
        let commit_ref = verified.reference();
        let retained: RetainedCircleActivations =
            serde_json::from_slice(bytes).map_err(|error| {
                CircleStateError(format!("parse retained Circle activations: {error}"))
            })?;
        let canonical = serde_json::to_vec(&retained).map_err(|error| {
            CircleStateError(format!(
                "serialize parsed retained Circle activations: {error}"
            ))
        })?;
        if canonical != bytes {
            return Err(CircleStateError(
                "retained Circle activation bytes are not canonical".to_string(),
            ));
        }
        if retained.activating_commit != *commit_ref
            || retained.circles.len() != commit.circle_controls().len()
        {
            return Err(CircleStateError(
                "retained Circle activations differ from their exact Store commit".to_string(),
            ));
        }

        let circles = retained
            .circles
            .into_iter()
            .zip(commit.circle_controls())
            .map(|(retained, reference)| {
                retained.verify_and_open(verified, recipient_pubkey, reference)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut expected_bootstraps = BTreeMap::new();
        for circle in &circles {
            let Some(access) = circle.local_access.as_ref() else {
                continue;
            };
            let CircleAccessDisposition::Active {
                bootstrap: Some(reference),
                ..
            } = &access.leaf.value.disposition
            else {
                continue;
            };
            if expected_bootstraps
                .insert(
                    (circle.circle_id, circle.control.coord.clone()),
                    (&access.leaf.value, reference),
                )
                .is_some()
            {
                return Err(CircleStateError(
                    "retained Circle activations repeat a bootstrap recipient".to_string(),
                ));
            }
        }
        if retained.bootstraps.len() != expected_bootstraps.len() {
            return Err(CircleStateError(
                "retained Circle bootstrap set is incomplete".to_string(),
            ));
        }
        for bootstrap in &retained.bootstraps {
            let (access, reference) = expected_bootstraps
                .remove(&(bootstrap.circle_id, bootstrap.control.clone()))
                .ok_or_else(|| {
                    CircleStateError(
                        "retained Circle bootstrap has no signed access leaf".to_string(),
                    )
                })?;
            if bootstrap.reference != *reference {
                return Err(CircleStateError(
                    "retained Circle bootstrap reference differs from its access leaf".to_string(),
                ));
            }
            bootstrap.verify_for_access(access)?;
        }
        Ok(Self {
            circles,
            stream_activations: VerifiedStreamActivations::from_verified_circle_commit(
                commit, commit_ref,
            )
            .map_err(|error| CircleStateError(error.to_string()))?,
            bootstraps: retained.bootstraps,
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn parse_retained(
        bytes: &[u8],
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        author: &StoreDeviceRegistration,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleStateError> {
        let verified = VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            commit.store_root_hash,
            commit_ref,
            author,
        )
        .map_err(|error| CircleStateError(error.to_string()))?;
        Self::parse_retained_for_verified_commit(bytes, &verified, recipient_pubkey)
    }
}

impl RetainedCircleReference {
    fn from_verified(verified: &VerifiedCircleReference) -> Self {
        Self {
            reference: verified.reference.clone(),
            circle_id: verified.circle_id,
            control: verified.control.clone(),
            local_access: verified
                .local_access
                .as_ref()
                .map(RetainedCircleAccess::from_verified),
        }
    }

    fn verify_and_open(
        self,
        verified: &VerifiedStoreBatchCommit,
        recipient_pubkey: Option<&str>,
        reference: &CircleControlRef,
    ) -> Result<VerifiedCircleReference, CircleStateError> {
        let commit = verified.value();
        if self.reference != *reference || self.circle_id != reference.circle_id() {
            return Err(CircleStateError(
                "retained Circle reference differs from its exact Store commit".to_string(),
            ));
        }
        verify_control_context_for_verified_commit(reference, &self.control, verified)?;
        let local_access = self
            .local_access
            .map(|access| {
                access.verify_and_open(commit, reference, &self.control, recipient_pubkey)
            })
            .transpose()?;
        let verified = VerifiedCircleReference {
            reference: self.reference,
            circle_id: self.circle_id,
            control: self.control,
            local_access,
        };
        CircleCurrentState::from_verified(commit.candidate_family(), &verified).map_err(
            |error| {
                CircleStateError(format!(
                    "retained Circle activation state failed verification: {error}"
                ))
            },
        )?;
        Ok(verified)
    }
}

impl RetainedCircleAccess {
    fn from_verified(verified: &VerifiedCircleAccess) -> Self {
        let state = match &verified.active {
            Some(active) => RetainedCircleAccessState::Active {
                roster: active.roster.clone(),
                metadata: active.metadata.clone(),
            },
            None => RetainedCircleAccessState::Inactive,
        };
        Self {
            access: PreparedCircleAccess {
                leaf: verified.leaf.clone(),
                envelope: verified.envelope.clone(),
            },
            state,
        }
    }

    fn verify_and_open(
        self,
        commit: &StoreBatchCommit,
        reference: &CircleControlRef,
        control: &PreparedCircleControl,
        recipient_pubkey: Option<&str>,
    ) -> Result<VerifiedCircleAccess, CircleStateError> {
        if !self.access.leaf.verify_envelope(
            control,
            &self.access.envelope,
            commit.candidate_family(),
        ) {
            return Err(CircleStateError(
                "retained Circle access leaf and envelope failed verification".to_string(),
            ));
        }
        if let Some(recipient_pubkey) = recipient_pubkey {
            if self.access.leaf.value.recipient_pubkey != recipient_pubkey {
                return Err(CircleStateError(
                    "retained Circle access names another local recipient".to_string(),
                ));
            }
        }
        if !reference
            .objects()
            .access
            .iter()
            .any(|candidate| retained_access_matches(candidate, &self.access))
        {
            return Err(CircleStateError(
                "retained Circle access differs from every exact commit reference".to_string(),
            ));
        }
        let active = match (self.access.leaf.value.disposition.clone(), self.state) {
            (
                CircleAccessDisposition::Active { .. },
                RetainedCircleAccessState::Active { roster, metadata },
            ) => Some(VerifiedCircleActive { roster, metadata }),
            (CircleAccessDisposition::Inactive, RetainedCircleAccessState::Inactive) => None,
            _ => {
                return Err(CircleStateError(
                    "retained Circle access state differs from its signed disposition".to_string(),
                ));
            }
        };
        Ok(VerifiedCircleAccess {
            envelope: self.access.envelope,
            leaf: self.access.leaf,
            active,
        })
    }
}

fn retained_access_matches(
    reference: &CircleAccessObjectRef,
    access: &PreparedCircleAccess,
) -> bool {
    reference.envelope.owner_pubkey == access.envelope.owner_pubkey
        && reference.envelope.recipient_slot == access.envelope.recipient_slot
        && reference.envelope.control_hash == access.envelope.control_hash
        && reference.envelope.leaf_id == access.envelope.leaf_id
        && reference.envelope.leaf_hash == access.envelope.leaf_hash
        && reference.leaf.owner_pubkey == access.leaf.value.owner_pubkey
        && reference.leaf.epoch_id == access.leaf.value.epoch_id
        && reference.leaf.recipient_slot == access.leaf.value.recipient_slot
        && reference.leaf.leaf_id == access.leaf.value.leaf_id
        && reference.leaf.leaf_hash == access.leaf.leaf_hash
        && reference.leaf.object.stored_hash() == access.leaf.leaf_hash
        && u64::try_from(access.leaf.bytes.len())
            .is_ok_and(|size| reference.leaf.object.stored_size() == size)
        && reference.bootstrap
            == match &access.leaf.value.disposition {
                crate::protocol::circle::CircleAccessDisposition::Active { bootstrap, .. } => {
                    bootstrap.as_ref().map(|bootstrap| bootstrap.image.clone())
                }
                crate::protocol::circle::CircleAccessDisposition::Inactive => None,
            }
}
