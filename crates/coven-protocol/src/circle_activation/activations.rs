use super::access::*;
use super::*;
use crate::circle_control::CircleMetadataCoord;
use crate::circle_roster::{CircleAuthorStreamKey, CircleRosterCoord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedStreamActivations {
    activating_commit: StoreBatchCommitRef,
    activations: Vec<StreamActivation>,
}

impl VerifiedStreamActivations {
    pub fn none(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::store_commit::StoreProtocolError> {
        if !commit.stream_activations().is_empty() {
            return Err(crate::store_commit::StoreProtocolError::Malformed(
                "Store commit stream activations have not been verified".to_string(),
            ));
        }
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: Vec::new(),
        })
    }

    /// The author-stream activations a verified commit carries.
    ///
    /// Only a Store membership control activates author streams. A Circle
    /// operations commit owns none: its controls, rosters and metadata are
    /// named by accepted Store history alone.
    pub fn for_verified_commit(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::store_commit::StoreProtocolError> {
        if commit.control().is_some() {
            Self::from_verified_store_control(commit, activating_commit)
        } else {
            Self::none(commit, activating_commit)
        }
    }

    pub(crate) fn from_verified_store_control(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        if commit.control().is_none() {
            return Err(crate::store_commit::StoreProtocolError::Malformed(
                "verified Store membership activations carry another control".to_string(),
            ));
        }
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub fn as_slice(&self) -> &[StreamActivation] {
        &self.activations
    }

    pub fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

/// Accepted Circle activations verified earlier in the same pull but not yet
/// installed, indexed by the Store commit that activated them.
///
/// An inherited Circle roster or metadata entry names the exact accepted Store
/// commit that introduced it. A device replaying a batch of commits — a newly
/// admitted device staging foreign history, or a pull applying several commits
/// at once — has not installed the earlier commits yet, so this carries them
/// for the introduction proof.
#[derive(Debug, Clone, Default)]
pub struct VerifiedCircleActivationPrefix {
    by_commit: BTreeMap<StoreBatchCommitRef, Vec<VerifiedCircleReference>>,
}

impl VerifiedCircleActivationPrefix {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn include(
        &mut self,
        verified: &VerifiedCircleActivations,
    ) -> Result<(), crate::store_commit::StoreProtocolError> {
        let circles = verified.circles().to_vec();
        match self.by_commit.entry(verified.activating_commit().clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(circles);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &circles => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(crate::store_commit::StoreProtocolError::Malformed(
                    "verified Circle activation prefix contains conflicting activation authority"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Every activation this prefix holds for `circle_id`, in commit order.
    /// A position check reads all of them: an entry introduced earlier in the
    /// same pull is already accepted history for the commit being verified.
    pub fn activations_for(
        &self,
        circle_id: CircleId,
    ) -> impl Iterator<Item = &VerifiedCircleReference> {
        self.by_commit
            .values()
            .flatten()
            .filter(move |activation| activation.circle_id == circle_id)
    }

    /// The activation `activating_commit` carries for `circle_id`, when this
    /// prefix holds that commit.
    pub fn activation(
        &self,
        activating_commit: &StoreBatchCommitRef,
        circle_id: CircleId,
    ) -> Option<&VerifiedCircleReference> {
        self.by_commit
            .get(activating_commit)?
            .iter()
            .find(|activation| activation.circle_id == circle_id)
    }
}

/// The roster and metadata author-stream positions one Circle's accepted
/// history has already filled, each with the exact entry that filled it.
///
/// Monotone inheritance is what makes a control's own inventory the whole
/// accepted history behind it: a successor carries forward every entry each
/// covered predecessor published. So the union over the Circle's current
/// control — or over every retained branch of a conflict — is every position
/// ever accepted, without walking the chain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AcceptedCircleEntryPositions {
    roster: BTreeMap<(CircleAuthorStreamKey, u64), CircleRosterCoord>,
    metadata: BTreeMap<(CircleAuthorStreamKey, u64), CircleMetadataCoord>,
}

impl AcceptedCircleEntryPositions {
    pub fn include(&mut self, objects: &crate::store_commit::CircleActivationObjects) {
        for coord in objects.roster_entries.keys() {
            self.roster
                .insert((coord.stream_key(), coord.seq), coord.clone());
        }
        for coord in objects.metadata_entries.keys() {
            self.metadata
                .insert((coord.stream_key(), coord.seq), coord.clone());
        }
    }

    /// The entry already accepted at this roster coordinate's position, when it
    /// is a different entry than `coord`.
    pub fn roster_conflict(&self, coord: &CircleRosterCoord) -> Option<&CircleRosterCoord> {
        self.roster
            .get(&(coord.stream_key(), coord.seq))
            .filter(|accepted| *accepted != coord)
    }

    pub fn metadata_conflict(&self, coord: &CircleMetadataCoord) -> Option<&CircleMetadataCoord> {
        self.metadata
            .get(&(coord.stream_key(), coord.seq))
            .filter(|accepted| *accepted != coord)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCircleActivations {
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
    access: PreparedAccessLeaf,
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
    pub fn from_verified_parts(
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

    pub fn none(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, crate::store_commit::StoreProtocolError> {
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::none(commit, commit_ref)?,
            bootstraps: Vec::new(),
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
    }

    pub fn membership_control(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, crate::store_commit::StoreProtocolError> {
        if !commit.circle_controls().is_empty() {
            return Err(crate::store_commit::StoreProtocolError::Malformed(
                "Store membership control also carries Circle controls".to_string(),
            ));
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

    pub fn circles(&self) -> &[VerifiedCircleReference] {
        &self.circles
    }

    pub fn stream_activations(&self) -> &VerifiedStreamActivations {
        &self.stream_activations
    }

    /// The exact accepted Store commit these activations were verified against.
    pub fn activating_commit(&self) -> &StoreBatchCommitRef {
        self.stream_activations.activating_commit()
    }

    pub fn bootstraps(&self) -> &[VerifiedCircleImage] {
        &self.bootstraps
    }

    pub fn local_exclusions(&self) -> &[LocalCircleExclusion] {
        &self.local_exclusions
    }

    pub fn bootstrap_pending_exclusions(&self) -> &[LocalCircleExclusion] {
        &self.bootstrap_pending_exclusions
    }

    pub fn without_local_access(mut self) -> Self {
        for circle in &mut self.circles {
            circle.local_access = None;
        }
        self.bootstraps.clear();
        self.local_exclusions.clear();
        self.bootstrap_pending_exclusions.clear();
        self
    }

    pub fn to_retained(&self) -> Result<Vec<u8>, CircleStateError> {
        let retained = RetainedCircleActivations {
            activating_commit: self.stream_activations.activating_commit.clone(),
            circles: self
                .circles
                .iter()
                .map(RetainedCircleReference::from_verified)
                .collect(),
            bootstraps: self.bootstraps.clone(),
        };
        serde_json::to_vec(&retained).map_err(|source| CircleStateError::Json {
            operation: "serialize retained Circle activations",
            source,
        })
    }

    pub fn parse_retained_for_verified_commit(
        bytes: &[u8],
        verified: &VerifiedStoreBatchCommit,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleStateError> {
        let commit = verified.value();
        let commit_ref = verified.reference();
        let retained: RetainedCircleActivations =
            serde_json::from_slice(bytes).map_err(|source| CircleStateError::Json {
                operation: "parse retained Circle activations",
                source,
            })?;
        let canonical = serde_json::to_vec(&retained).map_err(|source| CircleStateError::Json {
            operation: "serialize parsed retained Circle activations",
            source,
        })?;
        if canonical != bytes {
            return Err(CircleStateError::Invariant(
                "retained Circle activation bytes are not canonical".to_string(),
            ));
        }
        if retained.activating_commit != *commit_ref
            || retained.circles.len() != commit.circle_controls().len()
        {
            return Err(CircleStateError::Invariant(
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
                return Err(CircleStateError::Invariant(
                    "retained Circle activations repeat a bootstrap recipient".to_string(),
                ));
            }
        }
        if retained.bootstraps.len() != expected_bootstraps.len() {
            return Err(CircleStateError::Invariant(
                "retained Circle bootstrap set is incomplete".to_string(),
            ));
        }
        for bootstrap in &retained.bootstraps {
            let (access, reference) = expected_bootstraps
                .remove(&(bootstrap.circle_id, bootstrap.control.clone()))
                .ok_or_else(|| {
                    CircleStateError::Invariant(
                        "retained Circle bootstrap has no signed access leaf".to_string(),
                    )
                })?;
            if bootstrap.reference != *reference {
                return Err(CircleStateError::Invariant(
                    "retained Circle bootstrap reference differs from its access leaf".to_string(),
                ));
            }
            bootstrap.verify_for_access(access)?;
        }
        Ok(Self {
            circles,
            stream_activations: VerifiedStreamActivations::for_verified_commit(commit, commit_ref)?,
            bootstraps: retained.bootstraps,
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn parse_retained(
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
        )?;
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
            return Err(CircleStateError::Invariant(
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
        CircleCurrentState::from_verified(commit.candidate_family(), &verified)?;
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
            access: verified.leaf.clone(),
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
        if !self.access.verify(control, commit.candidate_family()) {
            return Err(CircleStateError::Invariant(
                "retained Circle access leaf failed verification".to_string(),
            ));
        }
        if let Some(recipient_pubkey) = recipient_pubkey {
            if self.access.value.recipient_pubkey != recipient_pubkey {
                return Err(CircleStateError::Invariant(
                    "retained Circle access names another local recipient".to_string(),
                ));
            }
        }
        if let CircleAccessDisposition::Active {
            bootstrap: Some(bootstrap),
            ..
        } = &self.access.value.disposition
        {
            if !reference
                .objects()
                .names_bootstrap(&self.access.value, bootstrap)
            {
                return Err(CircleStateError::Invariant(
                    "retained Circle access bootstrap is absent from its signed object graph"
                        .to_string(),
                ));
            }
        }
        let active = match (self.access.value.disposition.clone(), self.state) {
            (
                CircleAccessDisposition::Active { .. },
                RetainedCircleAccessState::Active { roster, metadata },
            ) => Some(VerifiedCircleActive { roster, metadata }),
            (CircleAccessDisposition::Inactive, RetainedCircleAccessState::Inactive) => None,
            _ => {
                return Err(CircleStateError::Invariant(
                    "retained Circle access state differs from its signed disposition".to_string(),
                ));
            }
        };
        Ok(VerifiedCircleAccess {
            leaf: self.access,
            active,
        })
    }
}
