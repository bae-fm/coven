use super::access::*;
use super::*;

#[derive(Debug, Clone)]
pub struct CircleAuthoringState {
    pub candidate_family: CandidateFamilyId,
    pub control: PreparedCircleControl,
    pub access: CircleAccessLeaf,
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleCurrentControl {
    pub(super) control: PreparedCircleControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleInactiveAccess {
    NotGranted,
    Inactive {
        candidate_family: CandidateFamilyId,
        access: CircleAccessLeaf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CircleAccessibleState {
    pub(super) current: CircleCurrentControl,
    candidate_family: CandidateFamilyId,
    access: CircleAccessLeaf,
    roster: CircleMaterializedRoster,
    metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleInactiveState {
    current: CircleCurrentControl,
    access: CircleInactiveAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleCurrentState {
    Active(Box<CircleAccessibleState>),
    Closing(Box<CircleAccessibleState>),
    Inactive(Box<CircleInactiveState>),
    Deleted(Box<CircleCurrentControl>),
    ControlConflict { branches: Vec<CircleCurrentControl> },
}

/// The roster identities that hold no active Store membership grant at the
/// current materialized membership chain. Their presence in the resolved roster
/// is what makes a Circle rotation-required until an Owner closes the epoch and
/// activates a successor roster without them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationRequired {
    pub removed_members: Vec<String>,
}

impl CircleCurrentControl {
    fn from_verified(activation: &VerifiedCircleReference) -> Self {
        Self {
            control: activation.control.clone(),
        }
    }

    pub fn circle_id(&self) -> CircleId {
        self.control.value.circle_id
    }

    pub fn coordinate(&self) -> &CircleControlCoord {
        &self.control.coord
    }

    pub(super) fn control_hash(&self) -> ObjectHash {
        self.control.coord.control_hash()
    }

    fn causally_covers(&self, prior: &Self) -> bool {
        self.control.value.causally_covers(&prior.control.value)
    }

    fn verify(&self) -> bool {
        self.control.verify()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn control_mut_for_test(&mut self) -> &mut PreparedCircleControl {
        &mut self.control
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn control_hash_for_test(&self) -> ObjectHash {
        self.control_hash()
    }
}

impl CircleCurrentState {
    pub fn from_verified(
        candidate_family: CandidateFamilyId,
        activation: &VerifiedCircleReference,
    ) -> Result<Self, CircleStateError> {
        let current = CircleCurrentControl::from_verified(activation);
        // A deletion is terminal and carries no live access material; it reduces
        // to Deleted regardless of any retained access leaf.
        if current.control.value.state().is_deleted() {
            let state = Self::Deleted(Box::new(current));
            return if state.verify() {
                Ok(state)
            } else {
                Err(CircleStateError::Invariant(
                    "verified Circle deletion cannot form a valid current state".to_string(),
                ))
            };
        }
        let state = match &activation.local_access {
            None => Self::Inactive(Box::new(CircleInactiveState {
                current,
                access: CircleInactiveAccess::NotGranted,
            })),
            Some(VerifiedCircleAccess {
                leaf, active: None, ..
            }) => Self::Inactive(Box::new(CircleInactiveState {
                current,
                access: CircleInactiveAccess::Inactive {
                    candidate_family,
                    access: leaf.value.clone(),
                },
            })),
            Some(VerifiedCircleAccess {
                leaf,
                active: Some(active),
                ..
            }) => {
                let accessible = Box::new(CircleAccessibleState {
                    current,
                    candidate_family,
                    access: leaf.value.clone(),
                    roster: active.roster.clone(),
                    metadata: active.metadata.clone(),
                });
                match accessible.current.control.value.state() {
                    crate::circle::CircleControlState::ActiveEpoch(_) => Self::Active(accessible),
                    crate::circle::CircleControlState::EpochClose(_) => Self::Closing(accessible),
                    crate::circle::CircleControlState::Deleted(_) => {
                        return Err(CircleStateError::Invariant(
                            "verified Circle deletion cannot carry active access".to_string(),
                        ))
                    }
                }
            }
        };
        if state.verify() {
            Ok(state)
        } else {
            Err(CircleStateError::Invariant(
                "verified Circle activation cannot form a valid current state".to_string(),
            ))
        }
    }

    pub fn advance(self, next: Self) -> Result<Self, CircleStateError> {
        if !self.verify() || !next.verify() {
            return Err(CircleStateError::Invariant(
                "Circle current-state reduction received invalid state".to_string(),
            ));
        }
        if self.circle_id() != next.circle_id() {
            return Err(CircleStateError::Invariant(
                "Circle current-state reduction crossed Circle identities".to_string(),
            ));
        }
        match self {
            Self::Active(active) => advance_resolved_control(active.current, next),
            Self::Closing(closing) => advance_resolved_control(closing.current, next),
            Self::Inactive(inactive) => advance_resolved_control(inactive.current, next),
            // A deletion is terminal. Dependency-readiness materializes it
            // before anything descending from it, so a control that causally
            // covers it here is an invalid descendant and is rejected; a
            // concurrent branch that does not cover it surfaces as the conflict
            // the Owner must resolve, exactly like any racing successor.
            Self::Deleted(deleted) => {
                let next_current = next.resolved_control().ok_or_else(|| {
                    CircleStateError::Invariant(
                        "new Circle activation is already conflicted".to_string(),
                    )
                })?;
                if next_current.causally_covers(&deleted) {
                    return Err(CircleStateError::Invariant(
                        "Circle deletion is terminal; a control descending from it is invalid"
                            .to_string(),
                    ));
                }
                let mut branches = vec![*deleted, next_current.clone()];
                canonicalize_control_branches(&mut branches)?;
                Ok(Self::ControlConflict { branches })
            }
            Self::ControlConflict { mut branches } => {
                let next_current = next
                    .resolved_control()
                    .ok_or_else(|| {
                        CircleStateError::Invariant(
                            "new Circle activation is already conflicted".to_string(),
                        )
                    })?
                    .clone();
                branches.retain(|branch| !next_current.causally_covers(branch));
                if branches.is_empty() {
                    return Ok(next);
                }
                branches.push(next_current);
                canonicalize_control_branches(&mut branches)?;
                Ok(Self::ControlConflict { branches })
            }
        }
    }

    pub fn without_local_access(self) -> Self {
        match self {
            Self::Active(accessible) | Self::Closing(accessible) => {
                Self::Inactive(Box::new(CircleInactiveState {
                    current: accessible.current,
                    access: CircleInactiveAccess::NotGranted,
                }))
            }
            Self::Inactive(inactive) => Self::Inactive(Box::new(CircleInactiveState {
                current: inactive.current,
                access: CircleInactiveAccess::NotGranted,
            })),
            Self::Deleted(deleted) => Self::Deleted(deleted),
            Self::ControlConflict { branches } => Self::ControlConflict { branches },
        }
    }

    pub fn verify(&self) -> bool {
        match self {
            Self::Active(active) => {
                matches!(
                    active.current.control.value.state(),
                    crate::circle::CircleControlState::ActiveEpoch(_)
                ) && verify_accessible_state(active)
            }
            Self::Closing(closing) => {
                matches!(
                    closing.current.control.value.state(),
                    crate::circle::CircleControlState::EpochClose(_)
                ) && verify_accessible_state(closing)
            }
            Self::Inactive(inactive) => {
                inactive.current.verify()
                    && match &inactive.access {
                        CircleInactiveAccess::NotGranted => true,
                        CircleInactiveAccess::Inactive {
                            candidate_family,
                            access,
                        } => {
                            access.verify_for_control(&inactive.current.control, *candidate_family)
                                && matches!(access.disposition, CircleAccessDisposition::Inactive)
                        }
                    }
            }
            Self::Deleted(deleted) => {
                matches!(
                    deleted.control.value.state(),
                    crate::circle::CircleControlState::Deleted(_)
                ) && deleted.verify()
            }
            Self::ControlConflict { branches } => {
                branches.len() >= 2
                    && branches.iter().all(|branch| {
                        branch.verify() && branch.circle_id() == branches[0].circle_id()
                    })
                    && branches
                        .windows(2)
                        .all(|pair| pair[0].control_hash() < pair[1].control_hash())
            }
        }
    }

    pub fn circle_id(&self) -> CircleId {
        match self {
            Self::Active(active) => active.current.circle_id(),
            Self::Closing(closing) => closing.current.circle_id(),
            Self::Inactive(inactive) => inactive.current.circle_id(),
            Self::Deleted(deleted) => deleted.circle_id(),
            Self::ControlConflict { branches } => branches[0].circle_id(),
        }
    }

    /// A rotation is required when the resolved roster names identities that hold
    /// no active Store membership grant. Only meaningful for states that carry a
    /// roster; `Inactive` and `ControlConflict` return `None`.
    pub fn rotation_required(
        &self,
        active_store_members: &BTreeSet<String>,
    ) -> Option<RotationRequired> {
        let accessible = match self {
            Self::Active(accessible) | Self::Closing(accessible) => accessible,
            Self::Inactive(_) | Self::Deleted(_) | Self::ControlConflict { .. } => return None,
        };
        let removed_members: Vec<String> = accessible
            .roster
            .members()
            .into_keys()
            .filter(|pubkey| !active_store_members.contains(pubkey))
            .collect();
        if removed_members.is_empty() {
            None
        } else {
            Some(RotationRequired { removed_members })
        }
    }

    /// Map this internal current state to the public [`crate::circle::CircleState`].
    /// This is the single place the derivation lives.
    ///
    /// Rotation-required is surfaced only for an `Active` Circle. A `Closing`
    /// Circle whose roster still names a removed Store member stays `Closing`
    /// rather than reporting `RotationRequired`: an epoch close is already the
    /// exit path a rotation drives toward, so once a close is in flight the close
    /// is the operative state to show. `Inactive`, `Deleted`, and
    /// `ControlConflict` carry no roster to make a rotation judgment from.
    pub fn derived_state(
        &self,
        active_store_members: &BTreeSet<String>,
    ) -> crate::circle::CircleState {
        use crate::circle::CircleState;
        match self {
            Self::Active(_) => match self.rotation_required(active_store_members) {
                Some(RotationRequired { removed_members }) => {
                    CircleState::RotationRequired { removed_members }
                }
                None => CircleState::Active,
            },
            Self::Closing(_) => CircleState::Closing,
            Self::Inactive(_) => CircleState::Inactive,
            Self::Deleted(_) => CircleState::Deleted,
            Self::ControlConflict { branches } => CircleState::ControlConflict {
                branches: branches
                    .iter()
                    .map(|branch| branch.coordinate().clone())
                    .collect(),
            },
        }
    }

    /// The Circle's display name and the local identity's role, for the public
    /// list item. Both come from the resolved roster and metadata an accessible
    /// state carries (`Active` or `Closing`); an `Inactive`, `Deleted`, or
    /// conflicted Circle resolves neither.
    pub fn display(
        &self,
        identity_pubkey: &str,
    ) -> (Option<String>, Option<crate::circle::CircleRole>) {
        let accessible = match self {
            Self::Active(accessible) | Self::Closing(accessible) => accessible,
            Self::Inactive(_) | Self::Deleted(_) | Self::ControlConflict { .. } => {
                return (None, None)
            }
        };
        let role = accessible.roster.members().get(identity_pubkey).copied();
        (Some(accessible.metadata.name.clone()), role)
    }

    pub fn active(
        &self,
    ) -> Option<(
        &CircleCurrentControl,
        &CircleAccessLeaf,
        &CircleMaterializedRoster,
        &CircleMetadata,
    )> {
        match self {
            Self::Active(active) => Some((
                &active.current,
                &active.access,
                &active.roster,
                &active.metadata,
            )),
            Self::Closing(_)
            | Self::Inactive(_)
            | Self::Deleted(_)
            | Self::ControlConflict { .. } => None,
        }
    }

    pub fn active_record_count(&self) -> usize {
        match self {
            Self::Active(_) | Self::Closing(_) => 1,
            Self::Inactive(_) | Self::Deleted(_) => 0,
            Self::ControlConflict { branches } => branches.len(),
        }
    }

    pub fn authoring_state(&self) -> Option<CircleAuthoringState> {
        match self {
            Self::Active(active) => Some(CircleAuthoringState {
                candidate_family: active.candidate_family,
                control: active.current.control.clone(),
                access: active.access.clone(),
                roster: active.roster.clone(),
                metadata: active.metadata.clone(),
            }),
            Self::Closing(_)
            | Self::Inactive(_)
            | Self::Deleted(_)
            | Self::ControlConflict { .. } => None,
        }
    }

    pub fn closing_authoring_state(&self) -> Option<CircleAuthoringState> {
        match self {
            Self::Closing(closing) => Some(CircleAuthoringState {
                candidate_family: closing.candidate_family,
                control: closing.current.control.clone(),
                access: closing.access.clone(),
                roster: closing.roster.clone(),
                metadata: closing.metadata.clone(),
            }),
            Self::Active(_)
            | Self::Inactive(_)
            | Self::Deleted(_)
            | Self::ControlConflict { .. } => None,
        }
    }

    /// The authoring state a terminal deletion signs from. Deletion is the one
    /// command that authors from a closing epoch, so it accepts any state whose
    /// local device holds owner access — `Active` or `Closing` — and reads the
    /// frozen epoch spine through the control's `access_epoch`. `Inactive`,
    /// `Deleted`, and `ControlConflict` hold no owner access to sign a successor.
    pub fn deletable_authoring_state(&self) -> Option<CircleAuthoringState> {
        match self {
            Self::Active(accessible) | Self::Closing(accessible) => Some(CircleAuthoringState {
                candidate_family: accessible.candidate_family,
                control: accessible.current.control.clone(),
                access: accessible.access.clone(),
                roster: accessible.roster.clone(),
                metadata: accessible.metadata.clone(),
            }),
            Self::Inactive(_) | Self::Deleted(_) | Self::ControlConflict { .. } => None,
        }
    }

    pub fn epoch_access(
        &self,
        expected_control: &CircleControlCoord,
    ) -> Result<Option<CircleEpochAccess>, CircleStateError> {
        let Self::Active(active) = self else {
            return Ok(None);
        };
        if active.current.coordinate() != expected_control {
            return Ok(None);
        }
        if !verify_accessible_state(active) {
            return Err(CircleStateError::Invariant(format!(
                "Circle {} current package access is invalid",
                active.current.circle_id()
            )));
        }
        epoch_access_from(
            active.current.circle_id(),
            &active.current.control.value,
            &active.access.disposition,
            &active.roster,
        )
        .map(Some)
    }

    pub fn resolved_control(&self) -> Option<&CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&active.current),
            Self::Closing(closing) => Some(&closing.current),
            Self::Inactive(inactive) => Some(&inactive.current),
            Self::Deleted(deleted) => Some(deleted),
            Self::ControlConflict { .. } => None,
        }
    }

    /// Whether this Circle's control history has terminated in a deletion.
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    /// The retained conflicting branch coordinates, in canonical order, when
    /// this Circle's control history forked into concurrent valid successors.
    /// `None` for every resolved state.
    pub fn conflict_branches(&self) -> Option<Vec<CircleControlCoord>> {
        match self {
            Self::ControlConflict { branches } => Some(
                branches
                    .iter()
                    .map(|branch| branch.coordinate().clone())
                    .collect(),
            ),
            Self::Active(_) | Self::Closing(_) | Self::Inactive(_) | Self::Deleted(_) => None,
        }
    }

    pub fn closing_control(&self) -> Option<&PreparedCircleControl> {
        match self {
            Self::Closing(closing) => Some(&closing.current.control),
            Self::Active(_)
            | Self::Inactive(_)
            | Self::Deleted(_)
            | Self::ControlConflict { .. } => None,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn active_current_mut_for_test(&mut self) -> Option<&mut CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&mut active.current),
            _ => None,
        }
    }
}

fn verify_accessible_state(state: &CircleAccessibleState) -> bool {
    state.current.verify()
        && state
            .access
            .verify_for_control(&state.current.control, state.candidate_family)
        && matches!(
            state.access.disposition,
            CircleAccessDisposition::Active { .. }
        )
        && state.roster.verify()
        && state.metadata.verify()
        && state.metadata.circle_id == state.current.circle_id()
        && state.metadata.epoch_id == state.current.control.value.epoch_id()
        && state.metadata.key_fingerprint == state.current.control.value.key_fingerprint()
        && metadata_matches_control(&state.metadata, &state.current.control.value)
        && roster_matches_control(&state.roster, &state.current.control.value)
}

fn advance_resolved_control(
    current: CircleCurrentControl,
    next: CircleCurrentState,
) -> Result<CircleCurrentState, CircleStateError> {
    let next_current = next.resolved_control().ok_or_else(|| {
        CircleStateError::Invariant("new Circle activation is already conflicted".to_string())
    })?;
    if next_current.causally_covers(&current) {
        Ok(next)
    } else {
        let mut branches = vec![current, next_current.clone()];
        canonicalize_control_branches(&mut branches)?;
        Ok(CircleCurrentState::ControlConflict { branches })
    }
}

fn canonicalize_control_branches(
    branches: &mut [CircleCurrentControl],
) -> Result<(), CircleStateError> {
    branches.sort_by_key(CircleCurrentControl::control_hash);
    if branches
        .windows(2)
        .any(|pair| pair[0].control_hash() == pair[1].control_hash())
    {
        return Err(CircleStateError::Invariant(
            "Circle control conflict contains a duplicate branch".to_string(),
        ));
    }
    Ok(())
}

fn roster_matches_control(roster: &CircleMaterializedRoster, control: &CircleControl) -> bool {
    control.roster_state_ref().state_hash == roster.state_hash()
}

fn metadata_matches_control(metadata: &CircleMetadata, control: &CircleControl) -> bool {
    let state = control.metadata_state_ref();
    state.selected == metadata.coord() && state.state_hash == metadata.metadata_hash()
}
