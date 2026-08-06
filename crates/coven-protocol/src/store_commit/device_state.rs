use super::validation::{validate_commit_frontier, validate_store_device_state_ref};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceStateRef {
    frontier: CommitFrontier,
    recovery: Vec<OwnerRecoveryCursor>,
    state_hash: ObjectHash,
}

impl StoreDeviceStateRef {
    pub fn from_resolved(
        frontier: CommitFrontier,
        state: &ResolvedStoreDeviceState,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&frontier)?;
        validate_recovery_cursors(&state.recovery)?;
        Ok(Self {
            frontier,
            recovery: state.recovery.clone(),
            state_hash: state.state_hash,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn recovery(&self) -> &[OwnerRecoveryCursor] {
        &self.recovery
    }

    pub fn frontier(&self) -> &CommitFrontier {
        &self.frontier
    }

    pub fn with_frontier(&self, frontier: CommitFrontier) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&frontier)?;
        Ok(Self {
            frontier,
            recovery: self.recovery.clone(),
            state_hash: self.state_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceStatus {
    Active,
    Inactive {
        terminals: Vec<StoreDeviceExclusionRef>,
        accepted_cut: StoreHistoryCut,
    },
}

mod exclusion;
mod resolution;
mod retained;

pub use exclusion::*;
pub use resolution::*;
pub use retained::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRecord {
    pub registration: StoreDeviceRegistrationRef,
    pub proposals: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    pub status: StoreDeviceStatus,
}

impl OwnerRecoveryNodeRef {
    pub fn slot(&self) -> &ObjectSlot {
        self.object.slot()
    }
}
