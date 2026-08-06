use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::circle::{
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleBootstrapRef, CircleControl,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleMetadata, PreparedAccessLeaf,
    PreparedCircleAccess, PreparedCircleControl,
};
use crate::circle_roster::CircleMaterializedRoster;
use crate::store_commit::{
    CandidateFamilyId, CircleAccessObjectRef, CircleControlRef, CirclePackageRef, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StreamActivation, StreamActivationId, VerifiedStoreBatchCommit,
};
use coven_keys::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};

/// The local device's own exclusion from a Circle epoch close, derived strictly
/// from the verified successor outcome at materialization. It records the exact
/// close and successor an excluded device must reset from. Never derived from
/// unverified storage.
/// Verified Circle activation state that contradicts itself, its control, or
/// the commit that carries it. Produced by the activation values' own
/// validation; workflow errors wrap it at the operation boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid Circle state: {0}")]
pub struct CircleStateError(pub(crate) String);

mod access;
mod activations;
mod current_state;

pub use access::{
    CircleEpochAccess, LocalCircleExclusion, VerifiedCircleAccess, VerifiedCircleActive,
    VerifiedCircleImage, VerifiedCircleReference,
};
pub use activations::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix, VerifiedStreamActivations,
};
#[cfg(any(test, feature = "test-utils"))]
pub use current_state::CircleCurrentControl;
pub use current_state::{CircleAuthoringState, CircleCurrentState};

pub fn verify_control_context_for_verified_commit(
    reference: &CircleControlRef,
    control: &PreparedCircleControl,
    verified: &VerifiedStoreBatchCommit,
) -> Result<(), CircleStateError> {
    verified
        .reference()
        .verify_commit(verified.value())
        .map_err(|error| CircleStateError(error.to_string()))?;
    let commit = verified.value();
    let author = verified.author();
    let device_matches = control.value.value.order.device_id == author.device_id.to_string();
    if !control.verify()
        || reference.circle_id() != control.value.circle_id
        || reference.control() != &control.coord
        || control.value.store_root_hash != commit.store_root_hash
        || control.value.author_pubkey != author.author_pubkey
        || !device_matches
    {
        return Err(CircleStateError(
            "circle control context differs from its Store reference and commit".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod derived_state_tests;
