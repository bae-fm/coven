use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::verify_control_context_for_verified_commit;
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::protocol::circle::{
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleBootstrapRef, CircleControl,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleMetadata, PreparedAccessLeaf,
    PreparedCircleAccess, PreparedCircleControl,
};
use crate::protocol::circle_roster::CircleMaterializedRoster;
use crate::protocol::store_commit::{
    CandidateFamilyId, CircleAccessObjectRef, CircleControlRef, CirclePackageRef, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StreamActivation, StreamActivationId, VerifiedStoreBatchCommit,
};
use crate::sync::store::circle_controls::CircleOperationError;

/// The local device's own exclusion from a Circle epoch close, derived strictly
/// from the verified successor outcome at materialization. It records the exact
/// close and successor an excluded device must reset from. Never derived from
/// unverified storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalCircleExclusion {
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub excluded: StoreDeviceRegistrationRef,
    pub successor_control: CircleControlCoord,
    pub activating_commit: StoreBatchCommitRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleReference {
    pub reference: CircleControlRef,
    pub circle_id: CircleId,
    pub control: PreparedCircleControl,
    pub local_access: Option<VerifiedCircleAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleAccess {
    pub envelope: AccessEnvelope,
    pub leaf: PreparedAccessLeaf,
    pub active: Option<VerifiedCircleActive>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleActive {
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedCircleImage {
    circle_id: CircleId,
    control: CircleControlCoord,
    reference: CircleBootstrapRef,
    image_bytes: Vec<u8>,
}

impl VerifiedCircleImage {
    pub(crate) fn new(
        circle_id: CircleId,
        control: CircleControlCoord,
        access: &CircleAccessLeaf,
        reference: CircleBootstrapRef,
        image_bytes: Vec<u8>,
    ) -> Result<Self, CircleOperationError> {
        let verified = Self {
            circle_id,
            control,
            reference,
            image_bytes,
        };
        verified.verify_for_access(access)?;
        Ok(verified)
    }

    /// Reconstruct a verified Circle image from stored bytes and an exact
    /// reference — the coverage-row and restore-selection path, which has no
    /// access leaf (a standalone snapshot names none). The bytes are input to the
    /// verifier, never trusted for being local: their digest must equal the
    /// reference's image hash, and the caller separately runs
    /// `verify_circle_bootstrap_image` against the retained control and routing
    /// key for the full schema/routing/audience/blob-closure check.
    pub(crate) fn from_stored_image(
        circle_id: CircleId,
        control: CircleControlCoord,
        reference: CircleBootstrapRef,
        image_bytes: Vec<u8>,
    ) -> Result<Self, CircleOperationError> {
        if reference.image.image_hash != ObjectHash::digest(&image_bytes) {
            return Err(CircleOperationError::InvalidState(
                "stored Circle image differs from its exact image hash".to_string(),
            ));
        }
        Ok(Self {
            circle_id,
            control,
            reference,
            image_bytes,
        })
    }

    fn verify_for_access(&self, access: &CircleAccessLeaf) -> Result<(), CircleOperationError> {
        if self.circle_id != access.circle_id
            || !self.reference.verify_for_access(access)
            || self.reference.image.image_hash != ObjectHash::digest(&self.image_bytes)
        {
            return Err(CircleOperationError::InvalidState(
                "verified Circle bootstrap differs from its signed access leaf".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub(crate) fn control(&self) -> &CircleControlCoord {
        &self.control
    }

    pub(crate) fn reference(&self) -> &CircleBootstrapRef {
        &self.reference
    }

    pub(crate) fn image_bytes(&self) -> &[u8] {
        &self.image_bytes
    }
}

#[derive(Clone)]
pub(crate) struct CircleEpochAccess {
    circle_id: CircleId,
    encryption: EncryptionService,
    key_fingerprint: KeyFingerprint,
    writers: BTreeSet<String>,
}

struct VerifiedCircleKeyring {
    keyring: EncryptionService,
    key_fingerprint: KeyFingerprint,
}

impl VerifiedCircleKeyring {
    fn into_keyring(self) -> EncryptionService {
        self.keyring
    }

    fn epoch_encryption(
        &self,
        circle_id: CircleId,
    ) -> Result<EncryptionService, CircleOperationError> {
        self.keyring
            .service_for_fingerprint(self.key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "select Circle package key for {circle_id}: {error}"
                ))
            })
    }
}

impl CircleEpochAccess {
    #[cfg(test)]
    pub(crate) fn authorizes_writer(&self, author_pubkey: &str) -> bool {
        self.writers.contains(author_pubkey)
    }

    pub(crate) fn key_fingerprint(&self) -> KeyFingerprint {
        self.key_fingerprint
    }

    pub(crate) fn protocol_context(
        &self,
        store_root_hash: ObjectHash,
        domain: crate::protocol::objects::CircleProtocolObjectDomain,
    ) -> crate::protocol::objects::ProtocolObjectContext {
        crate::protocol::objects::ProtocolObjectContext::circle(
            store_root_hash,
            domain,
            self.encryption.clone(),
        )
    }

    pub(crate) fn blob_protection(&self) -> crate::protocol::objects::BlobSpoolProtection {
        crate::protocol::objects::BlobSpoolProtection::Opaque(self.encryption.clone())
    }

    pub(crate) fn from_historical(
        circle_id: CircleId,
        key_fingerprint: KeyFingerprint,
        serialized_keyring: &str,
        roster: &CircleMaterializedRoster,
    ) -> Result<Self, CircleOperationError> {
        if !roster.verify() {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} historical package roster is invalid"
            )));
        }
        let keyring = MasterKeyring::from_serialized(serialized_keyring).map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "parse Circle {circle_id} historical package keyring: {error}"
            ))
        })?;
        let encryption = EncryptionService::from(keyring)
            .service_for_fingerprint(key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "select Circle {circle_id} historical package key: {error}"
                ))
            })?;
        Ok(Self {
            circle_id,
            encryption,
            key_fingerprint,
            writers: roster.members().keys().cloned().collect(),
        })
    }

    pub(crate) fn authorize_package(
        &self,
        reference: &CirclePackageRef,
        author: &StoreDeviceRegistration,
    ) -> Result<(), CircleOperationError> {
        if reference.circle_id != self.circle_id {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle package names {}, but access belongs to {}",
                reference.circle_id, self.circle_id
            )));
        }
        if !self.writers.contains(&author.author_pubkey) {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle package author is not a member of {} at its exact control",
                reference.circle_id
            )));
        }
        if self.key_fingerprint != reference.key_fingerprint {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle package key for {} differs from its activated control",
                reference.circle_id
            )));
        }
        Ok(())
    }
}

impl VerifiedCircleReference {
    pub(crate) fn retained_keyring(
        &self,
    ) -> Result<Option<EncryptionService>, CircleOperationError> {
        let Some(access) = self.local_access.as_ref() else {
            return Ok(None);
        };
        let Some(active) = access.active.as_ref() else {
            return Ok(None);
        };
        verified_keyring_from(
            self.circle_id,
            &self.control.value,
            &access.leaf.value.disposition,
            &active.roster,
        )
        .map(|keyring| Some(keyring.into_keyring()))
    }

    pub(crate) fn epoch_access(&self) -> Result<Option<CircleEpochAccess>, CircleOperationError> {
        let Some(access) = self.local_access.as_ref() else {
            return Ok(None);
        };
        let Some(active) = access.active.as_ref() else {
            return Ok(None);
        };
        epoch_access_from(
            self.circle_id,
            &self.control.value,
            &access.leaf.value.disposition,
            &active.roster,
        )
        .map(Some)
    }
}

fn epoch_access_from(
    circle_id: CircleId,
    control: &CircleControl,
    disposition: &CircleAccessDisposition,
    roster: &CircleMaterializedRoster,
) -> Result<CircleEpochAccess, CircleOperationError> {
    let verified = verified_keyring_from(circle_id, control, disposition, roster)?;
    let encryption = verified.epoch_encryption(circle_id)?;
    let key_fingerprint = verified.key_fingerprint;
    Ok(CircleEpochAccess {
        circle_id,
        encryption,
        key_fingerprint,
        writers: roster.members().keys().cloned().collect(),
    })
}

fn verified_keyring_from(
    circle_id: CircleId,
    control: &CircleControl,
    disposition: &CircleAccessDisposition,
    roster: &CircleMaterializedRoster,
) -> Result<VerifiedCircleKeyring, CircleOperationError> {
    if control.circle_id != circle_id
        || !roster.verify()
        || roster.state_hash() != control.roster_state_ref().state_hash
    {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} package roster differs from its activated control"
        )));
    }
    let CircleAccessDisposition::Active {
        keyring,
        key_fingerprint,
        ..
    } = disposition
    else {
        return Err(CircleOperationError::InvalidState(format!(
            "active Circle access for {circle_id} has an inactive leaf"
        )));
    };
    if *key_fingerprint != control.key_fingerprint() {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle package key for {circle_id} differs from its activated control"
        )));
    }
    let keyring = MasterKeyring::from_serialized(keyring).map_err(|error| {
        CircleOperationError::InvalidState(format!(
            "parse Circle package keyring for {circle_id}: {error}"
        ))
    })?;
    let keyring = EncryptionService::from(keyring);
    keyring
        .service_for_fingerprint(key_fingerprint.as_bytes())
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "select Circle package key for {circle_id}: {error}"
            ))
        })?;
    Ok(VerifiedCircleKeyring {
        keyring,
        key_fingerprint: *key_fingerprint,
    })
}

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

    pub(crate) fn to_retained(&self) -> Result<Vec<u8>, CircleOperationError> {
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
            CircleOperationError::InvalidState(format!(
                "serialize retained Circle activations: {error}"
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn parse_retained(
        bytes: &[u8],
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        author: &StoreDeviceRegistration,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleOperationError> {
        let verified = VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            commit.store_root_hash,
            commit_ref,
            author,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        Self::parse_retained_for_verified_commit(bytes, &verified, recipient_pubkey)
    }

    pub(crate) fn parse_retained_for_verified_commit(
        bytes: &[u8],
        verified: &VerifiedStoreBatchCommit,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleOperationError> {
        let commit = verified.value();
        let commit_ref = verified.reference();
        let retained: RetainedCircleActivations =
            serde_json::from_slice(bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse retained Circle activations: {error}"
                ))
            })?;
        let canonical = serde_json::to_vec(&retained).map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "serialize parsed retained Circle activations: {error}"
            ))
        })?;
        if canonical != bytes {
            return Err(CircleOperationError::InvalidState(
                "retained Circle activation bytes are not canonical".to_string(),
            ));
        }
        if retained.activating_commit != *commit_ref
            || retained.circles.len() != commit.circle_controls().len()
        {
            return Err(CircleOperationError::InvalidState(
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
                return Err(CircleOperationError::InvalidState(
                    "retained Circle activations repeat a bootstrap recipient".to_string(),
                ));
            }
        }
        if retained.bootstraps.len() != expected_bootstraps.len() {
            return Err(CircleOperationError::InvalidState(
                "retained Circle bootstrap set is incomplete".to_string(),
            ));
        }
        for bootstrap in &retained.bootstraps {
            let (access, reference) = expected_bootstraps
                .remove(&(bootstrap.circle_id, bootstrap.control.clone()))
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "retained Circle bootstrap has no signed access leaf".to_string(),
                    )
                })?;
            if bootstrap.reference != *reference {
                return Err(CircleOperationError::InvalidState(
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
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
            bootstraps: retained.bootstraps,
            local_exclusions: Vec::new(),
            bootstrap_pending_exclusions: Vec::new(),
        })
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
    ) -> Result<VerifiedCircleReference, CircleOperationError> {
        let commit = verified.value();
        if self.reference != *reference || self.circle_id != reference.circle_id() {
            return Err(CircleOperationError::InvalidState(
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
                CircleOperationError::InvalidState(format!(
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
    ) -> Result<VerifiedCircleAccess, CircleOperationError> {
        if !self.access.leaf.verify_envelope(
            control,
            &self.access.envelope,
            commit.candidate_family(),
        ) {
            return Err(CircleOperationError::InvalidState(
                "retained Circle access leaf and envelope failed verification".to_string(),
            ));
        }
        if let Some(recipient_pubkey) = recipient_pubkey {
            if self.access.leaf.value.recipient_pubkey != recipient_pubkey {
                return Err(CircleOperationError::InvalidState(
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
            return Err(CircleOperationError::InvalidState(
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
                return Err(CircleOperationError::InvalidState(
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

#[derive(Debug, Clone)]
pub(crate) struct CircleAuthoringState {
    pub candidate_family: CandidateFamilyId,
    pub control: PreparedCircleControl,
    pub access: CircleAccessLeaf,
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleCurrentControl {
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
pub(crate) struct CircleAccessibleState {
    pub(super) current: CircleCurrentControl,
    candidate_family: CandidateFamilyId,
    access: CircleAccessLeaf,
    roster: CircleMaterializedRoster,
    metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleInactiveState {
    current: CircleCurrentControl,
    access: CircleInactiveAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleCurrentState {
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
pub(crate) struct RotationRequired {
    pub removed_members: Vec<String>,
}

impl CircleCurrentControl {
    fn from_verified(activation: &VerifiedCircleReference) -> Self {
        Self {
            control: activation.control.clone(),
        }
    }

    pub(crate) fn circle_id(&self) -> CircleId {
        self.control.value.circle_id
    }

    pub(crate) fn coordinate(&self) -> &CircleControlCoord {
        &self.control.coord
    }

    #[cfg(test)]
    pub(crate) fn control_mut_for_test(&mut self) -> &mut PreparedCircleControl {
        &mut self.control
    }

    pub(super) fn control_hash(&self) -> ObjectHash {
        self.control.coord.control_hash()
    }

    #[cfg(test)]
    pub(crate) fn control_hash_for_test(&self) -> ObjectHash {
        self.control_hash()
    }

    fn causally_covers(&self, prior: &Self) -> bool {
        self.control.value.causally_covers(&prior.control.value)
    }

    fn verify(&self) -> bool {
        self.control.verify()
    }
}

impl CircleCurrentState {
    pub(crate) fn from_verified(
        candidate_family: CandidateFamilyId,
        activation: &VerifiedCircleReference,
    ) -> Result<Self, String> {
        let current = CircleCurrentControl::from_verified(activation);
        // A deletion is terminal and carries no live access material; it reduces
        // to Deleted regardless of any retained access leaf.
        if current.control.value.state().is_deleted() {
            let state = Self::Deleted(Box::new(current));
            return if state.verify() {
                Ok(state)
            } else {
                Err("verified Circle deletion cannot form a valid current state".to_string())
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
                    crate::protocol::circle::CircleControlState::ActiveEpoch(_) => {
                        Self::Active(accessible)
                    }
                    crate::protocol::circle::CircleControlState::EpochClose(_) => {
                        Self::Closing(accessible)
                    }
                    crate::protocol::circle::CircleControlState::Deleted(_) => {
                        return Err(
                            "verified Circle deletion cannot carry active access".to_string()
                        )
                    }
                }
            }
        };
        if state.verify() {
            Ok(state)
        } else {
            Err("verified Circle activation cannot form a valid current state".to_string())
        }
    }

    pub(crate) fn advance(self, next: Self) -> Result<Self, String> {
        if !self.verify() || !next.verify() {
            return Err("Circle current-state reduction received invalid state".to_string());
        }
        if self.circle_id() != next.circle_id() {
            return Err("Circle current-state reduction crossed Circle identities".to_string());
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
                let next_current = next
                    .resolved_control()
                    .ok_or_else(|| "new Circle activation is already conflicted".to_string())?;
                if next_current.causally_covers(&deleted) {
                    return Err(
                        "Circle deletion is terminal; a control descending from it is invalid"
                            .to_string(),
                    );
                }
                let mut branches = vec![*deleted, next_current.clone()];
                canonicalize_control_branches(&mut branches)?;
                Ok(Self::ControlConflict { branches })
            }
            Self::ControlConflict { mut branches } => {
                let next_current = next
                    .resolved_control()
                    .ok_or_else(|| "new Circle activation is already conflicted".to_string())?
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

    pub(crate) fn without_local_access(self) -> Self {
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

    pub(crate) fn verify(&self) -> bool {
        match self {
            Self::Active(active) => {
                matches!(
                    active.current.control.value.state(),
                    crate::protocol::circle::CircleControlState::ActiveEpoch(_)
                ) && verify_accessible_state(active)
            }
            Self::Closing(closing) => {
                matches!(
                    closing.current.control.value.state(),
                    crate::protocol::circle::CircleControlState::EpochClose(_)
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
                    crate::protocol::circle::CircleControlState::Deleted(_)
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

    pub(crate) fn circle_id(&self) -> CircleId {
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
    pub(crate) fn rotation_required(
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

    /// Map this internal current state to the public [`crate::protocol::circle::CircleState`].
    /// This is the single place the derivation lives.
    ///
    /// Rotation-required is surfaced only for an `Active` Circle. A `Closing`
    /// Circle whose roster still names a removed Store member stays `Closing`
    /// rather than reporting `RotationRequired`: an epoch close is already the
    /// exit path a rotation drives toward, so once a close is in flight the close
    /// is the operative state to show. `Inactive`, `Deleted`, and
    /// `ControlConflict` carry no roster to make a rotation judgment from.
    pub(crate) fn derived_state(
        &self,
        active_store_members: &BTreeSet<String>,
    ) -> crate::protocol::circle::CircleState {
        use crate::protocol::circle::CircleState;
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
    pub(crate) fn display(
        &self,
        identity_pubkey: &str,
    ) -> (Option<String>, Option<crate::protocol::circle::CircleRole>) {
        let accessible = match self {
            Self::Active(accessible) | Self::Closing(accessible) => accessible,
            Self::Inactive(_) | Self::Deleted(_) | Self::ControlConflict { .. } => {
                return (None, None)
            }
        };
        let role = accessible.roster.members().get(identity_pubkey).copied();
        (Some(accessible.metadata.name.clone()), role)
    }

    pub(crate) fn active(
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

    pub(crate) fn active_record_count(&self) -> usize {
        match self {
            Self::Active(_) | Self::Closing(_) => 1,
            Self::Inactive(_) | Self::Deleted(_) => 0,
            Self::ControlConflict { branches } => branches.len(),
        }
    }

    pub(crate) fn authoring_state(&self) -> Option<CircleAuthoringState> {
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

    pub(crate) fn closing_authoring_state(&self) -> Option<CircleAuthoringState> {
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
    pub(crate) fn deletable_authoring_state(&self) -> Option<CircleAuthoringState> {
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

    pub(crate) fn epoch_access(
        &self,
        expected_control: &CircleControlCoord,
    ) -> Result<Option<CircleEpochAccess>, CircleOperationError> {
        let Self::Active(active) = self else {
            return Ok(None);
        };
        if active.current.coordinate() != expected_control {
            return Ok(None);
        }
        if !verify_accessible_state(active) {
            return Err(CircleOperationError::InvalidState(format!(
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

    pub(crate) fn resolved_control(&self) -> Option<&CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&active.current),
            Self::Closing(closing) => Some(&closing.current),
            Self::Inactive(inactive) => Some(&inactive.current),
            Self::Deleted(deleted) => Some(deleted),
            Self::ControlConflict { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn active_current_mut_for_test(&mut self) -> Option<&mut CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&mut active.current),
            _ => None,
        }
    }

    /// Whether this Circle's control history has terminated in a deletion.
    pub(crate) fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    /// The retained conflicting branch coordinates, in canonical order, when
    /// this Circle's control history forked into concurrent valid successors.
    /// `None` for every resolved state.
    pub(crate) fn conflict_branches(&self) -> Option<Vec<CircleControlCoord>> {
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

    pub(crate) fn closing_control(&self) -> Option<&PreparedCircleControl> {
        match self {
            Self::Closing(closing) => Some(&closing.current.control),
            Self::Active(_)
            | Self::Inactive(_)
            | Self::Deleted(_)
            | Self::ControlConflict { .. } => None,
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
) -> Result<CircleCurrentState, String> {
    let next_current = next
        .resolved_control()
        .ok_or_else(|| "new Circle activation is already conflicted".to_string())?;
    if next_current.causally_covers(&current) {
        Ok(next)
    } else {
        let mut branches = vec![current, next_current.clone()];
        canonicalize_control_branches(&mut branches)?;
        Ok(CircleCurrentState::ControlConflict { branches })
    }
}

fn canonicalize_control_branches(branches: &mut [CircleCurrentControl]) -> Result<(), String> {
    branches.sort_by_key(CircleCurrentControl::control_hash);
    if branches
        .windows(2)
        .any(|pair| pair[0].control_hash() == pair[1].control_hash())
    {
        return Err("Circle control conflict contains a duplicate branch".to_string());
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

#[cfg(test)]
mod derived_state_tests {
    use super::*;
    use crate::protocol::circle::{CircleRole, CircleState};
    use std::collections::BTreeSet;

    fn accessible(state: &CircleCurrentState) -> Box<CircleAccessibleState> {
        match state {
            CircleCurrentState::Active(accessible) => accessible.clone(),
            other => panic!("expected an active current state, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn active_maps_by_rotation_over_the_membership() {
        let owner_pubkey =
            crate::keys::public_key_hex(&crate::database::test_circle_owner_keypair());
        let state = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
            .install_test_active_circle_state("derived-state".to_string())
            .await
            .expect("install and read the active current state");

        // Owner still a Store member: Active.
        let members = BTreeSet::from([owner_pubkey.clone()]);
        assert_eq!(state.derived_state(&members), CircleState::Active);

        // Owner no longer a Store member: RotationRequired naming it.
        let empty = BTreeSet::new();
        assert_eq!(
            state.derived_state(&empty),
            CircleState::RotationRequired {
                removed_members: vec![owner_pubkey.clone()],
            }
        );

        // Display resolves the name and the local role.
        let (name, role) = state.display(&owner_pubkey);
        assert!(name.is_some());
        assert_eq!(role, Some(CircleRole::Owner));
    }

    #[tokio::test]
    async fn closing_maps_to_closing_regardless_of_rotation() {
        let owner_pubkey =
            crate::keys::public_key_hex(&crate::database::test_circle_owner_keypair());
        let active =
            crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
                .install_test_active_circle_state("derived-state".to_string())
                .await
                .expect("install and read the active current state");
        let closing = CircleCurrentState::Closing(accessible(&active));
        // A closing Circle whose roster still names the (removed) member stays
        // Closing rather than reporting RotationRequired.
        assert_eq!(
            closing.derived_state(&BTreeSet::new()),
            CircleState::Closing
        );
        assert_eq!(
            closing.derived_state(&BTreeSet::from([owner_pubkey])),
            CircleState::Closing
        );
    }

    #[tokio::test]
    async fn inactive_maps_to_inactive_with_no_name_or_role() {
        let state = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
            .install_test_inactive_circle_state("derived-state-inactive".to_string())
            .await
            .expect("install and read the inactive current state");
        assert_eq!(state.derived_state(&BTreeSet::new()), CircleState::Inactive);
        let (name, role) = state.display("anyone");
        assert_eq!(name, None);
        assert_eq!(role, None);
    }

    #[tokio::test]
    async fn deleted_maps_to_deleted() {
        let active =
            crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
                .install_test_active_circle_state("derived-state".to_string())
                .await
                .expect("install and read the active current state");
        let deleted = CircleCurrentState::Deleted(Box::new(accessible(&active).current.clone()));
        assert_eq!(
            deleted.derived_state(&BTreeSet::new()),
            CircleState::Deleted
        );
        assert_eq!(deleted.display("anyone"), (None, None));
    }

    #[tokio::test]
    async fn control_conflict_maps_to_its_retained_branches() {
        let active =
            crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
                .install_test_active_circle_state("derived-state".to_string())
                .await
                .expect("install and read the active current state");
        let current = accessible(&active).current.clone();
        let expected = vec![current.coordinate().clone()];
        let conflict = CircleCurrentState::ControlConflict {
            branches: vec![current],
        };
        assert_eq!(
            conflict.derived_state(&BTreeSet::new()),
            CircleState::ControlConflict { branches: expected }
        );
        assert_eq!(conflict.display("anyone"), (None, None));
    }
}
