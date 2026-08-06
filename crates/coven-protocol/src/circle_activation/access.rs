use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalCircleExclusion {
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub excluded: StoreDeviceRegistrationRef,
    pub successor_control: CircleControlCoord,
    pub activating_commit: StoreBatchCommitRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCircleReference {
    pub reference: CircleControlRef,
    pub circle_id: CircleId,
    pub control: PreparedCircleControl,
    pub local_access: Option<VerifiedCircleAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCircleAccess {
    pub envelope: AccessEnvelope,
    pub leaf: PreparedAccessLeaf,
    pub active: Option<VerifiedCircleActive>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCircleActive {
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedCircleImage {
    pub(super) circle_id: CircleId,
    pub(super) control: CircleControlCoord,
    pub(super) reference: CircleBootstrapRef,
    pub(super) image_bytes: Vec<u8>,
}

impl VerifiedCircleImage {
    pub fn new(
        circle_id: CircleId,
        control: CircleControlCoord,
        access: &CircleAccessLeaf,
        reference: CircleBootstrapRef,
        image_bytes: Vec<u8>,
    ) -> Result<Self, CircleStateError> {
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
    pub fn from_stored_image(
        circle_id: CircleId,
        control: CircleControlCoord,
        reference: CircleBootstrapRef,
        image_bytes: Vec<u8>,
    ) -> Result<Self, CircleStateError> {
        if reference.image.image_hash != ObjectHash::digest(&image_bytes) {
            return Err(CircleStateError(
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

    pub(super) fn verify_for_access(
        &self,
        access: &CircleAccessLeaf,
    ) -> Result<(), CircleStateError> {
        if self.circle_id != access.circle_id
            || !self.reference.verify_for_access(access)
            || self.reference.image.image_hash != ObjectHash::digest(&self.image_bytes)
        {
            return Err(CircleStateError(
                "verified Circle bootstrap differs from its signed access leaf".to_string(),
            ));
        }
        Ok(())
    }

    pub fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub fn control(&self) -> &CircleControlCoord {
        &self.control
    }

    pub fn reference(&self) -> &CircleBootstrapRef {
        &self.reference
    }

    pub fn image_bytes(&self) -> &[u8] {
        &self.image_bytes
    }
}

#[derive(Clone)]
pub struct CircleEpochAccess {
    circle_id: CircleId,
    encryption: EncryptionService,
    key_fingerprint: KeyFingerprint,
    writers: BTreeSet<String>,
}

pub(super) struct VerifiedCircleKeyring {
    keyring: EncryptionService,
    key_fingerprint: KeyFingerprint,
}

impl VerifiedCircleKeyring {
    fn into_keyring(self) -> EncryptionService {
        self.keyring
    }

    fn epoch_encryption(&self, circle_id: CircleId) -> Result<EncryptionService, CircleStateError> {
        self.keyring
            .service_for_fingerprint(self.key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleStateError(format!(
                    "select Circle package key for {circle_id}: {error}"
                ))
            })
    }
}

impl CircleEpochAccess {
    pub fn key_fingerprint(&self) -> KeyFingerprint {
        self.key_fingerprint
    }

    pub fn protocol_context(
        &self,
        store_root_hash: ObjectHash,
        domain: crate::objects::CircleProtocolObjectDomain,
    ) -> crate::objects::ProtocolObjectContext {
        crate::objects::ProtocolObjectContext::circle(
            store_root_hash,
            domain,
            self.encryption.clone(),
        )
    }

    pub fn blob_protection(&self) -> crate::objects::BlobSpoolProtection {
        crate::objects::BlobSpoolProtection::Opaque(self.encryption.clone())
    }

    pub fn from_historical(
        circle_id: CircleId,
        key_fingerprint: KeyFingerprint,
        serialized_keyring: &str,
        roster: &CircleMaterializedRoster,
    ) -> Result<Self, CircleStateError> {
        if !roster.verify() {
            return Err(CircleStateError(format!(
                "Circle {circle_id} historical package roster is invalid"
            )));
        }
        let keyring = MasterKeyring::from_serialized(serialized_keyring).map_err(|error| {
            CircleStateError(format!(
                "parse Circle {circle_id} historical package keyring: {error}"
            ))
        })?;
        let encryption = EncryptionService::from(keyring)
            .service_for_fingerprint(key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleStateError(format!(
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

    pub fn authorize_package(
        &self,
        reference: &CirclePackageRef,
        author: &StoreDeviceRegistration,
    ) -> Result<(), CircleStateError> {
        if reference.circle_id != self.circle_id {
            return Err(CircleStateError(format!(
                "Circle package names {}, but access belongs to {}",
                reference.circle_id, self.circle_id
            )));
        }
        if !self.writers.contains(&author.author_pubkey) {
            return Err(CircleStateError(format!(
                "Circle package author is not a member of {} at its exact control",
                reference.circle_id
            )));
        }
        if self.key_fingerprint != reference.key_fingerprint {
            return Err(CircleStateError(format!(
                "Circle package key for {} differs from its activated control",
                reference.circle_id
            )));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn authorizes_writer(&self, author_pubkey: &str) -> bool {
        self.writers.contains(author_pubkey)
    }
}

impl VerifiedCircleReference {
    pub fn retained_keyring(&self) -> Result<Option<EncryptionService>, CircleStateError> {
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

    pub fn epoch_access(&self) -> Result<Option<CircleEpochAccess>, CircleStateError> {
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

pub(super) fn epoch_access_from(
    circle_id: CircleId,
    control: &CircleControl,
    disposition: &CircleAccessDisposition,
    roster: &CircleMaterializedRoster,
) -> Result<CircleEpochAccess, CircleStateError> {
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

pub(super) fn verified_keyring_from(
    circle_id: CircleId,
    control: &CircleControl,
    disposition: &CircleAccessDisposition,
    roster: &CircleMaterializedRoster,
) -> Result<VerifiedCircleKeyring, CircleStateError> {
    if control.circle_id != circle_id
        || !roster.verify()
        || roster.state_hash() != control.roster_state_ref().state_hash
    {
        return Err(CircleStateError(format!(
            "Circle {circle_id} package roster differs from its activated control"
        )));
    }
    let CircleAccessDisposition::Active {
        keyring,
        key_fingerprint,
        ..
    } = disposition
    else {
        return Err(CircleStateError(format!(
            "active Circle access for {circle_id} has an inactive leaf"
        )));
    };
    if *key_fingerprint != control.key_fingerprint() {
        return Err(CircleStateError(format!(
            "Circle package key for {circle_id} differs from its activated control"
        )));
    }
    let keyring = MasterKeyring::from_serialized(keyring).map_err(|error| {
        CircleStateError(format!(
            "parse Circle package keyring for {circle_id}: {error}"
        ))
    })?;
    let keyring = EncryptionService::from(keyring);
    keyring
        .service_for_fingerprint(key_fingerprint.as_bytes())
        .map_err(|error| {
            CircleStateError(format!(
                "select Circle package key for {circle_id}: {error}"
            ))
        })?;
    Ok(VerifiedCircleKeyring {
        keyring,
        key_fingerprint: *key_fingerprint,
    })
}
