use super::probe::*;
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CrossPrincipalProviderEvidence {
    GoogleSharedDrive,
    DropboxSharedNamespace,
    OneDriveSharedFolder,
    CloudKit(CloudKitAcceptedShare),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudKitAcceptedShare {
    pub share: ExactObjectRef,
    pub share_record_name: String,
    pub owner_name: String,
    pub zone_name: String,
    pub participant_record_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalProbeTranscript {
    pub challenge: CrossPrincipalProbeChallenge,
    pub response: CrossPrincipalProbeResponse,
    pub administrator_read_peer_hash: ObjectHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalProbeChallenge {
    pub probe_id: ProviderProbeId,
    pub administrator_object: ProbeExactObjectReceipt,
    pub challenge_hash: ObjectHash,
    pub administrator_signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalProbeResponse {
    pub challenge_hash: ObjectHash,
    pub provider_evidence: CrossPrincipalProviderEvidence,
    pub peer_object: ProbeExactObjectReceipt,
    pub peer_read_administrator_hash: ObjectHash,
    pub response_hash: ObjectHash,
    pub peer_signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalProbeReceipt {
    pub transcript: CrossPrincipalProbeTranscript,
    pub transcript_hash: ObjectHash,
    pub administrator_completion_signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalChallengeContext {
    pub root: StoreRootRef,
    pub attempt_id: DeviceJoinAttemptId,
    pub access_request_hash: ObjectHash,
    pub provider_admin_grant: ProviderAdminGrantId,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub member_pubkey: String,
    pub administrator_binding: ProviderDeviceBinding,
    pub peer_binding: ProviderDeviceBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalResponseContext {
    pub challenge: CrossPrincipalChallengeContext,
    pub expected_registration_hash: ObjectHash,
    pub response_slot: ObjectSlot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinChallengePublicationAuthorization {
    pub attempt: DeviceJoinAttemptRef,
    pub attempt_activation: StoreBatchCommitRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeviceJoinChallengePublicationRecord {
    pub challenge: CrossPrincipalProbeChallenge,
    pub progress: DeviceJoinChallengePublicationProgress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DeviceJoinChallengePublicationProgress {
    Prepared,
    Published {
        authorization: DeviceJoinChallengePublicationAuthorization,
    },
}

#[async_trait]
pub(crate) trait DeviceJoinChallengePublicationJournal: Send + Sync {
    async fn prepare(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<DeviceJoinChallengePublicationRecord, StorageError>;

    /// Atomically claims publication for these exact signed facts. An exact
    /// replay of an existing `Published` claim succeeds; a claim naming a
    /// different authorization is rejected.
    async fn claim_published(
        &self,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<(), StorageError>;
}

impl CrossPrincipalProbeReceipt {
    pub(crate) fn signed(
        transcript: CrossPrincipalProbeTranscript,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<Self, ProviderProbeError> {
        validate_cross_transcript_payloads(&transcript, context)?;
        let transcript_hash = cross_transcript_hash(store, context, &transcript);
        Ok(Self {
            transcript,
            transcript_hash,
            administrator_completion_signature: hex::encode(
                administrator_signer.sign(transcript_hash.as_bytes()),
            ),
        })
    }

    pub fn verify(
        &self,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signing_pubkey: &str,
    ) -> Result<(), ProviderProbeError> {
        validate_cross_provider_evidence(
            store,
            &context.challenge.administrator_binding,
            &context.challenge.peer_binding,
            &self.transcript.response.provider_evidence,
        )?;
        self.transcript.challenge.verify(
            &context.challenge,
            store,
            administrator_signing_pubkey,
        )?;
        self.transcript.response.verify(
            &self.transcript.challenge,
            context,
            store,
            administrator_signing_pubkey,
            peer_signing_pubkey,
        )?;
        validate_cross_transcript_payloads(&self.transcript, context)?;
        let expected_hash = cross_transcript_hash(store, context, &self.transcript);
        if self.transcript_hash != expected_hash {
            return invalid("cross-principal transcript hash does not match its join context");
        }
        if !coven_keys::keys::verify_signature_hex(
            administrator_signing_pubkey,
            &self.administrator_completion_signature,
            self.transcript_hash.as_bytes(),
        ) {
            return invalid("cross-principal completion signature is invalid");
        }
        Ok(())
    }
}

impl CrossPrincipalProbeChallenge {
    pub fn verify(
        &self,
        context: &CrossPrincipalChallengeContext,
        store: &StoreProviderBinding,
        administrator_signing_pubkey: &str,
    ) -> Result<(), ProviderProbeError> {
        validate_cross_challenge_payload(self)?;
        validate_cross_provider_evidence_context(store, context)?;
        let expected_hash = cross_challenge_hash(store, context, self);
        if self.challenge_hash != expected_hash {
            return invalid("cross-principal challenge hash does not match its join context");
        }
        if !coven_keys::keys::verify_signature_hex(
            administrator_signing_pubkey,
            &self.administrator_signature,
            self.challenge_hash.as_bytes(),
        ) {
            return invalid("cross-principal challenge signature is invalid");
        }
        Ok(())
    }
}

impl CrossPrincipalProbeResponse {
    pub fn verify(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signing_pubkey: &str,
    ) -> Result<(), ProviderProbeError> {
        challenge.verify(&context.challenge, store, administrator_signing_pubkey)?;
        if context.challenge.member_pubkey != peer_signing_pubkey {
            return invalid("cross-principal response signer is not the joining member");
        }
        validate_cross_provider_evidence(
            store,
            &context.challenge.administrator_binding,
            &context.challenge.peer_binding,
            &self.provider_evidence,
        )?;
        validate_cross_response_payload(self, challenge, context)?;
        let expected_hash = cross_response_hash(store, context, challenge, self);
        if self.response_hash != expected_hash {
            return invalid("cross-principal response hash does not match its join context");
        }
        if !coven_keys::keys::verify_signature_hex(
            peer_signing_pubkey,
            &self.peer_signature,
            self.response_hash.as_bytes(),
        ) {
            return invalid("cross-principal response signature is invalid");
        }
        Ok(())
    }
}

pub(crate) fn cross_transcript_hash(
    store: &StoreProviderBinding,
    context: &CrossPrincipalResponseContext,
    transcript: &CrossPrincipalProbeTranscript,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        CROSS_TRANSCRIPT_DOMAIN,
        &(store, context, transcript),
    ))
}

pub(crate) fn validate_cross_transcript_payloads(
    transcript: &CrossPrincipalProbeTranscript,
    context: &CrossPrincipalResponseContext,
) -> Result<(), ProviderProbeError> {
    validate_cross_challenge_payload(&transcript.challenge)?;
    validate_cross_response_payload(&transcript.response, &transcript.challenge, context)?;
    let peer = probe_payload(&transcript.challenge.probe_id, ProbePayloadLabel::CrossPeer);
    if transcript.administrator_read_peer_hash != ObjectHash::digest(&peer) {
        return invalid("cross-principal object, read, or deletion evidence is invalid");
    }
    Ok(())
}

pub(crate) fn cross_challenge_hash(
    store: &StoreProviderBinding,
    context: &CrossPrincipalChallengeContext,
    challenge: &CrossPrincipalProbeChallenge,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        CROSS_CHALLENGE_DOMAIN,
        &(
            store,
            context,
            challenge.probe_id,
            &challenge.administrator_object,
        ),
    ))
}

pub(crate) fn cross_response_hash(
    store: &StoreProviderBinding,
    context: &CrossPrincipalResponseContext,
    challenge: &CrossPrincipalProbeChallenge,
    response: &CrossPrincipalProbeResponse,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        CROSS_RESPONSE_DOMAIN,
        &(
            store,
            context,
            challenge.challenge_hash,
            &response.provider_evidence,
            &response.peer_object,
            response.peer_read_administrator_hash,
        ),
    ))
}

pub(crate) fn validate_cross_challenge_payload(
    challenge: &CrossPrincipalProbeChallenge,
) -> Result<(), ProviderProbeError> {
    let expected_key = cross_administrator_logical_key(challenge.probe_id);
    let payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossAdministrator);
    validate_probe_exact_object(
        &challenge.administrator_object,
        &expected_key,
        &payload,
        "cross-principal challenge",
    )
}

pub(crate) fn validate_cross_response_payload(
    response: &CrossPrincipalProbeResponse,
    challenge: &CrossPrincipalProbeChallenge,
    context: &CrossPrincipalResponseContext,
) -> Result<(), ProviderProbeError> {
    let administrator = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossAdministrator);
    let peer = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossPeer);
    if response.challenge_hash != challenge.challenge_hash
        || response.peer_object.slot != context.response_slot
        || response.peer_read_administrator_hash != ObjectHash::digest(&administrator)
    {
        return invalid(
            "cross-principal response disagrees with its challenge or response context",
        );
    }
    validate_probe_exact_object(
        &response.peer_object,
        &cross_peer_logical_key(challenge.probe_id),
        &peer,
        "cross-principal response",
    )
}

pub(super) fn cross_administrator_logical_key(probe_id: ProviderProbeId) -> String {
    format!(
        "__coven_probe__/cross/{}/administrator",
        hex::encode(probe_id.as_bytes())
    )
}

pub(crate) fn cross_peer_logical_key(probe_id: ProviderProbeId) -> String {
    format!(
        "__coven_probe__/cross/{}/peer",
        hex::encode(probe_id.as_bytes())
    )
}

pub(crate) fn validate_cross_provider_evidence_context(
    store: &StoreProviderBinding,
    context: &CrossPrincipalChallengeContext,
) -> Result<(), ProviderProbeError> {
    context
        .administrator_binding
        .validate_for(store)
        .map_err(ProviderProbeError::Storage)?;
    context
        .peer_binding
        .validate_for(store)
        .map_err(ProviderProbeError::Storage)?;
    if context.administrator_binding == context.peer_binding {
        return invalid("cross-principal context uses the same provider principal twice");
    }
    Ok(())
}

pub(crate) fn validate_cross_provider_evidence(
    store: &StoreProviderBinding,
    administrator: &ProviderDeviceBinding,
    peer: &ProviderDeviceBinding,
    evidence: &CrossPrincipalProviderEvidence,
) -> Result<(), ProviderProbeError> {
    administrator
        .validate_for(store)
        .map_err(ProviderProbeError::Storage)?;
    peer.validate_for(store)
        .map_err(ProviderProbeError::Storage)?;
    if administrator == peer {
        return invalid("cross-principal receipt uses the same provider principal twice");
    }
    let compatible = matches!(
        (store, evidence),
        (
            StoreProviderBinding::GoogleDrive {
                corpus: crate::protocol::objects::GoogleDriveCorpus::SharedDrive { .. }
            },
            CrossPrincipalProviderEvidence::GoogleSharedDrive
        ) | (
            StoreProviderBinding::Dropbox { .. },
            CrossPrincipalProviderEvidence::DropboxSharedNamespace
        ) | (
            StoreProviderBinding::OneDrive { .. },
            CrossPrincipalProviderEvidence::OneDriveSharedFolder
        ) | (
            StoreProviderBinding::CloudKit { .. },
            CrossPrincipalProviderEvidence::CloudKit(_)
        )
    );
    if !compatible {
        return invalid("provider binding does not permit the cross-principal evidence");
    }
    if let (
        StoreProviderBinding::CloudKit {
            owner_name,
            zone_name,
            ..
        },
        CrossPrincipalProviderEvidence::CloudKit(accepted),
    ) = (store, evidence)
    {
        let crate::protocol::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant {
            record_name,
        } = &peer.principal
        else {
            return invalid("CloudKit peer is not a shared-zone participant");
        };
        if accepted.owner_name != *owner_name
            || accepted.zone_name != *zone_name
            || accepted.participant_record_name != *record_name
            || accepted.share_record_name.is_empty()
        {
            return invalid("CloudKit accepted-share evidence differs from the Store binding");
        }
    }
    Ok(())
}
