use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::keys::UserKeypair;
use crate::protocol::membership::{
    MembershipCoord, MembershipEntry, MembershipGrantId, OwnerStreamBarrier,
};
use crate::protocol::store_commit::{
    DeviceJoinAttemptId, DeviceJoinAttemptRef, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef,
};
use crate::storage::cloud::{BlobBody, CloudHomeError, ExactSlotStorage, ObjectSlot};
use crate::storage::{ExactObjectRef, ProviderDeviceBinding, StorageError, StoreProviderBinding};

const EXACT_TRANSCRIPT_DOMAIN: &[u8] = b"coven.provider-exact-slot-probe.v1\0";
const CROSS_TRANSCRIPT_DOMAIN: &[u8] = b"coven.provider-cross-principal-probe.v1\0";
const CROSS_CHALLENGE_DOMAIN: &[u8] = b"coven.provider-cross-principal-challenge.v1\0";
const CROSS_RESPONSE_DOMAIN: &[u8] = b"coven.provider-cross-principal-response.v1\0";
const PAYLOAD_DOMAIN: &[u8] = b"coven.provider-probe-payload.v1\0";
const MEMBER_ACCESS_GRANT_DOMAIN: &[u8] = b"coven.provider-member-access-grant.v1\0";
pub(crate) const PROBE_PAYLOAD_LEN: usize = 256;
pub(crate) const PROBE_RANGE_START: u64 = 31;
pub(crate) const PROBE_RANGE_END: u64 = 173;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderProbeId([u8; 32]);

impl ProviderProbeId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ProviderProbeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl Serialize for ProviderProbeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ProviderProbeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(serde::de::Error::custom(
                "provider probe id must be 64 lowercase hexadecimal characters",
            ));
        }
        let bytes: [u8; 32] = hex::decode(value)
            .map_err(serde::de::Error::custom)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("provider probe id has the wrong length"))?;
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ProbePayloadLabel {
    ExactCreateFirst,
    ExactCreateSecond,
    LostResponse,
    CrossAdministrator,
    CrossPeer,
}

impl ProbePayloadLabel {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::ExactCreateFirst => b"exact-create-first",
            Self::ExactCreateSecond => b"exact-create-second",
            Self::LostResponse => b"lost-response",
            Self::CrossAdministrator => b"cross-administrator",
            Self::CrossPeer => b"cross-peer",
        }
    }
}

pub(crate) fn probe_payload(probe_id: &ProviderProbeId, label: ProbePayloadLabel) -> Vec<u8> {
    let mut output = Vec::with_capacity(PROBE_PAYLOAD_LEN);
    let mut counter = 0u32;
    while output.len() < PROBE_PAYLOAD_LEN {
        let mut digest = Sha256::new();
        digest.update(PAYLOAD_DOMAIN);
        digest.update(probe_id.as_bytes());
        digest.update(label.bytes());
        digest.update(counter.to_be_bytes());
        output.extend_from_slice(&digest.finalize());
        counter += 1;
    }
    output.truncate(PROBE_PAYLOAD_LEN);
    output
}

pub(crate) fn canonical_custom_s3_origin(input: &str) -> Result<String, StorageError> {
    if input.ends_with('/') {
        return Err(StorageError::Configuration(
            "custom S3 endpoint must not have a trailing slash".to_string(),
        ));
    }
    let parsed = url::Url::parse(input).map_err(|error| {
        StorageError::Configuration(format!("invalid custom S3 endpoint: {error}"))
    })?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(StorageError::Configuration(
            "custom S3 endpoint must be an HTTP origin without user info, path, query, or fragment"
                .to_string(),
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::Configuration("custom S3 endpoint has no host".to_string()))?;
    let port = parsed.port();
    let default_port = matches!(
        (parsed.scheme(), port),
        ("http", Some(80)) | ("https", Some(443))
    );
    Ok(if let Some(port) = port.filter(|_| !default_port) {
        format!("{}://{}:{port}", parsed.scheme(), host.to_ascii_lowercase())
    } else {
        format!("{}://{}", parsed.scheme(), host.to_ascii_lowercase())
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilityProof {
    pub exact_slots: ExactSlotProbeReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderProviderAdminGrant {
    pub grant_id: ProviderAdminGrantId,
    pub provider: ProviderDeviceBinding,
    pub access: ProviderAccessLocator,
    pub capability: ProviderCapabilityProof,
}

impl FounderProviderAdminGrant {
    #[cfg(test)]
    pub(crate) fn from_test_label(label: &str) -> Self {
        let probe_id =
            ProviderProbeId::from_bytes(*ObjectHash::digest(label.as_bytes()).as_bytes());
        let slot = ObjectSlot::logical(format!("store-v1/test/{label}/provider-probe/exact"))
            .expect("valid exact-probe test slot");
        let first = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
        let second = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
        let accepted =
            ExactObjectRef::new(slot.clone(), first.len() as u64, ObjectHash::digest(&first));
        let lost_slot = ObjectSlot::logical(format!(
            "store-v1/test/{label}/provider-probe/lost-response"
        ))
        .expect("valid lost-response test slot");
        let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
        let lost_ref = ExactObjectRef::new(
            lost_slot.clone(),
            lost_payload.len() as u64,
            ObjectHash::digest(&lost_payload),
        );
        let device = ProviderDeviceBinding {
            principal: crate::storage::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
        };
        let store = StoreProviderBinding::S3 {
            endpoint: crate::storage::S3EndpointBinding::Custom {
                origin: "https://test.invalid".to_string(),
            },
            region: "test-region".to_string(),
            bucket: format!("{label}-bucket"),
            key_prefix: None,
        };
        let transcript = ExactSlotProbeTranscript {
            probe_id,
            logical_key: slot.logical_key().to_string(),
            slot,
            contenders: [
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&first),
                    outcome: ProbeCreateOutcome::Created,
                },
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&second),
                    outcome: ProbeCreateOutcome::RejectedOccupied,
                },
            ],
            accepted: accepted.clone(),
            full_read_hash: accepted.stored_hash(),
            range: ProbeRangeReceipt {
                start: PROBE_RANGE_START,
                end: PROBE_RANGE_END,
                bytes_hash: ObjectHash::digest(
                    &first[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize],
                ),
            },
            delete_verified_absent: true,
            lost_response: LostResponseProbeReceipt {
                logical_key: lost_slot.logical_key().to_string(),
                slot: lost_slot,
                payload_hash: ObjectHash::digest(&lost_payload),
                settled: lost_ref,
                readback_hash: ObjectHash::digest(&lost_payload),
                delete_verified_absent: true,
            },
        };
        Self {
            grant_id: ProviderAdminGrantId(ObjectHash::digest(
                format!("{label} provider admin grant").as_bytes(),
            )),
            provider: device.clone(),
            access: ProviderAccessLocator::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
            capability: ProviderCapabilityProof {
                exact_slots: ExactSlotProbeReceipt::from_transcript(transcript, &store, &device),
            },
        }
    }
}

impl ProviderCapabilityProof {
    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        device: &ProviderDeviceBinding,
    ) -> Result<(), ProviderProbeError> {
        self.exact_slots.verify(store, device)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactSlotProbeReceipt {
    pub transcript: ExactSlotProbeTranscript,
    pub transcript_hash: ObjectHash,
}

impl ExactSlotProbeReceipt {
    pub fn from_transcript(
        transcript: ExactSlotProbeTranscript,
        store: &StoreProviderBinding,
        device: &ProviderDeviceBinding,
    ) -> Self {
        let transcript_hash = exact_transcript_hash(store, device, &transcript);
        Self {
            transcript,
            transcript_hash,
        }
    }

    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        device: &ProviderDeviceBinding,
    ) -> Result<(), ProviderProbeError> {
        store.validate().map_err(ProviderProbeError::Storage)?;
        device
            .validate_for(store)
            .map_err(ProviderProbeError::Storage)?;
        let t = &self.transcript;
        if self.transcript_hash != exact_transcript_hash(store, device, t) {
            return invalid("exact-slot transcript hash does not match its context");
        }
        if t.logical_key != t.slot.logical_key() || t.accepted.slot() != &t.slot {
            return invalid("exact-slot transcript disagrees with its allocated slot");
        }
        let payloads = [
            probe_payload(&t.probe_id, ProbePayloadLabel::ExactCreateFirst),
            probe_payload(&t.probe_id, ProbePayloadLabel::ExactCreateSecond),
        ];
        let expected_hashes = [
            ObjectHash::digest(&payloads[0]),
            ObjectHash::digest(&payloads[1]),
        ];
        if t.contenders[0].payload_hash != expected_hashes[0]
            || t.contenders[1].payload_hash != expected_hashes[1]
        {
            return invalid("exact-slot contender payload hashes are not deterministic");
        }
        let winners: Vec<_> = t
            .contenders
            .iter()
            .enumerate()
            .filter_map(|(index, attempt)| {
                (attempt.outcome == ProbeCreateOutcome::Created).then_some(index)
            })
            .collect();
        let rejected = t
            .contenders
            .iter()
            .filter(|attempt| attempt.outcome == ProbeCreateOutcome::RejectedOccupied)
            .count();
        if winners.len() != 1 || rejected != 1 {
            return invalid("exact-slot race must contain one create and one occupied rejection");
        }
        let winner = &payloads[winners[0]];
        if t.accepted.stored_size() != winner.len() as u64
            || t.accepted.stored_hash() != ObjectHash::digest(winner)
            || t.full_read_hash != ObjectHash::digest(winner)
            || t.range.start != PROBE_RANGE_START
            || t.range.end != PROBE_RANGE_END
            || t.range.bytes_hash
                != ObjectHash::digest(&winner[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize])
            || !t.delete_verified_absent
        {
            return invalid("exact-slot read, range, reference, or deletion evidence is invalid");
        }
        let lost = probe_payload(&t.probe_id, ProbePayloadLabel::LostResponse);
        let lost_hash = ObjectHash::digest(&lost);
        if t.lost_response.logical_key != t.lost_response.slot.logical_key()
            || t.lost_response.settled.slot() != &t.lost_response.slot
            || t.lost_response.payload_hash != lost_hash
            || t.lost_response.settled.stored_size() != lost.len() as u64
            || t.lost_response.settled.stored_hash() != lost_hash
            || t.lost_response.readback_hash != lost_hash
            || !t.lost_response.delete_verified_absent
        {
            return invalid("lost-response exact-slot evidence is invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactSlotProbeTranscript {
    pub probe_id: ProviderProbeId,
    pub logical_key: String,
    pub slot: ObjectSlot,
    pub contenders: [ProbeCreateAttempt; 2],
    pub accepted: ExactObjectRef,
    pub full_read_hash: ObjectHash,
    pub range: ProbeRangeReceipt,
    pub delete_verified_absent: bool,
    pub lost_response: LostResponseProbeReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeCreateAttempt {
    pub payload_hash: ObjectHash,
    pub outcome: ProbeCreateOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeCreateOutcome {
    Created,
    RejectedOccupied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LostResponseProbeReceipt {
    pub logical_key: String,
    pub slot: ObjectSlot,
    pub payload_hash: ObjectHash,
    pub settled: ExactObjectRef,
    pub readback_hash: ObjectHash,
    pub delete_verified_absent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeRangeReceipt {
    pub start: u64,
    pub end: u64,
    pub bytes_hash: ObjectHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeExactObjectReceipt {
    pub slot: ObjectSlot,
    pub payload_hash: ObjectHash,
    pub object: ExactObjectRef,
}

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
    pub administrator_delete_peer_verified_absent: bool,
    pub administrator_delete_own_verified_absent: bool,
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

#[async_trait]
impl DeviceJoinChallengePublicationJournal for crate::database::StoreDatabase {
    async fn prepare(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<DeviceJoinChallengePublicationRecord, StorageError> {
        self.prepare_device_join_challenge_publication(challenge.clone())
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }

    async fn claim_published(
        &self,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<(), StorageError> {
        self.publish_device_join_challenge(authorization.clone(), challenge.clone())
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }
}

impl CrossPrincipalProbeReceipt {
    fn signed(
        transcript: CrossPrincipalProbeTranscript,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
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
        if !crate::keys::verify_signature_hex(
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
        if !crate::keys::verify_signature_hex(
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
        if !crate::keys::verify_signature_hex(
            peer_signing_pubkey,
            &self.peer_signature,
            self.response_hash.as_bytes(),
        ) {
            return invalid("cross-principal response signature is invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderAdminGrantId(pub ObjectHash);

impl ProviderAdminGrantId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderAccessGrantId(pub ObjectHash);

impl ProviderAccessGrantId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }
}

/// Stable provider authority that can be withdrawn without rediscovering a
/// member by mutable account metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAccessLocator {
    S3SharedCredentialGeneration {
        generation: u64,
        access_key_id_hash: ObjectHash,
    },
    GoogleDrivePermission {
        drive_id: String,
        permission_id: String,
    },
    DropboxSharedFolderMember {
        namespace_id: String,
        account_id: String,
    },
    OneDrivePermission {
        drive_id: String,
        item_id: String,
        permission_id: String,
    },
    CloudKitPrivateZoneOwner {
        owner_name: String,
        zone_name: String,
        owner_record_name: String,
    },
    CloudKitParticipant {
        share_record_name: String,
        owner_name: String,
        zone_name: String,
        participant_record_name: String,
    },
}

impl ProviderAccessLocator {
    pub fn for_current_administrator(
        binding: &crate::storage::ResolvedProviderBinding,
    ) -> Result<Self, StorageError> {
        binding.validate()?;
        match (&binding.store, &binding.device.principal) {
            (
                StoreProviderBinding::S3 { .. },
                crate::storage::ProviderPrincipalId::CustomS3Credential { access_key_id_hash },
            ) => Ok(Self::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: *access_key_id_hash,
            }),
            (
                StoreProviderBinding::GoogleDrive {
                    corpus: crate::storage::GoogleDriveCorpus::SharedDrive { drive_id, .. },
                },
                crate::storage::ProviderPrincipalId::GoogleDrive { permission_id },
            ) => Ok(Self::GoogleDrivePermission {
                drive_id: drive_id.clone(),
                permission_id: permission_id.clone(),
            }),
            (
                StoreProviderBinding::Dropbox { namespace_id },
                crate::storage::ProviderPrincipalId::Dropbox { account_id },
            ) => Ok(Self::DropboxSharedFolderMember {
                namespace_id: namespace_id.clone(),
                account_id: account_id.clone(),
            }),
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::storage::ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name },
            ) => Ok(Self::CloudKitPrivateZoneOwner {
                owner_name: owner_name.clone(),
                zone_name: zone_name.clone(),
                owner_record_name: record_name.clone(),
            }),
            _ => Err(StorageError::Configuration(
                "provider adapter did not expose the administrator's exact access locator"
                    .to_string(),
            )),
        }
    }

    pub fn validate_for(
        &self,
        store: &StoreProviderBinding,
        provider: &ProviderDeviceBinding,
    ) -> Result<(), StorageError> {
        provider.validate_for(store)?;
        let valid = match (store, &provider.principal, self) {
            (
                StoreProviderBinding::S3 { .. },
                crate::storage::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: provider_hash,
                },
                Self::S3SharedCredentialGeneration {
                    generation,
                    access_key_id_hash,
                },
            ) => *generation > 0 && provider_hash == access_key_id_hash,
            (
                StoreProviderBinding::S3 { .. },
                crate::storage::ProviderPrincipalId::Aws { .. },
                Self::S3SharedCredentialGeneration { generation, .. },
            ) => *generation > 0,
            (
                StoreProviderBinding::GoogleDrive {
                    corpus: crate::storage::GoogleDriveCorpus::SharedDrive { drive_id, .. },
                },
                crate::storage::ProviderPrincipalId::GoogleDrive { permission_id },
                Self::GoogleDrivePermission {
                    drive_id: locator_drive,
                    permission_id: locator_permission,
                },
            ) => drive_id == locator_drive && permission_id == locator_permission,
            (
                StoreProviderBinding::Dropbox { namespace_id },
                crate::storage::ProviderPrincipalId::Dropbox { account_id },
                Self::DropboxSharedFolderMember {
                    namespace_id: locator_namespace,
                    account_id: locator_account,
                },
            ) => namespace_id == locator_namespace && account_id == locator_account,
            (
                StoreProviderBinding::OneDrive {
                    drive_id,
                    folder_id,
                },
                crate::storage::ProviderPrincipalId::OneDrive { .. },
                Self::OneDrivePermission {
                    drive_id: locator_drive,
                    item_id,
                    permission_id,
                },
            ) => drive_id == locator_drive && folder_id == item_id && !permission_id.is_empty(),
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::storage::ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name },
                Self::CloudKitPrivateZoneOwner {
                    owner_name: locator_owner,
                    zone_name: locator_zone,
                    owner_record_name,
                },
            ) => {
                owner_name == locator_owner
                    && zone_name == locator_zone
                    && record_name == owner_record_name
            }
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::storage::ProviderPrincipalId::CloudKitSharedZoneParticipant { record_name },
                Self::CloudKitParticipant {
                    share_record_name,
                    owner_name: locator_owner,
                    zone_name: locator_zone,
                    participant_record_name,
                },
            ) => {
                !share_record_name.is_empty()
                    && owner_name == locator_owner
                    && zone_name == locator_zone
                    && record_name == participant_record_name
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(StorageError::Configuration(
                "provider access locator differs from its Store and provider binding".to_string(),
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMemberProviderAccessGrant {
    pub grant_id: ProviderAccessGrantId,
    pub member_pubkey: String,
    pub provider: ProviderDeviceBinding,
    pub locator: ProviderAccessLocator,
    pub administrator_grant: ProviderAdminGrantId,
    pub administrator: StoreDeviceRegistrationRef,
    pub signature: String,
}

#[derive(Serialize)]
struct StoreMemberProviderAccessGrantSignedFields<'a> {
    grant_id: &'a ProviderAccessGrantId,
    member_pubkey: &'a str,
    provider: &'a ProviderDeviceBinding,
    locator: &'a ProviderAccessLocator,
    administrator_grant: &'a ProviderAdminGrantId,
    administrator: &'a StoreDeviceRegistrationRef,
}

impl StoreMemberProviderAccessGrant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed(
        grant_id: ProviderAccessGrantId,
        member_pubkey: String,
        provider: ProviderDeviceBinding,
        locator: ProviderAccessLocator,
        administrator_grant: ProviderAdminGrantId,
        administrator: StoreDeviceRegistrationRef,
        store: &StoreProviderBinding,
        administrator_registration: &StoreDeviceRegistration,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
    ) -> Result<Self, ProviderProbeError> {
        administrator
            .verify_registration(administrator_registration)
            .map_err(|error| ProviderProbeError::InvalidReceipt(error.to_string()))?;
        if administrator_signer.public_key_hex() != administrator_registration.device_signing_pubkey
        {
            return invalid("provider access grant signer is not the administrator device");
        }
        locator.validate_for(store, &provider)?;
        let mut grant = Self {
            grant_id,
            member_pubkey,
            provider,
            locator,
            administrator_grant,
            administrator,
            signature: String::new(),
        };
        grant.signature = hex::encode(
            administrator_signer
                .sign(ObjectHash::digest(&grant.canonical_signed_bytes()).as_bytes()),
        );
        Ok(grant)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            MEMBER_ACCESS_GRANT_DOMAIN,
            &StoreMemberProviderAccessGrantSignedFields {
                grant_id: &self.grant_id,
                member_pubkey: &self.member_pubkey,
                provider: &self.provider,
                locator: &self.locator,
                administrator_grant: &self.administrator_grant,
                administrator: &self.administrator,
            },
        )
    }

    pub fn grant_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("provider member access grant serialization cannot fail")
    }

    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), ProviderProbeError> {
        self.administrator
            .verify_registration(administrator)
            .map_err(|error| ProviderProbeError::InvalidReceipt(error.to_string()))?;
        self.provider
            .validate_for(store)
            .map_err(ProviderProbeError::Storage)?;
        self.locator
            .validate_for(store, &self.provider)
            .map_err(ProviderProbeError::Storage)?;
        if !crate::keys::verify_signature_hex(
            &administrator.device_signing_pubkey,
            &self.signature,
            self.grant_hash().as_bytes(),
        ) {
            return invalid("provider access grant signature is invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMemberProviderAccessGrantRef {
    pub grant_id: ProviderAccessGrantId,
    pub grant_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StoreMemberProviderAccessGrantRef {
    pub fn from_grant(grant: &StoreMemberProviderAccessGrant, object: ExactObjectRef) -> Self {
        Self {
            grant_id: grant.grant_id.clone(),
            grant_hash: grant.grant_hash(),
            object,
        }
    }

    pub fn verify(&self, grant: &StoreMemberProviderAccessGrant) -> Result<(), ProviderProbeError> {
        if self.grant_id != grant.grant_id || self.grant_hash != grant.grant_hash() {
            return invalid("provider access grant reference differs from its signed grant");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedStoreMemberProviderAccessGrant {
    pub grant: StoreMemberProviderAccessGrant,
    pub grant_ref: StoreMemberProviderAccessGrantRef,
    pub activation: StoreBatchCommitRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAccessWithdrawal {
    Direct {
        locator: ProviderAccessLocator,
        verified_absent: bool,
    },
    S3CredentialRotation {
        retired_generation: u64,
        active_generation: u64,
        retired_credential_verified_rejected: bool,
    },
}

impl ProviderAccessWithdrawal {
    fn validate(&self) -> Result<(), ProviderProbeError> {
        let valid = match self {
            Self::Direct {
                verified_absent, ..
            } => *verified_absent,
            Self::S3CredentialRotation {
                retired_generation,
                active_generation,
                retired_credential_verified_rejected,
            } => {
                *retired_generation > 0
                    && retired_generation.checked_add(1) == Some(*active_generation)
                    && *retired_credential_verified_rejected
            }
        };
        if valid {
            Ok(())
        } else {
            invalid("provider access withdrawal does not prove the stored authority is unusable")
        }
    }

    pub(crate) fn verify_for_locator(
        &self,
        locator: &ProviderAccessLocator,
    ) -> Result<(), ProviderProbeError> {
        self.validate()?;
        let matches = match (self, locator) {
            (
                Self::Direct {
                    locator: withdrawn, ..
                },
                expected,
            ) => withdrawn == expected,
            (
                Self::S3CredentialRotation {
                    retired_generation, ..
                },
                ProviderAccessLocator::S3SharedCredentialGeneration { generation, .. },
            ) => retired_generation == generation,
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            invalid("provider access withdrawal differs from the stored authority locator")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminGrantRecord {
    pub grant_id: ProviderAdminGrantId,
    pub administrator: StoreDeviceRegistrationRef,
    pub provider: ProviderDeviceBinding,
    pub access: ProviderAccessLocator,
    pub capability: ProviderCapabilityProof,
    pub created_at: ProviderAdminGrantOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminGrantOrigin {
    Founder { root: StoreRootRef },
    Membership { coord: MembershipCoord },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminMembershipChange {
    pub change: ProviderAdminChange,
    #[serde(with = "ordered_owner_barriers")]
    pub owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminChange {
    Set {
        administrator: StoreDeviceRegistrationRef,
        provider: ProviderDeviceBinding,
        access: ProviderAccessLocator,
        capability: ProviderCapabilityProof,
        grant_id: ProviderAdminGrantId,
        replaces: BTreeSet<ProviderAdminGrantId>,
    },
    Remove {
        removes: BTreeSet<ProviderAdminGrantId>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminState {
    records: BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
    tombstones: BTreeSet<ProviderAdminGrantId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminBranch {
    pub heads: Vec<MembershipCoord>,
    pub state: ProviderAdminState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminConflict {
    pub raw_heads: Vec<MembershipCoord>,
    pub cyclic_sources: Vec<MembershipCoord>,
    pub involved_grants: BTreeSet<ProviderAdminGrantId>,
    pub maximal_valid_branches: Vec<ProviderAdminBranch>,
    pub combined: ProviderAdminState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminResolution {
    Resolved(ProviderAdminState),
    RevocationConflict(ProviderAdminConflict),
}

impl ProviderAdminResolution {
    pub fn combined_state(&self) -> &ProviderAdminState {
        match self {
            Self::Resolved(state) => state,
            Self::RevocationConflict(conflict) => &conflict.combined,
        }
    }

    pub fn state_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(b"coven.provider-admin-resolution.v1\0", self))
    }
}

impl ProviderAdminState {
    pub fn founder(grant: ProviderAdminGrantRecord) -> Self {
        let grant_id = grant.grant_id.clone();
        Self {
            records: BTreeMap::from([(grant_id.clone(), grant)]),
            tombstones: BTreeSet::new(),
        }
    }

    pub fn founder_from_root(
        root: StoreRootRef,
        administrator: StoreDeviceRegistrationRef,
        grant: &FounderProviderAdminGrant,
    ) -> Self {
        Self::founder(ProviderAdminGrantRecord {
            grant_id: grant.grant_id.clone(),
            administrator,
            provider: grant.provider.clone(),
            access: grant.access.clone(),
            capability: grant.capability.clone(),
            created_at: ProviderAdminGrantOrigin::Founder { root },
        })
    }

    pub fn authorizes(
        &self,
        grant_id: &ProviderAdminGrantId,
        administrator: &StoreDeviceRegistrationRef,
    ) -> bool {
        !self.tombstones.contains(grant_id)
            && self
                .records
                .get(grant_id)
                .is_some_and(|record| &record.administrator == administrator)
    }

    pub fn records(&self) -> &BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord> {
        &self.records
    }

    pub fn active(&self) -> BTreeSet<ProviderAdminGrantId> {
        self.records
            .keys()
            .filter(|grant_id| !self.tombstones.contains(*grant_id))
            .cloned()
            .collect()
    }

    pub fn tombstones(&self) -> &BTreeSet<ProviderAdminGrantId> {
        &self.tombstones
    }

    pub fn apply(
        &mut self,
        change: ProviderAdminChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        let mut next = self.clone();
        next.apply_unchecked(change, origin)?;
        if next.active().is_empty() {
            return Err(ProviderAdminReducerError::NoEffectiveAdministrator);
        }
        *self = next;
        Ok(())
    }

    fn apply_unchecked(
        &mut self,
        change: ProviderAdminChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        match change {
            ProviderAdminChange::Set {
                administrator,
                provider,
                access,
                capability,
                grant_id,
                replaces,
            } => {
                let record = ProviderAdminGrantRecord {
                    grant_id: grant_id.clone(),
                    administrator,
                    provider,
                    access,
                    capability,
                    created_at: origin,
                };
                if let Some(existing) = self.records.get(&grant_id) {
                    if existing != &record {
                        return Err(ProviderAdminReducerError::GrantIdReuse);
                    }
                    if !replaces.iter().all(|id| self.tombstones.contains(id)) {
                        return Err(ProviderAdminReducerError::UnknownReplacement);
                    }
                    return Ok(());
                }
                if !replaces
                    .iter()
                    .all(|id| self.records.contains_key(id) && !self.tombstones.contains(id))
                {
                    return Err(ProviderAdminReducerError::UnknownReplacement);
                }
                for replaced in replaces {
                    self.tombstones.insert(replaced);
                }
                self.records.insert(grant_id, record);
            }
            ProviderAdminChange::Remove { removes } => {
                if removes.is_empty()
                    || !removes
                        .iter()
                        .all(|id| self.records.contains_key(id) || self.tombstones.contains(id))
                {
                    return Err(ProviderAdminReducerError::UnknownRemoval);
                }
                for removed in removes {
                    self.tombstones.insert(removed);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_membership_change(
        &mut self,
        change: ProviderAdminMembershipChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        if !matches!(origin, ProviderAdminGrantOrigin::Membership { .. }) {
            return Err(ProviderAdminReducerError::PolicyOriginMismatch);
        }
        self.apply(change.change, origin)
    }

    pub fn state_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(
            b"coven.provider-admin-state.v1\0",
            &(self.records(), self.tombstones()),
        ))
    }

    pub fn merge(
        states: impl IntoIterator<Item = Self>,
    ) -> Result<Self, ProviderAdminReducerError> {
        let mut records = BTreeMap::new();
        let mut tombstones = BTreeSet::new();
        for state in states {
            for (grant_id, record) in state.records {
                if records
                    .insert(grant_id.clone(), record.clone())
                    .is_some_and(|current| current != record)
                {
                    return Err(ProviderAdminReducerError::GrantIdReuse);
                }
            }
            tombstones.extend(state.tombstones);
        }
        Ok(Self {
            records,
            tombstones,
        })
    }

    pub(crate) fn reduce_merge(
        genesis: &Self,
        entries: &[MembershipEntry],
        included: &BTreeSet<MembershipCoord>,
    ) -> Result<ProviderAdminResolution, ProviderAdminReducerError> {
        let by_coord = entries
            .iter()
            .filter(|entry| included.contains(&entry.coord()))
            .map(|entry| (entry.coord(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut states = BTreeMap::<MembershipCoord, Self>::new();
        let mut pending = by_coord.keys().cloned().collect::<BTreeSet<_>>();
        while !pending.is_empty() {
            let ready = pending.iter().find(|coord| {
                let entry = by_coord[*coord];
                let predecessor = (entry.seq > 1)
                    .then(|| {
                        by_coord.keys().find(|candidate| {
                            candidate.author_pubkey == entry.author_pubkey
                                && candidate.author_owner_grant == entry.author_owner_grant
                                && candidate.stream_id == entry.stream_id
                                && candidate.seq + 1 == entry.seq
                                && Some(candidate.entry_hash) == entry.previous_hash
                        })
                    })
                    .flatten();
                (entry.seq == 1 || predecessor.is_some_and(|value| states.contains_key(value)))
                    && entry
                        .dependencies
                        .iter()
                        .filter(|dependency| included.contains(*dependency))
                        .all(|dependency| states.contains_key(dependency))
            });
            let Some(coord) = ready.cloned() else {
                if pending.iter().any(|coord| {
                    let entry = by_coord[coord];
                    entry.seq > 1
                        && !by_coord.keys().any(|candidate| {
                            candidate.author_pubkey == entry.author_pubkey
                                && candidate.author_owner_grant == entry.author_owner_grant
                                && candidate.stream_id == entry.stream_id
                                && candidate.seq + 1 == entry.seq
                                && Some(candidate.entry_hash) == entry.previous_hash
                        })
                }) {
                    return Err(ProviderAdminReducerError::MissingPredecessor);
                }
                return Err(ProviderAdminReducerError::CausalCycle);
            };
            let entry = by_coord[&coord];
            let mut causal_states = entry
                .dependencies
                .iter()
                .filter_map(|dependency| states.get(dependency).cloned())
                .collect::<Vec<_>>();
            if entry.seq > 1 {
                if let Some(predecessor) = by_coord.keys().find(|candidate| {
                    candidate.author_pubkey == entry.author_pubkey
                        && candidate.author_owner_grant == entry.author_owner_grant
                        && candidate.stream_id == entry.stream_id
                        && candidate.seq + 1 == entry.seq
                        && Some(candidate.entry_hash) == entry.previous_hash
                }) {
                    if !entry.dependencies.contains(predecessor) {
                        causal_states.push(states[predecessor].clone());
                    }
                }
            }
            let mut state = if causal_states.is_empty() {
                genesis.clone()
            } else {
                Self::merge(causal_states)?
            };
            if let Some(change) = entry.provider_admin.clone() {
                state.apply_membership_change(
                    change,
                    ProviderAdminGrantOrigin::Membership {
                        coord: coord.clone(),
                    },
                )?;
            }
            states.insert(coord.clone(), state);
            pending.remove(&coord);
        }
        let raw_heads = by_coord
            .keys()
            .filter(|coord| {
                !by_coord.values().any(|entry| {
                    entry.dependencies.contains(*coord)
                        || (entry.seq == coord.seq + 1
                            && entry.author_pubkey == coord.author_pubkey
                            && entry.author_owner_grant == coord.author_owner_grant
                            && entry.stream_id == coord.stream_id
                            && entry.previous_hash == Some(coord.entry_hash))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let combined =
            Self::merge(std::iter::once(genesis.clone()).chain(states.values().cloned()))?;
        if !combined.active().is_empty() {
            return Ok(ProviderAdminResolution::Resolved(combined));
        }
        if raw_heads.len() > 12 {
            return Err(ProviderAdminReducerError::ConflictTooWide(raw_heads.len()));
        }
        let head_states = raw_heads
            .iter()
            .map(|head| (head.clone(), states[head].clone()))
            .collect::<Vec<_>>();
        let mut valid = Vec::<ProviderAdminBranch>::new();
        for mask in 1usize..(1usize << head_states.len()) {
            let heads = head_states
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1usize << index) != 0)
                .map(|(_, (head, _))| head.clone())
                .collect::<Vec<_>>();
            let state = Self::merge(
                head_states
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1usize << index) != 0)
                    .map(|(_, (_, state))| state.clone()),
            )?;
            if !state.active().is_empty() {
                valid.push(ProviderAdminBranch { heads, state });
            }
        }
        let valid_head_sets = valid
            .iter()
            .map(|branch| branch.heads.iter().cloned().collect::<BTreeSet<_>>())
            .collect::<Vec<_>>();
        let maximal_valid_branches = valid
            .into_iter()
            .enumerate()
            .filter(|(index, _)| {
                !valid_head_sets.iter().enumerate().any(|(other, heads)| {
                    other != *index && valid_head_sets[*index].is_subset(heads)
                })
            })
            .map(|(_, branch)| branch)
            .collect();
        let mut cyclic_sources = Vec::new();
        let mut involved_grants = BTreeSet::new();
        for (coord, entry) in &by_coord {
            if let Some(ProviderAdminMembershipChange {
                change: ProviderAdminChange::Remove { removes },
                ..
            }) = &entry.provider_admin
            {
                cyclic_sources.push(coord.clone());
                involved_grants.extend(removes.iter().cloned());
            }
        }
        cyclic_sources.sort();
        Ok(ProviderAdminResolution::RevocationConflict(
            ProviderAdminConflict {
                raw_heads,
                cyclic_sources,
                involved_grants,
                maximal_valid_branches,
                combined,
            },
        ))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProviderAdminReducerError {
    #[error("provider administrator grant id was reused with different facts")]
    GrantIdReuse,
    #[error("provider administrator replacement names an inactive grant")]
    UnknownReplacement,
    #[error("provider administrator removal names an inactive grant")]
    UnknownRemoval,
    #[error("provider administrator change leaves no effective administrator")]
    NoEffectiveAdministrator,
    #[error("provider administrator change policy does not match its derived origin")]
    PolicyOriginMismatch,
    #[error("provider administrator causal history is missing an exact stream predecessor")]
    MissingPredecessor,
    #[error("provider administrator causal history contains a cycle")]
    CausalCycle,
    #[error("provider administrator revocation conflict has {0} heads, exceeding 12")]
    ConflictTooWide(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderProbeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("provider capability receipt is invalid: {0}")]
    InvalidReceipt(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProviderProbeJournalRecord {
    Exact(ExactProbeJournal),
    CrossPrincipal(CrossPrincipalCompletionJournal),
}

impl ProviderProbeJournalRecord {
    pub(crate) fn probe_id(&self) -> ProviderProbeId {
        match self {
            Self::Exact(record) => record.probe_id,
            Self::CrossPrincipal(record) => record.probe_id,
        }
    }

    pub(crate) fn validate_begin(&self) -> Result<(), ProviderProbeJournalError> {
        let prepared = match self {
            Self::Exact(record) => matches!(record.progress, ExactProbeProgress::Prepared),
            Self::CrossPrincipal(record) => {
                matches!(record.progress, CrossPrincipalCompletionProgress::Prepared)
            }
        };
        if !prepared {
            return Err(ProviderProbeJournalError::BeginNotPrepared);
        }
        Ok(())
    }

    pub(crate) fn validate_transition(&self, next: &Self) -> Result<(), ProviderProbeJournalError> {
        match (self, next) {
            (Self::Exact(previous), Self::Exact(next)) => {
                if previous.probe_id != next.probe_id
                    || previous.binding != next.binding
                    || previous.slot != next.slot
                    || previous.lost_response_slot != next.lost_response_slot
                {
                    return Err(ProviderProbeJournalError::ImmutableFactsChanged);
                }
                validate_exact_progress_transition(&previous.progress, &next.progress)
            }
            (Self::CrossPrincipal(previous), Self::CrossPrincipal(next)) => {
                if previous.probe_id != next.probe_id
                    || previous.store != next.store
                    || previous.context != next.context
                    || previous.challenge != next.challenge
                    || previous.response != next.response
                {
                    return Err(ProviderProbeJournalError::ImmutableFactsChanged);
                }
                let expected_read_hash = ObjectHash::digest(&probe_payload(
                    &previous.probe_id,
                    ProbePayloadLabel::CrossPeer,
                ));
                if cross_progress_evidence_hash(&next.progress)
                    .is_some_and(|hash| hash != expected_read_hash)
                {
                    return Err(ProviderProbeJournalError::EvidenceChanged);
                }
                validate_cross_progress_transition(&previous.progress, &next.progress)
            }
            _ => Err(ProviderProbeJournalError::ProbeKindChanged),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ProviderProbeJournalError {
    #[error("provider probe journal must begin at prepared")]
    BeginNotPrepared,
    #[error("provider probe journal advance changes immutable facts")]
    ImmutableFactsChanged,
    #[error("provider probe journal advance changes the probe kind")]
    ProbeKindChanged,
    #[error("provider probe journal advance skips or reverses progress")]
    NonAdjacentProgress,
    #[error("provider probe journal advance changes established evidence")]
    EvidenceChanged,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExactProbeJournal {
    pub probe_id: ProviderProbeId,
    pub binding: crate::storage::ResolvedProviderBinding,
    pub slot: ObjectSlot,
    pub lost_response_slot: ObjectSlot,
    pub progress: ExactProbeProgress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ExactProbeProgress {
    Prepared,
    Created { outcomes: [ProbeCreateOutcome; 2] },
    ReadsVerified { outcomes: [ProbeCreateOutcome; 2] },
    PrimaryAbsent { outcomes: [ProbeCreateOutcome; 2] },
    LostResponseCreated { outcomes: [ProbeCreateOutcome; 2] },
    LostResponseReadVerified { outcomes: [ProbeCreateOutcome; 2] },
    Absent { outcomes: [ProbeCreateOutcome; 2] },
    ReceiptReady { receipt: ExactSlotProbeReceipt },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CrossPrincipalCompletionJournal {
    pub probe_id: ProviderProbeId,
    pub store: StoreProviderBinding,
    pub context: CrossPrincipalResponseContext,
    pub challenge: CrossPrincipalProbeChallenge,
    pub response: CrossPrincipalProbeResponse,
    pub progress: CrossPrincipalCompletionProgress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CrossPrincipalCompletionProgress {
    Prepared,
    ReadsVerified {
        administrator_read_peer_hash: ObjectHash,
    },
    PeerAbsent {
        administrator_read_peer_hash: ObjectHash,
    },
    Absent {
        administrator_read_peer_hash: ObjectHash,
    },
    ReceiptReady {
        receipt: CrossPrincipalProbeReceipt,
    },
}

fn validate_exact_progress_transition(
    previous: &ExactProbeProgress,
    next: &ExactProbeProgress,
) -> Result<(), ProviderProbeJournalError> {
    let evidence_matches = match (previous, next) {
        (ExactProbeProgress::Prepared, ExactProbeProgress::Created { .. }) => true,
        (
            ExactProbeProgress::Created { outcomes: previous },
            ExactProbeProgress::ReadsVerified { outcomes: next },
        )
        | (
            ExactProbeProgress::ReadsVerified { outcomes: previous },
            ExactProbeProgress::PrimaryAbsent { outcomes: next },
        )
        | (
            ExactProbeProgress::PrimaryAbsent { outcomes: previous },
            ExactProbeProgress::LostResponseCreated { outcomes: next },
        )
        | (
            ExactProbeProgress::LostResponseCreated { outcomes: previous },
            ExactProbeProgress::LostResponseReadVerified { outcomes: next },
        )
        | (
            ExactProbeProgress::LostResponseReadVerified { outcomes: previous },
            ExactProbeProgress::Absent { outcomes: next },
        ) => previous == next,
        (ExactProbeProgress::Absent { outcomes }, ExactProbeProgress::ReceiptReady { receipt }) => {
            receipt
                .transcript
                .contenders
                .iter()
                .map(|attempt| attempt.outcome)
                .eq(outcomes.iter().copied())
        }
        _ => return Err(ProviderProbeJournalError::NonAdjacentProgress),
    };
    if !evidence_matches {
        return Err(ProviderProbeJournalError::EvidenceChanged);
    }
    Ok(())
}

fn validate_cross_progress_transition(
    previous: &CrossPrincipalCompletionProgress,
    next: &CrossPrincipalCompletionProgress,
) -> Result<(), ProviderProbeJournalError> {
    let evidence_matches = match (previous, next) {
        (
            CrossPrincipalCompletionProgress::Prepared,
            CrossPrincipalCompletionProgress::ReadsVerified { .. },
        ) => true,
        (
            CrossPrincipalCompletionProgress::ReadsVerified {
                administrator_read_peer_hash: previous,
            },
            CrossPrincipalCompletionProgress::PeerAbsent {
                administrator_read_peer_hash: next,
            },
        )
        | (
            CrossPrincipalCompletionProgress::PeerAbsent {
                administrator_read_peer_hash: previous,
            },
            CrossPrincipalCompletionProgress::Absent {
                administrator_read_peer_hash: next,
            },
        ) => previous == next,
        (
            CrossPrincipalCompletionProgress::Absent {
                administrator_read_peer_hash,
            },
            CrossPrincipalCompletionProgress::ReceiptReady { receipt },
        ) => receipt.transcript.administrator_read_peer_hash == *administrator_read_peer_hash,
        _ => return Err(ProviderProbeJournalError::NonAdjacentProgress),
    };
    if !evidence_matches {
        return Err(ProviderProbeJournalError::EvidenceChanged);
    }
    Ok(())
}

fn cross_progress_evidence_hash(progress: &CrossPrincipalCompletionProgress) -> Option<ObjectHash> {
    match progress {
        CrossPrincipalCompletionProgress::Prepared => None,
        CrossPrincipalCompletionProgress::ReadsVerified {
            administrator_read_peer_hash,
        }
        | CrossPrincipalCompletionProgress::PeerAbsent {
            administrator_read_peer_hash,
        }
        | CrossPrincipalCompletionProgress::Absent {
            administrator_read_peer_hash,
        } => Some(*administrator_read_peer_hash),
        CrossPrincipalCompletionProgress::ReceiptReady { receipt } => {
            Some(receipt.transcript.administrator_read_peer_hash)
        }
    }
}

#[async_trait]
pub(crate) trait ProviderProbeJournal: Send + Sync {
    async fn load(
        &self,
        probe_id: ProviderProbeId,
    ) -> Result<Option<ProviderProbeJournalRecord>, StorageError>;

    /// Atomically inserts `prepared` when absent or returns the exact existing
    /// record for this probe id. A different record under the id is corruption.
    async fn begin(
        &self,
        prepared: ProviderProbeJournalRecord,
    ) -> Result<ProviderProbeJournalRecord, StorageError>;

    /// Atomically replaces the exact current record. Implementations reject a
    /// stale predecessor instead of merging progress.
    async fn advance(
        &self,
        previous: &ProviderProbeJournalRecord,
        next: ProviderProbeJournalRecord,
    ) -> Result<(), StorageError>;
}

pub(crate) async fn prepare_cross_principal_challenge(
    administrator: &dyn ExactSlotStorage,
    publication_journal: &dyn DeviceJoinChallengePublicationJournal,
    probe_id: ProviderProbeId,
    store: &StoreProviderBinding,
    context: &CrossPrincipalChallengeContext,
    administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
) -> Result<CrossPrincipalProbeChallenge, ProviderProbeError> {
    let administrator_live = administrator
        .provider_binding()
        .await
        .map_err(StorageError::from)?;
    if administrator_live.store != *store
        || administrator_live.device != context.administrator_binding
    {
        return invalid("cross-principal administrator does not match the challenge context");
    }
    validate_cross_provider_evidence_context(store, context)?;
    let suffix = hex::encode(probe_id.as_bytes());
    let administrator_key = format!("__coven_probe__/cross/{suffix}/administrator");
    let administrator_slot = administrator
        .allocate_slot(&administrator_key)
        .await
        .map_err(StorageError::from)?;
    if administrator_slot.logical_key() != administrator_key {
        return invalid("cross-principal administrator slot changed its logical key");
    }
    let administrator_payload = probe_payload(&probe_id, ProbePayloadLabel::CrossAdministrator);
    let administrator_object = ProbeExactObjectReceipt {
        slot: administrator_slot.clone(),
        payload_hash: ObjectHash::digest(&administrator_payload),
        object: ExactObjectRef::new(
            administrator_slot,
            administrator_payload.len() as u64,
            ObjectHash::digest(&administrator_payload),
        ),
    };
    let unsigned = CrossPrincipalProbeChallenge {
        probe_id,
        administrator_object,
        challenge_hash: ObjectHash::digest(&[]),
        administrator_signature: String::new(),
    };
    let challenge_hash = cross_challenge_hash(store, context, &unsigned);
    let challenge = CrossPrincipalProbeChallenge {
        challenge_hash,
        administrator_signature: hex::encode(administrator_signer.sign(challenge_hash.as_bytes())),
        ..unsigned
    };
    challenge.verify(context, store, &administrator_signer.public_key_hex())?;
    let durable = publication_journal.prepare(&challenge).await?;
    if durable.challenge != challenge {
        return invalid("durable cross-principal challenge differs from its prepared bytes");
    }
    Ok(challenge)
}

pub(crate) async fn settle_cross_principal_challenge(
    administrator: &dyn ExactSlotStorage,
    publication_journal: &dyn DeviceJoinChallengePublicationJournal,
    authorization: &DeviceJoinChallengePublicationAuthorization,
    challenge: &CrossPrincipalProbeChallenge,
    context: &CrossPrincipalChallengeContext,
    store: &StoreProviderBinding,
) -> Result<CrossPrincipalProbeChallenge, ProviderProbeError> {
    let live = administrator
        .provider_binding()
        .await
        .map_err(StorageError::from)?;
    if live.store != *store || live.device != context.administrator_binding {
        return invalid("cross-principal administrator does not match the published challenge");
    }
    publication_journal
        .claim_published(authorization, challenge)
        .await?;
    let payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossAdministrator);
    settle_exact_create(
        administrator,
        &challenge.administrator_object.slot,
        &payload,
    )
    .await?;
    let observed = administrator
        .read_at(&challenge.administrator_object.slot)
        .await
        .map_err(StorageError::from)?;
    if observed != payload {
        return invalid("published cross-principal challenge differs from its signed bytes");
    }
    Ok(challenge.clone())
}

pub(crate) async fn create_cross_principal_response(
    peer: &dyn ExactSlotStorage,
    challenge: &CrossPrincipalProbeChallenge,
    context: &CrossPrincipalResponseContext,
    store: &StoreProviderBinding,
    administrator_signing_pubkey: &str,
    peer_signer: &UserKeypair,
) -> Result<CrossPrincipalProbeResponse, ProviderProbeError> {
    challenge.verify(&context.challenge, store, administrator_signing_pubkey)?;
    let peer_pubkey = crate::keys::public_key_hex(peer_signer);
    if context.challenge.member_pubkey != peer_pubkey {
        return invalid("cross-principal peer signer is not the joining member");
    }
    let live = peer.provider_binding().await.map_err(StorageError::from)?;
    if live.store != *store || live.device != context.challenge.peer_binding {
        return invalid("cross-principal peer does not match the response context");
    }
    let evidence = peer
        .cross_principal_evidence()
        .await
        .map_err(StorageError::from)?;
    validate_cross_provider_evidence(
        store,
        &context.challenge.administrator_binding,
        &context.challenge.peer_binding,
        &evidence,
    )?;
    let expected_peer_key = cross_peer_logical_key(challenge.probe_id);
    if context.response_slot.logical_key() != expected_peer_key {
        return invalid("cross-principal response slot uses the wrong logical key");
    }
    let administrator_payload =
        probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossAdministrator);
    let administrator_read = peer
        .read_at(&challenge.administrator_object.slot)
        .await
        .map_err(StorageError::from)?;
    if administrator_read != administrator_payload {
        return invalid("peer read bytes differ from the signed cross-principal challenge");
    }
    let peer_payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossPeer);
    settle_exact_create(peer, &context.response_slot, &peer_payload).await?;
    let peer_read = peer
        .read_at(&context.response_slot)
        .await
        .map_err(StorageError::from)?;
    if peer_read != peer_payload {
        return invalid("peer response readback differs from its deterministic bytes");
    }
    let peer_object = ProbeExactObjectReceipt {
        slot: context.response_slot.clone(),
        payload_hash: ObjectHash::digest(&peer_payload),
        object: ExactObjectRef::new(
            context.response_slot.clone(),
            peer_payload.len() as u64,
            ObjectHash::digest(&peer_payload),
        ),
    };
    let unsigned = CrossPrincipalProbeResponse {
        challenge_hash: challenge.challenge_hash,
        provider_evidence: evidence,
        peer_object,
        peer_read_administrator_hash: ObjectHash::digest(&administrator_read),
        response_hash: ObjectHash::digest(&[]),
        peer_signature: String::new(),
    };
    let response_hash = cross_response_hash(store, context, challenge, &unsigned);
    let response = CrossPrincipalProbeResponse {
        response_hash,
        peer_signature: hex::encode(peer_signer.sign(response_hash.as_bytes())),
        ..unsigned
    };
    response.verify(
        challenge,
        context,
        store,
        administrator_signing_pubkey,
        &peer_pubkey,
    )?;
    Ok(response)
}

pub(crate) async fn complete_cross_principal_probe(
    administrator: &dyn ExactSlotStorage,
    journal: &dyn ProviderProbeJournal,
    challenge: &CrossPrincipalProbeChallenge,
    response: &CrossPrincipalProbeResponse,
    context: &CrossPrincipalResponseContext,
    store: &StoreProviderBinding,
    administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
    peer_signing_pubkey: &str,
) -> Result<CrossPrincipalProbeReceipt, ProviderProbeError> {
    let administrator_pubkey = administrator_signer.public_key_hex();
    challenge.verify(&context.challenge, store, &administrator_pubkey)?;
    response.verify(
        challenge,
        context,
        store,
        &administrator_pubkey,
        peer_signing_pubkey,
    )?;
    let live = administrator
        .provider_binding()
        .await
        .map_err(StorageError::from)?;
    if live.store != *store || live.device != context.challenge.administrator_binding {
        return invalid("cross-principal administrator does not match the completion context");
    }
    let prepared = ProviderProbeJournalRecord::CrossPrincipal(CrossPrincipalCompletionJournal {
        probe_id: challenge.probe_id,
        store: store.clone(),
        context: context.clone(),
        challenge: challenge.clone(),
        response: response.clone(),
        progress: CrossPrincipalCompletionProgress::Prepared,
    });
    let mut durable = match journal.load(challenge.probe_id).await? {
        Some(existing) => existing,
        None => journal.begin(prepared).await?,
    };
    let ProviderProbeJournalRecord::CrossPrincipal(mut record) = durable.clone() else {
        return invalid("cross-principal probe id belongs to another durable probe kind");
    };
    if record.probe_id != challenge.probe_id
        || record.store != *store
        || record.context != *context
        || record.challenge != *challenge
        || record.response != *response
    {
        return invalid("durable cross-principal completion differs from the requested proof");
    }
    if let CrossPrincipalCompletionProgress::ReceiptReady { receipt } = &record.progress {
        receipt.verify(context, store, &administrator_pubkey, peer_signing_pubkey)?;
        return Ok(receipt.clone());
    }
    let peer_payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossPeer);
    if matches!(record.progress, CrossPrincipalCompletionProgress::Prepared) {
        let observed = administrator
            .read_at(&response.peer_object.slot)
            .await
            .map_err(StorageError::from)?;
        if observed != peer_payload {
            return invalid("administrator read differs from the signed peer response");
        }
        advance_cross_completion(
            journal,
            &mut durable,
            &mut record,
            CrossPrincipalCompletionProgress::ReadsVerified {
                administrator_read_peer_hash: ObjectHash::digest(&observed),
            },
        )
        .await?;
    }
    let administrator_read_peer_hash = cross_completion_read_hash(&record.progress)?;
    if matches!(
        record.progress,
        CrossPrincipalCompletionProgress::ReadsVerified { .. }
    ) {
        administrator
            .delete_and_verify_absent(&response.peer_object.slot)
            .await
            .map_err(StorageError::from)?;
        advance_cross_completion(
            journal,
            &mut durable,
            &mut record,
            CrossPrincipalCompletionProgress::PeerAbsent {
                administrator_read_peer_hash,
            },
        )
        .await?;
    }
    if matches!(
        record.progress,
        CrossPrincipalCompletionProgress::PeerAbsent { .. }
    ) {
        administrator
            .delete_and_verify_absent(&challenge.administrator_object.slot)
            .await
            .map_err(StorageError::from)?;
        advance_cross_completion(
            journal,
            &mut durable,
            &mut record,
            CrossPrincipalCompletionProgress::Absent {
                administrator_read_peer_hash,
            },
        )
        .await?;
    }
    let transcript = CrossPrincipalProbeTranscript {
        challenge: challenge.clone(),
        response: response.clone(),
        administrator_read_peer_hash,
        administrator_delete_peer_verified_absent: true,
        administrator_delete_own_verified_absent: true,
    };
    let receipt =
        CrossPrincipalProbeReceipt::signed(transcript, context, store, administrator_signer)?;
    advance_cross_completion(
        journal,
        &mut durable,
        &mut record,
        CrossPrincipalCompletionProgress::ReceiptReady {
            receipt: receipt.clone(),
        },
    )
    .await?;
    Ok(receipt)
}

async fn advance_cross_completion(
    journal: &dyn ProviderProbeJournal,
    durable: &mut ProviderProbeJournalRecord,
    record: &mut CrossPrincipalCompletionJournal,
    progress: CrossPrincipalCompletionProgress,
) -> Result<(), ProviderProbeError> {
    record.progress = progress;
    let next = ProviderProbeJournalRecord::CrossPrincipal(record.clone());
    journal.advance(durable, next.clone()).await?;
    *durable = next;
    Ok(())
}

fn cross_completion_read_hash(
    progress: &CrossPrincipalCompletionProgress,
) -> Result<ObjectHash, ProviderProbeError> {
    match progress {
        CrossPrincipalCompletionProgress::ReadsVerified {
            administrator_read_peer_hash,
        }
        | CrossPrincipalCompletionProgress::PeerAbsent {
            administrator_read_peer_hash,
        }
        | CrossPrincipalCompletionProgress::Absent {
            administrator_read_peer_hash,
        } => Ok(*administrator_read_peer_hash),
        CrossPrincipalCompletionProgress::Prepared
        | CrossPrincipalCompletionProgress::ReceiptReady { .. } => {
            invalid("cross-principal completion has no durable administrator read")
        }
    }
}

async fn settle_exact_create(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
    payload: &[u8],
) -> Result<(), ProviderProbeError> {
    match storage.read_at(slot).await {
        Ok(bytes) if bytes == payload => Ok(()),
        Ok(_) => invalid("durable provider probe slot contains different bytes"),
        Err(CloudHomeError::NotFound(_)) => storage
            .create_at(slot, BlobBody::from_bytes(payload.to_vec()), &|_| {})
            .await
            .map_err(StorageError::from)
            .map_err(ProviderProbeError::Storage),
        Err(error) => Err(ProviderProbeError::Storage(StorageError::from(error))),
    }
}

pub(crate) async fn probe_exact_slots(
    first: &dyn ExactSlotStorage,
    second: &dyn ExactSlotStorage,
    journal: &dyn ProviderProbeJournal,
    probe_id: ProviderProbeId,
    binding: &crate::storage::ResolvedProviderBinding,
) -> Result<ExactSlotProbeReceipt, ProviderProbeError> {
    binding.validate().map_err(ProviderProbeError::Storage)?;
    let first_binding = first.provider_binding().await.map_err(StorageError::from)?;
    let second_binding = second
        .provider_binding()
        .await
        .map_err(StorageError::from)?;
    if first_binding != *binding || second_binding != *binding {
        return invalid("exact-slot probe clients do not match the receipt binding");
    }
    let id = hex::encode(probe_id.as_bytes());
    let logical_key = format!("__coven_probe__/exact/{id}");
    let lost_logical_key = format!("__coven_probe__/lost-response/{id}");
    let mut durable = match journal.load(probe_id).await? {
        Some(existing) => existing,
        None => {
            let allocated_slot = first
                .allocate_slot(&logical_key)
                .await
                .map_err(StorageError::from)?;
            let allocated_lost_slot = first
                .allocate_slot(&lost_logical_key)
                .await
                .map_err(StorageError::from)?;
            journal
                .begin(ProviderProbeJournalRecord::Exact(ExactProbeJournal {
                    probe_id,
                    binding: binding.clone(),
                    slot: allocated_slot,
                    lost_response_slot: allocated_lost_slot,
                    progress: ExactProbeProgress::Prepared,
                }))
                .await?
        }
    };
    let ProviderProbeJournalRecord::Exact(mut record) = durable.clone() else {
        return invalid("exact probe id belongs to a different durable probe kind");
    };
    if record.probe_id != probe_id || record.binding != *binding {
        return invalid("durable exact probe differs from its requested binding or id");
    }
    let slot = record.slot.clone();
    let lost_slot = record.lost_response_slot.clone();
    if slot.logical_key() != logical_key || lost_slot.logical_key() != lost_logical_key {
        return invalid("exact-slot allocator changed the probe logical key");
    }
    let payloads = [
        probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst),
        probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond),
    ];
    if matches!(record.progress, ExactProbeProgress::Prepared) {
        let (outcomes, _winner) = match first.read_at(&slot).await {
            Err(CloudHomeError::NotFound(_)) => {
                let (left, right) = tokio::join!(
                    first.create_at(&slot, BlobBody::from_bytes(payloads[0].clone()), &|_| {}),
                    second.create_at(&slot, BlobBody::from_bytes(payloads[1].clone()), &|_| {}),
                );
                classify_exact_create_race(left, right)?
            }
            Ok(bytes) if bytes == payloads[0] => {
                require_occupied_rejection(
                    second
                        .create_at(&slot, BlobBody::from_bytes(payloads[1].clone()), &|_| {})
                        .await,
                )?;
                (
                    [
                        ProbeCreateOutcome::Created,
                        ProbeCreateOutcome::RejectedOccupied,
                    ],
                    0,
                )
            }
            Ok(bytes) if bytes == payloads[1] => {
                require_occupied_rejection(
                    first
                        .create_at(&slot, BlobBody::from_bytes(payloads[0].clone()), &|_| {})
                        .await,
                )?;
                (
                    [
                        ProbeCreateOutcome::RejectedOccupied,
                        ProbeCreateOutcome::Created,
                    ],
                    1,
                )
            }
            Ok(_) => return invalid("durable exact probe slot contains unknown bytes"),
            Err(error) => return Err(ProviderProbeError::Storage(StorageError::from(error))),
        };
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::Created { outcomes },
        )
        .await?;
    }
    let (outcomes, winner) = exact_race_state(&record.progress)?;
    let (full, range) = if matches!(record.progress, ExactProbeProgress::Created { .. }) {
        let full = first.read_at(&slot).await.map_err(StorageError::from)?;
        if full != payloads[winner] {
            return invalid("authoritative exact read does not match the create winner");
        }
        let range = first
            .read_range_at(&slot, PROBE_RANGE_START, PROBE_RANGE_END)
            .await
            .map_err(StorageError::from)?;
        if range != full[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize] {
            return invalid("exact range read does not match the authoritative full read");
        }
        (full, range)
    } else {
        (
            payloads[winner].clone(),
            payloads[winner][PROBE_RANGE_START as usize..PROBE_RANGE_END as usize].to_vec(),
        )
    };
    if matches!(record.progress, ExactProbeProgress::Created { .. }) {
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::ReadsVerified { outcomes },
        )
        .await?;
    }
    let accepted = ExactObjectRef::new(slot.clone(), full.len() as u64, ObjectHash::digest(&full));
    if matches!(record.progress, ExactProbeProgress::ReadsVerified { .. }) {
        first
            .delete_and_verify_absent(&slot)
            .await
            .map_err(StorageError::from)?;
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::PrimaryAbsent { outcomes },
        )
        .await?;
    }
    let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
    if matches!(record.progress, ExactProbeProgress::PrimaryAbsent { .. }) {
        match first.read_at(&lost_slot).await {
            Ok(bytes) if bytes == lost_payload => {}
            Ok(_) => return invalid("lost-response slot contains unknown bytes"),
            Err(CloudHomeError::NotFound(_)) => first
                .create_at(
                    &lost_slot,
                    BlobBody::from_bytes(lost_payload.clone()),
                    &|_| {},
                )
                .await
                .map_err(StorageError::from)?,
            Err(error) => return Err(ProviderProbeError::Storage(StorageError::from(error))),
        }
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::LostResponseCreated { outcomes },
        )
        .await?;
    }
    let lost_readback = if matches!(
        record.progress,
        ExactProbeProgress::LostResponseCreated { .. }
    ) {
        let readback = first
            .read_at(&lost_slot)
            .await
            .map_err(StorageError::from)?;
        if readback != lost_payload {
            return invalid("lost-response authoritative readback differs from committed bytes");
        }
        readback
    } else {
        lost_payload.clone()
    };
    let settled = ExactObjectRef::new(
        lost_slot.clone(),
        lost_readback.len() as u64,
        ObjectHash::digest(&lost_readback),
    );
    if matches!(
        record.progress,
        ExactProbeProgress::LostResponseCreated { .. }
    ) {
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::LostResponseReadVerified { outcomes },
        )
        .await?;
    }
    if matches!(
        record.progress,
        ExactProbeProgress::LostResponseReadVerified { .. }
    ) {
        first
            .delete_and_verify_absent(&lost_slot)
            .await
            .map_err(StorageError::from)?;
        advance_exact(
            journal,
            &mut durable,
            &mut record,
            ExactProbeProgress::Absent { outcomes },
        )
        .await?;
    }

    if let ExactProbeProgress::ReceiptReady { receipt } = &record.progress {
        receipt.verify(&binding.store, &binding.device)?;
        return Ok(receipt.clone());
    }
    let transcript = ExactSlotProbeTranscript {
        probe_id,
        logical_key,
        slot,
        contenders: [
            ProbeCreateAttempt {
                payload_hash: ObjectHash::digest(&payloads[0]),
                outcome: outcomes[0],
            },
            ProbeCreateAttempt {
                payload_hash: ObjectHash::digest(&payloads[1]),
                outcome: outcomes[1],
            },
        ],
        accepted,
        full_read_hash: ObjectHash::digest(&full),
        range: ProbeRangeReceipt {
            start: PROBE_RANGE_START,
            end: PROBE_RANGE_END,
            bytes_hash: ObjectHash::digest(&range),
        },
        delete_verified_absent: true,
        lost_response: LostResponseProbeReceipt {
            logical_key: lost_logical_key,
            slot: lost_slot,
            payload_hash: ObjectHash::digest(&lost_payload),
            settled,
            readback_hash: ObjectHash::digest(&lost_readback),
            delete_verified_absent: true,
        },
    };
    let receipt =
        ExactSlotProbeReceipt::from_transcript(transcript, &binding.store, &binding.device);
    receipt.verify(&binding.store, &binding.device)?;
    advance_exact(
        journal,
        &mut durable,
        &mut record,
        ExactProbeProgress::ReceiptReady {
            receipt: receipt.clone(),
        },
    )
    .await?;
    Ok(receipt)
}

async fn advance_exact(
    journal: &dyn ProviderProbeJournal,
    durable: &mut ProviderProbeJournalRecord,
    record: &mut ExactProbeJournal,
    progress: ExactProbeProgress,
) -> Result<(), ProviderProbeError> {
    record.progress = progress;
    let next = ProviderProbeJournalRecord::Exact(record.clone());
    journal.advance(durable, next.clone()).await?;
    *durable = next;
    Ok(())
}

fn exact_race_state(
    progress: &ExactProbeProgress,
) -> Result<([ProbeCreateOutcome; 2], usize), ProviderProbeError> {
    let (outcomes, winner) = match progress {
        ExactProbeProgress::Prepared => return invalid("exact probe has no durable create result"),
        ExactProbeProgress::Created { outcomes }
        | ExactProbeProgress::ReadsVerified { outcomes }
        | ExactProbeProgress::PrimaryAbsent { outcomes }
        | ExactProbeProgress::LostResponseCreated { outcomes }
        | ExactProbeProgress::LostResponseReadVerified { outcomes }
        | ExactProbeProgress::Absent { outcomes } => {
            let winner = outcomes
                .iter()
                .position(|outcome| *outcome == ProbeCreateOutcome::Created)
                .ok_or_else(|| {
                    ProviderProbeError::InvalidReceipt(
                        "durable exact probe has no create winner".to_string(),
                    )
                })?;
            (*outcomes, winner)
        }
        ExactProbeProgress::ReceiptReady { receipt } => {
            let winner = receipt
                .transcript
                .contenders
                .iter()
                .position(|attempt| attempt.outcome == ProbeCreateOutcome::Created)
                .ok_or_else(|| {
                    ProviderProbeError::InvalidReceipt(
                        "durable exact receipt has no create winner".to_string(),
                    )
                })?;
            (
                [
                    receipt.transcript.contenders[0].outcome,
                    receipt.transcript.contenders[1].outcome,
                ],
                winner,
            )
        }
    };
    if winner > 1 || outcomes[winner] != ProbeCreateOutcome::Created {
        return invalid("durable exact probe has an invalid winner");
    }
    Ok((outcomes, winner))
}

fn classify_exact_create_race(
    left: Result<(), CloudHomeError>,
    right: Result<(), CloudHomeError>,
) -> Result<([ProbeCreateOutcome; 2], usize), ProviderProbeError> {
    match (left, right) {
        (Ok(()), Err(CloudHomeError::AlreadyExists(_))) => Ok((
            [
                ProbeCreateOutcome::Created,
                ProbeCreateOutcome::RejectedOccupied,
            ],
            0,
        )),
        (Err(CloudHomeError::AlreadyExists(_)), Ok(())) => Ok((
            [
                ProbeCreateOutcome::RejectedOccupied,
                ProbeCreateOutcome::Created,
            ],
            1,
        )),
        (left, right) => invalid(&format!(
            "exact-slot race did not produce one create and one occupied rejection: left={left:?}, right={right:?}"
        )),
    }
}

fn require_occupied_rejection(
    result: Result<(), CloudHomeError>,
) -> Result<(), ProviderProbeError> {
    match result {
        Err(CloudHomeError::AlreadyExists(_)) => Ok(()),
        Ok(()) => invalid("settled exact probe contender unexpectedly created a second object"),
        Err(error) => Err(ProviderProbeError::Storage(StorageError::from(error))),
    }
}

fn cross_transcript_hash(
    store: &StoreProviderBinding,
    context: &CrossPrincipalResponseContext,
    transcript: &CrossPrincipalProbeTranscript,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        CROSS_TRANSCRIPT_DOMAIN,
        &(store, context, transcript),
    ))
}

fn validate_cross_transcript_payloads(
    transcript: &CrossPrincipalProbeTranscript,
    context: &CrossPrincipalResponseContext,
) -> Result<(), ProviderProbeError> {
    validate_cross_challenge_payload(&transcript.challenge)?;
    validate_cross_response_payload(&transcript.response, &transcript.challenge, context)?;
    let peer = probe_payload(&transcript.challenge.probe_id, ProbePayloadLabel::CrossPeer);
    if transcript.administrator_read_peer_hash != ObjectHash::digest(&peer)
        || !transcript.administrator_delete_peer_verified_absent
        || !transcript.administrator_delete_own_verified_absent
    {
        return invalid("cross-principal object, read, or deletion evidence is invalid");
    }
    Ok(())
}

fn cross_challenge_hash(
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

fn cross_response_hash(
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

fn validate_cross_challenge_payload(
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

fn validate_cross_response_payload(
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

fn validate_probe_exact_object(
    receipt: &ProbeExactObjectReceipt,
    expected_logical_key: &str,
    payload: &[u8],
    label: &str,
) -> Result<(), ProviderProbeError> {
    let payload_hash = ObjectHash::digest(payload);
    if receipt.slot.logical_key() != expected_logical_key
        || receipt.slot != *receipt.object.slot()
        || receipt.payload_hash != payload_hash
        || receipt.object.stored_size() != payload.len() as u64
        || receipt.object.stored_hash() != payload_hash
    {
        return invalid(&format!(
            "{label} object reference or payload hash is invalid"
        ));
    }
    Ok(())
}

fn cross_administrator_logical_key(probe_id: ProviderProbeId) -> String {
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

fn validate_cross_provider_evidence_context(
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

fn validate_cross_provider_evidence(
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
                corpus: crate::storage::GoogleDriveCorpus::SharedDrive { .. }
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
        let crate::storage::ProviderPrincipalId::CloudKitSharedZoneParticipant { record_name } =
            &peer.principal
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

fn invalid<T>(reason: &str) -> Result<T, ProviderProbeError> {
    Err(ProviderProbeError::InvalidReceipt(reason.to_string()))
}

fn domain_json<T: Serialize>(domain: &[u8], value: &T) -> Vec<u8> {
    let mut bytes = domain.to_vec();
    bytes.extend(
        serde_json::to_vec(value).expect("closed provider transcript serialization cannot fail"),
    );
    bytes
}

fn exact_transcript_hash(
    store: &StoreProviderBinding,
    device: &ProviderDeviceBinding,
    transcript: &ExactSlotProbeTranscript,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        EXACT_TRANSCRIPT_DOMAIN,
        &(store, device, transcript),
    ))
}

mod ordered_owner_barriers {
    use super::*;

    pub(super) fn serialize<S>(
        map: &BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<MembershipGrantId, OwnerStreamBarrier>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(MembershipGrantId, OwnerStreamBarrier)>::deserialize(deserializer)?;
        let count = entries.len();
        let map = entries.into_iter().collect::<BTreeMap<_, _>>();
        if map.len() != count {
            return Err(serde::de::Error::custom(
                "provider administrator owner barriers contain a duplicate grant",
            ));
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::store_commit::ObjectHash;

    #[test]
    fn credential_rotation_generation_exhaustion_is_not_a_successor() {
        assert!(ProviderAccessWithdrawal::S3CredentialRotation {
            retired_generation: u64::MAX,
            active_generation: u64::MAX,
            retired_credential_verified_rejected: true,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn exact_probe_verifier_rejects_two_created_contenders() {
        let mut receipt = test_exact_receipt();
        receipt
            .verify(&test_store_binding(), &test_device_binding())
            .expect("baseline exact receipt verifies");
        receipt.transcript.contenders[1].outcome = ProbeCreateOutcome::Created;

        assert!(receipt
            .verify(&test_store_binding(), &test_device_binding())
            .is_err());
    }

    #[test]
    fn custom_s3_origin_rejects_paths_and_canonicalizes_default_port() {
        assert_eq!(
            canonical_custom_s3_origin("HTTPS://Objects.Example:443").unwrap(),
            "https://objects.example"
        );
        assert!(canonical_custom_s3_origin("https://objects.example/").is_err());
        assert!(canonical_custom_s3_origin("https://objects.example/bucket").is_err());
    }

    #[test]
    fn private_cloudkit_owner_exposes_its_exact_administrator_locator() {
        let binding = private_cloudkit_binding();

        let locator = ProviderAccessLocator::for_current_administrator(&binding)
            .expect("private CloudKit owner exposes its exact administrator locator");

        assert_eq!(
            locator,
            ProviderAccessLocator::CloudKitPrivateZoneOwner {
                owner_name: "private-owner".to_string(),
                zone_name: "private-zone".to_string(),
                owner_record_name: "current-user".to_string(),
            }
        );
        locator
            .validate_for(&binding.store, &binding.device)
            .expect("private CloudKit owner locator matches its binding");
    }

    #[test]
    fn shared_cloudkit_participant_is_not_treated_as_the_zone_owner() {
        let mut binding = private_cloudkit_binding();
        binding.device.principal =
            crate::storage::ProviderPrincipalId::CloudKitSharedZoneParticipant {
                record_name: "current-user".to_string(),
            };

        assert!(ProviderAccessLocator::for_current_administrator(&binding).is_err());
    }

    #[test]
    fn provider_admin_grants_coexist_and_replay_exactly() {
        let founder = admin_record(1, "founder");
        let mut state = ProviderAdminState::founder(founder.clone());
        let second = admin_record(2, "second");
        let change = set_change(&second, BTreeSet::new());
        state
            .apply(change.clone(), second.created_at.clone())
            .expect("a second administrator may coexist");
        state
            .apply(change, second.created_at.clone())
            .expect("an exact replay is idempotent");
        assert_eq!(state.active().len(), 2);
        assert!(state.tombstones().is_empty());
    }

    #[test]
    fn provider_admin_rejects_conflicting_id_reuse() {
        let founder = admin_record(1, "founder");
        let mut state = ProviderAdminState::founder(founder);
        let second = admin_record(2, "second");
        state
            .apply(
                set_change(&second, BTreeSet::new()),
                second.created_at.clone(),
            )
            .unwrap();
        let mut conflicting = second.clone();
        conflicting.provider = ProviderDeviceBinding {
            principal: crate::storage::ProviderPrincipalId::Aws {
                account_id: "999999999999".to_string(),
                principal: crate::storage::AwsPrincipal::Root,
            },
        };
        assert_eq!(
            state.apply(
                set_change(&conflicting, BTreeSet::new()),
                conflicting.created_at.clone()
            ),
            Err(ProviderAdminReducerError::GrantIdReuse)
        );
    }

    #[test]
    fn provider_admin_removal_and_replacement_retain_tombstones() {
        let founder = admin_record(1, "founder");
        let founder_id = founder.grant_id.clone();
        let mut state = ProviderAdminState::founder(founder);
        let replacement = admin_record(2, "replacement");
        state
            .apply(
                set_change(&replacement, BTreeSet::from([founder_id.clone()])),
                replacement.created_at.clone(),
            )
            .unwrap();
        assert!(state.records().contains_key(&founder_id));
        assert!(state.tombstones().contains(&founder_id));
        assert_eq!(
            state.apply(
                ProviderAdminChange::Remove {
                    removes: BTreeSet::from([replacement.grant_id.clone()]),
                },
                replacement.created_at.clone(),
            ),
            Err(ProviderAdminReducerError::NoEffectiveAdministrator)
        );
        assert!(state.records().contains_key(&replacement.grant_id));
        assert!(!state.tombstones().contains(&replacement.grant_id));
    }

    #[test]
    fn provider_admin_replay_cannot_tombstone_a_newly_active_replacement() {
        let founder = admin_record(1, "founder");
        let mut state = ProviderAdminState::founder(founder);
        let second = admin_record(2, "second");
        state
            .apply(
                set_change(&second, BTreeSet::new()),
                second.created_at.clone(),
            )
            .unwrap();
        let third = admin_record(3, "third");
        state
            .apply(
                set_change(&third, BTreeSet::new()),
                third.created_at.clone(),
            )
            .unwrap();
        assert_eq!(
            state.apply(
                set_change(&second, BTreeSet::from([third.grant_id.clone()])),
                second.created_at.clone(),
            ),
            Err(ProviderAdminReducerError::UnknownReplacement)
        );
        assert!(state.active().contains(&third.grant_id));
    }

    #[tokio::test]
    async fn database_probe_journal_rejects_a_skipped_progress_state() {
        let db = crate::sync::test_helpers::open_test_db();
        let journal = crate::database::StoreDatabase::new(&db);
        let probe_id = ProviderProbeId::from_bytes([44; 32]);
        let binding = crate::storage::ResolvedProviderBinding {
            store: test_store_binding(),
            device: test_device_binding(),
        };
        let prepared = ProviderProbeJournalRecord::Exact(ExactProbeJournal {
            probe_id,
            binding,
            slot: ObjectSlot::logical("__coven_probe__/exact/journal".to_string()).unwrap(),
            lost_response_slot: ObjectSlot::logical(
                "__coven_probe__/lost-response/journal".to_string(),
            )
            .unwrap(),
            progress: ExactProbeProgress::Prepared,
        });
        assert_eq!(journal.begin(prepared.clone()).await.unwrap(), prepared);
        let ProviderProbeJournalRecord::Exact(mut final_record) = prepared.clone() else {
            unreachable!()
        };
        final_record.progress = ExactProbeProgress::ReceiptReady {
            receipt: test_exact_receipt(),
        };
        let final_record = ProviderProbeJournalRecord::Exact(final_record);
        assert!(journal.advance(&prepared, final_record).await.is_err());
        assert_eq!(journal.load(probe_id).await.unwrap(), Some(prepared));
    }

    fn admin_record(id: u8, label: &str) -> ProviderAdminGrantRecord {
        let root_object = crate::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!("roots/{label}")).unwrap(),
            1,
            ObjectHash::digest(&[id]),
        );
        let root = StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{label} id").as_bytes()),
            store_root_hash: ObjectHash::digest(label.as_bytes()),
            object: root_object,
        };
        let registration: StoreDeviceRegistrationRef = serde_json::from_value(serde_json::json!({
            "device_id": ObjectHash::digest(&[id, 1]),
            "registration_hash": ObjectHash::digest(&[id, 2]),
            "object": {
                "slot": {"logical_key": format!("registrations/{label}"), "physical": {"kind": "logical_key"}},
                "stored_size": 1,
                "stored_hash": ObjectHash::digest(&[id, 3]),
            }
        }))
        .unwrap();
        ProviderAdminGrantRecord {
            grant_id: ProviderAdminGrantId(ObjectHash::digest(&[id, 4])),
            administrator: registration,
            provider: test_device_binding(),
            access: ProviderAccessLocator::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: ObjectHash::digest(b"test access key"),
            },
            capability: ProviderCapabilityProof {
                exact_slots: test_exact_receipt(),
            },
            created_at: ProviderAdminGrantOrigin::Founder { root },
        }
    }

    fn set_change(
        record: &ProviderAdminGrantRecord,
        replaces: BTreeSet<ProviderAdminGrantId>,
    ) -> ProviderAdminChange {
        ProviderAdminChange::Set {
            administrator: record.administrator.clone(),
            provider: record.provider.clone(),
            access: record.access.clone(),
            capability: record.capability.clone(),
            grant_id: record.grant_id.clone(),
            replaces,
        }
    }

    fn test_store_binding() -> crate::storage::StoreProviderBinding {
        crate::storage::StoreProviderBinding::S3 {
            endpoint: crate::storage::S3EndpointBinding::Aws {
                partition: "aws".to_string(),
            },
            region: "us-east-1".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: None,
        }
    }

    fn private_cloudkit_binding() -> crate::storage::ResolvedProviderBinding {
        crate::storage::ResolvedProviderBinding {
            store: crate::storage::StoreProviderBinding::CloudKit {
                container_id: "iCloud.example.coven".to_string(),
                environment: crate::storage::CloudKitEnvironment::Development,
                owner_name: "private-owner".to_string(),
                zone_name: "private-zone".to_string(),
            },
            device: crate::storage::ProviderDeviceBinding {
                principal: crate::storage::ProviderPrincipalId::CloudKitPrivateZoneOwner {
                    record_name: "current-user".to_string(),
                },
            },
        }
    }

    fn test_device_binding() -> crate::storage::ProviderDeviceBinding {
        crate::storage::ProviderDeviceBinding {
            principal: crate::storage::ProviderPrincipalId::Aws {
                account_id: "123456789012".to_string(),
                principal: crate::storage::AwsPrincipal::Root,
            },
        }
    }

    fn test_exact_receipt() -> ExactSlotProbeReceipt {
        let probe_id = ProviderProbeId::from_bytes([7; 32]);
        let slot = crate::storage::cloud::ObjectSlot::logical("store-v1/probes/exact".to_string())
            .unwrap();
        let first = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
        let second = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
        let accepted = crate::storage::ExactObjectRef::new(
            slot.clone(),
            first.len() as u64,
            ObjectHash::digest(&first),
        );
        let lost_slot =
            crate::storage::cloud::ObjectSlot::logical("store-v1/probes/lost".to_string()).unwrap();
        let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
        let lost_ref = crate::storage::ExactObjectRef::new(
            lost_slot.clone(),
            lost_payload.len() as u64,
            ObjectHash::digest(&lost_payload),
        );
        let transcript = ExactSlotProbeTranscript {
            probe_id,
            logical_key: slot.logical_key().to_string(),
            slot,
            contenders: [
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&first),
                    outcome: ProbeCreateOutcome::Created,
                },
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&second),
                    outcome: ProbeCreateOutcome::RejectedOccupied,
                },
            ],
            accepted: accepted.clone(),
            full_read_hash: accepted.stored_hash(),
            range: ProbeRangeReceipt {
                start: PROBE_RANGE_START,
                end: PROBE_RANGE_END,
                bytes_hash: ObjectHash::digest(
                    &first[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize],
                ),
            },
            delete_verified_absent: true,
            lost_response: LostResponseProbeReceipt {
                logical_key: "store-v1/probes/lost".to_string(),
                slot: lost_slot,
                payload_hash: ObjectHash::digest(&lost_payload),
                settled: lost_ref,
                readback_hash: ObjectHash::digest(&lost_payload),
                delete_verified_absent: true,
            },
        };
        ExactSlotProbeReceipt::from_transcript(
            transcript,
            &test_store_binding(),
            &test_device_binding(),
        )
    }
}
