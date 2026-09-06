use super::*;
pub(super) use crate::store_commit::domain_json;

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
pub enum ProbePayloadLabel {
    ExactCreateFirst,
    ExactCreateSecond,
    ConditionalInitial,
    ConditionalFirst,
    ConditionalSecond,
    LostResponse,
    CrossAdministrator,
    CrossPeer,
}

impl ProbePayloadLabel {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::ExactCreateFirst => b"exact-create-first",
            Self::ExactCreateSecond => b"exact-create-second",
            Self::ConditionalInitial => b"conditional-initial",
            Self::ConditionalFirst => b"conditional-first",
            Self::ConditionalSecond => b"conditional-second",
            Self::LostResponse => b"lost-response",
            Self::CrossAdministrator => b"cross-administrator",
            Self::CrossPeer => b"cross-peer",
        }
    }
}

pub fn probe_payload(probe_id: &ProviderProbeId, label: ProbePayloadLabel) -> Vec<u8> {
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
        {
            return invalid("exact-slot read, range, reference, or deletion evidence is invalid");
        }
        t.conditional.verify(&t.probe_id)?;
        let lost = probe_payload(&t.probe_id, ProbePayloadLabel::LostResponse);
        let lost_hash = ObjectHash::digest(&lost);
        if t.lost_response.logical_key != t.lost_response.slot.logical_key()
            || t.lost_response.settled.slot() != &t.lost_response.slot
            || t.lost_response.payload_hash != lost_hash
            || t.lost_response.settled.stored_size() != lost.len() as u64
            || t.lost_response.settled.stored_hash() != lost_hash
            || t.lost_response.readback_hash != lost_hash
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
    pub conditional: ConditionalUpdateProbeReceipt,
    pub lost_response: LostResponseProbeReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConditionalUpdateProbeReceipt {
    pub logical_key: String,
    pub slot: ObjectSlot,
    pub starting_payload_hash: ObjectHash,
    pub contenders: [ProbeConditionalAttempt; 2],
    pub accepted_payload_hash: ObjectHash,
}

impl ConditionalUpdateProbeReceipt {
    fn verify(&self, probe_id: &ProviderProbeId) -> Result<(), ProviderProbeError> {
        if self.logical_key != self.slot.logical_key() {
            return invalid("conditional-update transcript disagrees with its allocated slot");
        }
        let initial = probe_payload(probe_id, ProbePayloadLabel::ConditionalInitial);
        let payloads = [
            probe_payload(probe_id, ProbePayloadLabel::ConditionalFirst),
            probe_payload(probe_id, ProbePayloadLabel::ConditionalSecond),
        ];
        let allowed_starting_hashes = [
            ObjectHash::digest(&initial),
            ObjectHash::digest(&payloads[0]),
            ObjectHash::digest(&payloads[1]),
        ];
        if !allowed_starting_hashes.contains(&self.starting_payload_hash)
            || self.contenders[0].payload_hash != ObjectHash::digest(&payloads[0])
            || self.contenders[1].payload_hash != ObjectHash::digest(&payloads[1])
        {
            return invalid("conditional-update payload hashes are not deterministic");
        }
        let winners = self
            .contenders
            .iter()
            .enumerate()
            .filter_map(|(index, attempt)| {
                (attempt.outcome == ProbeConditionalOutcome::Replaced).then_some(index)
            })
            .collect::<Vec<_>>();
        let rejected = self
            .contenders
            .iter()
            .filter(|attempt| attempt.outcome == ProbeConditionalOutcome::RejectedRevision)
            .count();
        if winners.len() != 1
            || rejected != 1
            || self.accepted_payload_hash != ObjectHash::digest(&payloads[winners[0]])
        {
            return invalid(
                "conditional-update race must contain one replacement and one revision rejection",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeConditionalAttempt {
    pub payload_hash: ObjectHash,
    pub outcome: ProbeConditionalOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeConditionalOutcome {
    Replaced,
    RejectedRevision,
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

#[derive(Debug, thiserror::Error)]
pub enum ProviderProbeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("provider capability receipt Store protocol: {0}")]
    Protocol(#[from] crate::store_commit::StoreProtocolError),
    #[error("provider capability receipt is invalid: {0}")]
    InvalidReceipt(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderProbeJournalRecord {
    Exact(ExactProbeJournal),
    CrossPrincipal(CrossPrincipalCompletionJournal),
}

impl ProviderProbeJournalRecord {
    pub fn probe_id(&self) -> ProviderProbeId {
        match self {
            Self::Exact(record) => record.probe_id,
            Self::CrossPrincipal(record) => record.probe_id,
        }
    }

    pub fn validate_begin(&self) -> Result<(), ProviderProbeJournalError> {
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

    pub fn validate_transition(&self, next: &Self) -> Result<(), ProviderProbeJournalError> {
        match (self, next) {
            (Self::Exact(previous), Self::Exact(next)) => {
                if previous.probe_id != next.probe_id
                    || previous.binding != next.binding
                    || previous.slot != next.slot
                    || previous.conditional_slot != next.conditional_slot
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
pub enum ProviderProbeJournalError {
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
pub struct ExactProbeJournal {
    pub probe_id: ProviderProbeId,
    pub binding: crate::objects::ResolvedProviderBinding,
    pub slot: ObjectSlot,
    pub conditional_slot: ObjectSlot,
    pub lost_response_slot: ObjectSlot,
    pub progress: ExactProbeProgress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ExactProbeProgress {
    Prepared,
    Created {
        outcomes: [ProbeCreateOutcome; 2],
    },
    ReadsVerified {
        outcomes: [ProbeCreateOutcome; 2],
    },
    ConditionalVerified {
        outcomes: [ProbeCreateOutcome; 2],
        conditional: ConditionalUpdateProbeReceipt,
    },
    PrimaryAbsent {
        outcomes: [ProbeCreateOutcome; 2],
        conditional: ConditionalUpdateProbeReceipt,
    },
    LostResponseCreated {
        outcomes: [ProbeCreateOutcome; 2],
        conditional: ConditionalUpdateProbeReceipt,
    },
    LostResponseReadVerified {
        outcomes: [ProbeCreateOutcome; 2],
        conditional: ConditionalUpdateProbeReceipt,
    },
    Absent {
        outcomes: [ProbeCreateOutcome; 2],
        conditional: ConditionalUpdateProbeReceipt,
    },
    ReceiptReady {
        receipt: ExactSlotProbeReceipt,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalCompletionJournal {
    pub probe_id: ProviderProbeId,
    pub store: StoreProviderBinding,
    pub context: CrossPrincipalResponseContext,
    pub challenge: CrossPrincipalProbeChallenge,
    pub response: CrossPrincipalProbeResponse,
    pub progress: CrossPrincipalCompletionProgress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CrossPrincipalCompletionProgress {
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

pub(super) fn validate_exact_progress_transition(
    previous: &ExactProbeProgress,
    next: &ExactProbeProgress,
) -> Result<(), ProviderProbeJournalError> {
    let evidence_matches = match (previous, next) {
        (ExactProbeProgress::Prepared, ExactProbeProgress::Created { .. }) => true,
        (
            ExactProbeProgress::Created { outcomes: previous },
            ExactProbeProgress::ReadsVerified { outcomes: next },
        ) => previous == next,
        (
            ExactProbeProgress::ReadsVerified { outcomes: previous },
            ExactProbeProgress::ConditionalVerified { outcomes: next, .. },
        ) => previous == next,
        (
            ExactProbeProgress::ConditionalVerified {
                outcomes: previous_outcomes,
                conditional: previous_conditional,
            },
            ExactProbeProgress::PrimaryAbsent {
                outcomes: next_outcomes,
                conditional: next_conditional,
            },
        )
        | (
            ExactProbeProgress::PrimaryAbsent {
                outcomes: previous_outcomes,
                conditional: previous_conditional,
            },
            ExactProbeProgress::LostResponseCreated {
                outcomes: next_outcomes,
                conditional: next_conditional,
            },
        )
        | (
            ExactProbeProgress::LostResponseCreated {
                outcomes: previous_outcomes,
                conditional: previous_conditional,
            },
            ExactProbeProgress::LostResponseReadVerified {
                outcomes: next_outcomes,
                conditional: next_conditional,
            },
        )
        | (
            ExactProbeProgress::LostResponseReadVerified {
                outcomes: previous_outcomes,
                conditional: previous_conditional,
            },
            ExactProbeProgress::Absent {
                outcomes: next_outcomes,
                conditional: next_conditional,
            },
        ) => previous_outcomes == next_outcomes && previous_conditional == next_conditional,
        (
            ExactProbeProgress::Absent {
                outcomes,
                conditional,
            },
            ExactProbeProgress::ReceiptReady { receipt },
        ) => {
            receipt
                .transcript
                .contenders
                .iter()
                .map(|attempt| attempt.outcome)
                .eq(outcomes.iter().copied())
                && receipt.transcript.conditional == *conditional
        }
        _ => return Err(ProviderProbeJournalError::NonAdjacentProgress),
    };
    if !evidence_matches {
        return Err(ProviderProbeJournalError::EvidenceChanged);
    }
    Ok(())
}

pub(super) fn validate_cross_progress_transition(
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

pub(super) fn cross_progress_evidence_hash(
    progress: &CrossPrincipalCompletionProgress,
) -> Option<ObjectHash> {
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
pub trait ProviderProbeJournal: Send + Sync {
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

pub(super) fn validate_probe_exact_object(
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

pub fn invalid<T>(reason: &str) -> Result<T, ProviderProbeError> {
    Err(ProviderProbeError::InvalidReceipt(reason.to_string()))
}

pub(super) fn exact_transcript_hash(
    store: &StoreProviderBinding,
    device: &ProviderDeviceBinding,
    transcript: &ExactSlotProbeTranscript,
) -> ObjectHash {
    ObjectHash::digest(&domain_json(
        EXACT_TRANSCRIPT_DOMAIN,
        &(store, device, transcript),
    ))
}
