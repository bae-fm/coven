//! Cross-principal provider probe execution: reserving, creating, and
//! settling exact probe slots on the primary and peer provider storage, over
//! the probe transcript model in [`coven_protocol::provider`].

use std::sync::Arc;

use crate::cloud::{
    CloudHomeError, ConditionalWriteOutcome, ExactCloudHome, ExactCreateOutcome, ExactSlotStorage,
    ExactUpload,
};
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ExactObjectRef, ObjectSlot, StorageError};
use coven_protocol::provider::*;
use coven_protocol::provider::{
    advance_cross_completion, advance_exact, cross_challenge_hash, cross_response_hash, invalid,
    validate_cross_provider_evidence, validate_cross_provider_evidence_context,
};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::StoreProviderBinding;

mod exact_slots;

pub struct ProviderProbeStorage {
    storage: Arc<dyn ExactCloudHome>,
}

impl ProviderProbeStorage {
    pub fn new(storage: Arc<dyn ExactCloudHome>) -> Self {
        Self { storage }
    }

    pub async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: ProviderProbeId,
    ) -> Result<ObjectSlot, ProviderProbeError> {
        let logical = cross_peer_logical_key(probe_id);
        let slot = self
            .storage
            .allocate_slot(&logical)
            .await
            .map_err(StorageError::from)?;
        if slot.logical_key() != logical {
            return invalid("cross-principal response slot changed its logical key");
        }
        Ok(slot)
    }

    pub async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn DeviceJoinChallengePublicationJournal,
        probe_id: ProviderProbeId,
        store: &StoreProviderBinding,
        context: &CrossPrincipalChallengeContext,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<CrossPrincipalProbeChallenge, ProviderProbeError> {
        let administrator_live = self
            .storage
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
        let administrator_slot = self
            .storage
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
        let conditional_slot = self
            .storage
            .allocate_slot(&cross_conditional_logical_key(probe_id))
            .await
            .map_err(StorageError::from)?;
        let unsigned = CrossPrincipalProbeChallenge {
            probe_id,
            administrator_object,
            conditional_slot,
            challenge_hash: ObjectHash::digest(&[]),
            administrator_signature: String::new(),
        };
        let challenge_hash = cross_challenge_hash(store, context, &unsigned);
        let challenge = CrossPrincipalProbeChallenge {
            challenge_hash,
            administrator_signature: hex::encode(
                administrator_signer.sign(challenge_hash.as_bytes()),
            ),
            ..unsigned
        };
        challenge.verify(context, store, &administrator_signer.public_key_hex())?;
        let durable = publication_journal.prepare(&challenge).await?;
        if durable.challenge != challenge {
            return invalid("durable cross-principal challenge differs from its prepared bytes");
        }
        Ok(challenge)
    }

    pub async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn DeviceJoinChallengePublicationJournal,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
        context: &CrossPrincipalChallengeContext,
        store: &StoreProviderBinding,
    ) -> Result<CrossPrincipalProbeChallenge, ProviderProbeError> {
        let live = self
            .storage
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
        self.settle_exact_create(&challenge.administrator_object.slot, &payload)
            .await?;
        let observed = self
            .storage
            .read_at(&challenge.administrator_object.slot)
            .await
            .map_err(StorageError::from)?;
        if observed != payload {
            return invalid("published cross-principal challenge differs from its signed bytes");
        }
        let initial = probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalInitial);
        let current = match self
            .storage
            .read_versioned_at(&challenge.conditional_slot)
            .await
        {
            Ok(current) => current,
            Err(CloudHomeError::NotFound(_)) => {
                create_versioned_bytes(
                    self.storage.as_ref(),
                    &challenge.conditional_slot,
                    &initial,
                )
                .await?;
                self.storage
                    .read_versioned_at(&challenge.conditional_slot)
                    .await
                    .map_err(StorageError::from)?
            }
            Err(error) => return Err(StorageError::from(error).into()),
        };
        let peer = probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalFirst);
        if current.bytes != initial && current.bytes != peer {
            return invalid("published cross-principal conditional slot contains unknown bytes");
        }
        Ok(challenge.clone())
    }

    pub async fn create_cross_principal_response(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &UserKeypair,
    ) -> Result<CrossPrincipalProbeResponse, ProviderProbeError> {
        challenge.verify(&context.challenge, store, administrator_signing_pubkey)?;
        let peer_pubkey = coven_keys::keys::public_key_hex(peer_signer);
        if context.challenge.member_pubkey != peer_pubkey {
            return invalid("cross-principal peer signer is not the joining member");
        }
        let live = self
            .storage
            .provider_binding()
            .await
            .map_err(StorageError::from)?;
        if live.store != *store || live.device != context.challenge.peer_binding {
            return invalid("cross-principal peer does not match the response context");
        }
        let evidence = self
            .storage
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
        let administrator_read = self
            .storage
            .read_at(&challenge.administrator_object.slot)
            .await
            .map_err(StorageError::from)?;
        if administrator_read != administrator_payload {
            return invalid("peer read bytes differ from the signed cross-principal challenge");
        }
        let conditional_start = match self.storage.read_at(&context.response_slot).await {
            Ok(bytes) => {
                let start: CrossPrincipalConditionalStart =
                    serde_json::from_slice(&bytes).map_err(StorageError::from)?;
                if start.challenge_hash != challenge.challenge_hash
                    || start.canonical_bytes() != bytes
                {
                    return invalid(
                        "durable peer observation differs from its challenge or canonical bytes",
                    );
                }
                start
            }
            Err(CloudHomeError::NotFound(_)) => {
                let current = self
                    .storage
                    .read_versioned_at(&challenge.conditional_slot)
                    .await
                    .map_err(StorageError::from)?;
                let initial =
                    probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalInitial);
                if current.bytes != initial {
                    return invalid("peer conditional observation does not start at the administrator's initial bytes");
                }
                let start = CrossPrincipalConditionalStart {
                    challenge_hash: challenge.challenge_hash,
                    version: current.version,
                };
                self.settle_exact_create(&context.response_slot, &start.canonical_bytes())
                    .await?;
                start
            }
            Err(error) => return Err(StorageError::from(error).into()),
        };
        let peer_payload = conditional_start.canonical_bytes();
        let peer_read = self
            .storage
            .read_at(&context.response_slot)
            .await
            .map_err(StorageError::from)?;
        if peer_read != peer_payload {
            return invalid("peer response readback differs from its prepared observation");
        }
        let replacement = probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalFirst);
        // Retry the same expected revision. A lost successful response settles
        // through exact readback; it never authorizes a write against a new revision.
        self.storage
            .replace_at_if_version(
                &challenge.conditional_slot,
                &conditional_start.version,
                replacement.clone(),
            )
            .await
            .map_err(StorageError::from)?;
        let accepted = self
            .storage
            .read_versioned_at(&challenge.conditional_slot)
            .await
            .map_err(StorageError::from)?;
        if accepted.bytes != replacement || accepted.version == conditional_start.version {
            return invalid("peer conditional readback does not establish its replacement");
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
            conditional_start,
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

    pub async fn complete_cross_principal_probe(
        &self,
        journal: &dyn ProviderProbeJournal,
        challenge: &CrossPrincipalProbeChallenge,
        response: &CrossPrincipalProbeResponse,
        context: &CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
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
        let live = self
            .storage
            .provider_binding()
            .await
            .map_err(StorageError::from)?;
        if live.store != *store || live.device != context.challenge.administrator_binding {
            return invalid("cross-principal administrator does not match the completion context");
        }
        let prepared =
            ProviderProbeJournalRecord::CrossPrincipal(CrossPrincipalCompletionJournal {
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
        let peer_payload = response.conditional_start.canonical_bytes();
        if matches!(record.progress, CrossPrincipalCompletionProgress::Prepared) {
            let observed = self
                .storage
                .read_at(&response.peer_object.slot)
                .await
                .map_err(StorageError::from)?;
            if observed != peer_payload {
                return invalid("administrator read differs from the signed peer response");
            }
            let peer_replacement =
                probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalFirst);
            let administrator_replacement =
                probe_payload(&challenge.probe_id, ProbePayloadLabel::ConditionalSecond);
            let before = self
                .storage
                .read_versioned_at(&challenge.conditional_slot)
                .await
                .map_err(StorageError::from)?;
            if before.bytes != peer_replacement
                || before.version == response.conditional_start.version
            {
                return invalid(
                    "administrator does not observe the peer's conditional replacement",
                );
            }
            let rejected = self
                .storage
                .replace_at_if_version(
                    &challenge.conditional_slot,
                    &response.conditional_start.version,
                    administrator_replacement.clone(),
                )
                .await
                .map_err(StorageError::from)?;
            if rejected != ConditionalWriteOutcome::VersionChanged {
                return invalid("administrator replaced a revision already consumed by the peer");
            }
            let after = self
                .storage
                .read_versioned_at(&challenge.conditional_slot)
                .await
                .map_err(StorageError::from)?;
            if after != before {
                return invalid("rejected administrator update changed the exact peer readback");
            }
            let conditional = ConditionalUpdateProbeReceipt {
                logical_key: cross_conditional_logical_key(challenge.probe_id),
                slot: challenge.conditional_slot.clone(),
                starting_payload_hash: ObjectHash::digest(&probe_payload(
                    &challenge.probe_id,
                    ProbePayloadLabel::ConditionalInitial,
                )),
                contenders: [
                    ProbeConditionalAttempt {
                        payload_hash: ObjectHash::digest(&peer_replacement),
                        outcome: ProbeConditionalOutcome::Replaced,
                    },
                    ProbeConditionalAttempt {
                        payload_hash: ObjectHash::digest(&administrator_replacement),
                        outcome: ProbeConditionalOutcome::RejectedRevision,
                    },
                ],
                accepted_payload_hash: ObjectHash::digest(&after.bytes),
            };
            advance_cross_completion(
                journal,
                &mut durable,
                &mut record,
                CrossPrincipalCompletionProgress::ReadsVerified {
                    administrator_read_peer_hash: ObjectHash::digest(&observed),
                    conditional,
                },
            )
            .await?;
        }
        let (administrator_read_peer_hash, conditional) =
            cross_completion_evidence(&record.progress)?;
        let conditional = conditional.clone();
        if matches!(
            record.progress,
            CrossPrincipalCompletionProgress::ReadsVerified { .. }
        ) {
            delete_versioned_probe(self.storage.as_ref(), &challenge.conditional_slot).await?;
            self.storage
                .delete_and_verify_absent(&response.peer_object.slot)
                .await
                .map_err(StorageError::from)?;
            advance_cross_completion(
                journal,
                &mut durable,
                &mut record,
                CrossPrincipalCompletionProgress::ResponseObjectsAbsent {
                    administrator_read_peer_hash,
                    conditional: conditional.clone(),
                },
            )
            .await?;
        }
        if matches!(
            record.progress,
            CrossPrincipalCompletionProgress::ResponseObjectsAbsent { .. }
        ) {
            self.storage
                .delete_and_verify_absent(&challenge.administrator_object.slot)
                .await
                .map_err(StorageError::from)?;
            advance_cross_completion(
                journal,
                &mut durable,
                &mut record,
                CrossPrincipalCompletionProgress::Absent {
                    administrator_read_peer_hash,
                    conditional: conditional.clone(),
                },
            )
            .await?;
        }
        let transcript = CrossPrincipalProbeTranscript {
            challenge: challenge.clone(),
            response: response.clone(),
            administrator_read_peer_hash,
            conditional,
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

    async fn settle_exact_create(
        &self,
        slot: &ObjectSlot,
        payload: &[u8],
    ) -> Result<(), ProviderProbeError> {
        match self.storage.read_at(slot).await {
            Ok(bytes) if bytes == payload => Ok(()),
            Ok(_) => invalid("durable provider probe slot contains different bytes"),
            Err(CloudHomeError::NotFound(_)) => {
                create_exact_bytes(self.storage.as_ref(), slot, payload)
                    .await
                    .map(drop)
                    .map_err(StorageError::from)
                    .map_err(ProviderProbeError::Storage)
            }
            Err(error) => Err(ProviderProbeError::Storage(StorageError::from(error))),
        }
    }
}

fn cross_completion_evidence(
    progress: &CrossPrincipalCompletionProgress,
) -> Result<(ObjectHash, &ConditionalUpdateProbeReceipt), ProviderProbeError> {
    match progress {
        CrossPrincipalCompletionProgress::ReadsVerified {
            administrator_read_peer_hash,
            conditional,
        }
        | CrossPrincipalCompletionProgress::ResponseObjectsAbsent {
            administrator_read_peer_hash,
            conditional,
        }
        | CrossPrincipalCompletionProgress::Absent {
            administrator_read_peer_hash,
            conditional,
        } => Ok((*administrator_read_peer_hash, conditional)),
        CrossPrincipalCompletionProgress::Prepared
        | CrossPrincipalCompletionProgress::ReceiptReady { .. } => {
            invalid("cross-principal completion has no durable administrator read")
        }
    }
}

async fn create_exact_bytes(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
    bytes: &[u8],
) -> Result<ExactCreateOutcome, CloudHomeError> {
    let object = ExactObjectRef::new(slot.clone(), bytes.len() as u64, ObjectHash::digest(bytes));
    let upload = ExactUpload::from_bytes(&object, bytes).map_err(CloudHomeError::from)?;
    storage
        .create_at(
            &upload,
            &crate::cloud::UploadControl::running(crate::cloud::no_progress()),
        )
        .await
}

async fn create_versioned_bytes(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
    bytes: &[u8],
) -> Result<(), ProviderProbeError> {
    let object = ExactObjectRef::new(slot.clone(), bytes.len() as u64, ObjectHash::digest(bytes));
    let upload = ExactUpload::from_bytes(&object, bytes)?;
    storage
        .create_versioned_at(
            &upload,
            &crate::cloud::UploadControl::running(crate::cloud::no_progress()),
        )
        .await
        .map(drop)
        .map_err(StorageError::from)
        .map_err(ProviderProbeError::Storage)
}

async fn delete_versioned_probe(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
) -> Result<(), ProviderProbeError> {
    storage
        .delete_versioned_at(slot)
        .await
        .map_err(StorageError::from)?;
    match storage.read_versioned_at(slot).await {
        Err(CloudHomeError::NotFound(_)) => Ok(()),
        Ok(_) => invalid("conditional-update probe record remains after deletion"),
        Err(error) => Err(StorageError::from(error).into()),
    }
}

#[cfg(test)]
#[path = "provider_probe_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "provider_probe_s3_tests.rs"]
mod s3_tests;
