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
        let unsigned = CrossPrincipalProbeChallenge {
            probe_id,
            administrator_object,
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
        let peer_payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossPeer);
        self.settle_exact_create(&context.response_slot, &peer_payload)
            .await?;
        let peer_read = self
            .storage
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
        let peer_payload = probe_payload(&challenge.probe_id, ProbePayloadLabel::CrossPeer);
        if matches!(record.progress, CrossPrincipalCompletionProgress::Prepared) {
            let observed = self
                .storage
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
            self.storage
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
                },
            )
            .await?;
        }
        let transcript = CrossPrincipalProbeTranscript {
            challenge: challenge.clone(),
            response: response.clone(),
            administrator_read_peer_hash,
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

    pub async fn probe_exact_slots(
        &self,
        journal: &dyn ProviderProbeJournal,
        probe_id: ProviderProbeId,
        binding: &coven_protocol::objects::ResolvedProviderBinding,
    ) -> Result<ExactSlotProbeReceipt, ProviderProbeError> {
        let first = self.storage.as_ref();
        let second = self.storage.as_ref();
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
        let conditional_logical_key = format!("__coven_probe__/conditional/{id}");
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
                let allocated_conditional_slot = first
                    .allocate_slot(&conditional_logical_key)
                    .await
                    .map_err(StorageError::from)?;
                journal
                    .begin(ProviderProbeJournalRecord::Exact(ExactProbeJournal {
                        probe_id,
                        binding: binding.clone(),
                        slot: allocated_slot,
                        conditional_slot: allocated_conditional_slot,
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
        let conditional_slot = record.conditional_slot.clone();
        let lost_slot = record.lost_response_slot.clone();
        if slot.logical_key() != logical_key
            || conditional_slot.logical_key() != conditional_logical_key
            || lost_slot.logical_key() != lost_logical_key
        {
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
                        create_exact_bytes(first, &slot, &payloads[0]),
                        create_exact_bytes(second, &slot, &payloads[1]),
                    );
                    classify_exact_create_race(left, right)?
                }
                Ok(bytes) if bytes == payloads[0] => {
                    require_occupied_rejection(
                        create_exact_bytes(second, &slot, &payloads[1]).await,
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
                        create_exact_bytes(first, &slot, &payloads[0]).await,
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
        let accepted =
            ExactObjectRef::new(slot.clone(), full.len() as u64, ObjectHash::digest(&full));
        if matches!(record.progress, ExactProbeProgress::ReadsVerified { .. }) {
            let initial = probe_payload(&probe_id, ProbePayloadLabel::ConditionalInitial);
            let conditional_payloads = [
                probe_payload(&probe_id, ProbePayloadLabel::ConditionalFirst),
                probe_payload(&probe_id, ProbePayloadLabel::ConditionalSecond),
            ];
            let mut current = match first.read_versioned_at(&conditional_slot).await {
                Ok(current) => current,
                Err(CloudHomeError::NotFound(_)) => {
                    create_versioned_bytes(first, &conditional_slot, &initial).await?;
                    first
                        .read_versioned_at(&conditional_slot)
                        .await
                        .map_err(StorageError::from)?
                }
                Err(error) => return Err(ProviderProbeError::Storage(StorageError::from(error))),
            };
            if current.bytes != initial
                && current.bytes != conditional_payloads[0]
                && current.bytes != conditional_payloads[1]
            {
                return invalid("conditional-update probe slot contains unknown bytes");
            }
            let starting_payload_hash = ObjectHash::digest(&current.bytes);
            let expected = current.version.clone();
            let (left, right) = tokio::join!(
                first.replace_at_if_version(
                    &conditional_slot,
                    &expected,
                    conditional_payloads[0].clone(),
                ),
                second.replace_at_if_version(
                    &conditional_slot,
                    &expected,
                    conditional_payloads[1].clone(),
                ),
            );
            let (conditional_outcomes, conditional_winner) =
                classify_conditional_update_race(left, right)?;
            current = first
                .read_versioned_at(&conditional_slot)
                .await
                .map_err(StorageError::from)?;
            if current.bytes != conditional_payloads[conditional_winner] {
                return invalid("conditional-update readback does not match its winning write");
            }
            let conditional = ConditionalUpdateProbeReceipt {
                logical_key: conditional_logical_key.clone(),
                slot: conditional_slot.clone(),
                starting_payload_hash,
                contenders: [
                    ProbeConditionalAttempt {
                        payload_hash: ObjectHash::digest(&conditional_payloads[0]),
                        outcome: conditional_outcomes[0],
                    },
                    ProbeConditionalAttempt {
                        payload_hash: ObjectHash::digest(&conditional_payloads[1]),
                        outcome: conditional_outcomes[1],
                    },
                ],
                accepted_payload_hash: ObjectHash::digest(&current.bytes),
            };
            advance_exact(
                journal,
                &mut durable,
                &mut record,
                ExactProbeProgress::ConditionalVerified {
                    outcomes,
                    conditional,
                },
            )
            .await?;
        }
        let conditional = exact_conditional_evidence(&record.progress)?.clone();
        if matches!(
            record.progress,
            ExactProbeProgress::ConditionalVerified { .. }
        ) {
            first
                .delete_and_verify_absent(&slot)
                .await
                .map_err(StorageError::from)?;
            first
                .delete_versioned_at(&conditional_slot)
                .await
                .map_err(StorageError::from)?;
            match first.read_versioned_at(&conditional_slot).await {
                Err(CloudHomeError::NotFound(_)) => {}
                Ok(_) => return invalid("conditional-update probe record remains after deletion"),
                Err(error) => return Err(ProviderProbeError::Storage(StorageError::from(error))),
            }
            advance_exact(
                journal,
                &mut durable,
                &mut record,
                ExactProbeProgress::PrimaryAbsent {
                    outcomes,
                    conditional: conditional.clone(),
                },
            )
            .await?;
        }
        let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
        if matches!(record.progress, ExactProbeProgress::PrimaryAbsent { .. }) {
            match first.read_at(&lost_slot).await {
                Ok(bytes) if bytes == lost_payload => {}
                Ok(_) => return invalid("lost-response slot contains unknown bytes"),
                Err(CloudHomeError::NotFound(_)) => {
                    create_exact_bytes(first, &lost_slot, &lost_payload)
                        .await
                        .map_err(StorageError::from)?;
                }
                Err(error) => return Err(ProviderProbeError::Storage(StorageError::from(error))),
            }
            advance_exact(
                journal,
                &mut durable,
                &mut record,
                ExactProbeProgress::LostResponseCreated {
                    outcomes,
                    conditional: conditional.clone(),
                },
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
                return invalid(
                    "lost-response authoritative readback differs from committed bytes",
                );
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
                ExactProbeProgress::LostResponseReadVerified {
                    outcomes,
                    conditional: conditional.clone(),
                },
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
                ExactProbeProgress::Absent {
                    outcomes,
                    conditional: conditional.clone(),
                },
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
            conditional,
            lost_response: LostResponseProbeReceipt {
                logical_key: lost_logical_key,
                slot: lost_slot,
                payload_hash: ObjectHash::digest(&lost_payload),
                settled,
                readback_hash: ObjectHash::digest(&lost_readback),
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

fn exact_race_state(
    progress: &ExactProbeProgress,
) -> Result<([ProbeCreateOutcome; 2], usize), ProviderProbeError> {
    let (outcomes, winner) = match progress {
        ExactProbeProgress::Prepared => return invalid("exact probe has no durable create result"),
        ExactProbeProgress::Created { outcomes }
        | ExactProbeProgress::ReadsVerified { outcomes }
        | ExactProbeProgress::ConditionalVerified { outcomes, .. }
        | ExactProbeProgress::PrimaryAbsent { outcomes, .. }
        | ExactProbeProgress::LostResponseCreated { outcomes, .. }
        | ExactProbeProgress::LostResponseReadVerified { outcomes, .. }
        | ExactProbeProgress::Absent { outcomes, .. } => {
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

fn exact_conditional_evidence(
    progress: &ExactProbeProgress,
) -> Result<&ConditionalUpdateProbeReceipt, ProviderProbeError> {
    match progress {
        ExactProbeProgress::ConditionalVerified { conditional, .. }
        | ExactProbeProgress::PrimaryAbsent { conditional, .. }
        | ExactProbeProgress::LostResponseCreated { conditional, .. }
        | ExactProbeProgress::LostResponseReadVerified { conditional, .. }
        | ExactProbeProgress::Absent { conditional, .. } => Ok(conditional),
        ExactProbeProgress::ReceiptReady { receipt } => Ok(&receipt.transcript.conditional),
        ExactProbeProgress::Prepared
        | ExactProbeProgress::Created { .. }
        | ExactProbeProgress::ReadsVerified { .. } => {
            invalid("exact probe has no conditional-update evidence")
        }
    }
}

fn classify_conditional_update_race(
    left: Result<ConditionalWriteOutcome, CloudHomeError>,
    right: Result<ConditionalWriteOutcome, CloudHomeError>,
) -> Result<([ProbeConditionalOutcome; 2], usize), ProviderProbeError> {
    match (left, right) {
        (
            Ok(ConditionalWriteOutcome::Replaced(_)),
            Ok(ConditionalWriteOutcome::VersionChanged),
        ) => Ok((
            [
                ProbeConditionalOutcome::Replaced,
                ProbeConditionalOutcome::RejectedRevision,
            ],
            0,
        )),
        (
            Ok(ConditionalWriteOutcome::VersionChanged),
            Ok(ConditionalWriteOutcome::Replaced(_)),
        ) => Ok((
            [
                ProbeConditionalOutcome::RejectedRevision,
                ProbeConditionalOutcome::Replaced,
            ],
            1,
        )),
        (left, right) => invalid(&format!(
            "conditional-update race did not produce one replacement and one revision rejection: left={left:?}, right={right:?}"
        )),
    }
}

fn classify_exact_create_race(
    left: Result<ExactCreateOutcome, CloudHomeError>,
    right: Result<ExactCreateOutcome, CloudHomeError>,
) -> Result<([ProbeCreateOutcome; 2], usize), ProviderProbeError> {
    match (left, right) {
        (
            Ok(ExactCreateOutcome::Created),
            Err(CloudHomeError::SlotCollision(_) | CloudHomeError::AlreadyExists(_)),
        ) => Ok((
            [
                ProbeCreateOutcome::Created,
                ProbeCreateOutcome::RejectedOccupied,
            ],
            0,
        )),
        (
            Err(CloudHomeError::SlotCollision(_) | CloudHomeError::AlreadyExists(_)),
            Ok(ExactCreateOutcome::Created),
        ) => Ok((
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
    result: Result<ExactCreateOutcome, CloudHomeError>,
) -> Result<(), ProviderProbeError> {
    match result {
        Err(CloudHomeError::SlotCollision(_) | CloudHomeError::AlreadyExists(_)) => Ok(()),
        Ok(ExactCreateOutcome::Created) => {
            invalid("settled exact probe contender unexpectedly created a second object")
        }
        result => invalid(&format!(
            "settled exact probe contender was not rejected as occupied: result={result:?}"
        )),
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
