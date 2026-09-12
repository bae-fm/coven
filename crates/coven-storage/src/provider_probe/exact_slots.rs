use super::*;

impl ProviderProbeStorage {
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
        let mut durable = match journal.load(probe_id).await? {
            Some(existing) => existing,
            None => {
                let allocated_slot = first
                    .allocate_slot(&logical_key)
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
        if slot.logical_key() != logical_key
            || conditional_slot.logical_key() != conditional_logical_key
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
            delete_versioned_probe(first, &conditional_slot).await?;
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

fn exact_race_state(
    progress: &ExactProbeProgress,
) -> Result<([ProbeCreateOutcome; 2], usize), ProviderProbeError> {
    let (outcomes, winner) = match progress {
        ExactProbeProgress::Prepared => return invalid("exact probe has no durable create result"),
        ExactProbeProgress::Created { outcomes }
        | ExactProbeProgress::ReadsVerified { outcomes }
        | ExactProbeProgress::ConditionalVerified { outcomes, .. }
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

#[cfg(test)]
#[path = "exact_slots_tests.rs"]
mod tests;
