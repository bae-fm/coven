use super::*;
use crate::store_ack_records::{
    load_expected_outbound_store_ack_on, set_outbound_store_ack_activation_on,
};

impl StoreDatabase {
    pub async fn prepare_acknowledgement_activation(
        &self,
        expected: StoreAckRef,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<(), DbError> {
        let store_dir = self.store_dir.clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_expected_outbound_store_ack_on(
                    &tx,
                    &expected,
                    "prepared activation names a different Store acknowledgement",
                )?;
                match outbound.activation {
                    OutboundStoreAckActivation::AwaitingCandidate => {}
                    OutboundStoreAckActivation::Prepared(existing)
                        if existing.reference == candidate.reference =>
                    {
                        return Ok(())
                    }
                    OutboundStoreAckActivation::Prepared(_)
                    | OutboundStoreAckActivation::Nonactivating(_) => {
                        return Err(DbError::Message(
                            "Store acknowledgement already has a different activation candidate"
                                .to_string(),
                        ));
                    }
                }
                for remote in candidate
                    .acknowledgement_remote_objects(&outbound.ack)
                    .map_err(|error| DbError::Message(error.to_string()))?
                {
                    persist_exact_remote_object_on(
                        &tx,
                        &store_dir,
                        &remote,
                        "Merge Store acknowledgement activation object",
                    )?;
                }
                for circle in &outbound.circle_acknowledgements {
                    for remote in candidate
                        .circle_acknowledgement_remote_objects(&circle.ack)
                        .map_err(|error| DbError::Message(error.to_string()))?
                    {
                        persist_exact_remote_object_on(
                            &tx,
                            &store_dir,
                            &remote,
                            "Merge Circle acknowledgement activation object",
                        )?;
                    }
                }
                set_outbound_store_ack_activation_on(
                    &tx,
                    &expected,
                    &OutboundStoreAckActivation::Prepared(candidate),
                    "outbound Store acknowledgement disappeared during activation preparation",
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn begin_acknowledgement_nonactivation(
        &self,
        expected: StoreAckRef,
        nonactivation: VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let verified_candidate = nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if !matches!(
            nonactivation.proof(),
            CandidateNonactivationProof::MergeWinner { .. }
        ) {
            return Err(DbError::Message(
                "Merge acknowledgement requires a Merge-winner nonactivation proof".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_expected_outbound_store_ack_on(
                    &tx,
                    &expected,
                    "nonactivation names another Store acknowledgement",
                )?;
                let (candidate, already_nonactivating) = match outbound.activation {
                    OutboundStoreAckActivation::Prepared(candidate) => (candidate, false),
                    OutboundStoreAckActivation::Nonactivating(candidate) => (candidate, true),
                    OutboundStoreAckActivation::AwaitingCandidate => {
                        return Err(DbError::Message(
                            "Store acknowledgement has no prepared Merge activation candidate"
                                .to_string(),
                        ));
                    }
                };
                if candidate.reference != verified_candidate {
                    return Err(DbError::Message(
                        "verified nonactivation names another Store acknowledgement candidate"
                            .to_string(),
                    ));
                }
                if nonactivation.candidate().canonical_signed_bytes != candidate.commit.to_bytes()
                {
                    return Err(DbError::Message(
                        "verified nonactivation bytes differ from the Store acknowledgement candidate"
                            .to_string(),
                    ));
                }
                let head = candidate.head_ref();
                if already_nonactivating {
                    let commit = load_remote_object_on(
                        &tx,
                        remote_object_id(&candidate.reference.object),
                    )?;
                    if commit
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(nonactivation.proof())
                    {
                        return Err(DbError::Message(
                            "nonactivating Merge acknowledgement commit carries a different durable proof"
                                .to_string(),
                        ));
                    }
                    let inert = load_protocol_inert_object_on(
                        &tx,
                        remote_object_id(&outbound.reference.object),
                    )?;
                    if inert
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(nonactivation.proof())
                    {
                        return Err(DbError::Message(
                            "nonactivating Merge acknowledgement carries a different durable proof"
                                .to_string(),
                        ));
                    }
                    let head_remote = load_remote_object_on(&tx, remote_object_id(&head.object))?;
                    if head_remote
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(nonactivation.proof())
                    {
                        return Err(DbError::Message(
                            "nonactivating Merge acknowledgement head carries a different durable proof"
                                .to_string(),
                        ));
                    }
                    return Ok(());
                }
                if begin_remote_candidate_nonactivation_on(
                    &tx,
                    remote_object_id(&outbound.reference.object),
                    nonactivation.clone(),
                )?
                .is_some()
                {
                    return Err(DbError::Message(
                        "Store acknowledgement became an exact cleanup target".to_string(),
                    ));
                }
                if begin_remote_candidate_nonactivation_on(
                    &tx,
                    remote_object_id(&head.object),
                    nonactivation.clone(),
                )?
                .is_some()
                {
                    return Err(DbError::Message(
                        "Store activation head became an exact cleanup target".to_string(),
                    ));
                }
                if begin_remote_candidate_nonactivation_on(
                    &tx,
                    remote_object_id(&candidate.reference.object),
                    nonactivation,
                )?
                .is_none()
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement commit has no exact cleanup target"
                            .to_string(),
                    ));
                }
                set_outbound_store_ack_activation_on(
                    &tx,
                    &expected,
                    &OutboundStoreAckActivation::Nonactivating(candidate),
                    "outbound Store acknowledgement disappeared during nonactivation",
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn adopt_acknowledgement_head(
        &self,
        expected: StoreAckRef,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        let store_dir = self.store_dir.clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_expected_outbound_store_ack_on(
                    &tx,
                    &expected,
                    "alternate Merge head names another Store acknowledgement",
                )?;
                let OutboundStoreAckActivation::Prepared(candidate) = outbound.activation else {
                    return Err(DbError::Message(
                        "Store acknowledgement has no prepared Merge candidate".to_string(),
                    ));
                };
                let current = candidate.head_ref();
                let root = required_store_root_authority_on(&tx)?;
                let registration = load_activated_registration_on(
                    &tx,
                    &root,
                    &candidate.commit.author_registration,
                )?;
                let verified = StoreDeviceHead::parse_at(
                    &winner.to_bytes(),
                    root.store_root_hash,
                    &registration,
                    &candidate.reference,
                )
                .map_err(|error| DbError::context("verify alternate Merge head", error))?;
                if verified != winner {
                    return Err(DbError::Message(
                        "alternate Merge head changed during exact verification".to_string(),
                    ));
                }
                replace_prepared_merge_head_remote_on(
                    &tx,
                    &store_dir,
                    &current.object,
                    &winner,
                    winner_prepared.reference(),
                    &candidate.reference,
                )?;
                let mut candidate = candidate;
                candidate
                    .adopt_merge_head(winner, winner_prepared.reference().clone())
                    .map_err(|error| DbError::Message(error.to_string()))?;
                set_outbound_store_ack_activation_on(
                    &tx,
                    &expected,
                    &OutboundStoreAckActivation::Prepared(candidate),
                    "outbound Store acknowledgement disappeared during head adoption",
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn acknowledgement_cleanup_target(
        &self,
        expected: StoreAckRef,
    ) -> Result<Option<CandidateCleanupObject>, DbError> {
        self.connection
            .call(move |conn| {
                let outbound = load_expected_outbound_store_ack_on(
                    conn,
                    &expected,
                    "Store acknowledgement cleanup names another exact object",
                )?;
                let OutboundStoreAckActivation::Nonactivating(candidate) = outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Merge".to_string(),
                    ));
                };
                Ok(super::candidate_records::candidate_cleanup_targets_on(
                    conn,
                    &candidate.reference,
                    std::slice::from_ref(&candidate.reference.object),
                )?
                .into_iter()
                .next())
            })
            .await
    }

    pub async fn complete_nonactivating_acknowledgement(
        &self,
        expected: StoreAckRef,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_expected_outbound_store_ack_on(
                    &tx,
                    &expected,
                    "Store acknowledgement completion names another exact object",
                )?;
                let OutboundStoreAckActivation::Nonactivating(candidate) = &outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Merge".to_string(),
                    ));
                };
                let head = candidate.head_ref();
                if !super::candidate_records::candidate_cleanup_targets_on(
                    &tx,
                    &candidate.reference,
                    &[candidate.reference.object.clone(), head.object.clone()],
                )?
                .is_empty()
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement cleanup is incomplete".to_string(),
                    ));
                }
                let commit_id = remote_object_id(&candidate.reference.object);
                let commit = load_remote_object_on(&tx, commit_id)?;
                let proof = commit
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    .ok_or_else(|| {
                        DbError::Message(
                            "losing Store acknowledgement commit lacks its proof".to_string(),
                        )
                    })?;
                if !matches!(proof, CandidateNonactivationProof::MergeWinner { .. }) {
                    return Err(DbError::Message(
                        "nonactivating Merge acknowledgement carries another proof".to_string(),
                    ));
                }
                let head_id = remote_object_id(&head.object);
                let head_remote = load_remote_object_on(&tx, head_id)?;
                if head_remote
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(proof)
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement head carries a different proof".to_string(),
                    ));
                }
                let inert = load_protocol_inert_object_on(
                    &tx,
                    remote_object_id(&outbound.reference.object),
                )?;
                if inert
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(proof)
                {
                    return Err(DbError::Message(
                        "protocol-inert acknowledgement lacks its candidate proof".to_string(),
                    ));
                }
                super::candidate_records::delete_remote_objects_on(
                    &tx,
                    [commit_id, head_id],
                    "nonactivating acknowledgement",
                )?;
                finish_outbound_store_ack_on(
                    &tx,
                    &expected,
                    &outbound.ack.value.successor.next_slot,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}
