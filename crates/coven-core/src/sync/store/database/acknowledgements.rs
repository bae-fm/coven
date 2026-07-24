use super::*;

impl StoreDatabase {
    pub(in crate::sync::store) async fn prepare_acknowledgement_activation(
        &self,
        expected: StoreAckRef,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "prepared activation names a different Store acknowledgement".to_string(),
                    ));
                }
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
                            &remote,
                            "Merge Circle acknowledgement activation object",
                        )?;
                    }
                }
                let activation =
                    serde_json::to_string(&OutboundStoreAckActivation::Prepared(candidate))
                        .map_err(|error| {
                            DbError::Message(format!(
                                "serialize prepared Merge Store acknowledgement activation: {error}"
                            ))
                        })?;
                let updated = tx
                    .execute(
                        "UPDATE outbound_store_acks SET activation = ?2 \
                         WHERE singleton = 1 AND ack_ref = ?1",
                        rusqlite::params![
                            serde_json::to_string(&expected).map_err(|error| DbError::Message(
                                format!("serialize Store acknowledgement ref: {error}")
                            ))?,
                            activation,
                        ],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "outbound Store acknowledgement disappeared during activation preparation"
                            .to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn begin_acknowledgement_nonactivation(
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
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "nonactivation names another Store acknowledgement".to_string(),
                    ));
                }
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
                let head = StoreDeviceHeadRef {
                    head_hash: candidate.head.head_hash(),
                    object: candidate.prepared_head.reference().clone(),
                };
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
                let activation = serde_json::to_string(
                    &OutboundStoreAckActivation::Nonactivating(candidate),
                )
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize nonactivating Merge Store acknowledgement: {error}"
                    ))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE outbound_store_acks SET activation = ?2
                         WHERE singleton = 1 AND ack_ref = ?1",
                        rusqlite::params![
                            serde_json::to_string(&expected).map_err(|error| DbError::Message(
                                format!("serialize Store acknowledgement ref: {error}")
                            ))?,
                            activation,
                        ],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "outbound Store acknowledgement disappeared during nonactivation"
                            .to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn adopt_acknowledgement_head(
        &self,
        expected: StoreAckRef,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "alternate Merge head names another Store acknowledgement".to_string(),
                    ));
                }
                let OutboundStoreAckActivation::Prepared(candidate) = outbound.activation else {
                    return Err(DbError::Message(
                        "Store acknowledgement has no prepared Merge candidate".to_string(),
                    ));
                };
                let current = StoreDeviceHeadRef {
                    head_hash: candidate.head.head_hash(),
                    object: candidate.prepared_head.reference().clone(),
                };
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
                .map_err(|error| {
                    DbError::Message(format!("verify alternate Merge head: {error}"))
                })?;
                if verified != winner {
                    return Err(DbError::Message(
                        "alternate Merge head changed during exact verification".to_string(),
                    ));
                }
                replace_prepared_merge_head_remote_on(
                    &tx,
                    &current.object,
                    &winner,
                    &winner_prepared,
                    &candidate.reference,
                )?;
                let mut candidate = candidate;
                candidate
                    .adopt_merge_head(winner, winner_prepared)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let activation =
                    serde_json::to_string(&OutboundStoreAckActivation::Prepared(candidate))
                        .map_err(|error| {
                            DbError::Message(format!(
                                "serialize alternate Store acknowledgement activation: {error}"
                            ))
                        })?;
                let updated = tx
                    .execute(
                        "UPDATE outbound_store_acks SET activation = ?2
                         WHERE singleton = 1 AND ack_ref = ?1",
                        rusqlite::params![
                            serde_json::to_string(&expected).map_err(|error| DbError::Message(
                                format!("serialize Store acknowledgement ref: {error}")
                            ))?,
                            activation,
                        ],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "outbound Store acknowledgement disappeared during head adoption"
                            .to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn acknowledgement_cleanup_target(
        &self,
        expected: StoreAckRef,
    ) -> Result<Option<CandidateCleanupObject>, DbError> {
        self.database
            .call(move |conn| {
                let outbound = load_outbound_store_ack_on(conn)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "Store acknowledgement cleanup names another exact object".to_string(),
                    ));
                }
                let OutboundStoreAckActivation::Nonactivating(candidate) = outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Merge".to_string(),
                    ));
                };
                let commit =
                    load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
                if let Some(object) = commit.cleanup_target() {
                    return Ok(Some(CandidateCleanupObject {
                        object: object.clone(),
                    }));
                }
                if !commit
                    .candidate_cleanup_complete(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement commit is not awaiting cleanup".to_string(),
                    ));
                }
                Ok(None)
            })
            .await
    }

    pub(in crate::sync::store) async fn complete_nonactivating_acknowledgement(
        &self,
        expected: StoreAckRef,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "Store acknowledgement completion names another exact object".to_string(),
                    ));
                }
                let OutboundStoreAckActivation::Nonactivating(candidate) = &outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Merge".to_string(),
                    ));
                };
                let commit_id = remote_object_id(&candidate.reference.object);
                let commit = load_remote_object_on(&tx, commit_id)?;
                if !commit
                    .candidate_cleanup_complete(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement commit cleanup is incomplete".to_string(),
                    ));
                }
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
                let head = StoreDeviceHeadRef {
                    head_hash: candidate.head.head_hash(),
                    object: candidate.prepared_head.reference().clone(),
                };
                let head_id = remote_object_id(&head.object);
                let head_remote = load_remote_object_on(&tx, head_id)?;
                if !head_remote
                    .candidate_cleanup_complete(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                {
                    return Err(DbError::Message(
                        "losing Store acknowledgement head proof is incomplete".to_string(),
                    ));
                }
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
                for object_id in [commit_id, head_id] {
                    let removed = tx
                        .execute(
                            "DELETE FROM remote_objects WHERE object_id = ?1",
                            [object_id.to_string()],
                        )
                        .map_err(DbError::from)?;
                    if removed != 1 {
                        return Err(DbError::Message(format!(
                            "nonactivating remote object {object_id} disappeared during completion"
                        )));
                    }
                }
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
