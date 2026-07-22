use super::*;

fn require_serial_candidate(candidate: &PreparedSerialStoreOperationCommit) -> Result<(), DbError> {
    if candidate.reference.coord.policy() == WritePolicy::Serial {
        Ok(())
    } else {
        Err(DbError::Message(
            "Serial acknowledgement candidate carries a Merge coordinate".to_string(),
        ))
    }
}

impl SerialDatabase<'_> {
    pub(in crate::sync::store_engine) async fn prepare_acknowledgement_activation(
        self,
        expected: StoreAckRef,
        candidate: PreparedSerialStoreOperationCommit,
    ) -> Result<(), DbError> {
        require_serial_candidate(&candidate)?;
        let candidate = PreparedStoreOperationCommit::Serial(candidate);
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
                    OutboundStoreAckActivation::Prepared(PreparedStoreOperationCommit::Serial(
                        existing,
                    )) if existing.reference == candidate.reference => return Ok(()),
                    OutboundStoreAckActivation::Prepared(
                        PreparedStoreOperationCommit::MergeConcurrent(_),
                    )
                    | OutboundStoreAckActivation::Nonactivating(
                        PreparedStoreOperationCommit::MergeConcurrent(_),
                    ) => {
                        return Err(DbError::Message(
                            "Serial acknowledgement contains a Merge activation candidate"
                                .to_string(),
                        ));
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
                        "Serial Store acknowledgement activation object",
                    )?;
                }
                let activation =
                    serde_json::to_string(&OutboundStoreAckActivation::Prepared(candidate))
                        .map_err(|error| {
                            DbError::Message(format!(
                        "serialize prepared Serial Store acknowledgement activation: {error}"
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

    pub(in crate::sync::store_engine) async fn begin_acknowledgement_nonactivation(
        self,
        expected: StoreAckRef,
        nonactivation: VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let verified_candidate = nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if !matches!(
            nonactivation.proof(),
            CandidateNonactivationProof::SerialImmediateSuccessor { .. }
        ) {
            return Err(DbError::Message(
                "Serial acknowledgement requires an immediate-successor nonactivation proof"
                    .to_string(),
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
                    OutboundStoreAckActivation::Prepared(
                        PreparedStoreOperationCommit::Serial(candidate),
                    ) => (candidate, false),
                    OutboundStoreAckActivation::Nonactivating(
                        PreparedStoreOperationCommit::Serial(candidate),
                    ) => (candidate, true),
                    OutboundStoreAckActivation::Prepared(
                        PreparedStoreOperationCommit::MergeConcurrent(_),
                    )
                    | OutboundStoreAckActivation::Nonactivating(
                        PreparedStoreOperationCommit::MergeConcurrent(_),
                    ) => {
                        return Err(DbError::Message(
                            "Serial acknowledgement contains a Merge activation candidate"
                                .to_string(),
                        ));
                    }
                    OutboundStoreAckActivation::AwaitingCandidate => {
                        return Err(DbError::Message(
                            "Store acknowledgement has no prepared Serial activation candidate"
                                .to_string(),
                        ));
                    }
                };
                require_serial_candidate(&candidate)?;
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
                            "nonactivating Serial acknowledgement commit carries a different durable proof"
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
                            "nonactivating Serial acknowledgement carries a different durable proof"
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
                    &OutboundStoreAckActivation::Nonactivating(
                        PreparedStoreOperationCommit::Serial(candidate),
                    ),
                )
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize nonactivating Serial Store acknowledgement: {error}"
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

    pub(in crate::sync::store_engine) async fn adopt_acknowledgement_base_head(
        self,
        expected: StoreAckRef,
        observed: VersionedObject,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store acknowledgement is absent".to_string())
                })?;
                if outbound.reference != expected {
                    return Err(DbError::Message(
                        "Serial head receipt names another Store acknowledgement".to_string(),
                    ));
                }
                let OutboundStoreAckActivation::Prepared(PreparedStoreOperationCommit::Serial(
                    candidate,
                )) = outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement has no prepared Serial candidate".to_string(),
                    ));
                };
                require_serial_candidate(&candidate)?;
                if candidate.base_head == observed {
                    return Err(DbError::Message(
                        "Store acknowledgement already carries the observed Serial head receipt"
                            .to_string(),
                    ));
                }
                let root = required_store_root_authority_on(&tx)?;
                let unverified: StoreSerialHead =
                    serde_json::from_slice(&observed.bytes).map_err(|error| {
                        DbError::Message(format!("parse observed Serial head: {error}"))
                    })?;
                let author = match &unverified.state {
                    StoreSerialHeadState::Genesis {
                        founder_registration,
                        ..
                    } => founder_registration,
                    StoreSerialHeadState::Commit {
                        author_registration,
                        ..
                    } => author_registration,
                };
                let registration = load_activated_registration_on(&tx, &root, author)?;
                let verified =
                    StoreSerialHead::parse(&observed.bytes, root.store_root_hash, &registration)
                        .map_err(|error| {
                            DbError::Message(format!("verify observed Serial head: {error}"))
                        })?;
                let observed_position = match verified.state {
                    StoreSerialHeadState::Genesis {
                        root,
                        founder_registration,
                    } => StoreSerialPredecessor::Genesis {
                        root,
                        founder_registration,
                    },
                    StoreSerialHeadState::Commit { commit, .. } => {
                        StoreSerialPredecessor::Commit(commit)
                    }
                };
                let crate::sync::store_commit::StoreCommitOrder::Serial { predecessor, .. } =
                    &candidate.commit.order
                else {
                    return Err(DbError::Message(
                        "Serial acknowledgement candidate carries Merge order".to_string(),
                    ));
                };
                if &observed_position != predecessor {
                    return Err(DbError::Message(
                        "observed Serial head advanced beyond the candidate predecessor"
                            .to_string(),
                    ));
                }
                let mut wrapped = PreparedStoreOperationCommit::Serial(candidate);
                wrapped
                    .adopt_serial_base_head(observed)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let activation = serde_json::to_string(&OutboundStoreAckActivation::Prepared(
                    wrapped,
                ))
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize updated Serial acknowledgement activation: {error}"
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
                        "outbound Store acknowledgement disappeared during Serial receipt adoption"
                            .to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store_engine) async fn acknowledgement_cleanup_target(
        self,
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
                let OutboundStoreAckActivation::Nonactivating(
                    PreparedStoreOperationCommit::Serial(candidate),
                ) = outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Serial".to_string(),
                    ));
                };
                require_serial_candidate(&candidate)?;
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

    pub(in crate::sync::store_engine) async fn complete_nonactivating_acknowledgement(
        self,
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
                let OutboundStoreAckActivation::Nonactivating(
                    PreparedStoreOperationCommit::Serial(candidate),
                ) = &outbound.activation
                else {
                    return Err(DbError::Message(
                        "Store acknowledgement activation is not nonactivating Serial".to_string(),
                    ));
                };
                require_serial_candidate(candidate)?;
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
                if !matches!(
                    proof,
                    CandidateNonactivationProof::SerialImmediateSuccessor { .. }
                ) {
                    return Err(DbError::Message(
                        "nonactivating Serial acknowledgement carries another proof".to_string(),
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
                let removed = tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [commit_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                if removed != 1 {
                    return Err(DbError::Message(format!(
                        "nonactivating remote object {commit_id} disappeared during completion"
                    )));
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
