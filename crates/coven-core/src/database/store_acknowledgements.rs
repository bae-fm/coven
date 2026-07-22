use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
use crate::database::remote_object_records::load_protocol_inert_object_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::replace_prepared_merge_head_remote_on;
use crate::database::store_ack_records::load_outbound_store_ack_on;
use crate::database::store_ack_records::load_published_store_ack_on;
use crate::database::store_ack_records::record_published_store_ack_on;
use crate::database::store_ack_records::verify_next_local_store_ack_on;

use super::*;

impl Database {
    pub(crate) async fn latest_local_store_ack(
        &self,
    ) -> Result<Option<PublishedStoreAck>, DbError> {
        self.call(load_published_store_ack_on).await
    }

    pub(crate) async fn activated_store_ack(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Result<Option<StoreAckRef>, DbError> {
        let registration = registration.clone();
        self.call(move |conn| {
            conn.query_row(
                "SELECT ack_ref FROM activated_store_acks WHERE device_id = ?1",
                [registration.device_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|raw| {
                let reference: StoreAckRef = serde_json::from_str(&raw).map_err(|error| {
                    DbError::Message(format!("activated Store acknowledgement ref: {error}"))
                })?;
                if reference.registration != registration {
                    return Err(DbError::Message(
                        "activated Store acknowledgement names another registration".to_string(),
                    ));
                }
                Ok(reference)
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_store_ack(
        &self,
        ack: StoreAck,
        prepared: PreparedExactObject,
    ) -> Result<StoreAckRef, DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let bytes = ack.to_bytes();
            let (reference, verified) = verify_next_local_store_ack_on(&tx, &bytes, &prepared)?;
            if verified != ack {
                return Err(DbError::Message(
                    "staged Store acknowledgement changed during exact verification".to_string(),
                ));
            }
            let ack_ref = serde_json::to_string(&reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize exact Store acknowledgement ref: {error}"
                ))
            })?;
            let prepared = serde_json::to_string(&prepared).map_err(|error| {
                DbError::Message(format!("serialize prepared Store acknowledgement: {error}"))
            })?;
            let activation = serde_json::to_string(&OutboundStoreAckActivation::AwaitingCandidate)
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize Store acknowledgement activation state: {error}"
                    ))
                })?;
            tx.execute(
                "INSERT INTO outbound_store_acks \
                 (singleton, ack_ref, ack_bytes, prepared_object, activation) \
                 VALUES (1, ?1, ?2, ?3, ?4)",
                rusqlite::params![ack_ref, bytes, prepared, activation],
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)?;
            Ok(reference)
        })
        .await
    }

    pub(crate) async fn adopt_outbound_store_ack_slot_winner(
        &self,
        expected: StoreAckRef,
        winner_bytes: Vec<u8>,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != expected {
                return Err(DbError::Message(
                    "acknowledgement slot winner names another queued object".to_string(),
                ));
            }
            let OutboundStoreAckActivation::Prepared(candidate) = &outbound.activation else {
                return Err(DbError::Message(
                    "acknowledgement slot collision has no prepared activation candidate"
                        .to_string(),
                ));
            };
            if candidate.commit.acknowledgement() != Some(&expected) {
                return Err(DbError::Message(
                    "prepared activation candidate names another acknowledgement".to_string(),
                ));
            }
            if winner_prepared.reference().slot() != expected.object.slot()
                || winner_prepared.reference() == &expected.object
            {
                return Err(DbError::Message(
                    "acknowledgement slot winner is not a distinct object at the occupied slot"
                        .to_string(),
                ));
            }
            let (winner_reference, _) =
                verify_next_local_store_ack_on(&tx, &winner_bytes, &winner_prepared)?;
            let expected_records = candidate
                .acknowledgement_remote_objects(&outbound.ack)
                .map_err(|error| DbError::Message(error.to_string()))?;
            for expected_record in &expected_records {
                let object_id = expected_record.object_id();
                let stored = load_remote_object_on(&tx, object_id)?;
                if stored != *expected_record {
                    return Err(DbError::Message(
                        "losing acknowledgement candidate is no longer wholly unuploaded"
                            .to_string(),
                    ));
                }
            }
            for expected_record in expected_records {
                let removed = tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [expected_record.object_id().to_string()],
                    )
                    .map_err(DbError::from)?;
                if removed != 1 {
                    return Err(DbError::Message(
                        "losing acknowledgement candidate object disappeared".to_string(),
                    ));
                }
            }
            let activation = serde_json::to_string(&OutboundStoreAckActivation::AwaitingCandidate)
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize adopted Store acknowledgement activation: {error}"
                    ))
                })?;
            let winner_ref = serde_json::to_string(&winner_reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize adopted Store acknowledgement ref: {error}"
                ))
            })?;
            let winner_prepared = serde_json::to_string(&winner_prepared).map_err(|error| {
                DbError::Message(format!(
                    "serialize adopted prepared Store acknowledgement: {error}"
                ))
            })?;
            let updated = tx
                .execute(
                    "UPDATE outbound_store_acks
                     SET ack_ref = ?2, ack_bytes = ?3, prepared_object = ?4, activation = ?5
                     WHERE singleton = 1 AND ack_ref = ?1",
                    rusqlite::params![
                        serde_json::to_string(&expected).map_err(|error| DbError::Message(
                            format!("serialize losing Store acknowledgement ref: {error}")
                        ))?,
                        winner_ref,
                        winner_bytes,
                        winner_prepared,
                        activation,
                    ],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "outbound Store acknowledgement changed during winner adoption".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn oldest_outbound_store_ack(
        &self,
    ) -> Result<Option<OutboundStoreAck>, DbError> {
        self.call(load_outbound_store_ack_on).await
    }

    pub(crate) async fn prepare_outbound_store_ack_activation(
        &self,
        expected: StoreAckRef,
        prepared: crate::sync::store_outbound::PreparedStoreOperationCommit,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
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
                    if existing.reference == prepared.reference =>
                {
                    return Ok(());
                }
                OutboundStoreAckActivation::Prepared(_)
                | OutboundStoreAckActivation::Nonactivating(_) => {
                    return Err(DbError::Message(
                        "Store acknowledgement already has a different activation candidate"
                            .to_string(),
                    ));
                }
            }
            for remote in prepared
                .acknowledgement_remote_objects(&outbound.ack)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                persist_exact_remote_object_on(
                    &tx,
                    &remote,
                    "Store acknowledgement activation object",
                )?;
            }
            let activation = serde_json::to_string(&OutboundStoreAckActivation::Prepared(prepared))
                .map_err(|error| {
                    DbError::Message(format!(
                        "serialize prepared Store acknowledgement activation: {error}"
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

    pub(crate) async fn begin_outbound_store_ack_nonactivation(
        &self,
        expected: StoreAckRef,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let verified_candidate = nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let nonactivation = nonactivation.into_durable();
        self.call(move |conn| {
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
                        "Store acknowledgement has no prepared activation candidate".to_string(),
                    ));
                }
            };
            if candidate.reference != verified_candidate {
                return Err(DbError::Message(
                    "verified nonactivation names another Store acknowledgement candidate"
                        .to_string(),
                ));
            }
            let head = match (nonactivation.proof(), candidate.merge_head_ref()) {
                (
                    crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. },
                    Some(head),
                ) if candidate.reference.coord.policy() == WritePolicy::MergeConcurrent => {
                    Some(head)
                }
                (
                    crate::sync::remote_object::CandidateNonactivationProof::SerialImmediateSuccessor { .. },
                    None,
                ) if candidate.reference.coord.policy() == WritePolicy::Serial => None,
                _ => {
                    return Err(DbError::Message(
                        "Store acknowledgement nonactivation proof differs from its publication policy"
                            .to_string(),
                    ));
                }
            };
            if nonactivation.candidate().canonical_signed_bytes != candidate.commit.to_bytes() {
                return Err(DbError::Message(
                    "verified nonactivation bytes differ from the Store acknowledgement candidate"
                        .to_string(),
                ));
            }
            if already_nonactivating {
                let commit_id = remote_object_id(&candidate.reference.object);
                let commit_remote = load_remote_object_on(&tx, commit_id)?;
                let proof_matches = commit_remote
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    == Some(nonactivation.proof());
                let inert = load_protocol_inert_object_on(&tx, remote_object_id(&expected.object))?;
                if !proof_matches
                    || inert
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(nonactivation.proof())
                {
                    return Err(DbError::Message(
                        "nonactivating Store acknowledgement carries different durable proof"
                            .to_string(),
                    ));
                }
                if let Some(head) = head {
                    let head_remote = load_remote_object_on(&tx, remote_object_id(&head.object))?;
                    if head_remote
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(nonactivation.proof())
                    {
                        return Err(DbError::Message(
                            "nonactivating Store acknowledgement head carries different durable proof"
                                .to_string(),
                        ));
                    }
                }
                return Ok(());
            }
            let acknowledgement_id = remote_object_id(&expected.object);
            if begin_remote_candidate_nonactivation_on(
                &tx,
                acknowledgement_id,
                nonactivation.clone(),
            )?
            .is_some()
            {
                return Err(DbError::Message(
                    "Store acknowledgement became an exact cleanup target".to_string(),
                ));
            }
            if let Some(head) = head {
                let head_id = remote_object_id(&head.object);
                if begin_remote_candidate_nonactivation_on(&tx, head_id, nonactivation.clone())?
                    .is_some()
                {
                    return Err(DbError::Message(
                        "Store activation head became an exact cleanup target".to_string(),
                    ));
                }
            }
            let commit_id = remote_object_id(&candidate.reference.object);
            if begin_remote_candidate_nonactivation_on(&tx, commit_id, nonactivation)?.is_none() {
                return Err(DbError::Message(
                    "losing Store acknowledgement commit has no exact cleanup target".to_string(),
                ));
            }
            let activation =
                serde_json::to_string(&OutboundStoreAckActivation::Nonactivating(candidate))
                    .map_err(|error| {
                        DbError::Message(format!(
                            "serialize nonactivating Store acknowledgement: {error}"
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
                    "outbound Store acknowledgement disappeared during nonactivation".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn adopt_outbound_store_ack_merge_head(
        &self,
        expected: StoreAckRef,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != expected {
                return Err(DbError::Message(
                    "alternate Merge head names another Store acknowledgement".to_string(),
                ));
            }
            let OutboundStoreAckActivation::Prepared(mut candidate) = outbound.activation else {
                return Err(DbError::Message(
                    "Store acknowledgement has no prepared Merge candidate".to_string(),
                ));
            };
            let current = candidate.merge_head_ref().ok_or_else(|| {
                DbError::Message(
                    "Serial Store acknowledgement cannot adopt a Merge head".to_string(),
                )
            })?;
            let root = required_store_root_authority_on(&tx)?;
            let registration =
                load_activated_registration_on(&tx, &root, &candidate.commit.author_registration)?;
            let verified = StoreDeviceHead::parse_at(
                &winner.to_bytes(),
                root.store_root_hash,
                &registration,
                &candidate.reference,
            )
            .map_err(|error| DbError::Message(format!("verify alternate Merge head: {error}")))?;
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
            candidate
                .adopt_merge_head(winner, winner_prepared)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let activation = serde_json::to_string(&OutboundStoreAckActivation::Prepared(
                candidate,
            ))
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
                    "outbound Store acknowledgement disappeared during head adoption".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn adopt_outbound_store_ack_serial_base_head(
        &self,
        expected: StoreAckRef,
        observed: VersionedObject,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != expected {
                return Err(DbError::Message(
                    "Serial head receipt names another Store acknowledgement".to_string(),
                ));
            }
            let OutboundStoreAckActivation::Prepared(mut candidate) = outbound.activation else {
                return Err(DbError::Message(
                    "Store acknowledgement has no prepared Serial candidate".to_string(),
                ));
            };
            let Some(current) = candidate.serial_base_head() else {
                return Err(DbError::Message(
                    "Merge Store acknowledgement cannot adopt a Serial head receipt".to_string(),
                ));
            };
            if current == &observed {
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
                    "observed Serial head advanced beyond the candidate predecessor".to_string(),
                ));
            }
            candidate
                .adopt_serial_base_head(observed)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let activation = serde_json::to_string(&OutboundStoreAckActivation::Prepared(
                candidate,
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

    pub(crate) async fn complete_outbound_store_ack(
        &self,
        accepted: StoreAckRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != accepted {
                return Err(DbError::Message(
                    "accepted Store acknowledgement differs from the prepared exact object"
                        .to_string(),
                ));
            }
            let deleted = tx
                .execute(
                    "DELETE FROM outbound_store_acks WHERE singleton = 1 AND ack_ref = ?1",
                    [serde_json::to_string(&accepted).map_err(|error| {
                        DbError::Message(format!(
                            "serialize accepted Store acknowledgement ref: {error}"
                        ))
                    })?],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "outbound Store acknowledgement disappeared".to_string(),
                ));
            }
            record_published_store_ack_on(&tx, &outbound)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn nonactivating_outbound_store_ack_cleanup_targets(
        &self,
        expected: StoreAckRef,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.call(move |conn| {
            let outbound = load_outbound_store_ack_on(conn)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != expected {
                return Err(DbError::Message(
                    "Store acknowledgement cleanup names another exact object".to_string(),
                ));
            }
            let OutboundStoreAckActivation::Nonactivating(candidate) = outbound.activation else {
                return Err(DbError::Message(
                    "Store acknowledgement activation is not nonactivating".to_string(),
                ));
            };
            let commit =
                load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
            let mut targets = Vec::new();
            if let Some(object) = commit.cleanup_target() {
                targets.push(CandidateCleanupObject {
                    object: object.clone(),
                });
            } else if !commit
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                return Err(DbError::Message(
                    "losing Store acknowledgement commit is not awaiting cleanup".to_string(),
                ));
            }
            Ok(targets)
        })
        .await
    }

    pub(crate) async fn complete_nonactivating_outbound_store_ack(
        &self,
        expected: StoreAckRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let outbound = load_outbound_store_ack_on(&tx)?.ok_or_else(|| {
                DbError::Message("outbound Store acknowledgement is absent".to_string())
            })?;
            if outbound.reference != expected {
                return Err(DbError::Message(
                    "Store acknowledgement completion names another exact object".to_string(),
                ));
            }
            let OutboundStoreAckActivation::Nonactivating(candidate) = &outbound.activation else {
                return Err(DbError::Message(
                    "Store acknowledgement activation is not nonactivating".to_string(),
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
            let mut owned_object_ids = vec![commit_id];
            match (
                candidate.reference.coord.policy(),
                proof,
                candidate.merge_head_ref(),
            ) {
                (
                    WritePolicy::MergeConcurrent,
                    crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. },
                    Some(head),
                ) => {
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
                            "losing Store acknowledgement head carries a different proof"
                                .to_string(),
                        ));
                    }
                    owned_object_ids.push(head_id);
                }
                (
                    WritePolicy::Serial,
                    crate::sync::remote_object::CandidateNonactivationProof::SerialImmediateSuccessor { .. },
                    None,
                ) => {}
                _ => {
                    return Err(DbError::Message(
                        "nonactivating Store acknowledgement proof differs from its publication policy"
                            .to_string(),
                    ));
                }
            }
            let inert =
                load_protocol_inert_object_on(&tx, remote_object_id(&outbound.reference.object))?;
            if inert
                .candidate_nonactivation_proof(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                != Some(proof)
            {
                return Err(DbError::Message(
                    "protocol-inert acknowledgement lacks its candidate proof".to_string(),
                ));
            }
            for object_id in owned_object_ids {
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
            let removed = tx
                .execute(
                    "DELETE FROM outbound_store_acks WHERE singleton = 1 AND ack_ref = ?1",
                    [serde_json::to_string(&expected).map_err(|error| {
                        DbError::Message(format!(
                            "serialize nonactivating Store acknowledgement ref: {error}"
                        ))
                    })?],
                )
                .map_err(DbError::from)?;
            if removed != 1 {
                return Err(DbError::Message(
                    "nonactivating Store acknowledgement disappeared".to_string(),
                ));
            }
            record_published_store_ack_on(&tx, &outbound)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }
}
