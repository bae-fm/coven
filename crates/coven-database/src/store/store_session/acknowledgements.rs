use super::*;
use crate::store_ack_records::{
    load_expected_outbound_store_ack_on, verify_next_local_store_ack_on,
};
use crate::{
    update_remote_object_on, ActiveStorePublication, ActiveStorePublicationOwner,
    ExactProtocolObject, RemoteObjectRecord, StoreAck,
};

impl StoreSession<'_> {
    fn replace_acknowledgement_activation(
        &mut self,
        expected: StoreAckRef,
        snapshot: coven_protocol::store_commit::AcceptedStoreSnapshotRef,
        acknowledgement: ExactProtocolObject<StoreAck>,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<(), DbError> {
        let authority = self.local_store_authority()?;
        self.verified_store_transaction(move |transaction| {
            let tx = transaction.store.transaction;
            let outbound = load_expected_outbound_store_ack_on(
                tx, &authority, &expected,
                "acknowledgement replacement names another queued object",
            )?;
            let OutboundStoreAckActivation::Prepared(previous) = &outbound.activation else {
                return Err(DbError::Message("acknowledgement replacement has no prepared candidate".into()));
            };
            previous.validate_closed_shape()?;
            candidate.validate_closed_shape()?;
            let old_commit = coven_protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
                &previous.commit.to_bytes(), authority.value().store_root.store_root_hash,
                previous.reference.coord.clone(), previous.reference.object.clone(), authority.value(),
            )?;
            let new_commit = coven_protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
                &candidate.commit.to_bytes(), authority.value().store_root.store_root_hash,
                candidate.reference.coord.clone(), candidate.reference.object.clone(), authority.value(),
            )?;
            let proof = transaction.snapshot_candidate_nonactivation(&snapshot, &old_commit)?;
            let active = super::active_store_publication::load_active_store_publication_on(tx)?
                .ok_or_else(|| DbError::Message("acknowledgement replacement has no reserved publication".into()))?;
            let installed = super::observed_store_publication::load_store_current_publication_on(tx)?;
            if installed.record() != &candidate.publication.previous
                || installed.observed_version() != Some(&candidate.publication.previous_version)
                || new_commit.publication_base != coven_protocol::store_commit::StorePublicationBase::Snapshot(snapshot)
            {
                return Err(DbError::Message("replacement acknowledgement extends another installed boundary".into()));
            }
            candidate.publication.verify_commit(&new_commit)?;
            let retained = candidate.history_evidence.acknowledgement.as_ref()
                .ok_or_else(|| DbError::Message("replacement acknowledgement omits its proof".into()))?;
            let previous_proof = previous.history_evidence.acknowledgement.as_ref()
                .ok_or_else(|| DbError::Message("queued acknowledgement omits its proof".into()))?;
            if previous_proof.acknowledgement != (outbound.reference.clone(), outbound.ack.value.clone())
                || retained.predecessors != previous_proof.proof_objects().cloned().collect::<Vec<_>>()
                || retained.acknowledgement.1 != acknowledgement.value
                || retained.acknowledgement.0.object != *acknowledgement.prepared.reference()
                || acknowledgement.value.to_bytes() != acknowledgement.bytes
                || acknowledgement.value.store_cut != new_commit.order.predecessor_cut()?
                || acknowledgement.value.device_state != new_commit.device_state
                || candidate.commit.circle_acknowledgements() != previous.commit.circle_acknowledgements()
            {
                return Err(DbError::Message("replacement acknowledgement changes its queued proof or Circle statements".into()));
            }
            for (reference, value) in retained.proof_objects() {
                StoreAck::parse_at(&value.to_bytes(), &authority.value().store_root, reference, authority.value())?;
            }
            let mut publications = vec![active.attempt()?.reference()?];
            if let Some(prior) = active.superseded_entry() {
                publications.push(prior.clone());
            }
            let cleanup = crate::RetiredStoreCandidate {
                nonactivation: proof,
                inputs: crate::RetiredStoreCandidateInputs::Acknowledgement((**previous_proof).clone()),
                publications,
            };
            let replacement = active.replace_acknowledgement_candidate(&candidate, cleanup.clone())?;
            let old_objects = cleanup.objects()?.into_iter().collect::<std::collections::BTreeSet<_>>();
            let mut objects = candidate.acknowledgement_remote_objects(&acknowledgement)?;
            for circle in &outbound.circle_acknowledgements {
                objects.extend(candidate.circle_acknowledgement_remote_objects(&circle.ack)?);
            }
            let mut persisted = std::collections::BTreeSet::new();
            for proposed in objects {
                if !persisted.insert(proposed.object_id()) {
                    continue;
                }
                if old_objects.contains(proposed.object()) {
                    let mut current = load_remote_object_on(tx, proposed.object_id())?;
                    match (&current, proposed.record()) {
                        (RemoteObjectRecord::RetainedAuthority(current), RemoteObjectRecord::RetainedAuthority(proposed))
                            if current.identity == proposed.identity && current.payloads == proposed.payloads => {}
                        _ => return Err(DbError::Message("replacement changed a retained acknowledgement object".into())),
                    }
                    current.add_retained_authority_candidate(candidate.reference.clone())?;
                    update_remote_object_on(tx, proposed.object_id(), &current)?;
                } else {
                    persist_exact_remote_object_on(tx, transaction.store.store_dir, &proposed, "replacement acknowledgement candidate")?;
                }
            }
            super::candidate_records::begin_candidate_nonactivation_targets_on(
                tx, &previous.reference, &cleanup.objects()?, &cleanup.nonactivation,
            )?;
            let changed = tx.execute(
                "UPDATE outbound_store_acks SET ack_ref = ?2, ack_bytes = ?3, prepared_object = ?4, activation = ?5 WHERE singleton = 1 AND ack_ref = ?1",
                rusqlite::params![
                    serde_json::to_string(&expected)?,
                    serde_json::to_string(&retained.acknowledgement.0)?,
                    acknowledgement.bytes,
                    serde_json::to_string(&acknowledgement.prepared)?,
                    serde_json::to_string(&OutboundStoreAckActivation::Prepared(candidate))?,
                ],
            )?;
            if changed != 1 {
                return Err(DbError::Message("outbound acknowledgement changed during candidate replacement".into()));
            }
            super::active_store_publication::update_active_store_publication_on(tx, &active, &replacement)?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }

    fn prepare_acknowledgement_activation(
        &mut self,
        expected: &StoreAckRef,
        acknowledgement: ExactProtocolObject<StoreAck>,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<bool, DbError> {
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let outbound = load_expected_outbound_store_ack_on(
            &tx,
            &authority,
            expected,
            "prepared activation names a different Store acknowledgement",
        )?;
        match &outbound.activation {
            OutboundStoreAckActivation::AwaitingCandidate | OutboundStoreAckActivation::Created => {
            }
            OutboundStoreAckActivation::Prepared(existing)
                if *existing == candidate
                    && outbound.ack.bytes == acknowledgement.bytes
                    && outbound.ack.prepared == acknowledgement.prepared
                    && outbound.ack.value == acknowledgement.value =>
            {
                return Ok(true);
            }
            OutboundStoreAckActivation::Prepared(_) => {
                return Err(DbError::Message(
                    "Store acknowledgement already has a different activation candidate"
                        .to_string(),
                ));
            }
        }
        candidate.validate_closed_shape()?;
        let reference = candidate.commit.acknowledgement().ok_or_else(|| {
            DbError::Message("prepared acknowledgement has no exact statement".into())
        })?;
        let value = StoreAck::parse_at(
            &acknowledgement.bytes,
            &authority.value().store_root,
            reference,
            authority.value(),
        )?;
        let retained = candidate
            .history_evidence
            .acknowledgement
            .as_ref()
            .ok_or_else(|| {
                DbError::Message("prepared acknowledgement omits its retained statement".into())
            })?;
        if value != acknowledgement.value
            || reference.object != *acknowledgement.prepared.reference()
            || retained.acknowledgement != (reference.clone(), value.clone())
            || value.store_cut != candidate.commit.order.predecessor_cut()?
            || value.device_state != candidate.commit.device_state
            || value.last_sync != outbound.ack.value.last_sync
            || candidate.commit.circle_acknowledgements()
                != outbound
                    .circle_acknowledgements
                    .iter()
                    .map(|circle| circle.reference.clone())
                    .collect::<Vec<_>>()
        {
            return Err(DbError::Message("prepared acknowledgement differs from its queued statement or accepted predecessor".into()));
        }
        let created = match &outbound.activation {
            OutboundStoreAckActivation::AwaitingCandidate => {
                let verified = verify_next_local_store_ack_on(
                    &tx,
                    &authority,
                    &acknowledgement.bytes,
                    &acknowledgement.prepared,
                )?;
                if verified != *reference
                    || reference.object.slot() != expected.object.slot()
                    || value.successor != outbound.ack.value.successor
                    || !retained.predecessors.is_empty()
                {
                    return Err(DbError::Message(
                        "uncreated acknowledgement changed its reserved stream position".into(),
                    ));
                }
                None
            }
            OutboundStoreAckActivation::Created => {
                if reference == expected {
                    if acknowledgement.bytes != outbound.ack.bytes
                        || acknowledgement.prepared != outbound.ack.prepared
                        || acknowledgement.value != outbound.ack.value
                        || !retained.predecessors.is_empty()
                    {
                        return Err(DbError::Message(
                            "created acknowledgement bytes cannot change".into(),
                        ));
                    }
                } else if reference.sequence
                    != expected.sequence.checked_add(1).ok_or_else(|| {
                        DbError::Message("acknowledgement sequence overflow".into())
                    })?
                    || reference.object.slot() != &outbound.ack.value.successor.next_slot
                    || value.successor.predecessor.as_ref() != Some(&expected.object)
                    || retained.predecessors != vec![(expected.clone(), outbound.ack.value.clone())]
                {
                    return Err(DbError::Message(
                        "created acknowledgement replacement omits its exact predecessor".into(),
                    ));
                }
                Some(&expected.object)
            }
            OutboundStoreAckActivation::Prepared(_) => {
                unreachable!("prepared candidates returned above")
            }
        };
        let active_publication = ActiveStorePublication::for_commit(
            ActiveStorePublicationOwner::StoreAcknowledgement,
            &candidate,
        )?;
        match super::active_store_publication::claim_active_store_publication_on(
            &tx,
            &active_publication,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "Store acknowledgement candidate already owns publication before its journal"
                        .to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(_) => {
                return Ok(false);
            }
        }
        for remote in candidate
            .acknowledgement_remote_objects(&acknowledgement)
            .map_err(DbError::from)?
        {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                &remote,
                "Merge Store acknowledgement activation object",
            )?;
            if created == Some(remote.object()) {
                let object_id = remote.object_id();
                let mut uploaded = remote.into_record();
                uploaded.mark_uploaded_verified()?;
                update_remote_object_on(&tx, object_id, &uploaded)?;
            }
        }
        for circle in &outbound.circle_acknowledgements {
            for remote in candidate
                .circle_acknowledgement_remote_objects(&circle.ack)
                .map_err(DbError::from)?
            {
                persist_exact_remote_object_on(
                    &tx,
                    self.store_dir,
                    &remote,
                    "Merge Circle acknowledgement activation object",
                )?;
            }
        }
        let changed = tx.execute(
            "UPDATE outbound_store_acks SET ack_ref = ?2, ack_bytes = ?3, prepared_object = ?4, activation = ?5 WHERE singleton = 1 AND ack_ref = ?1",
            rusqlite::params![
                serde_json::to_string(expected)?,
                serde_json::to_string(reference)?,
                acknowledgement.bytes,
                serde_json::to_string(&acknowledgement.prepared)?,
                serde_json::to_string(&OutboundStoreAckActivation::Prepared(candidate))?,
            ],
        )?;
        if changed != 1 {
            return Err(DbError::Message(
                "outbound acknowledgement changed during activation preparation".into(),
            ));
        }
        tx.commit().map_err(DbError::from)?;
        Ok(true)
    }
}
impl StoreDatabase {
    pub async fn replace_acknowledgement_activation(
        &self,
        expected: StoreAckRef,
        snapshot: coven_protocol::store_commit::AcceptedStoreSnapshotRef,
        acknowledgement: ExactProtocolObject<StoreAck>,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.replace_acknowledgement_activation(
                expected,
                snapshot,
                acknowledgement,
                candidate,
            )
        })
        .await
    }

    pub async fn prepare_acknowledgement_activation(
        &self,
        expected: StoreAckRef,
        acknowledgement: ExactProtocolObject<StoreAck>,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.prepare_acknowledgement_activation(&expected, acknowledgement, candidate)
        })
        .await
    }
}
