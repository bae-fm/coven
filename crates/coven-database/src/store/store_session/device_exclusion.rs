use rusqlite::{Connection, OptionalExtension};

use super::*;
use crate::mark_remote_object_uploaded_on;
use crate::store::StoreSession;
use crate::ActiveStorePublication;
use coven_protocol::device_exclusion_journal::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
use coven_protocol::remote_object::{
    ClosedRemoteObject, RemoteObjectRecord, RetainedAuthorityObjectState,
};
use coven_protocol::store_commit::ObjectHash;

pub(crate) fn store_device_exclusion_journal_error(
    error: StoreDeviceExclusionJournalError,
) -> DbError {
    DbError::from(error)
}

pub(crate) fn parse_store_device_exclusion_operation(
    operation_id: ObjectHash,
    raw: &str,
) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
    let operation: DurableStoreDeviceExclusionOperation =
        serde_json::from_str(raw).map_err(|error| {
            DbError::context(
                format!(
                    "Store-device exclusion operation {operation_id} has invalid durable state"
                ),
                error,
            )
        })?;
    operation
        .validate()
        .map_err(store_device_exclusion_journal_error)?;
    if operation.operation_id() != operation_id {
        return Err(DbError::Message(format!(
            "Store-device exclusion operation key {operation_id} differs from its signed object {}",
            operation.operation_id()
        )));
    }
    Ok(operation)
}

pub(crate) fn load_store_device_exclusion_on(
    conn: &Connection,
    operation_id: ObjectHash,
) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
    conn.query_row(
        "SELECT state FROM outbound_store_device_exclusion WHERE operation_id = ?1",
        [operation_id.to_string()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|raw| parse_store_device_exclusion_operation(operation_id, &raw))
    .transpose()
}

pub(crate) fn load_active_store_device_exclusion_on(
    conn: &Connection,
) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
    conn.query_row(
        "SELECT operation_id, state FROM outbound_store_device_exclusion WHERE active_key = 1",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(raw_id, raw)| {
        let operation_id = raw_id
            .parse::<ObjectHash>()
            .map_err(|error| DbError::context("Store-device exclusion operation id", error))?;
        let operation = parse_store_device_exclusion_operation(operation_id, &raw)?;
        if operation.is_completed() {
            return Err(DbError::Message(
                "completed Store-device exclusion remains active".to_string(),
            ));
        }
        Ok(operation)
    })
    .transpose()
}

pub(crate) fn insert_store_device_exclusion_on(
    conn: &Connection,
    operation: &DurableStoreDeviceExclusionOperation,
    active: bool,
) -> Result<(), DbError> {
    operation
        .validate()
        .map_err(store_device_exclusion_journal_error)?;
    if active == operation.is_completed() {
        return Err(DbError::Message(
            "Store-device exclusion active marker differs from its closed state".to_string(),
        ));
    }
    let encoded = serde_json::to_string(operation)
        .map_err(|error| DbError::context("serialize Store-device exclusion operation", error))?;
    conn.execute(
        "INSERT INTO outbound_store_device_exclusion (operation_id, active_key, state)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![
            operation.operation_id().to_string(),
            active.then_some(1_i64),
            encoded,
        ],
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn require_store_device_exclusion_transition_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    next: &DurableStoreDeviceExclusionOperation,
) -> Result<(), DbError> {
    if !expected.allows_transition_to(next) {
        return Err(DbError::Message(
            "invalid Store-device exclusion journal transition".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize expected Store-device exclusion", error))?;
    let current = conn
        .query_row(
            "SELECT state FROM outbound_store_device_exclusion WHERE operation_id = ?1",
            [expected.operation_id().to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message("Store-device exclusion journal disappeared".to_string())
        })?;
    if current != expected_state {
        return Err(DbError::Message(
            "Store-device exclusion journal changed during transition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn update_store_device_exclusion_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    next: &DurableStoreDeviceExclusionOperation,
    active: bool,
) -> Result<(), DbError> {
    require_store_device_exclusion_transition_on(conn, expected, next)?;
    if active == next.is_completed() {
        return Err(DbError::Message(
            "Store-device exclusion active marker differs from its next state".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize expected Store-device exclusion", error))?;
    let next_state = serde_json::to_string(next)
        .map_err(|error| DbError::context("serialize next Store-device exclusion", error))?;
    let updated = conn
        .execute(
            "UPDATE outbound_store_device_exclusion
             SET active_key = ?3, state = ?4
             WHERE operation_id = ?1 AND state = ?2",
            rusqlite::params![
                expected.operation_id().to_string(),
                expected_state,
                active.then_some(1_i64),
                next_state,
            ],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(
            "Store-device exclusion journal disappeared during transition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn complete_store_device_exclusion_activation_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    acceptance: &crate::AcceptedStoreCommitEvidence,
) -> Result<(), DbError> {
    let next = expected
        .activated()
        .map_err(store_device_exclusion_journal_error)?;
    let candidate = expected
        .candidate()
        .expect("validated pending exclusion has a candidate");
    if &candidate.reference != acceptance.commit_ref() {
        return Err(DbError::Message(
            "Store-device exclusion completion names another accepted candidate".into(),
        ));
    }
    update_store_device_exclusion_on(conn, expected, &next, false)?;
    super::active_store_publication::clear_active_store_commit_for_owner_on(
        conn,
        &crate::ActiveStorePublicationOwner::DeviceExclusion(expected.operation_id()),
        &candidate.reference,
    )
}

impl StoreSession<'_> {
    fn begin_outbound_store_device_exclusion(
        &mut self,
        operation: DurableStoreDeviceExclusionOperation,
        remotes: Vec<ClosedRemoteObject>,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        if let Some(active) = load_active_store_device_exclusion_on(&tx)? {
            if active.operation_id() != operation.operation_id() {
                return Err(DbError::Message(format!(
                    "Store-device exclusion operation {} remains active",
                    active.operation_id()
                )));
            }
            return Ok(active);
        }
        let operation_id = operation.operation_id();
        if let Some(existing) = load_store_device_exclusion_on(&tx, operation_id)? {
            if existing != operation || !existing.is_completed() {
                return Err(DbError::Message(format!(
                    "Store-device exclusion operation {operation_id} already has different durable state"
                )));
            }
            return Ok(existing);
        }
        let candidate = operation.candidate().ok_or_else(|| {
            DbError::Message(
                "active Store-device exclusion has no publication candidate".to_string(),
            )
        })?;
        let active_publication = ActiveStorePublication::for_commit(
            crate::ActiveStorePublicationOwner::DeviceExclusion(operation_id),
            candidate,
        )?;
        match super::active_store_publication::claim_active_store_publication_on(
            &tx,
            &active_publication,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "Store-device exclusion candidate already owns publication before its journal"
                        .to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(source) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {source:?}"
                )));
            }
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "Store-device exclusion candidate object",
            )?;
        }
        insert_store_device_exclusion_on(&tx, &operation, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(operation)
    }

    fn active_outbound_store_device_exclusion(
        &mut self,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
        load_active_store_device_exclusion_on(self.conn)
    }

    fn complete_outbound_store_device_exclusion_slot_loss(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
        remotes: Vec<ClosedRemoteObject>,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        for remote in &remotes {
            let object_id = remote.object_id();
            let current = load_remote_object_on(&tx, object_id)?;
            let unuploaded = matches!(
                &current,
                RemoteObjectRecord::CandidateCommit(record)
                    if matches!(record.state, coven_protocol::remote_object::CandidateCommitState::Prepared)
            ) || matches!(
                &current,
                RemoteObjectRecord::CandidateExclusive(record)
                    if matches!(
                        record.state,
                        coven_protocol::remote_object::CandidateObjectState::Prepared { .. }
                    )
            ) || matches!(
                &current,
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        record.state,
                        coven_protocol::remote_object::RetainedAuthorityObjectState::Prepared { .. }
                    )
            );
            if current != **remote || !unuploaded {
                return Err(DbError::Message(format!(
                    "outcome-slot loss cannot discard uploaded exclusion object {object_id}"
                )));
            }
            if !crate::remote_object_records::delete_remote_object_on(&tx, object_id)? {
                return Err(DbError::Message(format!(
                    "unuploaded exclusion object {object_id} disappeared during slot resolution"
                )));
            }
        }
        let candidate = expected.candidate().ok_or_else(|| {
            DbError::Message("Store-device exclusion slot loss has no candidate".to_string())
        })?;
        let active_publication = ActiveStorePublication::for_commit(
            crate::ActiveStorePublicationOwner::DeviceExclusion(expected.operation_id()),
            candidate,
        )?;
        update_store_device_exclusion_on(&tx, &expected, &next, false)?;
        super::active_store_publication::clear_active_store_publication_on(
            &tx,
            &active_publication,
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn mark_store_device_exclusion_authority_uploaded(
        &mut self,
        expected: ClosedRemoteObject,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let object_id = expected.object_id();
        let current = load_remote_object_on(conn, object_id)?;
        let (
            RemoteObjectRecord::RetainedAuthority(expected_record),
            RemoteObjectRecord::RetainedAuthority(current_record),
        ) = (expected.record(), &current)
        else {
            return Err(DbError::Message(
                "Store-device exclusion authority is not retained authority".to_string(),
            ));
        };
        if expected_record.identity != current_record.identity
            || expected_record.payloads != current_record.payloads
        {
            return Err(DbError::Message(
                "Store-device exclusion authority changed before upload completion".to_string(),
            ));
        }
        match &current_record.state {
            RetainedAuthorityObjectState::Prepared { ownership }
                if ownership.pending.contains(&candidate) =>
            {
                mark_remote_object_uploaded_on(conn, current)?;
            }
            RetainedAuthorityObjectState::UploadedVerified { ownership }
                if ownership.pending.contains(&candidate) => {}
            _ => {
                return Err(DbError::Message(
                    "Store-device exclusion authority does not belong to its current candidate"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn outbound_store_device_exclusion_operations(
        &mut self,
    ) -> Result<Vec<DurableStoreDeviceExclusionOperation>, DbError> {
        let conn = self.conn;
        let mut statement = conn
            .prepare(
                "SELECT operation_id, state
                 FROM outbound_store_device_exclusion
                 ORDER BY operation_id",
            )
            .map_err(DbError::from)?;
        let operations = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .map(|row| {
                let (raw_id, raw) = row.map_err(DbError::from)?;
                let operation_id = raw_id.parse::<ObjectHash>().map_err(|error| {
                    DbError::context("Store-device exclusion operation id", error)
                })?;
                parse_store_device_exclusion_operation(operation_id, &raw)
            })
            .collect();
        operations
    }
}

impl StoreDatabase {
    pub async fn begin_outbound_store_device_exclusion(
        &self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        operation
            .validate()
            .map_err(store_device_exclusion_journal_error)?;
        if !matches!(
            operation,
            DurableStoreDeviceExclusionOperation::CandidatePrepared { .. }
        ) {
            return Err(DbError::Message(
                "a new Store-device exclusion journal must own its exact activation candidate"
                    .to_string(),
            ));
        }
        let remotes = operation
            .remote_objects()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.call_store(move |session| {
            session.begin_outbound_store_device_exclusion(operation, remotes)
        }))
        .await
    }

    pub async fn active_outbound_store_device_exclusion(
        &self,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(self.call_store(|session| session.active_outbound_store_device_exclusion())).await
    }

    pub async fn complete_outbound_store_device_exclusion_slot_loss(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        winner: DurableStoreDeviceExclusionObject,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let next = DurableStoreDeviceExclusionOperation::Completed(
            StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended: expected.object().clone(),
                winner,
            },
        );
        next.validate()
            .map_err(store_device_exclusion_journal_error)?;
        let remotes = expected
            .remote_objects()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.call_store(move |session| {
            session.complete_outbound_store_device_exclusion_slot_loss(expected, next, remotes)
        }))
        .await
    }

    pub async fn mark_store_device_exclusion_authority_uploaded(
        &self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<(), DbError> {
        let expected = operation
            .authority_remote_object()
            .map_err(store_device_exclusion_journal_error)?;
        let candidate = operation
            .candidate()
            .ok_or_else(|| {
                DbError::Message(
                    "Store-device exclusion authority has no current candidate".to_string(),
                )
            })?
            .reference
            .clone();
        self.call_store(move |session| {
            session.mark_store_device_exclusion_authority_uploaded(expected, candidate)
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn outbound_store_device_exclusion_operations(
        &self,
    ) -> Result<Vec<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(self.call_store(|session| session.outbound_store_device_exclusion_operations()))
            .await
    }
}
