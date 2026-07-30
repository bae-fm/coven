use crate::database::remote_object_records::record_reclaimed_store_package_on;

use super::*;

pub(crate) fn store_reclaim_journal_error(error: StoreReclaimJournalError) -> DbError {
    DbError::Message(error.to_string())
}

pub(crate) fn parse_store_reclaim_operation(
    operation_id: ObjectHash,
    raw: &str,
) -> Result<DurableStoreReclaimOperation, DbError> {
    let operation: DurableStoreReclaimOperation = serde_json::from_str(raw).map_err(|error| {
        DbError::Message(format!(
            "Store reclaim operation {operation_id} has invalid durable state: {error}"
        ))
    })?;
    operation.validate().map_err(store_reclaim_journal_error)?;
    if operation.operation_id() != operation_id {
        return Err(DbError::Message(format!(
            "Store reclaim operation key {operation_id} differs from its authorization {}",
            operation.operation_id()
        )));
    }
    Ok(operation)
}

pub(crate) fn load_store_reclaim_operation_on(
    conn: &Connection,
    operation_id: ObjectHash,
) -> Result<Option<DurableStoreReclaimOperation>, DbError> {
    conn.query_row(
        "SELECT state FROM store_reclaim_operations WHERE authorization_hash = ?1",
        [operation_id.to_string()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|raw| parse_store_reclaim_operation(operation_id, &raw))
    .transpose()
}

pub(crate) fn insert_store_reclaim_operation_on(
    conn: &Connection,
    operation: &DurableStoreReclaimOperation,
) -> Result<(), DbError> {
    operation.validate().map_err(store_reclaim_journal_error)?;
    let state = serde_json::to_string(operation)
        .map_err(|error| DbError::Message(format!("serialize Store reclaim operation: {error}")))?;
    conn.execute(
        "INSERT INTO store_reclaim_operations (authorization_hash, state) VALUES (?1, ?2)",
        (operation.operation_id().to_string(), state),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn update_store_reclaim_operation_on(
    conn: &Connection,
    expected: &DurableStoreReclaimOperation,
    next: &DurableStoreReclaimOperation,
) -> Result<(), DbError> {
    expected.validate().map_err(store_reclaim_journal_error)?;
    next.validate().map_err(store_reclaim_journal_error)?;
    if expected.operation_id() != next.operation_id() {
        return Err(DbError::Message(
            "Store reclaim transition changes its authorization identity".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected).map_err(|error| {
        DbError::Message(format!("serialize expected Store reclaim state: {error}"))
    })?;
    let next_state = serde_json::to_string(next).map_err(|error| {
        DbError::Message(format!("serialize next Store reclaim state: {error}"))
    })?;
    let updated = conn
        .execute(
            "UPDATE store_reclaim_operations SET state = ?3
             WHERE authorization_hash = ?1 AND state = ?2",
            (
                expected.operation_id().to_string(),
                expected_state,
                next_state,
            ),
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(
            "Store reclaim operation changed during transition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn record_store_reclaim_activation_on(
    conn: &Connection,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    activation: &ReclaimCommitActivation,
) -> Result<(), DbError> {
    activation.validate().map_err(store_reclaim_journal_error)?;
    if activation.commit() != commit_ref {
        return Err(DbError::Message(
            "Store reclaim activation evidence names another commit".to_string(),
        ));
    }
    if let Some(authorization) = commit.reclaim_authorization() {
        let operation_id = authorization.authorization_hash;
        let next = DurableStoreReclaimOperation::Authorized {
            authorization: authorization.clone(),
            activation: activation.clone(),
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        match load_store_reclaim_operation_on(conn, operation_id)? {
            Some(expected)
                if matches!(
                    &expected,
                    DurableStoreReclaimOperation::AuthorizationCandidate { object, .. }
                        | DurableStoreReclaimOperation::AuthorizationReplacing { object, .. }
                        if object.authorization_ref() == authorization
                ) =>
            {
                update_store_reclaim_operation_on(conn, &expected, &next)?;
            }
            Some(existing) if existing == next => {}
            Some(_) => {
                return Err(DbError::Message(
                    "reclaim authorization conflicts with its durable operation".to_string(),
                ));
            }
            None => insert_store_reclaim_operation_on(conn, &next)?,
        }
    }
    if let Some(receipt) = commit.reclaim_receipt() {
        let operation_id = receipt.authorization.authorization_hash;
        let expected = load_store_reclaim_operation_on(conn, operation_id)?.ok_or_else(|| {
            DbError::Message("reclaim receipt has no durable authorization".to_string())
        })?;
        let (authorization, authorization_activation) = match &expected {
            DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                return Err(DbError::Message(
                    "reclaim receipt precedes authorization activation".to_string(),
                ));
            }
            DurableStoreReclaimOperation::AuthorizationReplacing { .. } => {
                return Err(DbError::Message(
                    "reclaim receipt precedes replacement authorization activation".to_string(),
                ));
            }
            DurableStoreReclaimOperation::Authorized {
                authorization,
                activation,
            } => (authorization.clone(), activation.clone()),
            DurableStoreReclaimOperation::AbsentVerified {
                authorization,
                authorization_activation,
                ..
            } => (authorization.clone(), authorization_activation.clone()),
            DurableStoreReclaimOperation::ReceiptCandidate {
                authorization,
                authorization_activation,
                object,
                ..
            } if matches!(
                &**object,
                crate::sync::store::DurableStoreReclaimObject::Receipt {
                    receipt_ref,
                    ..
                } if receipt_ref == receipt
            ) =>
            {
                (authorization.clone(), authorization_activation.clone())
            }
            DurableStoreReclaimOperation::ReceiptReplacing {
                authorization,
                authorization_activation,
                object,
                ..
            } if matches!(
                &**object,
                crate::sync::store::DurableStoreReclaimObject::Receipt {
                    receipt_ref,
                    ..
                } if receipt_ref == receipt
            ) =>
            {
                (authorization.clone(), authorization_activation.clone())
            }
            DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                return Err(DbError::Message(
                    "reclaim receipt differs from its durable candidate".to_string(),
                ));
            }
            DurableStoreReclaimOperation::ReceiptReplacing { .. } => {
                return Err(DbError::Message(
                    "reclaim receipt differs from its replacement candidate".to_string(),
                ));
            }
            DurableStoreReclaimOperation::Completed { .. } => {
                return Err(DbError::Message(
                    "reclaim authorization already has a receipt".to_string(),
                ));
            }
        };
        let next = DurableStoreReclaimOperation::Completed {
            authorization: authorization.clone(),
            authorization_activation: authorization_activation.clone(),
            receipt: receipt.clone(),
            receipt_activation: activation.clone(),
        };
        let reclaimed = ReclaimedStorePackage::receipted(
            authorization,
            authorization_activation,
            receipt.clone(),
            activation.clone(),
        )
        .map_err(store_reclaim_journal_error)?;
        record_reclaimed_store_package_on(conn, &reclaimed)?;
        update_store_reclaim_operation_on(conn, &expected, &next)?;
    }
    Ok(())
}
