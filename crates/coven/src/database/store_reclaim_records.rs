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
