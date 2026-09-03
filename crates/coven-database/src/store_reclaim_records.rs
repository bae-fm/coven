use super::*;

pub fn store_reclaim_journal_error(error: StoreReclaimJournalError) -> DbError {
    DbError::from(error)
}

pub fn parse_store_reclaim_operation(
    operation_id: ObjectHash,
    raw: &str,
) -> Result<DurableStoreReclaimOperation, DbError> {
    let operation: DurableStoreReclaimOperation = serde_json::from_str(raw).map_err(|error| {
        DbError::context(
            format!("Store reclaim operation {operation_id} has invalid durable state"),
            error,
        )
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
        .map_err(|error| DbError::context("serialize Store reclaim operation", error))?;
    conn.execute(
        "INSERT INTO store_reclaim_operations (authorization_hash, state) VALUES (?1, ?2)",
        (operation.operation_id().to_string(), state),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

/// Record why an operation cannot proceed, so every later cycle skips it.
pub(crate) fn mark_store_reclaim_operation_stuck_on(
    conn: &Connection,
    operation_id: ObjectHash,
    error: &str,
) -> Result<(), DbError> {
    // A stuck operation is one a person has to read about, so the mark carries
    // a message even when the error's own display is empty.
    let error = if error.trim().is_empty() {
        format!("Store reclaim operation {operation_id} failed without a message")
    } else {
        error.to_string()
    };
    let updated = conn
        .execute(
            "UPDATE store_reclaim_operations SET stuck_error = ?2 WHERE authorization_hash = ?1",
            (operation_id.to_string(), error),
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(format!(
            "Store reclaim operation {operation_id} is absent and cannot be marked stuck"
        )));
    }
    Ok(())
}

/// Clear a stuck mark so the next cycle runs the operation again. Refuses an
/// operation that is not stuck, so a retry the host sends twice cannot pass as
/// a second decision.
pub(crate) fn clear_store_reclaim_operation_stuck_on(
    conn: &Connection,
    operation_id: ObjectHash,
) -> Result<(), DbError> {
    let updated = conn
        .execute(
            "UPDATE store_reclaim_operations SET stuck_error = NULL
             WHERE authorization_hash = ?1 AND stuck_error IS NOT NULL",
            [operation_id.to_string()],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(format!(
            "Store reclaim operation {operation_id} is not stuck"
        )));
    }
    Ok(())
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
    let expected_state = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize expected Store reclaim state", error))?;
    let next_state = serde_json::to_string(next)
        .map_err(|error| DbError::context("serialize next Store reclaim state", error))?;
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
