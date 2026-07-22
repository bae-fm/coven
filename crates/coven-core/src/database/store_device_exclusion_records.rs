use super::*;

pub(super) fn store_device_exclusion_journal_error(
    error: StoreDeviceExclusionJournalError,
) -> DbError {
    DbError::Message(error.to_string())
}

pub(super) fn parse_store_device_exclusion_operation(
    operation_id: ObjectHash,
    raw: &str,
) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
    let operation: DurableStoreDeviceExclusionOperation =
        serde_json::from_str(raw).map_err(|error| {
            DbError::Message(format!(
                "Store-device exclusion operation {operation_id} has invalid durable state: {error}"
            ))
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

pub(super) fn load_store_device_exclusion_on(
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

pub(super) fn load_active_store_device_exclusion_on(
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
        let operation_id = raw_id.parse::<ObjectHash>().map_err(|error| {
            DbError::Message(format!("Store-device exclusion operation id: {error}"))
        })?;
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

pub(super) fn insert_store_device_exclusion_on(
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
    let encoded = serde_json::to_string(operation).map_err(|error| {
        DbError::Message(format!(
            "serialize Store-device exclusion operation: {error}"
        ))
    })?;
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

pub(super) fn require_store_device_exclusion_transition_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    next: &DurableStoreDeviceExclusionOperation,
) -> Result<(), DbError> {
    if !expected.allows_transition_to(next) {
        return Err(DbError::Message(
            "invalid Store-device exclusion journal transition".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected).map_err(|error| {
        DbError::Message(format!(
            "serialize expected Store-device exclusion: {error}"
        ))
    })?;
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

pub(super) fn update_store_device_exclusion_on(
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
    let expected_state = serde_json::to_string(expected).map_err(|error| {
        DbError::Message(format!(
            "serialize expected Store-device exclusion: {error}"
        ))
    })?;
    let next_state = serde_json::to_string(next).map_err(|error| {
        DbError::Message(format!("serialize next Store-device exclusion: {error}"))
    })?;
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
