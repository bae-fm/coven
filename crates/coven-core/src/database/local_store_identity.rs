use super::*;

pub(crate) fn local_store_authority_on(
    conn: &Connection,
) -> Result<
    (
        crate::sync::store_commit::StoreRootRef,
        StoreDeviceRegistrationRef,
        StoreDeviceRegistration,
    ),
    DbError,
> {
    let root = required_store_root_authority_on(conn)?;
    let reference = local_activated_registration_ref_on(conn)?.ok_or_else(|| {
        DbError::Message("local Store device has no activated registration".to_string())
    })?;
    let (bytes, encoded): (Vec<u8>, String) = conn
        .query_row(
            "SELECT registration_bytes, registration_object \
             FROM store_device_registration_activations WHERE device_id = ?1",
            [reference.device_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    let stored_reference: StoreDeviceRegistrationRef =
        serde_json::from_str(&encoded).map_err(|error| {
            DbError::Message(format!("local Store registration exact ref: {error}"))
        })?;
    if stored_reference != reference {
        return Err(DbError::Message(
            "local Store registration changed during exact load".to_string(),
        ));
    }
    let registration = StoreDeviceRegistration::parse_at(&bytes, &root, reference.device_id)
        .map_err(|error| DbError::Message(format!("local Store registration: {error}")))?;
    reference
        .verify_registration(&registration)
        .map_err(|error| DbError::Message(error.to_string()))?;
    Ok((root, reference, registration))
}

pub(crate) fn local_activated_registration_ref_on(
    conn: &Connection,
) -> Result<Option<StoreDeviceRegistrationRef>, DbError> {
    let device_id: Option<String> = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [LOCAL_DEVICE_ID_STATE_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(device_id) = device_id else {
        return Ok(None);
    };
    let encoded: Option<String> = conn
        .query_row(
            "SELECT registration_object FROM store_device_registration_activations \
             WHERE device_id = ?1",
            [&device_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(encoded) = encoded else {
        return Ok(None);
    };
    let reference: StoreDeviceRegistrationRef =
        serde_json::from_str(&encoded).map_err(|error| {
            DbError::Message(format!("local Store registration exact ref: {error}"))
        })?;
    if reference.device_id.to_string() != device_id {
        return Err(DbError::Message(
            "local Store device state differs from its activated registration".to_string(),
        ));
    }
    Ok(Some(reference))
}

pub(crate) fn local_merge_stream_id_on(conn: &Connection) -> Result<Option<String>, DbError> {
    let local_device_id = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [LOCAL_DEVICE_ID_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(_) = local_device_id else {
        return Ok(None);
    };
    let (root, reference, _) = local_store_authority_on(conn)?;
    Ok(Some(
        crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &reference,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
        .to_string(),
    ))
}

pub(super) fn pin_host_device_id_on(
    conn: &Connection,
    expected: &str,
    initialized: bool,
) -> Result<(), DbError> {
    if initialized {
        return validate_host_device_id_on(conn, expected);
    }
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (HOST_DEVICE_ID_STATE_KEY, expected),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(super) fn validate_host_device_id_on(conn: &Connection, expected: &str) -> Result<(), DbError> {
    let stored = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [HOST_DEVICE_ID_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| DbError::Message("Coven host device identity is absent".to_string()))?;
    if stored != expected {
        return Err(DbError::Message(format!(
            "Coven host device identity is {stored:?}, not configured identity {expected:?}"
        )));
    }
    Ok(())
}
