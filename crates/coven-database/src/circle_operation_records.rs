use super::*;

use coven_protocol::circle_journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationProgress, PreparedCircleOperation,
};

/// What `circle_operations.prepared` holds: the operation as prepared and the
/// intent that named it.
///
/// These two travel together because neither changes while the operation
/// publishes — the phase beside them and the upload rows below them are what
/// move. The objects appear here as references; their bytes are in the payload
/// spool.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedCircleOperationPayload {
    operation_id: coven_protocol::circle::CircleOperationId,
    circle_id: coven_protocol::circle::CircleId,
    intent: CircleOperationIntent,
    operation: PreparedCircleOperation,
}

pub struct PreparedCircleOperationRow {
    pub operation_id: String,
    pub circle_id: String,
    pub prepared: Vec<u8>,
    pub phase: String,
}

impl PreparedCircleOperationRow {
    pub fn from_journal(journal: &CircleOperationJournal) -> Result<Self, DbError> {
        journal
            .validate_identity()
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(Self {
            operation_id: journal.operation_id.as_str().to_string(),
            circle_id: journal.circle_id.to_string(),
            prepared: prepared_circle_operation_payload(journal)?,
            phase: circle_operation_phase_json(&journal.progress)?,
        })
    }
}

/// The bytes `circle_operations.prepared` holds for one operation.
pub(crate) fn prepared_circle_operation_payload(
    journal: &CircleOperationJournal,
) -> Result<Vec<u8>, DbError> {
    serde_json::to_vec(&PreparedCircleOperationPayload {
        operation_id: journal.operation_id.clone(),
        circle_id: journal.circle_id,
        intent: journal.intent.clone(),
        operation: journal.operation.clone(),
    })
    .map_err(|error| DbError::context("serialize prepared circle operation", error))
}

pub(crate) fn circle_operation_phase_json(
    progress: &CircleOperationProgress,
) -> Result<String, DbError> {
    serde_json::to_string(progress)
        .map_err(|error| DbError::context("serialize circle operation phase", error))
}

/// Rebuild one operation from the three places it is stored.
///
/// The stored ids are checked against the ones inside `prepared` rather than
/// trusted: the columns are what queries dispatch on, so a row whose payload
/// names a different operation or circle would route work at one identity and
/// perform it at another.
pub fn parse_circle_operation_row(
    stored_operation_id: &str,
    stored_circle_id: &str,
    prepared: &[u8],
    phase: &str,
    uploaded: BTreeSet<String>,
) -> Result<CircleOperationJournal, DbError> {
    let payload: PreparedCircleOperationPayload = serde_json::from_slice(prepared)
        .map_err(|error| DbError::context("parse prepared circle operation", error))?;
    let progress: CircleOperationProgress = serde_json::from_str(phase)
        .map_err(|error| DbError::context("parse circle operation phase", error))?;
    if payload.operation_id.as_str() != stored_operation_id {
        return Err(DbError::Message(format!(
            "circle operation id row names {stored_operation_id} but its payload operation id is {}",
            payload.operation_id
        )));
    }
    if payload.circle_id.to_string() != stored_circle_id {
        return Err(DbError::Message(format!(
            "circle operation {stored_operation_id} row names circle {stored_circle_id} but its payload circle id is {}",
            payload.circle_id
        )));
    }
    let journal = CircleOperationJournal {
        operation_id: payload.operation_id,
        circle_id: payload.circle_id,
        intent: payload.intent,
        operation: payload.operation,
        progress,
        uploaded,
    };
    journal
        .validate_identity()
        .map_err(|error| DbError::Message(error.to_string()))?;
    journal
        .validate_uploaded()
        .map_err(|error| DbError::Message(error.to_string()))?;
    Ok(journal)
}

pub fn circle_operation_uploaded_steps_on(
    conn: &Connection,
    operation_id: &str,
) -> Result<BTreeSet<String>, DbError> {
    crate::query_mapped_rows(
        conn,
        "SELECT step FROM circle_operation_uploads WHERE operation_id = ?1 ORDER BY step",
        [operation_id],
        |row| row.get::<_, String>(0),
    )
    .map_err(DbError::from)
    .map(BTreeSet::from_iter)
}

pub fn load_circle_operation_on(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<CircleOperationJournal>, DbError> {
    let Some((stored_operation_id, circle_id, prepared, phase)) = conn
        .query_row(
            "SELECT operation_id, circle_id, prepared, phase
             FROM circle_operations
             WHERE operation_id = ?1",
            [operation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?
    else {
        return Ok(None);
    };
    let uploaded = circle_operation_uploaded_steps_on(conn, &stored_operation_id)?;
    parse_circle_operation_row(
        &stored_operation_id,
        &circle_id,
        &prepared,
        &phase,
        uploaded,
    )
    .map(Some)
}

/// Every operation whose phase is one this caller acts on, oldest first.
///
/// The phase is its own column, so the filter runs before any operation is
/// parsed — a caller looking for the one discarding operation does not pay for
/// the prepared payload of every other.
pub(crate) fn circle_operation_ids_in_phase_on(
    conn: &Connection,
    accept: impl Fn(&CircleOperationProgress) -> bool,
) -> Result<Vec<String>, DbError> {
    let rows = crate::query_mapped_rows(
        conn,
        "SELECT operation_id, phase FROM circle_operations ORDER BY rowid",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .map_err(DbError::from)?;
    let mut matching = Vec::new();
    for (operation_id, phase) in rows {
        let progress: CircleOperationProgress = serde_json::from_str(&phase)
            .map_err(|error| DbError::context("parse circle operation phase", error))?;
        if accept(&progress) {
            matching.push(operation_id);
        }
    }
    Ok(matching)
}
