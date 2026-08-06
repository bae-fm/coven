use super::*;

pub struct PreparedCircleOperationRow {
    pub operation_id: String,
    pub circle_id: String,
    pub payload: Vec<u8>,
}

impl PreparedCircleOperationRow {
    pub fn from_journal(
        journal: coven_protocol::circle_journal::CircleOperationJournal,
    ) -> Result<Self, DbError> {
        journal
            .validate_identity()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let operation_id = journal.operation_id.as_str().to_string();
        let circle_id = journal.circle_id.to_string();
        let payload = serde_json::to_vec(&journal)
            .map_err(|error| DbError::context("serialize circle operation journal", error))?;
        Ok(Self {
            operation_id,
            circle_id,
            payload,
        })
    }
}

pub fn parse_circle_operation_row(
    stored_operation_id: &str,
    stored_circle_id: &str,
    payload: &[u8],
) -> Result<coven_protocol::circle_journal::CircleOperationJournal, DbError> {
    let journal: coven_protocol::circle_journal::CircleOperationJournal =
        serde_json::from_slice(payload)
            .map_err(|error| DbError::context("parse circle operation journal", error))?;
    journal
        .validate_identity()
        .map_err(|error| DbError::Message(error.to_string()))?;
    if journal.operation_id.as_str() != stored_operation_id {
        return Err(DbError::Message(format!(
            "circle operation id row names {stored_operation_id} but its payload operation id is {}",
            journal.operation_id
        )));
    }
    if journal.circle_id.to_string() != stored_circle_id {
        return Err(DbError::Message(format!(
            "circle operation {stored_operation_id} row names circle {stored_circle_id} but its payload circle id is {}",
            journal.circle_id
        )));
    }
    Ok(journal)
}

pub fn load_circle_operation_on(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<coven_protocol::circle_journal::CircleOperationJournal>, DbError> {
    conn.query_row(
        "SELECT operation_id, circle_id, payload
         FROM circle_operations
         WHERE operation_id = ?1",
        [operation_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(operation_id, circle_id, payload)| {
        parse_circle_operation_row(&operation_id, &circle_id, &payload)
    })
    .transpose()
}
