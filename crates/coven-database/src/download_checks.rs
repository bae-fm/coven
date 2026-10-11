//! Cross-record and application-schema checks before merge state is touched.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    merge_fields,
    write::{WriteHeader, WritePart},
};
use coven_merge::{Operation, WritePast};

pub(crate) fn past(database: &DatabaseConnection, header: &WriteHeader) -> Result<(), DbError> {
    let causal_past = header.had_read.causal_past(header.position);
    for frontier in causal_past.frontier() {
        let bytes: Vec<u8> = database.query_row(
            &format!(
                "SELECT had_read FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2"
            ),
            (
                frontier.device.0.to_be_bytes().as_slice(),
                frontier.number.to_be_bytes().as_slice(),
            ),
            |row| row.get(0),
        )?;
        let past = merge_fields::decode_write_positions(&bytes)?;
        if past.0.iter().any(|read| !causal_past.contains(read)) {
            return Err(DbError::InvalidWrite {
                write: header.position,
                error: coven_merge::MergeError::CausalClosure(header.position),
            });
        }
    }
    Ok(())
}

pub(crate) fn part(schema: &WriteSchema, part: &WritePart) -> Result<(), DbError> {
    for change in &part.rows {
        match &change.change.operation {
            Operation::Insert(columns) | Operation::Update(columns) => {
                schema.row_columns(
                    &change.row,
                    matches!(change.change.operation, Operation::Insert(_)),
                    columns,
                )?;
            }
            Operation::Delete => {
                schema.row_table(&change.row)?;
            }
        }
    }
    Ok(())
}
