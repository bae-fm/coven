//! Cross-record and application-schema checks before merge state is touched.
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    merge_fields,
    write::{WriteHeader, WritePart},
};
use coven_merge::{Operation, WriteId};

pub(crate) fn past(database: &DatabaseConnection, header: &WriteHeader) -> Result<(), DbError> {
    let own = (header.position.number > 1).then(|| WriteId {
        number: header.position.number - 1,
        ..header.position
    });
    for frontier in header.had_read.0.iter().copied().chain(own) {
        let bytes: Vec<u8> = database.query_row(
            "SELECT had_read FROM _coven_writes WHERE substr(timestamp,9,8)=?1 AND number=?2",
            (
                frontier.device.0.to_be_bytes().as_slice(),
                frontier.number.to_be_bytes().as_slice(),
            ),
            |row| row.get(0),
        )?;
        let past = merge_fields::decode_write_positions(&bytes)?;
        if past.0.iter().any(|read| {
            !(header.had_read.covers(*read)
                || read.device == header.position.device && read.number < header.position.number)
        }) {
            return Err(coven_format::Error::Invalid {
                field: "write causal closure",
                rule: coven_format::error::Rule::Coverage,
            }
            .into());
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
