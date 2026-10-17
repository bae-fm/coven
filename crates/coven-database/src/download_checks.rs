//! Cross-record and application-schema checks before merge state is touched.
use crate::{write_schema::WriteSchema, DbError};
use coven_format::write::WritePart;
use coven_merge::Operation;

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
