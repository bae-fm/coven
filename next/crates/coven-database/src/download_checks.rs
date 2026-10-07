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
            "SELECT had_read FROM coven_writes WHERE substr(timestamp,9,8)=?1 AND number=?2",
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
        let table = schema.row_table(&change.row)?;
        let invalid = || {
            crate::snapshot_error::invalid(
                "write columns or references disagree with the application schema",
            )
        };
        let columns = match &change.change.operation {
            Operation::Insert(columns) | Operation::Update(columns) => columns,
            Operation::Delete => continue,
        };
        for (name, value) in columns {
            if !table.columns.iter().any(|column| &column.name == name) {
                return Err(invalid());
            }
            for (key, parent) in &value.parents {
                if !key.columns.0.contains(name)
                    || parent.row.table != key.parent
                    || !table
                        .foreign_keys
                        .iter()
                        .any(|foreign| schema.foreign_key(table, foreign) == *key)
                {
                    return Err(invalid());
                }
                schema.row_table(&parent.row)?;
            }
        }
        let keys = coven_format::key::decode_key(&change.row.key)?;
        let mut values: crate::write_rows::AppValues = crate::write_rows::key_columns(table)
            .into_iter()
            .map(|column| column.name.clone())
            .zip(keys)
            .collect();
        for column in crate::write_rows::key_columns(table) {
            if let Some(value) = columns.get(&column.name) {
                values.insert(column.name.clone(), value.value.clone());
            } else if matches!(change.change.operation, Operation::Insert(_)) {
                return Err(invalid());
            }
        }
        if crate::write_rows::row_key(table, &values)? != change.row.key {
            return Err(invalid());
        }
        if let crate::declaration::AudienceSource::Column(name) =
            &schema.declaration(&table.name).audience
        {
            if columns.get(name).is_some_and(|value| {
                value.value
                    != coven_format::value::Value::Text(crate::write_encoding::audience_text(
                        &change.row.audience,
                    ))
            }) {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
