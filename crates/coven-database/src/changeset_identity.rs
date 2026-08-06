use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ChangesetIter};
use rusqlite::types::ValueRef;

use coven_protocol::synced_schema::{RowIdentityError, SyncedTable};

#[derive(Debug, thiserror::Error)]
pub enum ChangesetIdentityError {
    #[error("changeset row identity validation failed: {0}")]
    Parse(String),
    #[error("changeset contains undeclared table {0:?}")]
    UndeclaredTable(String),
    #[error(transparent)]
    Row(#[from] RowIdentityError),
}

pub(crate) fn validate_changeset_row_identities(
    bytes: &[u8],
    tables: &[SyncedTable],
) -> Result<(), ChangesetIdentityError> {
    if bytes.is_empty() {
        return Ok(());
    }

    let input: &mut dyn std::io::Read = &mut &bytes[..];
    let mut iter = ChangesetIter::start_strm(&input)
        .map_err(|error| ChangesetIdentityError::Parse(error.to_string()))?;
    while let Some(item) = iter
        .next()
        .map_err(|error| ChangesetIdentityError::Parse(error.to_string()))?
    {
        let op = item
            .op()
            .map_err(|error| ChangesetIdentityError::Parse(error.to_string()))?;
        let table_name = op.table_name();
        let table = tables
            .iter()
            .find(|table| table.name() == table_name)
            .ok_or_else(|| ChangesetIdentityError::UndeclaredTable(table_name.to_string()))?;
        match op.code() {
            Action::SQLITE_INSERT => {
                let id = required_changeset_id(item, table_name, "new", ChangesetSide::New)?;
                table.row_identity().validate(table_name, &id)?;
            }
            Action::SQLITE_DELETE => {
                let id = required_changeset_id(item, table_name, "old", ChangesetSide::Old)?;
                table.row_identity().validate(table_name, &id)?;
            }
            Action::SQLITE_UPDATE => {
                let old = required_changeset_id(item, table_name, "old", ChangesetSide::Old)?;
                let id = optional_changeset_id(item, table_name, "new", ChangesetSide::New)?
                    .unwrap_or(old);
                table.row_identity().validate(table_name, &id)?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ChangesetSide {
    Old,
    New,
}

fn required_changeset_id(
    item: &ChangesetItem,
    table: &str,
    side_name: &'static str,
    side: ChangesetSide,
) -> Result<String, RowIdentityError> {
    optional_changeset_id(item, table, side_name, side)?.ok_or_else(|| {
        RowIdentityError::MissingPrimaryKey {
            table: table.to_string(),
            side: side_name,
        }
    })
}

fn optional_changeset_id(
    item: &ChangesetItem,
    table: &str,
    side_name: &'static str,
    side: ChangesetSide,
) -> Result<Option<String>, RowIdentityError> {
    let value = match side {
        ChangesetSide::Old => item.old_value(0),
        ChangesetSide::New => item.new_value(0),
    };
    let value = match value {
        Ok(value) => value,
        Err(rusqlite::Error::InvalidColumnIndex(_)) => return Ok(None),
        Err(error) => {
            return Err(RowIdentityError::NonUtf8PrimaryKey {
                table: table.to_string(),
                reason: error.to_string(),
            })
        }
    };
    let ValueRef::Text(bytes) = value else {
        return Err(RowIdentityError::NonTextPrimaryKey {
            table: table.to_string(),
            side: side_name,
        });
    };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map(Some)
        .map_err(|error| RowIdentityError::NonUtf8PrimaryKey {
            table: table.to_string(),
            reason: error.to_string(),
        })
}
