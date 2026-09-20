use coven_database::{ChangesetOperation, ChangesetRow};

pub(crate) fn qualify_title(row: &mut ChangesetRow) {
    match &mut row.change {
        ChangesetOperation::Insert(columns) | ChangesetOperation::Delete(columns) => {
            for column in columns.iter_mut().filter(|column| column.name == "title") {
                if let rusqlite::types::Value::Text(text) = &mut column.value {
                    *text = format!("migrated:{text}");
                }
            }
        }
        ChangesetOperation::Update(columns) => {
            for column in columns.iter_mut().filter(|column| column.name == "title") {
                for value in [&mut column.value.old, &mut column.value.new] {
                    if let Some(rusqlite::types::Value::Text(text)) = value {
                        *text = format!("migrated:{text}");
                    }
                }
            }
        }
    }
}
