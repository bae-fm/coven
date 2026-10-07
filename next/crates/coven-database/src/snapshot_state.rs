//! Replace one audience's records inside the loading transaction.

use crate::merge_store::StoredRow;
use crate::snapshot_error::invalid;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    merge_fields,
    snapshot_rows::{MergeRow, SyncedRow},
    value::Value,
};
use coven_merge::{Audience, LostChange, RowId, RowState, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn begin(
    database: &DatabaseConnection,
    audience: &Audience,
) -> Result<BTreeSet<RowId>, DbError> {
    let audience = audience_text(audience);
    let touched = database
        .query(
            "SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE audience=?1",
            [&audience],
            crate::row_queries::read_identity,
        )?
        .into_iter()
        .collect();
    for table in ["coven_references", "coven_cells", "coven_claims"] {
        database.internal_execute(
            &format!(
                "DELETE FROM {table} WHERE row_id IN (SELECT id FROM coven_rows WHERE audience=?1)"
            ),
            [&audience],
        )?;
    }
    for table in [
        "coven_rows",
        "coven_lost",
        "coven_excluded_writes",
        "coven_fingerprint_leaves",
        "coven_fingerprint_sums",
    ] {
        database.internal_execute(
            &format!("DELETE FROM {table} WHERE audience=?1"),
            [&audience],
        )?;
    }

    Ok(touched)
}

pub(crate) fn create_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE coven_snapshot_values(audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,columns BLOB NOT NULL,matched INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(audience,table_name,key)) WITHOUT ROWID;
        CREATE TEMP TABLE coven_snapshot_writes(device BLOB NOT NULL,number BLOB NOT NULL,timestamp BLOB NOT NULL,had_read BLOB NOT NULL,PRIMARY KEY(device,number)) WITHOUT ROWID;
        CREATE TEMP TABLE coven_snapshot_columns(table_name TEXT NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(table_name,column_name)) WITHOUT ROWID;")?;
    Ok(())
}

pub(crate) fn synced(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    mut row: SyncedRow,
    additions: bool,
) -> Result<(), DbError> {
    let table = table(schema, &row.row)?;
    let values = row
        .columns
        .iter()
        .map(|(name, value)| (name.clone(), value.value.clone()))
        .collect();
    if row
        .columns
        .keys()
        .any(|name| !table.columns.iter().any(|column| column.name == *name))
    {
        return Err(invalid("synced row names an unknown column"));
    }
    let evaluated = crate::removal_sql::evaluate_values(database, table, &values)?;
    if values
        .iter()
        .any(|(name, value)| evaluated.get(name) != Some(value))
        || (!additions && values.len() != evaluated.len())
        || crate::write_rows::row_key(table, &evaluated)? != row.row.key
    {
        return Err(invalid("synced values disagree with their key or schema"));
    }
    for (name, value) in evaluated {
        row.columns.entry(name).or_insert(coven_merge::ColumnValue {
            value,
            parents: Default::default(),
        });
    }
    database.internal_execute(
        "INSERT INTO temp.coven_snapshot_values(table_name,key,columns,audience) VALUES(?1,?2,?3,?4)",
        params![
            row.row.table,
            row.row.key,
            encoded(merge_fields::encode_columns(&row.columns))?,
            audience_text(&row.row.audience)
        ],
    )?;
    Ok(())
}

pub(crate) fn table<'a>(
    schema: &'a WriteSchema,
    row: &RowId,
) -> Result<&'a crate::schema::TableSchema, DbError> {
    let declared = schema
        .declarations
        .iter()
        .any(|declaration| declaration.name == row.table);
    if !declared {
        return Err(invalid("row names a table that does not sync"));
    }
    let table = schema.table(&row.table);
    let key = decoded(coven_format::key::decode_key(&row.key))?;
    if key.len() != crate::write_rows::key_columns(table).len() {
        return Err(invalid("row key has the wrong number of columns"));
    }
    let values = crate::write_rows::key_columns(table)
        .into_iter()
        .map(|column| column.name.clone())
        .zip(key)
        .collect();
    crate::write_capture::validate_key(schema, table, &values)?;
    if matches!(
        schema.declaration(&row.table).audience,
        crate::declaration::AudienceSource::Store
    ) && row.audience != Audience::Store
    {
        return Err(invalid("store table is in a circle snapshot"));
    }
    Ok(table)
}

pub(crate) fn merged(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    merged: MergeRow,
) -> Result<RowId, DbError> {
    let state = &merged.state;
    let row = state.row();
    let table = table(schema, row)?;
    if state.generations().is_empty() {
        return Err(invalid("merge row has no generations"));
    }
    for (name, value) in state
        .cells()
        .iter()
        .map(|(name, cell)| (name, &cell.value))
        .chain(
            state
                .lost()
                .iter()
                .map(|(key, lost)| (&key.column, &lost.value)),
        )
    {
        if !table.columns.iter().any(|column| column.name == *name) {
            return Err(invalid("merge record names an unknown column"));
        }
        let declared: bool=database.query_row("SELECT EXISTS(SELECT 1 FROM temp.coven_snapshot_columns WHERE table_name=?1 AND column_name=?2)",params![row.table,name],|r| r.get(0))?;
        if !declared {
            return Err(invalid("merge cell has no column record"));
        }
        for (key, parent) in &value.parents {
            if !key.columns.0.contains(name)
                || !table
                    .foreign_keys
                    .iter()
                    .any(|fk| schema.foreign_key(table, fk) == *key)
                || parent.row.table != key.parent
            {
                return Err(invalid(
                    "reference does not match the application foreign key",
                ));
            }
            self::table(schema, &parent.row)?;
        }
    }
    if state.present() {
        if crate::write_rows::key_columns(table)
            .iter()
            .any(|column| !state.cells().contains_key(&column.name))
            || table
                .foreign_keys
                .iter()
                .flat_map(|fk| &fk.columns)
                .any(|name| !state.cells().contains_key(name))
        {
            return Err(invalid("present row is missing a key or reference cell"));
        }
        let values = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
            .collect();
        if crate::write_rows::row_key(table, &values)? != row.key {
            return Err(invalid("merge cells disagree with their row key"));
        }
        if let crate::declaration::AudienceSource::Column(column) =
            &schema.declaration(&row.table).audience
        {
            if values.get(column) != Some(&Value::Text(audience_text(&row.audience))) {
                return Err(invalid("merge cells disagree with their audience"));
            }
        }
    }
    if let crate::declaration::AudienceSource::ForeignKey(column) =
        &schema.declaration(&row.table).audience
    {
        if state.present() {
            let key = table
                .foreign_keys
                .iter()
                .find(|fk| {
                    fk.columns
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(column))
                })
                .expect("declared audience reference");
            let key = schema.foreign_key(table, key);
            let parent = state.cells()[column]
                .value
                .parents
                .get(&key)
                .ok_or_else(|| invalid("audience reference has no parent"))?;
            if parent.row.audience != row.audience {
                return Err(invalid("inherited audience differs from its parent"));
            }
        }
    }
    let synced = database
        .query(
            "SELECT columns FROM temp.coven_snapshot_values WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
            |r| decoded(merge_fields::decode_columns(&r.get::<_, Vec<u8>>(0)?)),
        )?
        .into_iter()
        .next();
    if state.present() && merged.removed.is_empty() {
        let synced = synced.ok_or_else(|| invalid("visible merge row has no synced row"))?;
        if state
            .cells()
            .iter()
            .any(|(name, cell)| synced.get(name) != Some(&cell.value))
        {
            return Err(invalid("synced and merge values disagree"));
        }
        let raw = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
            .collect();
        let evaluated = crate::removal_sql::evaluate_values(database, table, &raw)?;
        if synced
            .iter()
            .any(|(name, value)| evaluated.get(name) != Some(&value.value))
        {
            return Err(invalid("synced values disagree with merge defaults"));
        }
        database.internal_execute(
            "UPDATE temp.coven_snapshot_values SET matched=1 WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
        )?;
    } else if synced.is_some() {
        return Err(invalid("hidden merge row also has a synced row"));
    }
    let removed = if merged.removed.is_empty() {
        None
    } else {
        let columns = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.clone()))
            .collect();
        let setters = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.write))
            .collect();
        Some((
            state.generation(),
            encoded(merge_fields::encode_columns(&columns))?,
            encoded(merge_fields::encode_setters(&setters))?,
            encoded(merge_fields::encode_rules(&merged.removed))?,
        ))
    };
    let row = row.clone();
    let lost_changes = state
        .lost()
        .iter()
        .map(|(key, value)| LostChange::Put(key.clone(), value.clone()))
        .collect();
    let update = RowUpdate {
        state: merged.state,
        lost_changes,
    };
    crate::write_commit::persist(
        database,
        &BTreeMap::from([(row.clone(), update)]),
        |row| {
            Ok(StoredRow {
                state: RowState::new(row.clone()),
                ordinal: None,
                loss: None,
                lost_ids: BTreeMap::new(),
            })
        },
        |write| ordinal(database, write),
    )?;
    if let Some((generation, columns, setters, rules)) = removed {
        database.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by) VALUES(?1,?2,?3,?4,?5,?6,'rules',?7)",params![row.table,row.key,audience_text(&row.audience),generation.to_be_bytes().as_slice(),columns,setters,rules])?;
    }
    Ok(row)
}

pub(crate) fn retained(
    database: &DatabaseConnection,
    loss: coven_format::retained_loss::RetainedLoss,
) -> Result<(), DbError> {
    use coven_format::retained_loss::RetainedValues;
    let row = loss.row;
    let (generation, column, value, setter, kind, cause) = match loss.values {
        RetainedValues::Cell { key, value } => (
            value.incarnation,
            Some(crate::write_commit::column(
                database,
                &row.table,
                &key.column,
            )?),
            encoded(merge_fields::encode_column_value(&value.value))?,
            encoded(merge_fields::encode_write_id(&key.write))?,
            "write",
            encoded(merge_fields::encode_write_id(&value.replaced_by))?,
        ),
        RetainedValues::Row {
            generation,
            cells,
            replaced_by,
        } => {
            let values = cells
                .iter()
                .map(|(name, cell)| (name.clone(), cell.value.clone()))
                .collect();
            let setters = cells
                .iter()
                .map(|(name, cell)| (name.clone(), cell.write))
                .collect();
            (
                generation,
                None,
                encoded(merge_fields::encode_columns(&values))?,
                encoded(merge_fields::encode_setters(&setters))?,
                "rules",
                encoded(merge_fields::encode_rules(&replaced_by))?,
            )
        }
    };
    let ordinal=database.query_row("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,retired) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,1) RETURNING id",params![row.table,row.key,audience_text(&row.audience),generation.to_be_bytes().as_slice(),column,value,setter,kind,cause],|r|r.get(0))?;
    crate::fingerprint::retained(database, ordinal)
}

pub(crate) fn ordinal(database: &DatabaseConnection, id: WriteId) -> Result<i64, DbError> {
    database.query_row(
        "SELECT id FROM coven_writes WHERE substr(timestamp,9,8)=?1 AND number=?2",
        params![
            id.device.0.to_be_bytes().as_slice(),
            id.number.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )
}

pub(crate) fn finish(database: &DatabaseConnection) -> Result<(), DbError> {
    let unmatched: bool = database.query_row(
        "SELECT EXISTS(SELECT 1 FROM temp.coven_snapshot_values WHERE matched=0)",
        [],
        |r| r.get(0),
    )?;
    if unmatched {
        return Err(invalid("synced row has no merge record"));
    }
    Ok(())
}

pub(crate) fn values(
    database: &DatabaseConnection,
    row: &RowId,
) -> Result<crate::write_rows::AppValues, DbError> {
    database.query_row(
        "SELECT columns FROM temp.coven_snapshot_values WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, audience_text(&row.audience)],
        |r| {
            Ok(
                decoded(merge_fields::decode_columns(&r.get::<_, Vec<u8>>(0)?))?
                    .into_iter()
                    .map(|(name, value)| (name, value.value))
                    .collect(),
            )
        },
    )
}

pub(crate) fn drop_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("DROP TABLE temp.coven_snapshot_values; DROP TABLE temp.coven_snapshot_writes; DROP TABLE temp.coven_snapshot_columns")
}
