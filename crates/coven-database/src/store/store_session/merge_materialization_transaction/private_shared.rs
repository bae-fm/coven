use super::*;
use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::ChangesetIter;
use rusqlite::types::{Value, ValueRef};

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExactSqlValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ExactSqlValue {
    fn bind_value(&self) -> Result<Value, DbError> {
        match self {
            Self::Null => Ok(Value::Null),
            Self::Integer(value) => Ok(Value::Integer(*value)),
            Self::Real(bits) => Ok(Value::Real(f64::from_bits(*bits))),
            Self::Text(bytes) => std::str::from_utf8(bytes)
                .map(|value| Value::Text(value.to_string()))
                .map_err(|error| DbError::context("bind accepted TEXT metadata", error)),
            Self::Blob(bytes) => Ok(Value::Blob(bytes.clone())),
        }
    }
}

#[derive(Clone)]
pub(super) struct PrivateRowState {
    columns: Vec<ExactSqlValue>,
}

impl MergeMaterializationTransaction<'_, '_> {
    pub(super) fn capture_replay_rows_inner(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
    ) -> Result<ReplayRows, DbError> {
        let keys = gates.private_rows(self.store.transaction)?;
        let private = keys
            .into_iter()
            .map(|key| {
                let columns = self.row_columns(schema, &key.0, &key.1)?.ok_or_else(|| {
                    DbError::Message(format!(
                        "private row {}/{} disappeared while its materialization began",
                        key.0, key.1
                    ))
                })?;
                Ok((key, PrivateRowState { columns }))
            })
            .collect::<Result<_, DbError>>()?;
        Ok(ReplayRows {
            private,
            adopted_by: BTreeMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn adopt_equivalent_private_rows(
        &self,
        gates: &crate::Gates,
        blob_decls: &BlobDecls,
        schema: &TableSchema,
        replay_rows: &ReplayRows,
        effective_changeset: &[u8],
        package: &AudiencePackage,
        commit: &StoreBatchCommitRef,
        own_publication: bool,
        adopted: &mut BTreeSet<(String, String)>,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let exact_inserts = exact_insert_rows(effective_changeset)?;
        for change in crate::walk_changeset(effective_changeset).map_err(DbError::Changeset)? {
            if crate::is_routing_table(&change.table) {
                continue;
            }
            let Some(row_id) = change.pk() else {
                continue;
            };
            let key = (change.table.clone(), row_id.to_string());
            let Some(private) = replay_rows.private.get(&key) else {
                continue;
            };
            if own_publication {
                adopted.insert(key);
                continue;
            }
            if change.op != coven_foundation::changeset::ChangeOp::Insert
                || !self.private_row_is_equivalent(
                    gates,
                    blob_decls,
                    schema,
                    private,
                    exact_inserts.get(&key).ok_or_else(|| {
                        DbError::Message(format!(
                            "effective incoming INSERT {}/{} has no exact row image",
                            change.table, row_id
                        ))
                    })?,
                    &change,
                    package,
                )?
            {
                return Ok(Some(private_shared_hold(key, commit)));
            }
            self.install_accepted_row_metadata(
                gates,
                schema,
                &change.table,
                row_id,
                exact_inserts.get(&key).expect("exact INSERT checked above"),
            )?;
            adopted.insert(key);
        }
        Ok(None)
    }

    pub(super) fn record_adopted_rows(
        replay_rows: &mut ReplayRows,
        adopted: &BTreeSet<(String, String)>,
        commit: &StoreBatchCommitRef,
    ) {
        for key in adopted {
            replay_rows.private.remove(key);
            replay_rows.adopted_by.insert(key.clone(), commit.clone());
        }
    }

    pub(super) fn record_accepted_rows(
        &self,
        gates: &crate::Gates,
        replay_rows: &mut ReplayRows,
        rows: &[WinningRow],
        commit: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let shared = gates.shared_rows(self.store.transaction)?;
        for row in rows {
            if crate::is_routing_table(&row.table) || !gates.row_can_be_private(&row.table) {
                continue;
            }
            let key = (row.table.clone(), row.row_id.clone());
            if row.row_stamp.is_some() && shared.contains(&row.table, &row.row_id)? {
                replay_rows.adopted_by.insert(key, commit.clone());
            } else {
                replay_rows.adopted_by.remove(&key);
            }
        }
        Ok(())
    }

    pub(super) fn record_private_row(
        &self,
        schema: &TableSchema,
        replay_rows: &mut ReplayRows,
        table: &str,
        row_id: &str,
    ) -> Result<(), DbError> {
        let columns = self.row_columns(schema, table, row_id)?.ok_or_else(|| {
            DbError::Message(format!(
                "local replay row {table}/{row_id} disappeared after application"
            ))
        })?;
        let key = (table.to_string(), row_id.to_string());
        replay_rows.adopted_by.remove(&key);
        replay_rows.private.insert(key, PrivateRowState { columns });
        Ok(())
    }

    pub(super) fn validate_private_rows_unchanged(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        before: &ReplayRows,
        adopted: &BTreeSet<(String, String)>,
        commit: &StoreBatchCommitRef,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let private_after = gates.private_rows(self.store.transaction)?;
        for (key, prior) in &before.private {
            if adopted.contains(key) {
                continue;
            }
            let unchanged = private_after.contains(key)
                && self
                    .row_columns(schema, &key.0, &key.1)?
                    .is_some_and(|columns| columns == prior.columns);
            if !unchanged {
                return Ok(Some(private_shared_hold(key.clone(), commit)));
            }
        }
        Ok(None)
    }

    pub(super) fn validate_shared_rows_do_not_borrow_private_state(
        &self,
        gates: &crate::Gates,
        replay_rows: &ReplayRows,
        adopted: &BTreeSet<(String, String)>,
        commit: &StoreBatchCommitRef,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let shared = gates.shared_rows(self.store.transaction)?;
        for key in replay_rows.private.keys() {
            if !adopted.contains(key) && shared.contains(&key.0, &key.1)? {
                return Ok(Some(private_shared_hold(key.clone(), commit)));
            }
        }
        Ok(None)
    }

    fn private_row_is_equivalent(
        &self,
        gates: &crate::Gates,
        blob_decls: &BlobDecls,
        schema: &TableSchema,
        private: &PrivateRowState,
        incoming_columns: &[ExactSqlValue],
        change: &RowChange,
        package: &AudiencePackage,
    ) -> Result<bool, DbError> {
        let columns = schema.columns(&change.table).ok_or_else(|| {
            DbError::Message(format!("synced table {} has no column map", change.table))
        })?;
        if incoming_columns.len() != columns.len() || private.columns.len() != columns.len() {
            return Ok(false);
        }
        let updated_at = schema.updated_at(&change.table).ok_or_else(|| {
            DbError::Message(format!(
                "synced table {} has no _updated_at column index",
                change.table
            ))
        })?;
        let locality = gates.locality_column_index(&change.table);
        for (index, (private_column, incoming_column)) in
            private.columns.iter().zip(incoming_columns).enumerate()
        {
            if index == updated_at || locality == Some(index) {
                continue;
            }
            if private_column != incoming_column {
                return Ok(false);
            }
        }

        let row_id = change.pk().ok_or_else(|| {
            DbError::Message(format!("incoming {} INSERT has no row id", change.table))
        })?;
        let live_blob = blob_decls
            .publication_blob_for_row(self.store.transaction, &change.table, row_id)
            .map_err(DbError::from)?;
        let bindings = package
            .blob_bindings()
            .iter()
            .filter(|binding| binding.table() == change.table && binding.row_id() == row_id)
            .collect::<Vec<_>>();
        match (live_blob, bindings.as_slice()) {
            (None, []) => Ok(true),
            (Some(live), [incoming]) => {
                let locator = incoming.blob().locator();
                let live_hash = live.plaintext_hash.parse::<ObjectHash>().map_err(|error| {
                    DbError::context(
                        format!("parse private blob hash for {}/{row_id}", change.table),
                        error,
                    )
                })?;
                Ok(incoming.column() == live.column
                    && locator.namespace() == live.blob.namespace
                    && locator.blob_id() == live.blob.id
                    && locator.plaintext_size() == live.plaintext_size
                    && locator.plaintext_hash() == live_hash)
            }
            _ => Ok(false),
        }
    }

    fn install_accepted_row_metadata(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        table: &str,
        row_id: &str,
        incoming_columns: &[ExactSqlValue],
    ) -> Result<(), DbError> {
        let columns = schema
            .columns(table)
            .ok_or_else(|| DbError::Message(format!("synced table {table} has no column map")))?;
        let updated_at = schema.updated_at(table).ok_or_else(|| {
            DbError::Message(format!(
                "synced table {table} has no _updated_at column index"
            ))
        })?;
        let mut indices = vec![updated_at];
        if let Some(locality) = gates.locality_column_index(table) {
            indices.push(locality);
        }
        indices.sort_unstable();
        indices.dedup();
        let assignments = indices
            .iter()
            .enumerate()
            .map(|(offset, index)| {
                format!("{} = ?{}", crate::quote_ident(&columns[*index]), offset + 1)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut values = indices
            .iter()
            .map(|index| incoming_columns[*index].bind_value())
            .collect::<Result<Vec<_>, _>>()?;
        values.push(rusqlite::types::Value::Text(row_id.to_string()));
        let sql = format!(
            "UPDATE {} SET {assignments} WHERE {} = ?{}",
            crate::quote_ident(table),
            crate::quote_ident(&columns[0]),
            values.len()
        );
        let changed = self
            .store
            .transaction
            .execute(&sql, rusqlite::params_from_iter(values))
            .map_err(DbError::from)?;
        if changed != 1 {
            return Err(DbError::Message(format!(
                "private row {}/{} disappeared before accepted adoption",
                table, row_id
            )));
        }
        Ok(())
    }

    fn row_columns(
        &self,
        schema: &TableSchema,
        table: &str,
        row_id: &str,
    ) -> Result<Option<Vec<ExactSqlValue>>, DbError> {
        let columns = schema
            .columns(table)
            .ok_or_else(|| DbError::Message(format!("synced table {table} has no column map")))?;
        let sql = format!(
            "SELECT {} FROM {} WHERE {} = ?1",
            columns
                .iter()
                .map(|column| crate::quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            crate::quote_ident(table),
            crate::quote_ident(&columns[0])
        );
        self.store
            .transaction
            .query_row(&sql, [row_id], |row| {
                (0..columns.len())
                    .map(|index| row.get_ref(index).map(owned_value))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .optional()
            .map_err(DbError::from)
    }
}

fn exact_insert_rows(
    changeset: &[u8],
) -> Result<BTreeMap<(String, String), Vec<ExactSqlValue>>, DbError> {
    if changeset.is_empty() {
        return Ok(BTreeMap::new());
    }
    let input: &mut dyn std::io::Read = &mut &changeset[..];
    let mut iter = ChangesetIter::start_strm(&input).map_err(DbError::from)?;
    let mut rows = BTreeMap::new();
    while let Some(item) = iter.next().map_err(DbError::from)? {
        let op = item.op().map_err(DbError::from)?;
        if op.code() != Action::SQLITE_INSERT {
            continue;
        }
        let values = (0..op.number_of_columns() as usize)
            .map(|index| {
                item.new_value(index)
                    .map(owned_value)
                    .map_err(DbError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let Some(ExactSqlValue::Text(row_id)) = values.first() else {
            return Err(DbError::Message(format!(
                "effective incoming INSERT for {} has no text primary key",
                op.table_name()
            )));
        };
        let row_id = std::str::from_utf8(row_id)
            .map_err(|error| DbError::context("incoming INSERT primary key", error))?
            .to_string();
        if rows
            .insert((op.table_name().to_string(), row_id.clone()), values)
            .is_some()
        {
            return Err(DbError::Message(format!(
                "effective incoming changeset repeats row {}/{}",
                op.table_name(),
                row_id
            )));
        }
    }
    Ok(rows)
}

fn owned_value(value: ValueRef<'_>) -> ExactSqlValue {
    match value {
        ValueRef::Null => ExactSqlValue::Null,
        ValueRef::Integer(value) => ExactSqlValue::Integer(value),
        ValueRef::Real(value) => ExactSqlValue::Real(value.to_bits()),
        ValueRef::Text(value) => ExactSqlValue::Text(value.to_vec()),
        ValueRef::Blob(value) => ExactSqlValue::Blob(value.to_vec()),
    }
}

fn private_shared_hold(
    (table, row_id): (String, String),
    commit: &StoreBatchCommitRef,
) -> crate::MaterializationHold {
    crate::MaterializationHold::PrivateSharedConflict {
        table,
        row_id,
        commit: commit.clone(),
    }
}
