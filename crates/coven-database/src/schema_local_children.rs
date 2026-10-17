//! Rules for local tables whose foreign keys reach synced data (§9.1, A.9).
//!
//! Local rows exist only on this device, so no write accounts for them: a
//! local key must follow whatever coven does to its parent, never stop it.

use std::collections::{BTreeMap, BTreeSet};

use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::{DbError, SchemaError};

impl Schema {
    /// A synced row coven deletes or takes out can have local children, so a
    /// local key into a synced table, or into a local table one of these
    /// reaches, must cascade or set a nullable column to NULL on delete.
    pub(crate) fn validate_local_children(&self, synced: &BTreeSet<String>) -> Result<(), DbError> {
        let mut reached = synced.clone();
        let mut pending: Vec<_> = synced.iter().cloned().collect();
        while let Some(parent) = pending.pop() {
            for (name, table) in &self.tables {
                if synced.contains(name) {
                    continue;
                }
                for key in &table.foreign_keys {
                    if !key.target.eq_ignore_ascii_case(&parent) {
                        continue;
                    }
                    let invalid = key.columns.iter().find(|name| {
                        key.on_delete != "CASCADE"
                            && (key.on_delete != "SET NULL"
                                || table.columns.iter().any(|column| {
                                    column.name.eq_ignore_ascii_case(name) && column.not_null
                                }))
                    });
                    if let Some(column) = invalid {
                        return Err(SchemaError::LocalChildAction {
                            table: table.name.clone(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    if reached.insert(name.clone()) {
                        pending.push(name.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// Coven applies a change to a synced table's non-key column with an
    /// UPDATE, so a local key into such a column must follow it with ON UPDATE
    /// CASCADE or SET NULL; NO ACTION and RESTRICT would fail every apply of
    /// that change (`LocalUpdate.stuck_forever`). A following key rewrites its
    /// own columns, so a local key into those must follow too
    /// (`LocalUpdate.chain_holds`). Key columns change by delete and insert
    /// (§9.2), which the delete rule above covers.
    pub(crate) fn validate_local_updates(&self, synced: &BTreeSet<String>) -> Result<(), DbError> {
        let mut updated: BTreeMap<String, BTreeSet<String>> = synced
            .iter()
            .filter_map(|name| {
                let table = self.tables.get(name)?;
                let columns = table
                    .columns
                    .iter()
                    .filter(|column| column.primary_key == 0)
                    .map(|column| column.name.to_ascii_lowercase())
                    .collect();
                Some((name.clone(), columns))
            })
            .collect();
        loop {
            let mut grew = false;
            for (name, table) in &self.tables {
                if synced.contains(name) {
                    continue;
                }
                for key in &table.foreign_keys {
                    let target = key.target.to_ascii_lowercase();
                    let (Some(parent), Some(columns)) =
                        (self.tables.get(&target), updated.get(&target))
                    else {
                        continue;
                    };
                    let rewritten: Vec<&String> = key
                        .columns
                        .iter()
                        .zip(targets(parent, key))
                        .filter(|(_, target)| target.as_ref().is_some_and(|t| columns.contains(t)))
                        .map(|(column, _)| column)
                        .collect();
                    let Some(first) = rewritten.first() else {
                        continue;
                    };
                    let changed = match key.on_update.as_str() {
                        "CASCADE" => rewritten,
                        // SQLite sets every column of the key to NULL.
                        "SET NULL" => key.columns.iter().collect(),
                        _ => {
                            return Err(SchemaError::LocalChildUpdate {
                                table: table.name.clone(),
                                column: (*first).clone(),
                            }
                            .into())
                        }
                    };
                    let changed: Vec<String> =
                        changed.iter().map(|c| c.to_ascii_lowercase()).collect();
                    let entry = updated.entry(name.clone()).or_default();
                    for column in changed {
                        grew |= entry.insert(column);
                    }
                }
            }
            if !grew {
                return Ok(());
            }
        }
    }
}

/// The parent column each key column references, lowercase: the named target,
/// or the parent's primary key in order when the key names none. A key SQLite
/// cannot match to a primary key has no target here.
fn targets(parent: &TableSchema, key: &SchemaForeignKey) -> Vec<Option<String>> {
    let primary = crate::write_rows::key_columns(parent);
    key.target_columns
        .iter()
        .enumerate()
        .map(|(i, column)| match column {
            Some(column) => Some(column.to_ascii_lowercase()),
            None => primary.get(i).map(|c| c.name.to_ascii_lowercase()),
        })
        .collect()
}

#[cfg(test)]
#[path = "schema_local_children_tests.rs"]
mod tests;
