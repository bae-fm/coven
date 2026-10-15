//! A child row's references as the author saw them, and the foreign-key
//! action the write's schema derives on it (§8.4, §8.8, D5).
//!
//! A child's cell at a step is what an earlier step of the write set in the
//! row's current incarnation, or else the value its replaced setter's
//! authenticated original record wrote. A missing record waits. A reference
//! names its parent by the parent the setter recorded, or, when none was
//! recorded, by its values as the parent's key.

use crate::judge::StepIndex;
use crate::removal_schema::TableRules;
use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::sqlite::DatabaseConnection;
use crate::write_rows::{column_name, key_columns, target_columns};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot_rows::OriginalRow;
use coven_format::value::Value;
use coven_format::write::Step;
use coven_merge::{Audience, ColumnValue, Operation, Parent, RowId};
use std::collections::{BTreeMap, BTreeSet};

/// The schema a write was authored under: the current one, unless a
/// breaking migration since then retained the earlier one (§17.1).
pub(crate) struct Authoring<'a> {
    schema: &'a Schema,
    /// Reference groups, and SQLite's column affinities, of the current schema.
    current: Option<&'a BTreeMap<String, TableRules>>,
}

impl<'a> Authoring<'a> {
    pub(crate) fn new(
        db: &DatabaseConnection,
        schema: &'a WriteSchema,
        version: u32,
    ) -> Result<Self, DbError> {
        if version < db.schema_version()? {
            if let Some(before) = crate::migration_receive::authored(&schema.conversions, version) {
                return Ok(Self {
                    schema: before,
                    current: None,
                });
            }
        }
        Ok(Self {
            schema: &schema.schema,
            current: Some(&schema.rules),
        })
    }

    fn table(&self, name: &str) -> Option<&TableSchema> {
        self.schema.tables.get(&name.to_ascii_lowercase())
    }

    /// Values as a table's columns store them. Only the current schema has
    /// SQLite tables to apply affinity with; an earlier one compares as written.
    fn stored(
        &self,
        db: &DatabaseConnection,
        table: &TableSchema,
        columns: &[String],
        values: &[Value],
    ) -> Result<Vec<Value>, DbError> {
        match self.current {
            Some(_) => crate::removal_sql::reference_values(db, table, columns, values),
            None => Ok(values.to_vec()),
        }
    }

    /// The columns an action writing `columns` records: every column sharing
    /// a reference with them, expanded until none is added, without
    /// generated columns (D5).
    fn closure(&self, table: &TableSchema, columns: &[String]) -> BTreeSet<String> {
        let mut names: BTreeSet<String> = columns.iter().cloned().collect();
        loop {
            let count = names.len();
            if let Some(rules) = self.current.and_then(|rules| rules.get(&table.name)) {
                for group in &rules.reference_groups {
                    if group.iter().any(|c| names.contains(c)) {
                        names.extend(group.iter().cloned());
                    }
                }
            }
            for key in &table.foreign_keys {
                if key
                    .columns
                    .iter()
                    .any(|c| names.contains(column_name(table, c)))
                {
                    names.extend(key.columns.iter().map(|c| column_name(table, c).to_owned()));
                }
            }
            if names.len() == count {
                break;
            }
        }
        names.retain(|name| {
            table
                .columns
                .iter()
                .any(|c| c.name == *name && !c.generated)
        });
        names
    }
}

/// One child row's cells through the write's steps on it.
pub(crate) struct ChildView<'a> {
    authoring: &'a Authoring<'a>,
    row: &'a RowId,
    record: &'a OriginalRow,
    /// Cells the write's earlier steps set in the row's current incarnation.
    cells: BTreeMap<String, ColumnValue<Value>>,
    /// An earlier step began or ended an incarnation: no pre-write cell is current.
    fresh: bool,
    /// The write's schema version.
    version: u32,
}

impl<'a> ChildView<'a> {
    pub(crate) fn new(
        authoring: &'a Authoring<'a>,
        row: &'a RowId,
        record: &'a OriginalRow,
        version: u32,
    ) -> Self {
        Self {
            authoring,
            row,
            record,
            cells: BTreeMap::new(),
            fresh: false,
            version,
        }
    }

    pub(crate) fn apply(&mut self, step: &Step) {
        match &step.change.operation {
            Operation::Insert(columns) => {
                self.cells = columns.clone();
                self.fresh = true;
            }
            Operation::Update(columns) => self.cells.extend(columns.clone()),
            Operation::Delete => {
                self.cells.clear();
                self.fresh = true;
            }
        }
    }

    /// The cell's value at this moment, or none when it was never set.
    fn value(
        &self,
        db: &DatabaseConnection,
        column: &str,
    ) -> Result<Option<ColumnValue<Value>>, DbError> {
        if let Some(cell) = self.cells.get(column) {
            return Ok(Some(cell.clone()));
        }
        if self.fresh {
            return Ok(None);
        }
        let execution = self.record.execution();
        let net = match self.record {
            OriginalRow::Change(change) => change.sources.setters.get(column),
            OriginalRow::Execution(_) => None,
        };
        let Some(setter) = net.or_else(|| execution.sources.setters.get(column)) else {
            return Ok(None);
        };
        let evidence = crate::write_sources::evidence(db, &setter.change)?;
        let mut cell = match &evidence.original.change.operation {
            Operation::Insert(columns) | Operation::Update(columns) => {
                columns.get(&setter.column).cloned()
            }
            Operation::Delete => return Err(DbError::DamagedDatabase),
        };
        // A setter authored before a rename names its parents as it saw them.
        if let Some(cell) = &mut cell {
            if evidence.header.schema_version != self.version {
                for parent in cell.parents.values_mut() {
                    if let Some(table) = crate::source_names::table(
                        db,
                        evidence.header.schema_version,
                        &parent.row.table,
                        self.version,
                    )? {
                        parent.row.table = table;
                    }
                }
            }
        }
        Ok(cell)
    }

    /// The parent step this step is exactly the schema's action of, if any.
    pub(crate) fn action(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        step: &Step,
    ) -> Result<Option<(RowId, u32)>, DbError> {
        let Some(table) = self.authoring.table(&self.row.table) else {
            return Ok(None);
        };
        if step.key_update
            || table.foreign_keys.is_empty()
            || matches!(step.change.operation, Operation::Insert(_))
        {
            return Ok(None);
        }
        // Of several earlier steps it is the consequence of, its cause is the
        // nearest (Lean `Classification.cause`).
        let mut best: Option<(RowId, u32)> = None;
        for key in &table.foreign_keys {
            let Some(parent_table) = self.authoring.table(&key.target) else {
                continue;
            };
            if !index.deleted_before(&parent_table.name, step.sequence) {
                continue;
            }
            let columns: Vec<String> = key
                .columns
                .iter()
                .map(|c| column_name(table, c).to_owned())
                .collect();
            // An update action writes the reference's own columns.
            if let Operation::Update(written) = &step.change.operation {
                if !columns.iter().all(|c| written.contains_key(c)) {
                    continue;
                }
            }
            let mut values = Vec::new();
            for column in &columns {
                match self.value(db, column)? {
                    Some(cell) if cell.value != Value::Null => values.push(cell),
                    _ => break,
                }
            }
            if values.len() != columns.len() {
                continue;
            }
            let name = self.authoring.schema.foreign_key(table, key);
            let targets = target_columns(parent_table, key);
            let link = values.iter().find_map(|cell| {
                cell.parents.get(&name).or_else(|| {
                    cell.parents
                        .values()
                        .find(|parent| parent.row.table == parent_table.name)
                })
            });
            let raw: Vec<Value> = values.iter().map(|cell| cell.value.clone()).collect();
            let stored = self.authoring.stored(db, parent_table, &targets, &raw)?;
            let written = parent_key(parent_table, &targets, &stored)?;
            let candidates = match link {
                // The parent a reference names must hold its written key.
                Some(parent)
                    if parent.row.table != parent_table.name || parent.row.key != written =>
                {
                    continue
                }
                Some(parent) => vec![(parent.row.clone(), Some(parent.generation))],
                None => [self.row.audience.clone(), Audience::Store]
                    .into_iter()
                    .map(|audience| {
                        (
                            RowId {
                                table: parent_table.name.clone(),
                                key: written.clone(),
                                audience,
                            },
                            None,
                        )
                    })
                    .collect(),
            };
            for (parent, generation) in candidates {
                for parent_step in index.before(&parent, step.sequence) {
                    if best
                        .as_ref()
                        .is_some_and(|(_, s)| *s >= parent_step.sequence)
                    {
                        break;
                    }
                    if !parent_step.delete
                        || generation.is_some_and(|g| g != parent_step.generation)
                    {
                        continue;
                    }
                    let rule = if parent_step.key_update {
                        &key.on_update
                    } else {
                        &key.on_delete
                    }
                    .to_ascii_uppercase();
                    let replacement = match (rule.as_str(), parent_step.key_update) {
                        ("CASCADE", false) => {
                            if matches!(step.change.operation, Operation::Delete) {
                                best = Some((parent.clone(), parent_step.sequence));
                            }
                            continue;
                        }
                        ("CASCADE", true) => {
                            let Some((new_row, new_step)) =
                                index.new_half(&parent, parent_step.sequence)
                            else {
                                continue;
                            };
                            let new = key_values(parent_table, &targets, &new_row.key)?;
                            if new == key_values(parent_table, &targets, &parent.key)? {
                                continue;
                            }
                            let stored = self.authoring.stored(db, table, &columns, &new)?;
                            Replacement::Key {
                                values: stored,
                                parent: Parent {
                                    row: new_row.clone(),
                                    generation: new_step.after(),
                                },
                            }
                        }
                        ("SET NULL", _) => Replacement::Null,
                        _ => continue,
                    };
                    let expected =
                        self.update(db, index, table, key, &columns, replacement, step)?;
                    if step.change.operation == Operation::Update(expected) {
                        best = Some((parent.clone(), parent_step.sequence));
                    }
                }
            }
        }
        Ok(best)
    }

    /// The columns an action setting `key`'s columns writes, with every
    /// value and parent as the author records them at that moment (D5).
    #[allow(clippy::too_many_arguments)]
    fn update(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        table: &TableSchema,
        key: &SchemaForeignKey,
        columns: &[String],
        replacement: Replacement,
        step: &Step,
    ) -> Result<BTreeMap<String, ColumnValue<Value>>, DbError> {
        let names = self.authoring.closure(table, columns);
        let mut values = BTreeMap::new();
        let mut written = BTreeMap::new();
        for name in &names {
            let value = match columns.iter().position(|c| c == name) {
                Some(index) => match &replacement {
                    Replacement::Null => Value::Null,
                    Replacement::Key { values, .. } => values[index].clone(),
                },
                None => {
                    let cell = self.value(db, name)?;
                    if let Some(cell) = &cell {
                        written.insert(name.clone(), cell.parents.clone());
                    }
                    cell.map_or(Value::Null, |cell| cell.value)
                }
            };
            values.insert(
                name.clone(),
                ColumnValue {
                    value,
                    parents: BTreeMap::new(),
                },
            );
        }
        let acting = self.authoring.schema.foreign_key(table, key);
        for other in &table.foreign_keys {
            let other_columns: Vec<String> = other
                .columns
                .iter()
                .map(|c| column_name(table, c).to_owned())
                .collect();
            if !other_columns.iter().all(|c| values.contains_key(c))
                || other_columns.iter().any(|c| values[c].value == Value::Null)
            {
                continue;
            }
            let name = self.authoring.schema.foreign_key(table, other);
            let parent = if name == acting {
                match &replacement {
                    Replacement::Key { parent, .. } => Some(parent.clone()),
                    Replacement::Null => None,
                }
            } else if other_columns.iter().any(|c| columns.contains(c)) {
                // A tuple the action changed names whatever parent its new
                // values reach, which only SQLite's own capture knows here.
                return Ok(BTreeMap::new());
            } else {
                // An unchanged reference keeps the parent its setter
                // recorded, unless an earlier step of this write ended it.
                other_columns
                    .iter()
                    .find_map(|c| written.get(c).and_then(|parents| parents.get(&name)))
                    .cloned()
                    .and_then(
                        |parent| match index.last_before(&parent.row, step.sequence) {
                            Some(last) if last.delete => None,
                            Some(last) => Some(Parent {
                                row: parent.row,
                                generation: last.after(),
                            }),
                            None => Some(parent),
                        },
                    )
            };
            if let Some(parent) = parent {
                for column in &other_columns {
                    values
                        .get_mut(column)
                        .expect("a closed column set")
                        .parents
                        .insert(name.clone(), parent.clone());
                }
            }
        }
        Ok(values)
    }
}

/// What an update action writes into the child's reference.
enum Replacement {
    Null,
    Key { values: Vec<Value>, parent: Parent },
}

/// A parent's key, encoded, from its referenced values in target order.
fn parent_key(
    table: &TableSchema,
    targets: &[String],
    values: &[Value],
) -> Result<Vec<u8>, DbError> {
    let by_column: BTreeMap<&str, &Value> =
        targets.iter().map(String::as_str).zip(values).collect();
    let ordered: Vec<Value> = key_columns(table)
        .iter()
        .map(|c| (*by_column[c.name.as_str()]).clone())
        .collect();
    crate::write_rows::equality_key(&ordered)
}

/// A parent key's values in target column order.
fn key_values(table: &TableSchema, targets: &[String], key: &[u8]) -> Result<Vec<Value>, DbError> {
    let values: BTreeMap<String, Value> = key_columns(table)
        .iter()
        .map(|c| c.name.clone())
        .zip(coven_format::key::decode_key(key)?)
        .collect();
    targets
        .iter()
        .map(|t| values.get(t).cloned().ok_or(DbError::DamagedDatabase))
        .collect()
}

#[cfg(test)]
#[path = "judge_view_tests.rs"]
mod tests;
