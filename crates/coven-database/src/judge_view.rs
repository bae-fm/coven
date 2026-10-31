//! A child row's references as the author saw them, and the foreign-key
//! action the write's schema derives on it (§9.1, §9.6, D5).
//!
//! A child's cell at a step is what an earlier step of the write set in the
//! row's current incarnation, or else the value its replaced setter's
//! authenticated original record wrote. A missing record waits, unless no
//! history holds it, as for a migration write another raise replaced: then
//! the cell has no known value, and the change is lost anyway. A reference
//! names its parent by the parent the setter recorded, or, when none was
//! recorded, by its values as the parent's key, at the incarnation the
//! write's earlier steps leave that row. The references a step's row holds
//! and the step an action through each records are Lean's `Held` and
//! `heldConsequence`, which `tests::classification_matches_the_model`
//! compares.

use crate::judge::{Indexed, StepIndex};
use crate::removal_schema::TableRules;
use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::sqlite::DatabaseConnection;
use crate::write_rows::{column_name, key_columns, target_columns};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot_rows::OriginalRow;
use coven_format::value::Value;
use coven_format::write::Step;
use coven_merge::{Audience, ColumnValue, ForeignKey, Operation, Parent, RowId};
use std::collections::{BTreeMap, BTreeSet};

/// The schema a write was authored under: the current one, unless a
/// breaking migration since then retained the earlier one (§18.6).
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
        // A setter no history holds wrote a value no device can know; the
        // change replacing it is lost with that setter (§9.5).
        if crate::write_sources::uncarried(db, &setter.change)? {
            return Ok(None);
        }
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

    /// Whether `old` holds exactly the cells this row held when the write
    /// began: every cell its replaced setters wrote, with the value each
    /// wrote (D5). Asked before any step applies.
    pub(crate) fn began_with(
        &self,
        db: &DatabaseConnection,
        old: &BTreeMap<String, Value>,
    ) -> Result<bool, DbError> {
        let OriginalRow::Change(change) = self.record else {
            return Ok(true);
        };
        if old.len() != change.sources.setters.len() {
            return Ok(false);
        }
        for column in change.sources.setters.keys() {
            match (old.get(column), self.value(db, column)?) {
                (Some(value), Some(cell)) if *value == cell.value => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// The parent step this step is exactly the schema's action of, if any:
    /// of several, the nearest (Lean `Classification.cause` with `heldRule`).
    pub(crate) fn action(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        step: &Step,
    ) -> Result<Option<(RowId, u32)>, DbError> {
        let mut best: Option<(RowId, u32)> = None;
        for held in self.held(db, index, step)? {
            for parent_step in index.before(&held.parent, step.sequence) {
                if best
                    .as_ref()
                    .is_some_and(|(_, s)| *s >= parent_step.sequence)
                {
                    break;
                }
                if self
                    .consequence(db, index, &held, parent_step)?
                    .is_some_and(|expected| expected == step.change.operation)
                {
                    best = Some((held.parent.clone(), parent_step.sequence));
                }
            }
        }
        Ok(best)
    }

    /// Every reference this row holds at `step` through which the step can
    /// be a foreign-key action (Lean `Trace.refs`): one into a table a row of
    /// which an earlier step deleted, whose columns hold a complete key now,
    /// and, for an update, whose columns the step writes. An insert, or the
    /// old half of the row's own key update, is never an action. A reference
    /// names the parent its setter recorded, which must hold its key, or else
    /// the row its values key in the child's audience or the store, at the
    /// incarnation the write's earlier steps leave it.
    pub(crate) fn held(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        step: &Step,
    ) -> Result<Vec<HeldReference>, DbError> {
        let Some(table) = self.authoring.table(&self.row.table) else {
            return Ok(Vec::new());
        };
        if step.key_update || matches!(step.change.operation, Operation::Insert(_)) {
            return Ok(Vec::new());
        }
        let mut held = Vec::new();
        for (position, key) in table.foreign_keys.iter().enumerate() {
            let Some(parent_table) = self.authoring.table(&key.target) else {
                continue;
            };
            if !index.deleted_before(&parent_table.name, step.sequence) {
                continue;
            }
            let columns = key_names(table, key);
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
            let parents: Vec<(RowId, u64)> = match link {
                // The parent a reference names must hold its written key.
                Some(parent)
                    if parent.row.table != parent_table.name || parent.row.key != written =>
                {
                    continue
                }
                Some(parent) => vec![(parent.row.clone(), parent.generation)],
                // A reference recorded without a parent names, by its values,
                // the row as the write's earlier steps leave it: the
                // incarnation its last step ended, or the one it made.
                None => [self.row.audience.clone(), Audience::Store]
                    .into_iter()
                    .filter_map(|audience| {
                        let row = RowId {
                            table: parent_table.name.clone(),
                            key: written.clone(),
                            audience,
                        };
                        let last = index.last_before(&row, step.sequence)?;
                        let generation = if last.delete {
                            last.generation
                        } else {
                            last.after()
                        };
                        Some((row, generation))
                    })
                    .collect(),
            };
            let (carried, others) = match &step.change.operation {
                Operation::Update(recorded) => {
                    self.recorded(db, index, table, key, &columns, recorded, step)?
                }
                _ => (BTreeMap::new(), Some(BTreeMap::new())),
            };
            for (parent, generation) in parents {
                held.push(HeldReference {
                    key: position,
                    parent,
                    generation,
                    carried: carried.clone(),
                    others: others.clone(),
                });
            }
        }
        Ok(held)
    }

    /// What an action through `key` records besides the key's own columns
    /// (D5): every other column sharing a reference with them, with the value
    /// it holds now, and the parent each other reference over the recorded
    /// columns names, given the values `recorded` holds. A reference the
    /// action leaves as it was keeps the parent it names; one the action
    /// points elsewhere names the row its new values key, as the write's
    /// earlier steps leave it. None when the write does not determine that
    /// row's incarnation (Lean `Overlapping`).
    #[allow(clippy::type_complexity)]
    fn recorded(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        table: &TableSchema,
        key: &SchemaForeignKey,
        columns: &[String],
        recorded: &BTreeMap<String, ColumnValue<Value>>,
        step: &Step,
    ) -> Result<(BTreeMap<String, Value>, Option<OtherParents>), DbError> {
        let names = self.authoring.closure(table, columns);
        let mut current = BTreeMap::new();
        for name in &names {
            current.insert(name.clone(), self.value(db, name)?);
        }
        let carried: BTreeMap<String, Value> = current
            .iter()
            .filter(|(name, _)| !columns.contains(name))
            .map(|(name, cell)| {
                let value = cell.as_ref().map_or(Value::Null, |cell| cell.value.clone());
                (name.clone(), value)
            })
            .collect();
        let acting = self.authoring.schema.foreign_key(table, key);
        let mut others = OtherParents::new();
        for other in &table.foreign_keys {
            let name = self.authoring.schema.foreign_key(table, other);
            let other_columns = key_names(table, other);
            if name == acting || !other_columns.iter().all(|c| names.contains(c)) {
                continue;
            }
            let values: Vec<Value> = other_columns
                .iter()
                .map(|c| {
                    if columns.contains(c) {
                        recorded
                            .get(c)
                            .map_or(Value::Null, |cell| cell.value.clone())
                    } else {
                        carried[c].clone()
                    }
                })
                .collect();
            if values.contains(&Value::Null) {
                continue;
            }
            let unchanged = other_columns
                .iter()
                .zip(&values)
                .all(|(c, value)| current[c].as_ref().is_some_and(|cell| cell.value == *value));
            let parent = if unchanged {
                // The reference keeps the parent its setter recorded, unless
                // an earlier step of this write ended it.
                other_columns
                    .iter()
                    .find_map(|c| current[c].as_ref().and_then(|cell| cell.parents.get(&name)))
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
            } else {
                match self.retargeted(db, index, other, &values, step)? {
                    Some(parent) => parent,
                    None => return Ok((carried, None)),
                }
            };
            if let Some(parent) = parent {
                for column in other_columns {
                    others
                        .entry(column)
                        .or_default()
                        .insert(name.clone(), parent.clone());
                }
            }
        }
        Ok((carried, Some(others)))
    }

    /// The parent a reference names once an action gives it `values`: the
    /// row they key in the child's audience or the store, as the write's
    /// earlier steps leave it, or none when those steps deleted it. None at
    /// all when the write never touched a row that may hold that key: its
    /// incarnation is the authoring view's, which no step carries.
    fn retargeted(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        key: &SchemaForeignKey,
        values: &[Value],
        step: &Step,
    ) -> Result<Option<Option<Parent>>, DbError> {
        let Some(parent_table) = self.authoring.table(&key.target) else {
            return Ok(None);
        };
        let targets = target_columns(parent_table, key);
        let stored = self.authoring.stored(db, parent_table, &targets, values)?;
        let written = parent_key(parent_table, &targets, &stored)?;
        let mut untouched = false;
        for audience in [self.row.audience.clone(), Audience::Store] {
            let row = RowId {
                table: parent_table.name.clone(),
                key: written.clone(),
                audience,
            };
            match index.last_before(&row, step.sequence) {
                Some(last) if !last.delete => {
                    return Ok(Some(Some(Parent {
                        row,
                        generation: last.after(),
                    })))
                }
                Some(_) => {}
                None => untouched = true,
            }
        }
        Ok((!untouched).then_some(None))
    }

    /// The operation an action through `held` on the parent's step records,
    /// if the write's schema derives one (Lean `heldConsequence`): a cascade
    /// delete, or an update writing NULL or the parent's new key as the child
    /// stores it into the key's columns, with the carried cells and the
    /// parents every recorded reference names.
    fn consequence(
        &self,
        db: &DatabaseConnection,
        index: &StepIndex,
        held: &HeldReference,
        parent_step: &Indexed,
    ) -> Result<Option<Operation<Value>>, DbError> {
        let (Some(table), Some(others)) = (self.authoring.table(&self.row.table), &held.others)
        else {
            return Ok(None);
        };
        if !parent_step.delete || parent_step.generation != held.generation {
            return Ok(None);
        }
        let key = &table.foreign_keys[held.key];
        let Some(parent_table) = self.authoring.table(&key.target) else {
            return Ok(None);
        };
        let columns = key_names(table, key);
        let targets = target_columns(parent_table, key);
        let rule = if parent_step.key_update {
            &key.on_update
        } else {
            &key.on_delete
        }
        .to_ascii_uppercase();
        let replacement = match (rule.as_str(), parent_step.key_update) {
            ("CASCADE", false) => return Ok(Some(Operation::Delete)),
            ("CASCADE", true) => {
                let Some((new_row, new_step)) = index.new_half(&held.parent, parent_step.sequence)
                else {
                    return Ok(None);
                };
                let new = key_values(parent_table, &targets, &new_row.key)?;
                if new == key_values(parent_table, &targets, &held.parent.key)? {
                    return Ok(None);
                }
                Some((
                    self.authoring.stored(db, table, &columns, &new)?,
                    Parent {
                        row: new_row.clone(),
                        generation: new_step.after(),
                    },
                ))
            }
            ("SET NULL", _) => None,
            _ => return Ok(None),
        };
        let name = self.authoring.schema.foreign_key(table, key);
        let mut cells = BTreeMap::new();
        for (position, column) in columns.iter().enumerate() {
            let mut parents = others.get(column).cloned().unwrap_or_default();
            let value = match &replacement {
                Some((values, parent)) => {
                    parents.insert(name.clone(), parent.clone());
                    values[position].clone()
                }
                None => Value::Null,
            };
            cells.insert(column.clone(), ColumnValue { value, parents });
        }
        for (column, value) in &held.carried {
            cells.insert(
                column.clone(),
                ColumnValue {
                    value: value.clone(),
                    parents: others.get(column).cloned().unwrap_or_default(),
                },
            );
        }
        Ok(Some(Operation::Update(cells)))
    }
}

/// The parents references name, by column and reference.
pub(crate) type OtherParents = BTreeMap<String, BTreeMap<ForeignKey, Parent>>;

/// A reference a child row holds at one step, through which the step can be
/// a foreign-key action (Lean `Classification.Held`).
#[derive(Clone, Debug)]
pub(crate) struct HeldReference {
    /// The foreign key's position among the child table's.
    pub(crate) key: usize,
    /// The parent it names, and that parent's incarnation.
    pub(crate) parent: RowId,
    pub(crate) generation: u64,
    /// The other columns an action through it records, with the values they
    /// hold at this step.
    pub(crate) carried: BTreeMap<String, Value>,
    /// The parents the other references over the recorded columns name;
    /// none when the write does not determine them.
    pub(crate) others: Option<OtherParents>,
}

/// A foreign key's columns, as the table names them, in declaration order.
pub(crate) fn key_names(table: &TableSchema, key: &SchemaForeignKey) -> Vec<String> {
    key.columns
        .iter()
        .map(|c| column_name(table, c).to_owned())
        .collect()
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
pub(crate) fn key_values(
    table: &TableSchema,
    targets: &[String],
    key: &[u8],
) -> Result<Vec<Value>, DbError> {
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
