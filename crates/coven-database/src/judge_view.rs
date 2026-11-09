//! A child row's references as the author saw them, and the foreign-key
//! action the write's schema derives on it (§9.1, §9.6, D5).
//!
//! A child's cell at a step is what an earlier step of the write set in the
//! row's current incarnation, or else the value its replaced setter's kept
//! record holds: in full for a column that was a reference when the setter
//! wrote it, which every column read here is, and as a digest otherwise
//! (D7.3). A missing record waits, unless no
//! history holds it, as for a migration write another raise replaced: then
//! the cell has no known value, and the change is lost anyway. A reference
//! names the row its values key where the write's earlier steps leave that
//! key, moves included, or, when no step touched it, the parent its setter
//! recorded ([`StepIndex::place`]). The references a step's row holds
//! and the step an action through each records are Lean's `Held` and
//! `heldConsequence`, which `tests::classification_matches_the_model`
//! compares.

use crate::judge::{Indexed, Place, StepIndex};
use crate::removal_schema::TableRules;
use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::sqlite::DatabaseConnection;
use crate::write_rows::{column_name, key_columns, target_columns};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot_rows::KeptValue;
use coven_format::value::Value;
use coven_format::write::{Half, OriginalRow, Step};
use coven_merge::{Change, ColumnValue, ForeignKey, Operation, Parent, RowId};
use std::collections::BTreeMap;

/// The schema a write was authored under: the current one, unless a
/// breaking migration since then retained the earlier one (§18.6), and the
/// connection judging the write reads its history from.
pub(crate) struct Authoring<'a> {
    database: &'a DatabaseConnection,
    schema: &'a Schema,
    /// SQLite's column affinities of the current schema.
    current: Option<&'a BTreeMap<String, TableRules>>,
}

impl<'a> Authoring<'a> {
    pub(crate) fn new(
        db: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        version: u32,
    ) -> Result<Self, DbError> {
        if let Some(before) = schema.retained_before(version) {
            return Ok(Self { database: db, schema: before, current: None });
        }
        Ok(Self { database: db, schema: &schema.schema, current: Some(&schema.rules) })
    }

    pub(crate) fn table(&self, name: &str) -> Option<&TableSchema> {
        self.schema.tables.get(&name.to_ascii_lowercase())
    }

    /// Values as a table's columns store them. Only the current schema has
    /// SQLite tables to apply affinity with; an earlier one compares as written.
    fn stored(
        &self,
        table: &TableSchema,
        columns: &[String],
        values: &[Value],
    ) -> Result<Vec<Value>, DbError> {
        match self.current {
            Some(_) => crate::removal_sql::reference_values(self.database, table, columns, values),
            None => Ok(values.to_vec()),
        }
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
        Self { authoring, row, record, cells: BTreeMap::new(), fresh: false, version }
    }

    pub(crate) fn apply(&mut self, step: &Step) {
        if crate::judge::replay(&mut self.cells, &step.change.operation).is_some() {
            self.fresh = true;
        }
    }

    /// The cell's value at this moment, as far as it is kept, or none when
    /// it was never set.
    fn value(&self, column: &str) -> Result<Option<ColumnValue<KeptValue>>, DbError> {
        let db = self.authoring.database;
        if let Some(cell) = self.cells.get(column) {
            return Ok(Some(ColumnValue {
                value: KeptValue::Value(cell.value.clone()),
                parents: cell.parents.clone(),
            }));
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

    /// The value of a reference's column at this moment, whole. Its setter
    /// wrote it as a reference, so its kept record holds it in full (D7.3).
    fn reference(&self, column: &str) -> Result<Option<ColumnValue<Value>>, DbError> {
        match self.value(column)? {
            None => Ok(None),
            Some(ColumnValue { value: KeptValue::Value(value), parents }) => {
                Ok(Some(ColumnValue { value, parents }))
            }
            Some(ColumnValue { value: KeptValue::Digest(_), .. }) => Err(DbError::Invariant {
                what: "a column read as a reference was one when its setter wrote it",
            }),
        }
    }

    /// Whether `old` holds exactly the cells this row held when the write
    /// began: every cell its replaced setters wrote, with the value each
    /// wrote (D5). Asked before any step applies.
    pub(crate) fn began_with(&self, old: &BTreeMap<String, Value>) -> Result<bool, DbError> {
        let OriginalRow::Change(change) = self.record else {
            return Ok(true);
        };
        if old.len() != change.sources.setters.len() {
            return Ok(false);
        }
        for column in change.sources.setters.keys() {
            match (old.get(column), self.value(column)?) {
                (Some(value), Some(cell)) if cell.value.matches(value)? => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// The parent step this step is exactly the schema's action of, if any:
    /// of several, the nearest (Lean `Classification.cause` with `heldRule`).
    pub(crate) fn action(
        &self,
        index: &StepIndex,
        step: &Step,
    ) -> Result<Option<(RowId, u32)>, DbError> {
        let mut best: Option<(RowId, u32)> = None;
        for held in self.held(index, step)? {
            for parent_step in index.before(&held.parent, step.sequence) {
                if best.as_ref().is_some_and(|(_, s)| *s >= parent_step.sequence) {
                    break;
                }
                if self
                    .consequence(index, &held, parent_step)?
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
    /// and, for an update, whose columns the step writes. An insert, the old
    /// half of the row's own key update, and a half of its move are never
    /// actions. A reference names the row its key's last earlier step left,
    /// at the incarnation that step ended or made, or, when no step touched
    /// the key, the parent its setter recorded, which must hold its key.
    pub(crate) fn held(
        &self,
        index: &StepIndex,
        step: &Step,
    ) -> Result<Vec<HeldReference>, DbError> {
        let Some(table) = self.authoring.table(&self.row.table) else {
            return Ok(Vec::new());
        };
        if step.half != Half::Whole || matches!(step.change.operation, Operation::Insert(_)) {
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
                match self.reference(column)? {
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
                    cell.parents.values().find(|parent| parent.row.table == parent_table.name)
                })
            });
            let raw: Vec<Value> = values.iter().map(|cell| cell.value.clone()).collect();
            let stored = self.authoring.stored(parent_table, &targets, &raw)?;
            let written = parent_key(parent_table, &targets, &stored)?;
            // The parent a reference names must hold its written key.
            if link.is_some_and(|parent| {
                parent.row.table != parent_table.name || parent.row.key != written
            }) {
                continue;
            }
            let parents: Vec<(RowId, u64)> =
                match index.place(&parent_table.name, &written, step.sequence) {
                    // The row the key's last step left, at the incarnation
                    // that step ended or the one it made.
                    Place::Last(row, last) => {
                        vec![(row.clone(), if last.delete { last.generation } else { last.after })]
                    }
                    // As the authoring view holds it: the parent its setter
                    // recorded, if any.
                    Place::Untouched => link
                        .map(|parent| (parent.row.clone(), parent.generation))
                        .into_iter()
                        .collect(),
                    Place::Unplaced => continue,
                };
            let (carried, others) = match &step.change.operation {
                Operation::Update(recorded) => {
                    self.recorded(index, table, key, &columns, recorded, step)?
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
    /// columns names, given the values `recorded` holds: the row its values
    /// key, where the write's earlier steps leave it, when it is present
    /// there in the child's audience or the store, and none otherwise; when
    /// no step touched the key, the parent its setter recorded, if the action
    /// leaves the reference as it was (Lean `Place.named`). None when the
    /// write does not determine that row's incarnation (Lean `Overlapping`):
    /// a key no step touched under a changed reference, or one a row changed
    /// at no step may have moved.
    #[allow(clippy::type_complexity)]
    fn recorded(
        &self,
        index: &StepIndex,
        table: &TableSchema,
        key: &SchemaForeignKey,
        columns: &[String],
        recorded: &BTreeMap<String, ColumnValue<Value>>,
        step: &Step,
    ) -> Result<(BTreeMap<String, Value>, Option<OtherParents>), DbError> {
        let names = crate::write_rows::recorded_columns(table, columns.iter().cloned());
        let mut current = BTreeMap::new();
        for name in &names {
            current.insert(name.clone(), self.reference(name)?);
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
                        recorded.get(c).map_or(Value::Null, |cell| cell.value.clone())
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
            let Some(parent_table) = self.authoring.table(&other.target) else {
                return Ok((carried, None));
            };
            let targets = target_columns(parent_table, other);
            let stored = self.authoring.stored(parent_table, &targets, &values)?;
            let written = parent_key(parent_table, &targets, &stored)?;
            let place = index.place(&parent_table.name, &written, step.sequence);
            // Pointed at a row the write never touched, it names the
            // authoring view's incarnation, which no step carries.
            if matches!(place, Place::Untouched) && !unchanged {
                return Ok((carried, None));
            }
            // Left as it was, the reference keeps the parent its setter
            // recorded.
            let setter = other_columns
                .iter()
                .find_map(|c| current[c].as_ref().and_then(|cell| cell.parents.get(&name)));
            // A reference names a present parent in its own audience or the
            // store, as SQLite holds it at this step.
            let resolved = index.resolve(
                &parent_table.name,
                &written,
                place,
                &self.row.audience,
                setter,
                |_, last| last.after,
            );
            let Some(parent) = resolved else {
                return Ok((carried, None));
            };
            if let Some(parent) = parent {
                for column in other_columns {
                    others.entry(column).or_default().insert(name.clone(), parent.clone());
                }
            }
        }
        Ok((carried, Some(others)))
    }

    /// The row change the steps' combined `effect` makes: each reference
    /// over its columns names the parent where the whole write leaves its
    /// key, at the generation the write's record of that row leaves it at,
    /// or, when no step touched the key, the parent the steps name, its
    /// setter's (Lean `Place.namedNet`). None when a row with a key the
    /// write changed at no step of its own may have moved it where the
    /// reference looks.
    pub(crate) fn net(
        &self,
        index: &StepIndex,
        mut effect: Change<Value>,
    ) -> Result<Option<Change<Value>>, DbError> {
        let Some(table) = self.authoring.table(&self.row.table) else {
            return Ok(Some(effect));
        };
        let (Operation::Insert(cells) | Operation::Update(cells)) = &mut effect.operation else {
            return Ok(Some(effect));
        };
        for key in &table.foreign_keys {
            let Some(parent_table) = self.authoring.table(&key.target) else {
                continue;
            };
            let columns = key_names(table, key);
            // A row change sets all of a reference's columns or none (D5.5).
            let Some(values) = columns
                .iter()
                .map(|column| cells.get(column).map(|cell| cell.value.clone()))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if values.contains(&Value::Null) {
                continue;
            }
            let targets = target_columns(parent_table, key);
            let stored = self.authoring.stored(parent_table, &targets, &values)?;
            let written = parent_key(parent_table, &targets, &stored)?;
            let name = self.authoring.schema.foreign_key(table, key);
            let setter =
                columns.iter().find_map(|column| cells[column].parents.get(&name)).cloned();
            let resolved =
                index.net_parent(&parent_table.name, &written, &self.row.audience, setter.as_ref());
            let Some(parent) = resolved else {
                return Ok(None);
            };
            for column in &columns {
                let parents = &mut cells.get_mut(column).expect("the reference's column").parents;
                parents.remove(&name);
                parents.extend(parent.clone().map(|parent| (name.clone(), parent)));
            }
        }
        Ok(Some(effect))
    }

    /// The operation an action through `held` on the parent's step records,
    /// if the write's schema derives one (Lean `heldConsequence`); a move's
    /// delete derives none, as SQLite still holds the row: a cascade
    /// delete, or an update writing NULL or the parent's new key as the child
    /// stores it into the key's columns, with the carried cells and the
    /// parents every recorded reference names.
    fn consequence(
        &self,
        index: &StepIndex,
        held: &HeldReference,
        parent_step: &Indexed,
    ) -> Result<Option<Operation<Value>>, DbError> {
        let (Some(table), Some(others)) = (self.authoring.table(&self.row.table), &held.others)
        else {
            return Ok(None);
        };
        if !parent_step.delete
            || parent_step.half == Half::Move
            || parent_step.generation != held.generation
        {
            return Ok(None);
        }
        let key = &table.foreign_keys[held.key];
        let Some(parent_table) = self.authoring.table(&key.target) else {
            return Ok(None);
        };
        let columns = key_names(table, key);
        let targets = target_columns(parent_table, key);
        let key_update = parent_step.half == Half::OldKey;
        let rule = if key_update { &key.on_update } else { &key.on_delete }.to_ascii_uppercase();
        let replacement = match (rule.as_str(), key_update) {
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
                    self.authoring.stored(table, &columns, &new)?,
                    Parent { row: new_row.clone(), generation: new_step.after },
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
    key.columns.iter().map(|c| column_name(table, c).to_owned()).collect()
}

/// A parent's key, encoded, from its referenced values in target order.
fn parent_key(
    table: &TableSchema,
    targets: &[String],
    values: &[Value],
) -> Result<Vec<u8>, DbError> {
    let by_column: BTreeMap<&str, &Value> =
        targets.iter().map(String::as_str).zip(values).collect();
    let ordered: Vec<Value> =
        key_columns(table).iter().map(|c| (*by_column[c.name.as_str()]).clone()).collect();
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
    targets.iter().map(|t| values.get(t).cloned().ok_or(DbError::DamagedDatabase)).collect()
}

#[cfg(test)]
#[path = "judge_view_tests.rs"]
mod tests;
