//! A local write's row operations as steps on merge rows (§9.6, D5).
//!
//! The steps travel in the write without saying what produced them. Every
//! device, the author included, tells foreign-key actions from direct targets
//! with the same label-free rule over the steps ([`crate::judge`]).

use crate::declaration::AudienceSource;
use crate::schema::{SchemaForeignKey, TableSchema};
use crate::sqlite::{DatabaseConnection, Event};
use crate::write_capture::values;
use crate::write_rows::{
    column_audience, column_name, row_key, target_columns, AppKey, AppValues, AppView,
};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::value::Value;
use coven_format::write::Step;
use coven_merge::{Audience, Change, ColumnValue, Operation, Parent, RowId};
use std::collections::BTreeMap;

struct Tracker<'a> {
    db: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    before: &'a AppView<'a>,
    after: &'a AppView<'a>,
    /// Values as the operations so far left them.
    current: BTreeMap<AppKey, Option<AppValues>>,
    /// Each app row's merge row as of its last operation.
    rows: BTreeMap<AppKey, RowId>,
    /// Each merge row's generation as of the operations so far.
    generations: BTreeMap<RowId, u64>,
}

impl Tracker<'_> {
    fn values(&self, key: &AppKey) -> Result<Option<AppValues>, DbError> {
        match self.current.get(key) {
            Some(values) => Ok(values.clone()),
            None => Ok(self.before.row(key)?.map(|row| row.values)),
        }
    }

    /// The row a reference names, by its parent table's primary key.
    fn parent(
        &self,
        table: &TableSchema,
        key: &SchemaForeignKey,
        values: &AppValues,
    ) -> Result<Option<AppKey>, DbError> {
        let components: Vec<Value> = key
            .columns
            .iter()
            .map(|c| values[column_name(table, c)].clone())
            .collect();
        if components.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(None);
        }
        let target = self.schema.table(&key.target);
        let columns = target_columns(target, key);
        // The parent's key holds the reference converted to its own affinity.
        let components =
            crate::removal_sql::reference_values(self.db, target, &columns, &components)?;
        let by_column: AppValues = columns.into_iter().zip(components).collect();
        Ok(Some((target.name.clone(), row_key(target, &by_column)?)))
    }

    /// The audience a row with these values has now: its column's, or its
    /// audience parent's as the operations so far left it (§15.1). None while
    /// its audience parent does not exist, as within a cascade of deletes or
    /// before a deferred parent is inserted.
    fn audience(
        &self,
        table: &TableSchema,
        values: &AppValues,
    ) -> Result<Option<Audience>, DbError> {
        match &self.schema.declaration(&table.name).audience {
            AudienceSource::Store => Ok(Some(Audience::Store)),
            AudienceSource::Column(column) => column_audience(table, column, values).map(Some),
            AudienceSource::ForeignKey(column) => {
                let key = table
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                    .expect("audience foreign key");
                let Some(parent) = self.parent(table, key, values)? else {
                    return Ok(None);
                };
                let Some(parent_values) = self.values(&parent)? else {
                    return Ok(None);
                };
                self.audience(self.schema.table(&parent.0), &parent_values)
            }
            AudienceSource::Both { .. } => unreachable!("validated declaration"),
        }
    }

    /// The merge row an app row with these values is in now. Without a
    /// present audience parent, it is where the row last was, or where the
    /// write leaves it; a row the write leaves without a parent is refused
    /// when its net change is built (§15.3).
    fn row(&self, key: &AppKey, values: &AppValues) -> Result<RowId, DbError> {
        let audience = match self.audience(self.schema.table(&key.0), values)? {
            Some(audience) => audience,
            None => match self.rows.get(key) {
                Some(row) => row.audience.clone(),
                None => match self.after.row(key)?.or(self.before.row(key)?) {
                    Some(row) => row.audience,
                    None => {
                        return Err(crate::write_rows::reference_error(
                            self.schema.table(&key.0),
                            values,
                            &self.schema.table(&key.0).foreign_keys[0].columns[0],
                        ))
                    }
                },
            },
        };
        Ok(RowId {
            table: key.0.clone(),
            key: key.1.clone(),
            audience,
        })
    }

    fn generation(&mut self, row: &RowId) -> Result<u64, DbError> {
        if let Some(generation) = self.generations.get(row) {
            return Ok(*generation);
        }
        let generation = crate::write_record::generation(self.db, row)?;
        self.generations.insert(row.clone(), generation);
        Ok(generation)
    }

    fn advance(&mut self, row: &RowId) -> Result<u64, DbError> {
        let generation = self.generation(row)?;
        let next = generation.checked_add(1).expect("generation exhausted");
        self.generations.insert(row.clone(), next);
        Ok(next)
    }

    /// The merge row an existing app row is in now. When an earlier
    /// operation moved it, or its audience parent, into another audience,
    /// that move ended the old merge row and began this one (§15.4).
    fn existing(&mut self, key: &AppKey, values: &AppValues) -> Result<RowId, DbError> {
        let now = self.row(key, values)?;
        let last = match self.rows.get(key) {
            Some(row) => Some(row.clone()),
            None => self
                .before
                .row(key)?
                .map(|row| crate::write_rows::row_id(key, &row)),
        };
        if let Some(last) = last.filter(|last| *last != now) {
            if self.generation(&last)? % 2 == 1 {
                self.advance(&last)?;
            }
            if self.generation(&now)? % 2 == 0 {
                self.advance(&now)?;
            }
        }
        self.rows.insert(key.clone(), now.clone());
        Ok(now)
    }

    /// The written columns with complete reference tuples and their parents
    /// as of this operation. Generated columns are recomputed, not written.
    fn columns(
        &mut self,
        table: &TableSchema,
        values: &AppValues,
        audience: Option<&Audience>,
        changed: impl IntoIterator<Item = String>,
    ) -> Result<BTreeMap<String, ColumnValue<Value>>, DbError> {
        let mut names: std::collections::BTreeSet<String> = changed.into_iter().collect();
        loop {
            let count = names.len();
            for group in &self.schema.rules[&table.name].reference_groups {
                if group.iter().any(|c| names.contains(c)) {
                    names.extend(group.iter().cloned());
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
        let mut columns: BTreeMap<String, ColumnValue<Value>> = names
            .into_iter()
            .filter(|name| {
                table
                    .columns
                    .iter()
                    .any(|c| c.name == *name && !c.generated)
            })
            .map(|name| {
                let value = values[&name].clone();
                (
                    name,
                    ColumnValue {
                        value,
                        parents: BTreeMap::new(),
                    },
                )
            })
            .collect();
        for key in &table.foreign_keys {
            if !key
                .columns
                .iter()
                .any(|c| columns.contains_key(column_name(table, c)))
            {
                continue;
            }
            let Some(parent) = self.parent(table, key, values)? else {
                continue;
            };
            // A reference names a present parent in its own audience or the
            // store. Pointing at a row a deferred insert adds later, or one
            // not readable from here, records no parent at this moment.
            let Some(parent_values) = self.values(&parent)? else {
                continue;
            };
            let row = self.row(&parent, &parent_values)?;
            let generation = self.generation(&row)?;
            if generation % 2 == 0
                || (row.audience != Audience::Store && Some(&row.audience) != audience)
            {
                continue;
            }
            let name = self.schema.foreign_key(table, key);
            for column in &key.columns {
                if let Some(value) = columns.get_mut(column_name(table, column)) {
                    value.parents.insert(
                        name.clone(),
                        Parent {
                            row: row.clone(),
                            generation,
                        },
                    );
                }
            }
        }
        Ok(columns)
    }
}

/// Every operation of the app phase on a declared table, as steps of the
/// merge rows they changed: each row's steps, in sequence order.
pub(crate) fn capture(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    before: &AppView<'_>,
    after: &AppView<'_>,
    events: &[Event],
) -> Result<BTreeMap<RowId, Vec<Step>>, DbError> {
    let mut tracker = Tracker {
        db,
        schema,
        before,
        after,
        current: BTreeMap::new(),
        rows: BTreeMap::new(),
        generations: BTreeMap::new(),
    };
    let mut operations: BTreeMap<RowId, Vec<Step>> = BTreeMap::new();
    let mut sequence: u32 = 0;
    for event in events {
        let Some(declaration) = schema
            .declarations
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(&event.table))
        else {
            continue;
        };
        let table = schema.table(&declaration.name);
        // A virtual generated column has no stored value: compute it from the
        // stored ones, as SQLite would.
        let evaluated = |values: Option<AppValues>| -> Result<Option<AppValues>, DbError> {
            match values {
                Some(values) if table.columns.iter().any(|c| c.generated) => {
                    let mut computed = crate::removal_sql::evaluate_values(db, table, &values)?;
                    computed.extend(values);
                    Ok(Some(computed))
                }
                values => Ok(values),
            }
        };
        let old = evaluated(values(table, &event.old)?)?;
        let new = evaluated(values(table, &event.new)?)?;
        let old_key = old
            .as_ref()
            .map(|old| row_key(table, old))
            .transpose()?
            .map(|key| (table.name.clone(), key));
        let new_key = new
            .as_ref()
            .map(|new| row_key(table, new))
            .transpose()?
            .map(|key| (table.name.clone(), key));
        let mut steps = Vec::new();
        match (&old_key, &new_key) {
            (Some(old_key), Some(new_key)) if old_key == new_key => {
                let old = old.as_ref().expect("an update has old values");
                let new = new.as_ref().expect("an update has new values");
                tracker.existing(old_key, old)?;
                let changed = new
                    .iter()
                    .filter(|(name, value)| old.get(*name) != Some(*value))
                    .map(|(name, _)| name.clone());
                tracker.current.insert(old_key.clone(), Some(new.clone()));
                // An update changing the row's audience moves it: the old
                // audience's row ends, and the update is the first operation
                // on the new audience's row, which the move began (§15.4).
                let row = tracker.existing(old_key, new)?;
                let columns = tracker.columns(
                    table,
                    new,
                    Some(&row.audience),
                    changed.collect::<Vec<_>>(),
                )?;
                let generation = tracker.generation(&row)?;
                steps.push((
                    row,
                    Step {
                        sequence,
                        change: Change {
                            generation,
                            operation: Operation::Update(columns),
                        },
                        key_update: false,
                    },
                ));
            }
            _ => {
                if let (Some(key), Some(old)) = (&old_key, &old) {
                    let row = tracker.existing(key, old)?;
                    let generation = tracker.generation(&row)?;
                    tracker.advance(&row)?;
                    tracker.rows.remove(key);
                    tracker.current.insert(key.clone(), None);
                    steps.push((
                        row,
                        Step {
                            sequence,
                            change: Change {
                                generation,
                                operation: Operation::Delete,
                            },
                            key_update: new_key.is_some(),
                        },
                    ));
                }
                if let (Some(key), Some(new)) = (&new_key, &new) {
                    tracker.current.insert(key.clone(), Some(new.clone()));
                    let row = tracker.row(key, new)?;
                    let all = new.keys().cloned().collect::<Vec<_>>();
                    let columns = tracker.columns(table, new, Some(&row.audience), all)?;
                    let generation = tracker.generation(&row)?;
                    // Inserting a removed row's key updates it, setting every
                    // column (§8.4); only a deleted or new row starts an incarnation.
                    let operation = if generation % 2 == 1 {
                        Operation::Update(columns)
                    } else {
                        tracker.advance(&row)?;
                        Operation::Insert(columns)
                    };
                    tracker.rows.insert(key.clone(), row.clone());
                    steps.push((
                        row,
                        Step {
                            sequence,
                            change: Change {
                                generation,
                                operation,
                            },
                            key_update: false,
                        },
                    ));
                }
            }
        }
        for (row, step) in steps {
            operations.entry(row).or_default().push(step);
        }
        sequence = sequence
            .checked_add(1)
            .expect("a write has fewer operations");
    }
    Ok(operations)
}

#[cfg(test)]
#[path = "execution_capture_tests.rs"]
pub(crate) mod tests;
