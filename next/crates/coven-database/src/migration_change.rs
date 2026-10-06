//! App-facing conversion values retain the identity and references of their source columns.

use crate::migration_names::MigrationMatch;
use crate::schema::{Schema, TableSchema};
use crate::write_encoding::{sql_value, value};
use crate::{DbError, RowKey};
use coven_format::write::RowChange as WrittenRow;
use coven_merge::{ColumnValue, ForeignKey, Operation, Parent};
use rusqlite::types::Value;
use std::collections::BTreeMap;

/// One row change of a waiting write (§20.13).
#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    /// The table being changed.
    pub table: String,
    /// The row's primary key.
    pub key: RowKey,
    /// The row operation.
    pub op: ChangeOp,
    /// The changed columns, including old and new values.
    pub columns: Vec<ColumnChange>,
}

impl RowChange {
    /// Rename a column, carrying its original references with it.
    pub fn rename_column(&mut self, from: &str, to: &str) {
        for column in &mut self.columns {
            if column.name == from {
                column.name = to.to_owned();
            }
            if let Some(source) = &mut column.source {
                for name in source.renames.values_mut() {
                    if name == from {
                        *name = to.to_owned();
                    }
                }
                source.renames.insert(from.to_owned(), to.to_owned());
            }
        }
    }

    pub(crate) fn read(row: &WrittenRow, schema: &Schema) -> Result<Self, DbError> {
        let table = table(schema, &row.row.table)?;
        let (op, new) = match &row.change.operation {
            Operation::Insert(columns) => (ChangeOp::Insert, Some(columns)),
            Operation::Update(columns) => (ChangeOp::Update, Some(columns)),
            Operation::Delete => (ChangeOp::Delete, None),
        };
        let names = match new {
            Some(columns) => columns.keys().collect::<Vec<_>>(),
            None => row.old.keys().collect(),
        };
        let columns = names
            .into_iter()
            .map(|name| {
                let old = row.old.get(name).map(sql_value);
                let written = new.and_then(|columns| columns.get(name));
                let new = written.map(|column| sql_value(&column.value));
                let constraints = references(table, name, schema)?;
                let reference = (!constraints.is_empty()).then(|| ReferenceColumn {
                    constraints,
                    old: old.clone(),
                    new: new.clone(),
                    parents: written
                        .map(|column| column.parents.clone())
                        .unwrap_or_default(),
                });
                Ok(ColumnChange {
                    name: name.clone(),
                    old,
                    new,
                    source: Some(ColumnSource {
                        name: name.clone(),
                        reference,
                        renames: BTreeMap::new(),
                    }),
                })
            })
            .collect::<Result<_, DbError>>()?;
        Ok(Self {
            table: row.row.table.clone(),
            key: RowKey(
                coven_format::key::decode_key(&row.row.key)?
                    .iter()
                    .map(sql_value)
                    .collect(),
            ),
            op,
            columns,
        })
    }

    pub(crate) fn write(
        &self,
        original: &WrittenRow,
        schema: &Schema,
        names: &MigrationMatch,
    ) -> Result<WrittenRow, DbError> {
        let table = table(schema, &self.table)?;
        let renames = self
            .sources()
            .map(|(a, b)| (a.to_owned(), b.to_owned()))
            .collect();
        let mut reference_sources = std::collections::BTreeSet::new();
        let mut old = BTreeMap::new();
        let mut new = BTreeMap::new();
        let mut columns_seen = std::collections::BTreeSet::new();
        for column in &self.columns {
            if !columns_seen.insert(&column.name) {
                return Err(DbError::MigrationDuplicateColumn {
                    table: self.table.clone(),
                    column: column.name.clone(),
                });
            }
            let valid = match self.op {
                ChangeOp::Insert => column.old.is_none() && column.new.is_some(),
                ChangeOp::Update => column.old.is_some() && column.new.is_some(),
                ChangeOp::Delete => column.old.is_some() && column.new.is_none(),
            };
            if !valid {
                return Err(DbError::MigrationColumnOperation {
                    table: self.table.clone(),
                    column: column.name.clone(),
                    op: self.op,
                    has_old: column.old.is_some(),
                    has_new: column.new.is_some(),
                });
            }
            if !table.columns.iter().any(|c| c.name == column.name) {
                return Err(DbError::MigrationColumnMissing {
                    table: self.table.clone(),
                    column: column.name.clone(),
                });
            }
            let reference = column
                .source
                .as_ref()
                .and_then(|source| source.reference.as_ref());
            let constraints = references(table, &column.name, schema)?;
            if reference.is_some_and(|r| r.old != column.old || r.new != column.new)
                || (!constraints.is_empty() && reference.is_none())
            {
                return Err(reference_error(&self.table, &column.name));
            }
            if let Some(reference) = reference {
                let mapped = reference
                    .constraints
                    .iter()
                    .filter_map(|key| rename_key(key, &original.row.table, &renames, names))
                    .collect::<Vec<_>>();
                if constraints.iter().any(|key| !mapped.contains(key))
                    || (!constraints.is_empty()
                        && !reference_sources
                            .insert(&column.source.as_ref().expect("reference source").name))
                {
                    return Err(reference_error(&self.table, &column.name));
                }
            }
            if let Some(v) = &column.old {
                old.insert(column.name.clone(), value(v.into())?);
            }
            if let Some(v) = &column.new {
                let parents = match reference {
                    Some(reference) => rename_parents(
                        &reference.parents,
                        &original.row.table,
                        &renames,
                        names,
                        &constraints,
                    )?,
                    None => BTreeMap::new(),
                };
                new.insert(
                    column.name.clone(),
                    ColumnValue {
                        value: value(v.into())?,
                        parents,
                    },
                );
            }
        }
        let key: Vec<_> = self
            .key
            .values()
            .iter()
            .map(|v| value(v.into()))
            .collect::<Result<_, _>>()?;
        coven_format::key::encode_key(&key)?;
        let primary = table
            .indices
            .iter()
            .find(|index| index.primary)
            .ok_or_else(|| DbError::MigrationPrimaryKeyMissing {
                table: self.table.clone(),
            })?;
        if key.len() != primary.collations.len() {
            return Err(DbError::MigrationKeyArity {
                table: self.table.clone(),
                expected: primary.collations.len(),
                actual: key.len(),
            });
        }
        let key = crate::write_rows::equality_key(&key, &primary.collations)?;
        Ok(WrittenRow {
            row: coven_merge::RowId {
                table: table.name.clone(),
                key,
                audience: original.row.audience.clone(),
            },
            change: coven_merge::Change {
                generation: original.change.generation,
                operation: match self.op {
                    ChangeOp::Insert => Operation::Insert(new),
                    ChangeOp::Update => Operation::Update(new),
                    ChangeOp::Delete => Operation::Delete,
                },
            },
            old,
        })
    }

    fn sources(&self) -> impl Iterator<Item = (&str, &str)> {
        self.columns.iter().flat_map(|c| {
            c.source.iter().flat_map(move |s| {
                s.renames
                    .iter()
                    .map(|(from, to)| (from.as_str(), to.as_str()))
                    .chain(std::iter::once((s.name.as_str(), c.name.as_str())))
            })
        })
    }
}

/// The operation a waiting write performs on a row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeOp {
    /// Insert the row.
    Insert,
    /// Change columns of the row.
    Update,
    /// Delete the row.
    Delete,
}

/// One column of a change, retaining its original parent generations privately.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnChange {
    /// The column name.
    pub name: String,
    /// Absent on insert; `Some(Value::Null)` is SQL NULL.
    pub old: Option<Value>,
    /// Absent on delete; `Some(Value::Null)` is SQL NULL.
    pub new: Option<Value>,
    source: Option<ColumnSource>,
}

#[derive(Clone, Debug, PartialEq)]
struct ColumnSource {
    name: String,
    reference: Option<ReferenceColumn>,
    renames: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
struct ReferenceColumn {
    constraints: Vec<ForeignKey>,
    old: Option<Value>,
    new: Option<Value>,
    parents: BTreeMap<ForeignKey, Parent>,
}

impl ColumnChange {
    /// Add a column with no reference metadata.
    pub fn new(name: impl Into<String>, old: Option<Value>, new: Option<Value>) -> Self {
        Self {
            name: name.into(),
            old,
            new,
            source: None,
        }
    }
}

fn table<'a>(schema: &'a Schema, name: &str) -> Result<&'a TableSchema, DbError> {
    schema
        .tables
        .get(&name.to_ascii_lowercase())
        .ok_or_else(|| DbError::MigrationTableMissing { table: name.into() })
}

fn references(
    source: &TableSchema,
    name: &str,
    schema: &Schema,
) -> Result<Vec<ForeignKey>, DbError> {
    source
        .foreign_keys
        .iter()
        .filter(|fk| fk.columns.iter().any(|c| c.eq_ignore_ascii_case(name)))
        .map(|fk| {
            table(schema, &fk.target)?;
            Ok(schema.foreign_key(source, fk))
        })
        .collect()
}

fn rename_key(
    key: &ForeignKey,
    source_table: &str,
    renames: &BTreeMap<String, String>,
    names: &MigrationMatch,
) -> Option<ForeignKey> {
    Some(ForeignKey::new(
        coven_merge::ConstraintColumns(
            key.columns
                .0
                .iter()
                .map(|column| {
                    renames
                        .get(column)
                        .or_else(|| {
                            names
                                .columns
                                .get(&(source_table.to_owned(), column.clone()))
                        })
                        .cloned()
                })
                .collect::<Option<Vec<_>>>()?,
        ),
        names.tables.get(&key.parent)?.clone(),
        coven_merge::ConstraintColumns(
            key.parent_columns
                .0
                .iter()
                .map(|column| {
                    names
                        .columns
                        .get(&(key.parent.clone(), column.clone()))
                        .cloned()
                })
                .collect::<Option<Vec<_>>>()?,
        ),
    ))
}

fn rename_parents(
    parents: &BTreeMap<ForeignKey, Parent>,
    source_table: &str,
    renames: &BTreeMap<String, String>,
    names: &MigrationMatch,
    constraints: &[ForeignKey],
) -> Result<BTreeMap<ForeignKey, Parent>, DbError> {
    let mut renamed = BTreeMap::new();
    for (key, parent) in parents {
        let Some(identity) = rename_key(key, source_table, renames, names) else {
            continue;
        };
        if !constraints.contains(&identity) {
            continue;
        }
        let mut parent = parent.clone();
        parent.row.table = identity.parent.clone();
        match renamed.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(parent);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(DbError::MigrationReferenceCollision {
                    table: source_table.into(),
                    reference: entry.key().clone(),
                })
            }
        }
    }
    Ok(renamed)
}

fn reference_error(table: &str, column: &str) -> DbError {
    DbError::MigrationReference {
        table: table.to_owned(),
        column: column.to_owned(),
    }
}

#[cfg(test)]
#[path = "migration_change_tests.rs"]
mod tests;
