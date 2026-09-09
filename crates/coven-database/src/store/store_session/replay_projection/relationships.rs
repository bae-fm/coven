use super::{
    projection_table_columns, projection_table_rows, ProjectionKeyValue, ProjectionTableRows,
};
use crate::{DbError, ForeignKeyEdge};
use rusqlite::{types::Value, Connection};
use std::collections::{BTreeMap, BTreeSet};

mod row_identity;
mod values;
use row_identity::RowIdentityExposure;
use values::NativeProjectionValues;

type RowKey = Vec<ProjectionKeyValue>;
type DesiredRows = BTreeMap<RowKey, Option<Vec<Value>>>;

struct Relationship {
    parent: usize,
    child: usize,
    edge: ForeignKeyEdge,
    columns: Vec<(usize, usize)>,
    rows: Vec<(RowKey, RowKey)>,
}

/// Apply the logical row transition to current device-local descendants.
/// Matching is frozen against the original parent identities: replacement
/// deletes and a successor's reused key cannot transfer or destroy a child.
pub(super) fn with_local_relationships<T>(
    target: &Connection,
    tables: &mut Vec<ProjectionTableRows>,
    install: impl FnOnce(&[ProjectionTableRows]) -> Result<T, DbError>,
) -> Result<T, DbError> {
    RowIdentityExposure::new(target).run(|identity| {
        project_local_relationships(target, tables, identity)?;
        install(tables)
    })
}

fn project_local_relationships(
    target: &Connection,
    tables: &mut Vec<ProjectionTableRows>,
    identity: &mut RowIdentityExposure<'_>,
) -> Result<(), DbError> {
    let projected_count = tables.len();
    let mut pending = load_relationship_edges(target, tables)?;
    let mut relationships = Vec::new();
    let mut values = NativeProjectionValues::new(target)?;
    let mut desired = initial_rows(tables, projected_count);
    let mut defaults = BTreeMap::new();
    let mut seen = BTreeSet::new();
    loop {
        let previous_count = tables.len();
        discover_relationships(
            target,
            tables,
            &mut desired,
            &mut pending,
            &mut relationships,
            identity,
        )?;
        for table in &tables[previous_count..] {
            values.include(table)?;
        }
        if !seen.insert((relationships.len(), state_key(&desired))) {
            return Err(DbError::Message(
                "local foreign-key actions do not produce a consistent row transition".into(),
            ));
        }
        let mut next = initial_rows(tables, projected_count);
        let mut assignments = BTreeMap::<(usize, RowKey, usize), Vec<Value>>::new();
        for (relationship_index, relationship) in relationships.iter().enumerate() {
            for (parent_key, child_key) in &relationship.rows {
                let parent = desired[relationship.parent]
                    .get(parent_key)
                    .ok_or_else(|| {
                        DbError::Message(
                            "local relationship lost its original parent identity".into(),
                        )
                    })?;
                let action = match parent {
                    None => relationship.edge.on_delete.as_str(),
                    Some(row) if relationship.key_changed(target, tables, parent_key, row)? => {
                        relationship.edge.on_update.as_str()
                    }
                    Some(_) => continue,
                };
                if action == "CASCADE" && parent.is_none() {
                    next[relationship.child].insert(child_key.clone(), None);
                    continue;
                }
                match action {
                    "NO ACTION" | "RESTRICT" => continue,
                    "CASCADE" | "SET NULL" | "SET DEFAULT" => {}
                    _ => {
                        return Err(DbError::Message(format!(
                            "unknown SQLite foreign-key action {action:?}"
                        )));
                    }
                }
                for (parent_column, child_column) in &relationship.columns {
                    let value = match action {
                        "CASCADE" => parent.as_ref().expect("delete cascade was handled")
                            [*parent_column]
                            .clone(),
                        "SET NULL" => Value::Null,
                        "SET DEFAULT" => {
                            let key = (relationship_index, child_key.clone(), *child_column);
                            if !defaults.contains_key(&key) {
                                let value = values
                                    .default_value(&tables[relationship.child], *child_column)?;
                                defaults.insert(key.clone(), value);
                            }
                            defaults[&key].clone()
                        }
                        _ => unreachable!("non-mutating actions were handled"),
                    };
                    let assignment = (relationship.child, child_key.clone(), *child_column);
                    assignments.entry(assignment).or_default().push(value);
                }
            }
        }
        for ((table, key, column), proposals) in assignments {
            let row = next[table].get_mut(&key).ok_or_else(|| {
                DbError::Message("local relationship lost its original child identity".into())
            })?;
            if let Some(row) = row {
                if !tables[table].writable_columns.contains(&column) {
                    return Err(DbError::Message(format!(
                        "foreign-key action cannot write generated column {}.{}",
                        tables[table].table, tables[table].columns[column],
                    )));
                }
                let mut selected = None;
                for proposal in proposals {
                    let mut candidate = row.clone();
                    candidate[column] = proposal;
                    let normalized = values.normalize(&tables[table], &candidate)?;
                    let value = normalized[column].clone();
                    if selected.as_ref().is_some_and(|existing| existing != &value) {
                        return Err(DbError::Message(format!(
                            "foreign-key actions assign conflicting values to {}.{}",
                            tables[table].table, tables[table].columns[column],
                        )));
                    }
                    selected = Some(value);
                }
                row[column] = selected.expect("recorded assignments contain an action");
            }
        }
        for (table_index, rows) in next.iter_mut().enumerate().skip(projected_count) {
            for (key, row) in rows.iter_mut() {
                if let Some(row) = row {
                    if tables[table_index].target.get(key) != Some(row) {
                        *row = values.normalize(&tables[table_index], row)?;
                    }
                }
            }
        }
        if next == desired {
            validate_restrictions(target, tables, &relationships, &desired)?;
            for (index, rows) in desired.into_iter().enumerate().skip(projected_count) {
                let mut source = BTreeMap::new();
                for row in rows.into_values().flatten() {
                    let key = tables[index]
                        .primary_key
                        .iter()
                        .map(|column| ProjectionKeyValue::from(&row[*column]))
                        .collect();
                    if source.insert(key, row).is_some() {
                        return Err(DbError::Message(format!(
                            "local foreign-key actions collide on a row identity in {}",
                            tables[index].table,
                        )));
                    }
                }
                tables[index].source = source;
            }
            return Ok(());
        }
        desired = next;
    }
}

fn initial_rows(tables: &[ProjectionTableRows], projected_count: usize) -> Vec<DesiredRows> {
    tables
        .iter()
        .enumerate()
        .map(|(index, table)| {
            table
                .target
                .iter()
                .map(|(key, row)| {
                    (
                        key.clone(),
                        if index < projected_count {
                            table.source.get(key).cloned()
                        } else {
                            Some(row.clone())
                        },
                    )
                })
                .collect()
        })
        .collect()
}

fn state_key(rows: &[DesiredRows]) -> Vec<Vec<(RowKey, Option<Vec<ProjectionKeyValue>>)>> {
    rows.iter()
        .map(|table| {
            table
                .iter()
                .map(|(key, row)| {
                    (
                        key.clone(),
                        row.as_ref()
                            .map(|row| row.iter().map(ProjectionKeyValue::from).collect()),
                    )
                })
                .collect()
        })
        .collect()
}

fn load_relationship_edges(
    target: &Connection,
    tables: &[ProjectionTableRows],
) -> Result<Vec<(String, ForeignKeyEdge)>, DbError> {
    let projected = tables
        .iter()
        .map(|table| table.table.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut edges = Vec::new();
    for child in crate::coven_schema::user_table_names(target)? {
        if projected.contains(&child.to_ascii_lowercase()) {
            continue;
        }
        for edge in crate::foreign_key_edges(target, &child).map_err(|error| {
            DbError::context(
                "local replay relationships",
                crate::gate::GateError::ForeignKeySchema(error),
            )
        })? {
            edges.push((child.clone(), edge));
        }
    }
    Ok(edges)
}

fn discover_relationships(
    target: &Connection,
    tables: &mut Vec<ProjectionTableRows>,
    desired: &mut Vec<DesiredRows>,
    pending: &mut Vec<(String, ForeignKeyEdge)>,
    relationships: &mut Vec<Relationship>,
    identity: &mut RowIdentityExposure<'_>,
) -> Result<(), DbError> {
    let mut indices = tables
        .iter()
        .enumerate()
        .map(|(index, table)| (table.table.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();
    for (child_name, mut edge) in std::mem::take(pending) {
        identity.translate_edge(&child_name, &mut edge);
        let Some(&parent) = indices.get(&edge.parent_table.to_ascii_lowercase()) else {
            pending.push((child_name, edge));
            continue;
        };
        if !relationship_has_affected_rows(
            target,
            &tables[parent],
            &desired[parent],
            &child_name,
            &edge,
        )? {
            pending.push((child_name, edge));
            continue;
        }
        let child = match indices.get(&child_name.to_ascii_lowercase()) {
            Some(&index) => index,
            None => {
                let table = load_local_rows(target, &child_name, identity)?;
                desired.push(
                    table
                        .target
                        .iter()
                        .map(|(key, row)| (key.clone(), Some(row.clone())))
                        .collect(),
                );
                let index = tables.len();
                tables.push(table);
                indices.insert(child_name.to_ascii_lowercase(), index);
                index
            }
        };
        identity.translate_edge(&child_name, &mut edge);
        relationships.push(Relationship::load(target, tables, parent, child, edge)?);
    }
    Ok(())
}

fn relationship_has_affected_rows(
    target: &Connection,
    parent: &ProjectionTableRows,
    desired: &DesiredRows,
    child: &str,
    edge: &ForeignKeyEdge,
) -> Result<bool, DbError> {
    let deferred: bool = target.pragma_query_value(None, "defer_foreign_keys", |row| row.get(0))?;
    let columns = edge
        .columns
        .iter()
        .map(|pair| column_index(parent, &pair.parent))
        .collect::<Result<Vec<_>, DbError>>()?;
    for (key, row) in desired {
        let action = match row {
            None => edge.on_delete.as_str(),
            Some(row) if parent_key_changed(target, parent, &columns, key, row)? => {
                edge.on_update.as_str()
            }
            Some(_) => continue,
        };
        if action == "NO ACTION" || action == "RESTRICT" && deferred {
            continue;
        }
        let join = relationship_predicate(edge);
        let predicate = parent
            .primary_key
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "p.{} IS ?{}",
                    crate::quote_ident(&parent.columns[*column]),
                    index + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let original = &parent.target[key];
        let exists: bool = target.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM main.{} p JOIN main.{} c ON {join} WHERE {predicate})",
                crate::quote_ident(&parent.table),
                crate::quote_ident(child)
            ),
            rusqlite::params_from_iter(parent.primary_key.iter().map(|column| &original[*column])),
            |row| row.get(0),
        )?;
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

fn load_local_rows(
    target: &Connection,
    table: &str,
    identity: &mut RowIdentityExposure<'_>,
) -> Result<ProjectionTableRows, DbError> {
    let (mut columns, mut primary_key, mut writable_columns) =
        projection_table_columns(target, table)?;
    let without_rowid: bool = target.query_row(
        "SELECT wr FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    if !without_rowid {
        let primary_index: bool = target.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk')",
            [table],
            |row| row.get(0),
        )?;
        if primary_key.is_empty() || primary_index {
            let rowid = identity.expose(table, &mut columns)?;
            primary_key = vec![columns.len()];
            writable_columns.push(columns.len());
            columns.push(rowid.to_string());
        }
    }
    if primary_key.is_empty() {
        return Err(DbError::Message(format!(
            "local table {table:?} has no addressable row identity"
        )));
    }
    let target_rows = projection_table_rows(target, table, &columns, &primary_key)?;
    Ok(ProjectionTableRows {
        table: table.into(),
        columns,
        primary_key,
        writable_columns,
        source: target_rows.clone(),
        target: target_rows,
    })
}

impl Relationship {
    fn load(
        target: &Connection,
        tables: &[ProjectionTableRows],
        parent: usize,
        child: usize,
        edge: ForeignKeyEdge,
    ) -> Result<Self, DbError> {
        let columns = edge
            .columns
            .iter()
            .map(|pair| {
                Ok((
                    column_index(&tables[parent], &pair.parent)?,
                    column_index(&tables[child], &pair.child)?,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        let selection = [("p", &tables[parent]), ("c", &tables[child])]
            .into_iter()
            .flat_map(|(alias, table)| {
                table.primary_key.iter().map(move |column| {
                    format!("{alias}.{}", crate::quote_ident(&table.columns[*column]))
                })
            })
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = relationship_predicate(&edge);
        let parent_count = tables[parent].primary_key.len();
        let total = parent_count + tables[child].primary_key.len();
        let rows = crate::query_mapped_rows(
            target,
            &format!(
                "SELECT {selection} FROM main.{} p JOIN main.{} c ON {predicate}",
                crate::quote_ident(&tables[parent].table),
                crate::quote_ident(&tables[child].table)
            ),
            [],
            |row| {
                let values = (0..total)
                    .map(|column| row.get::<_, Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((
                    values[..parent_count]
                        .iter()
                        .map(ProjectionKeyValue::from)
                        .collect(),
                    values[parent_count..]
                        .iter()
                        .map(ProjectionKeyValue::from)
                        .collect(),
                ))
            },
        )?;
        Ok(Self {
            parent,
            child,
            edge,
            columns,
            rows,
        })
    }

    fn key_changed(
        &self,
        target: &Connection,
        tables: &[ProjectionTableRows],
        key: &RowKey,
        row: &[Value],
    ) -> Result<bool, DbError> {
        let columns = self
            .columns
            .iter()
            .map(|(parent, _)| *parent)
            .collect::<Vec<_>>();
        parent_key_changed(target, &tables[self.parent], &columns, key, row)
    }
}

fn parent_key_changed(
    target: &Connection,
    table: &ProjectionTableRows,
    columns: &[usize],
    key: &RowKey,
    row: &[Value],
) -> Result<bool, DbError> {
    let original = table
        .target
        .get(key)
        .ok_or_else(|| DbError::Message("relationship parent row is absent".into()))?;
    let mut parameters = columns
        .iter()
        .map(|column| &row[*column])
        .collect::<Vec<_>>();
    let comparison = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "{} IS ?{}",
                crate::quote_ident(&table.columns[*column]),
                index + 1
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let predicate = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "{} IS ?{}",
                crate::quote_ident(&table.columns[*column]),
                parameters.len() + index + 1
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    parameters.extend(table.primary_key.iter().map(|column| &original[*column]));
    Ok(target.query_row(
        &format!(
            "SELECT NOT({comparison}) FROM main.{} WHERE {predicate}",
            crate::quote_ident(&table.table)
        ),
        rusqlite::params_from_iter(parameters),
        |row| row.get(0),
    )?)
}

fn relationship_predicate(edge: &ForeignKeyEdge) -> String {
    edge.columns
        .iter()
        .map(|pair| {
            // A foreign key applies only the parent's affinity and collation.
            // Unary plus removes the child's affinity from the comparison.
            format!(
                "p.{} = +c.{}",
                crate::quote_ident(&pair.parent),
                crate::quote_ident(&pair.child),
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn column_index(table: &ProjectionTableRows, column: &str) -> Result<usize, DbError> {
    table
        .columns
        .iter()
        .position(|name| name.eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            DbError::Message(format!(
                "foreign-key column {}.{column} is absent",
                table.table
            ))
        })
}

fn validate_restrictions(
    target: &Connection,
    tables: &[ProjectionTableRows],
    relationships: &[Relationship],
    rows: &[DesiredRows],
) -> Result<(), DbError> {
    let deferred: bool = target.pragma_query_value(None, "defer_foreign_keys", |row| row.get(0))?;
    if deferred {
        return Ok(());
    }
    for relationship in relationships {
        for (parent_key, child_key) in &relationship.rows {
            let changed = match &rows[relationship.parent][parent_key] {
                None => relationship.edge.on_delete == "RESTRICT",
                Some(row) => {
                    relationship.edge.on_update == "RESTRICT"
                        && relationship.key_changed(target, tables, parent_key, row)?
                }
            };
            if changed && rows[relationship.child][child_key].is_some() {
                return Err(DbError::Message(format!(
                    "foreign-key RESTRICT prevents changing {} while {} references it",
                    tables[relationship.parent].table, tables[relationship.child].table
                )));
            }
        }
    }
    Ok(())
}
