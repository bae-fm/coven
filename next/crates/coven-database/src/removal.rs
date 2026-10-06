//! Materialize the affected removal region in the same writer transaction.
use crate::removal_view::DatabaseRemovalView;
use crate::schema::TableSchema;
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded, sql_value};
use crate::write_rows::{key_columns, AppValues, AppView};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use coven_merge::{RemovalResult, RowId};
use rusqlite::{params, params_from_iter};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn materialize(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    visible: &AppView<'_>,
    view: &DatabaseRemovalView<'_>,
    result: &RemovalResult,
) -> Result<(), DbError> {
    let mut removed_visible = BTreeMap::new();
    for id in result.removed.keys() {
        if let Some(app) = visible
            .row(&(id.table.clone(), id.key.clone()))?
            .filter(|app| app.audience == id.audience)
        {
            removed_visible.insert(id.clone(), app);
        }
    }
    let mut dependencies = BTreeMap::<RowId, BTreeSet<RowId>>::new();
    for (id, app) in &removed_visible {
        for parent in app.parents.values() {
            if let Some(row) = visible.row(parent)? {
                let parent = crate::write_rows::row_id(parent, &row);
                if removed_visible.contains_key(&parent) {
                    dependencies.entry(parent).or_default().insert(id.clone());
                }
            }
        }
    }
    let deletes = dependency_order(removed_visible.keys().cloned().collect(), &dependencies);
    database.materialize(|database| {
        for id in &deletes {
            let table = schema.table(&id.table);
            // A native CASCADE may have already deleted another member of a
            // cycle. Only rows merge decided to remove are in this delete set.
            if let Some(values) = crate::write_rows::read_values(database, table, &id.key)? {
                changed_once(database.app_execute(&format!("DELETE FROM main.{} WHERE {}",identifier(&table.name),predicate(table)),params_from_iter(key_columns(table).iter().map(|c| sql_value(&values[&c.name]))))?)?;
            }
        }
        let mut returning = BTreeSet::new();
        let mut parents = BTreeMap::<RowId,BTreeSet<RowId>>::new();
        for (id, references) in &result.references {
            if result.removed.contains_key(id) { continue; }
            returning.insert(id.clone());
            for reference in references.values() {
                let parent = match reference {
                    coven_merge::ReferenceValue::Original { parent, .. } | coven_merge::ReferenceValue::Default(Some(parent)) => Some(&parent.row),
                    _ => None,
                };
                if let Some(parent) = parent { parents.entry(id.clone()).or_default().insert(parent.clone()); }
            }
        }
        for id in dependency_order(returning, &parents) {
            let row = view.evaluated(&id)?;
            put(database,schema.table(&id.table),&id,&row.values,visible)?;
        }
        for id in &result.region {
            let row = view.evaluated(id)?;
            if !row.facts.present() { continue; }
            let state = view.state(id)?;
            let old = view.prior(id)?;
            let loss = old.loss.filter(|_| old.state.generation()==state.generation());
            let columns:BTreeMap<_,_> = state.cells().iter().map(|(name,cell)| {
                let mut value=cell.value.clone();value.value=row.values[name].clone();(name.clone(),value)
            }).collect();
            let row_ordinal: i64 = database.query_row("SELECT id FROM coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4", params![id.table,id.key,audience_text(&id.audience),state.generation().to_be_bytes().as_slice()], |r| r.get(0))?;
            let previous_claims: BTreeMap<i64,Vec<u8>> = database.query("SELECT constraint_id,value FROM coven_claims WHERE row_id=?1", [row_ordinal], |r| Ok((r.get(0)?,r.get(1)?)))?.into_iter().collect();
            let mut claims = BTreeMap::new();
            if result.removed.contains_key(id) {
                for (constraint, claim) in &row.constraints.unique {
                    let constraint = crate::row_queries::constraint(database, &id.table, constraint)?;
                    claims.insert(constraint, claim.value.clone());
                }
            }
            for constraint in previous_claims.keys().filter(|c| !claims.contains_key(c)) {
                database.internal_execute("DELETE FROM coven_claims WHERE row_id=?1 AND constraint_id=?2", params![row_ordinal,constraint])?;
            }
            for (constraint, value) in &claims {
                if previous_claims.get(constraint) != Some(value) {
                    database.internal_execute("INSERT INTO coven_claims(row_id,constraint_id,audience,value) VALUES(?1,?2,?3,?4) ON CONFLICT(row_id,constraint_id) DO UPDATE SET value=excluded.value", params![row_ordinal,constraint,audience_text(&id.audience),value])?;
                }
            }
            if let Some(rules) = result.removed.get(id) {
                let setters = state.cells().iter().map(|(n,c)| (n.clone(),c.write)).collect();
                let values=encoded(merge_fields::encode_columns(&columns))?;
                let setters=encoded(merge_fields::encode_setters(&setters))?;
                let rules=encoded(merge_fields::encode_rules(rules))?;
                match loss {
                    Some(loss)=> {
                        // Compare the stored fields so unchanged region neighbors
                        // do not produce metadata writes or local trigger effects.
                        database.internal_execute("UPDATE coven_lost SET value=?1,set_by=?2,replaced_by=?3 WHERE id=?4 AND (value<>?1 OR set_by<>?2 OR replaced_by<>?3)",params![values,setters,rules,loss])?;
                    }
                    None=> { database.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replaced_by) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7)",params![id.table,id.key,audience_text(&id.audience),state.generation().to_be_bytes().as_slice(),values,setters,rules])?; }
                }
            } else {
                if let Some(loss)=loss { database.internal_execute("DELETE FROM coven_lost WHERE id=?1",[loss])?; }
            }
        }
        Ok(())
    })?;
    Ok(())
}

/// Visit dependencies first. A back edge is a SQLite-deferred cycle; native
/// actions still execute, and any constraint failure rolls back the write.
fn dependency_order(
    rows: BTreeSet<RowId>,
    dependencies: &BTreeMap<RowId, BTreeSet<RowId>>,
) -> Vec<RowId> {
    let mut visited = BTreeSet::new();
    let mut ordered = Vec::new();
    for root in &rows {
        let mut stack = vec![(root, false)];
        while let Some((row, finish)) = stack.pop() {
            if finish {
                ordered.push(row.clone());
                continue;
            }
            if !visited.insert(row) {
                continue;
            }
            stack.push((row, true));
            if let Some(next) = dependencies.get(row) {
                stack.extend(
                    next.iter()
                        .filter(|r| rows.contains(*r))
                        .map(|r| (r, false)),
                );
            }
        }
    }
    ordered
}

fn put(
    database: &DatabaseConnection,
    table: &TableSchema,
    id: &RowId,
    values: &AppValues,
    visible: &AppView<'_>,
) -> Result<(), DbError> {
    let before = visible
        .row(&(id.table.clone(), id.key.clone()))?
        .filter(|app| app.audience == id.audience);
    match before {
        Some(before) => {
            let changed: Vec<_> = table
                .columns
                .iter()
                .filter(|c| !c.generated && before.values[&c.name] != values[&c.name])
                .collect();
            if !changed.is_empty() {
                let parameters = changed
                    .iter()
                    .map(|c| sql_value(&values[&c.name]))
                    .chain(
                        key_columns(table)
                            .iter()
                            .map(|c| sql_value(&before.values[&c.name])),
                    )
                    .collect::<Vec<_>>();
                changed_once(database.app_execute(
                    &format!(
                        "UPDATE main.{} SET {} WHERE {}",
                        identifier(&table.name),
                        changed
                            .iter()
                            .map(|c| format!("{}=?", identifier(&c.name)))
                            .collect::<Vec<_>>()
                            .join(","),
                        predicate(table)
                    ),
                    params_from_iter(parameters),
                )?)?;
            }
        }
        None => {
            let columns: Vec<_> = table.columns.iter().filter(|c| !c.generated).collect();
            changed_once(database.app_execute(
                &format!(
                    "INSERT INTO main.{} ({}) VALUES ({})",
                    identifier(&table.name),
                    columns
                        .iter()
                        .map(|c| identifier(&c.name))
                        .collect::<Vec<_>>()
                        .join(","),
                    vec!["?"; columns.len()].join(",")
                ),
                params_from_iter(columns.iter().map(|c| sql_value(&values[&c.name]))),
            )?)?;
        }
    }
    Ok(())
}

fn predicate(table: &TableSchema) -> String {
    key_columns(table)
        .iter()
        .map(|c| format!("{}=?", identifier(&c.name)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn changed_once(changed: usize) -> Result<(), DbError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(rusqlite::Error::StatementChangedRows(changed).into())
    }
}

#[cfg(test)]
#[path = "removal_tests.rs"]
pub(crate) mod tests;
