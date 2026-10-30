//! The rows of a reload the removal rules can reach (§8.4, §16.5).
//!
//! An installed row is taken out only for a reason of its own or of a row
//! it depends on: a reference to a parent generation that isn't current, a
//! failing CHECK, a deleted circle, its key in another audience, or a unique
//! value another row holds. A row of another audience changes only through
//! an installed row: a reference to one, or a key one shares. The removal
//! rules start from the rows that have such a reason, and reach their
//! children, parents and rivals from there (`coven_merge::recompute`). Every
//! other installed row is shown as its merged values, with no loss, claim or
//! written value but the generated values SQLite computes differently.

use crate::snapshot_layout::{SnapshotLayout, TableLayout};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::audience_text;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, RowId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// The rows with a reason the rules can take out, or whose removal can
/// change, among the audiences the reload replaces and the rows that
/// depend on them.
pub(crate) fn rows(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeMap<CircleId, coven_merge::Slot>,
) -> Result<BTreeSet<RowId>, DbError> {
    let layout = SnapshotLayout::new(schema);
    let replaced = db.query(
        "SELECT audience FROM temp._coven_download_replaced ORDER BY audience",
        [],
        |r| crate::write_encoding::audience(&r.get::<_, String>(0)?),
    )?;
    let cyclic = cyclic(schema);
    for table in &layout.tables {
        let rules = &schema.rules[&table.table];
        let installed = table.installed();
        let alias = identifier(&table.table);
        let key = table.column("key");
        let audience_column = table.column("audience");
        let present = table.column("present");
        // A table whose rows reference rows of the same table, directly or
        // through other tables, has its rows put in order one at a time.
        if cyclic.contains(&table.table) {
            db.internal_execute(
                &format!(
                    "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                     SELECT ?1,i.{key},i.{audience_column} FROM {installed} AS i WHERE i.{present}=1
                     ON CONFLICT DO NOTHING"
                ),
                params![table.table],
            )?;
            continue;
        }
        // A reference whose parent isn't at the generation it names.
        for (q, (_, foreign)) in table.pairs.iter().enumerate() {
            db.internal_execute(
                &format!(
                    "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                     SELECT ?1,i.{key},i.{audience_column} FROM {installed} AS i WHERE i.{present}=1 AND i.{parent_key} IS NOT NULL
                     AND i.{parent_generation} IS NOT (SELECT max(r.generation) FROM _coven_rows r WHERE r.table_name=?2 AND r.key=i.{parent_key} AND r.audience=i.{parent_audience})
                     ON CONFLICT DO NOTHING",
                    parent_key = table.parent_key(q),
                    parent_audience = table.parent_audience(q),
                    parent_generation = table.parent_generation(q),
                ),
                params![table.table, foreign.parent],
            )?;
        }
        // A failing CHECK, on the values as SQLite stores them.
        for (_, expression) in &rules.checks {
            db.internal_execute(
                &format!(
                    "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                     SELECT ?1,{key},{audience_column} FROM {installed} AS {alias} WHERE {present}=1 AND COALESCE(CAST(({expression}) AS NUMERIC)=0,0)
                     ON CONFLICT DO NOTHING"
                ),
                params![table.table],
            )?;
        }
        // A unique value two installed rows of one audience hold, or that a
        // row with another key shows in the app's table now.
        for unique in &rules.unique {
            let values = unique.expressions.join(",");
            let held = unique
                .expressions
                .iter()
                .map(|expression| format!("({expression}) IS NOT NULL"))
                .collect::<Vec<_>>()
                .join(" AND ");
            db.internal_execute(
                &format!(
                    "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                     SELECT ?1,{key},{audience_column} FROM {installed} AS {alias} WHERE {present}=1 AND ({predicate}) AND {held}
                     AND ({audience_column},{values}) IN (SELECT {audience_column},{values} FROM {installed} AS {alias}
                       WHERE {present}=1 AND ({predicate}) AND {held} GROUP BY {audience_column},{values} HAVING count(*)>1)
                     ON CONFLICT DO NOTHING",
                    predicate = unique.predicate,
                ),
                params![table.table],
            )?;
            taken(db, schema, table, unique, &held)?;
        }
        // A key a row of another audience has.
        db.internal_execute(
            &format!(
                "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                 SELECT ?1,i.{key},i.{audience_column} FROM {installed} AS i WHERE i.{present}=1
                 AND EXISTS(SELECT 1 FROM _coven_rows r WHERE r.table_name=?1 AND r.key=i.{key} AND r.audience<>i.{audience_column})
                 ON CONFLICT DO NOTHING"
            ),
            params![table.table],
        )?;
        // A merge row the snapshot's author took out, with no synced row.
        db.internal_execute(
            &format!(
                "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                 SELECT ?1,i.{key},i.{audience_column} FROM {installed} AS i WHERE i.{present}=1 AND i.{synced} IS NULL
                 ON CONFLICT DO NOTHING",
                synced = table.column("synced"),
            ),
            params![table.table],
        )?;
        // A row of another audience whose reference to a replaced row, as
        // the row is now or was before, names a generation that isn't
        // current.
        db.internal_execute(
            &format!(
                "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                 SELECT r.table_name,r.key,r.audience FROM {installed} AS i
                 CROSS JOIN _coven_references v ON v.parent_table=?1 AND v.parent_key=i.{key} AND v.parent_audience=i.{audience_column}
                 CROSS JOIN _coven_rows r ON r.id=v.row_id
                 WHERE r.audience NOT IN (SELECT audience FROM temp._coven_download_replaced)
                 AND v.parent_generation IS NOT (SELECT max(p.generation) FROM _coven_rows p WHERE p.table_name=?1 AND p.key=i.{key} AND p.audience=i.{audience_column})
                 ON CONFLICT DO NOTHING"
            ),
            params![table.table],
        )?;
    }
    db.internal_execute(
        "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
         SELECT r.table_name,r.key,r.audience FROM temp._coven_install_before AS b
         CROSS JOIN _coven_references v ON v.parent_table=b.table_name AND v.parent_key=b.key AND v.parent_audience=b.audience
         CROSS JOIN _coven_rows r ON r.id=v.row_id
         WHERE r.audience NOT IN (SELECT audience FROM temp._coven_download_replaced)
         AND v.parent_generation IS NOT (SELECT max(p.generation) FROM _coven_rows p WHERE p.table_name=b.table_name AND p.key=b.key AND p.audience=b.audience)
         ON CONFLICT DO NOTHING",
        [],
    )?;
    // A deleted circle's rows.
    for audience in &replaced {
        if let Audience::Circle(circle) = audience {
            if deleted.contains_key(circle) {
                for table in &layout.tables {
                    db.internal_execute(
                        &format!(
                            "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
                             SELECT ?1,i.{key},i.{audience_column} FROM {installed} AS i WHERE i.{audience_column}=?2 AND i.{present}=1
                             ON CONFLICT DO NOTHING",
                            installed = table.installed(),
                            present = table.column("present"),
                            key = table.column("key"),
                            audience_column = table.column("audience"),
                        ),
                        params![table.table, audience_text(audience)],
                    )?;
                }
            }
        }
    }
    // A row of another audience the rules take out now, which a replaced
    // row can bring back.
    db.internal_execute(
        "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
         SELECT DISTINCT table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'
         AND audience NOT IN (SELECT audience FROM temp._coven_download_replaced)
         ON CONFLICT DO NOTHING",
        [],
    )?;
    let rows = db.query(
        "SELECT table_name,key,audience FROM temp._coven_install_conflicts",
        [],
        crate::row_queries::read_identity,
    )?;
    Ok(rows.into_iter().collect())
}

/// Installed rows taking a unique value the app's table shows for a row
/// with another key: the row giving it up must be put first.
fn taken(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &TableLayout,
    unique: &crate::removal_schema::UniqueSql,
    held: &str,
) -> Result<(), DbError> {
    let alias = identifier(&table.table);
    let keys = crate::write_rows::key_columns(schema.table(&table.table));
    let shown: Vec<_> = unique
        .expressions
        .iter()
        .enumerate()
        .map(|(n, expression)| format!("{expression} AS value_{n}"))
        .chain(
            keys.iter()
                .enumerate()
                .map(|(n, column)| format!("{} AS key_{n}", identifier(&column.name))),
        )
        .collect();
    db.batch(&format!(
        "CREATE TEMP TABLE _coven_install_shown AS SELECT {} FROM main.{alias} AS {alias} WHERE ({}) AND {held}",
        shown.join(","),
        unique.predicate
    ))?;
    let same: Vec<_> = unique
        .expressions
        .iter()
        .enumerate()
        .map(|(n, expression)| format!("s.value_{n}=({expression})"))
        .collect();
    let other: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(n, column)| format!("s.key_{n} IS {alias}.{}", identifier(&column.name)))
        .collect();
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_conflicts(table_name,key,audience)
             SELECT ?1,{key},{audience} FROM {} AS {alias} WHERE {}=1 AND ({}) AND {held}
             AND EXISTS(SELECT 1 FROM temp._coven_install_shown s WHERE {} AND NOT ({}))
             ON CONFLICT DO NOTHING",
            table.installed(),
            table.column("present"),
            unique.predicate,
            same.join(" AND "),
            other.join(" AND "),
            key = table.column("key"),
            audience = table.column("audience"),
        ),
        params![table.table],
    )?;
    db.batch("DROP TABLE temp._coven_install_shown")
}

/// The synced tables whose references lead back to themselves.
fn cyclic(schema: &WriteSchema) -> BTreeSet<String> {
    let parents: BTreeMap<&str, BTreeSet<String>> = schema
        .declarations
        .iter()
        .map(|declaration| {
            let table = schema.table(&declaration.name);
            (
                table.name.as_str(),
                table
                    .foreign_keys
                    .iter()
                    .map(|key| schema.table(&key.target).name.clone())
                    .collect(),
            )
        })
        .collect();
    let mut cyclic = BTreeSet::new();
    for start in parents.keys() {
        let mut seen = BTreeSet::new();
        let mut pending: Vec<&str> = parents[start].iter().map(String::as_str).collect();
        while let Some(table) = pending.pop() {
            if table == *start {
                cyclic.insert((*start).to_owned());
                break;
            }
            if seen.insert(table) {
                if let Some(next) = parents.get(table) {
                    pending.extend(next.iter().map(String::as_str));
                }
            }
        }
    }
    cyclic
}

/// The synced tables, each after the tables its references name.
pub(crate) fn parents_first(schema: &WriteSchema) -> Vec<String> {
    let cyclic = cyclic(schema);
    let mut ordered: Vec<String> = Vec::new();
    let mut pending: Vec<String> = schema
        .declarations
        .iter()
        .map(|declaration| schema.table(&declaration.name).name.clone())
        .collect();
    while !pending.is_empty() {
        let before = pending.len();
        pending.retain(|name| {
            let ready = schema.table(name).foreign_keys.iter().all(|key| {
                let parent = &schema.table(&key.target).name;
                parent == name || ordered.contains(parent) || cyclic.contains(parent)
            });
            if ready {
                ordered.push(name.clone());
            }
            !ready
        });
        if pending.len() == before {
            // Tables in a cycle of references, in declaration order.
            ordered.append(&mut pending);
        }
    }
    ordered
}

#[cfg(test)]
#[path = "snapshot_conflicts_tests.rs"]
mod tests;
