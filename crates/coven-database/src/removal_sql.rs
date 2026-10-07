//! SQLite evaluates one region row at a time with the app's affinities and SQL.
use crate::removal_schema::TableRules;
use crate::schema::{SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{sql_value, value};
use crate::write_rows::{column_list, equality_key, read_row, AppValues};
use crate::write_schema::{evaluation_name, ordinal};
use crate::DbError;
use coven_format::value::Value;
use coven_merge::{Constraints, RowState, UniqueClaim};
use rusqlite::params_from_iter;
use std::collections::BTreeSet;

pub(crate) fn evaluate_values(
    db: &DatabaseConnection,
    table: &TableSchema,
    values: &AppValues,
) -> Result<AppValues, DbError> {
    let target = identifier(&evaluation_name(table));
    let ordinal = identifier(&ordinal(table));
    db.internal_execute(&format!("DELETE FROM temp.{target} WHERE {ordinal}=1"), [])?;
    let columns: Vec<_> = table.columns.iter().filter(|c| !c.generated).collect();
    db.internal_execute(
        &format!(
            "INSERT INTO temp.{target} ({ordinal},{}) VALUES (1,{})",
            columns
                .iter()
                .map(|c| identifier(&c.name))
                .collect::<Vec<_>>()
                .join(","),
            columns
                .iter()
                .map(|c| if values.contains_key(&c.name) {
                    "?"
                } else {
                    c.default.as_deref().unwrap_or("NULL")
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
        params_from_iter(
            columns
                .iter()
                .filter_map(|c| values.get(&c.name).map(sql_value)),
        ),
    )?;
    db.query_row(
        &format!(
            "SELECT {} FROM temp.{target} WHERE {ordinal}=1",
            column_list(table)
        ),
        [],
        |r| read_row(table, r),
    )
}

/// Bind reference values into the target's declared types. SQLite, rather than
/// Rust casts, applies its exact affinity rules (including numeric text).
pub(crate) fn reference_values(
    db: &DatabaseConnection,
    table: &TableSchema,
    columns: &[String],
    values: &[Value],
) -> Result<Vec<Value>, DbError> {
    let target = identifier(&crate::write_schema::affinity_name(table));
    let ordinal = identifier(&ordinal(table));
    db.internal_execute(&format!("DELETE FROM temp.{target} WHERE {ordinal}=1"), [])?;
    let names = columns
        .iter()
        .map(|c| identifier(c))
        .collect::<Vec<_>>()
        .join(",");
    db.internal_execute(
        &format!(
            "INSERT INTO temp.{target} ({ordinal},{names}) VALUES (1,{})",
            vec!["?"; columns.len()].join(",")
        ),
        params_from_iter(values.iter().map(sql_value)),
    )?;
    db.query_row(
        &format!("SELECT {names} FROM temp.{target} WHERE {ordinal}=1"),
        [],
        |r| (0..columns.len()).map(|i| value(r.get_ref(i)?)).collect(),
    )
}

pub(crate) fn constraints(
    db: &DatabaseConnection,
    table: &TableSchema,
    rules: &TableRules,
    state: &RowState<Value>,
    values: &AppValues,
    stamp: impl Fn(coven_merge::WriteId) -> coven_merge::Timestamp,
) -> Result<Constraints, DbError> {
    let values = unique_values(db, table, rules, values)?;
    let failed_checks = checks(db, table, rules)?;
    let mut unique = std::collections::BTreeMap::new();
    for claim in &rules.unique {
        let Some(value) = values.get(&claim.identity) else {
            continue;
        };
        let timestamp = claim
            .dependencies
            .iter()
            .filter_map(|c| state.cells().get(c).map(|cell| cell.write))
            .map(&stamp)
            .max()
            .unwrap_or_else(|| {
                let write = state.generations()[&state.generation()];
                stamp(write)
            });
        unique.insert(
            claim.identity.clone(),
            UniqueClaim {
                value: value.clone(),
                timestamp,
            },
        );
    }
    Ok(Constraints {
        failed_checks,
        unique,
    })
}

/// Materialization needs the prior SQL values' occupied unique slots, even when
/// a snapshot has removed that row's merge state. No write stamp participates.
pub(crate) fn unique_values(
    db: &DatabaseConnection,
    table: &TableSchema,
    rules: &TableRules,
    values: &AppValues,
) -> Result<std::collections::BTreeMap<coven_merge::UniqueConstraint, Vec<u8>>, DbError> {
    evaluate_values(db, table, values)?;
    let mut unique = std::collections::BTreeMap::new();
    for claim in &rules.unique {
        let values = db.query(
            &format!(
                "SELECT {} FROM temp.{} AS {} WHERE {}=1 AND ({})",
                claim.expressions.join(","),
                identifier(&evaluation_name(table)),
                identifier(&table.name),
                identifier(&ordinal(table)),
                claim.predicate
            ),
            [],
            |r| {
                (0..claim.expressions.len())
                    .map(|i| value(r.get_ref(i)?))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        let Some(values) = values.into_iter().next() else {
            continue;
        };
        if values.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        unique.insert(
            claim.identity.clone(),
            equality_key(&values, &claim.collations)?,
        );
    }
    Ok(unique)
}

pub(crate) fn permits(
    db: &DatabaseConnection,
    table: &TableSchema,
    rules: &TableRules,
    values: &AppValues,
    replacement: &AppValues,
) -> Result<bool, DbError> {
    if replacement.iter().any(|(n, v)| {
        matches!(v, Value::Null) && table.columns.iter().any(|c| c.name == *n && c.not_null)
    }) {
        return Ok(false);
    }
    let mut replaced = values.clone();
    replaced.extend(replacement.clone());
    evaluate_values(db, table, &replaced)?;
    Ok(checks(db, table, rules)?.is_empty())
}

fn checks(
    db: &DatabaseConnection,
    table: &TableSchema,
    rules: &TableRules,
) -> Result<BTreeSet<String>, DbError> {
    let mut failed = BTreeSet::new();
    for (name, expression) in &rules.checks {
        if db.query_row(&format!("SELECT COALESCE(CAST(({expression}) AS NUMERIC)=0,0) FROM temp.{} AS {} WHERE {}=1",identifier(&evaluation_name(table)),identifier(&table.name),identifier(&ordinal(table))),[],|r| r.get::<_,bool>(0))? { failed.insert(name.clone()); }
    }
    Ok(failed)
}

pub(crate) fn null_reference(table: &TableSchema, key: &SchemaForeignKey) -> AppValues {
    key.columns
        .iter()
        .map(|name| {
            (
                crate::write_rows::column_name(table, name).to_owned(),
                Value::Null,
            )
        })
        .collect()
}
