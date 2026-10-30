//! A statement's scopes for one run: its constant conditions bound to the
//! run's values, and the values its join columns matched in the run's
//! snapshot, read by statements of coven's own (§4).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::row_scope::RowScope;
use crate::scope_plan::{PartnerValue, Source, Sources};
use crate::select_syntax::quoted;
use crate::sql_value::SqlValue as Value;

/// Runs coven's SQL on a run's snapshot, bound to the parameters given,
/// and returns its first column.
pub(crate) type ReadValues<'a> = dyn FnMut(&str, &[Value]) -> rusqlite::Result<Vec<Value>> + 'a;

/// The most values a membership keeps. A query whose partner holds more
/// depends on that column's table without it: memory per live query stays
/// bounded, and so does the cost of reading the values.
pub(crate) const MATCHED_VALUES: usize = 1_000;

/// The scope of each table among `sources`, for a run bound to
/// `parameters`; `values` runs coven's SQL on the run's snapshot and
/// returns its first column.
pub(crate) fn scopes(
    sources: &Sources,
    parameters: &[Value],
    values: &mut ReadValues<'_>,
) -> rusqlite::Result<BTreeMap<String, RowScope>> {
    let mut bound: Vec<Option<RowScope>> = sources.sources.iter().map(|_| None).collect();
    for &position in &sources.order {
        let source = &sources.sources[position];
        let mut scope = source
            .term
            .as_ref()
            .and_then(|term| term.bind(parameters))
            .unwrap_or(RowScope::All);
        for membership in &source.memberships {
            let partner = &sources.sources[membership.partner];
            let partner_scope = bound[membership.partner]
                .as_ref()
                .expect("a membership's partner comes first");
            let Some(found) = matched(
                partner,
                partner_scope,
                &membership.value,
                parameters,
                values,
            )?
            else {
                continue;
            };
            // An expression has no affinity, so SQLite applies the column's
            // to its values: each must compare as it is.
            if matches!(membership.value, PartnerValue::Expression(_))
                && !found
                    .iter()
                    .all(|value| membership.affinity.compares(value))
            {
                continue;
            }
            scope = scope.and(RowScope::equal(membership.column.clone(), found));
        }
        bound[position] = Some(scope);
    }
    let mut scopes: BTreeMap<String, RowScope> = BTreeMap::new();
    for (source, scope) in sources.sources.iter().zip(bound) {
        let (Some(table), Some(scope)) = (&source.table, scope) else {
            continue;
        };
        let scope = match scopes.remove(table) {
            Some(other) => other.or(scope),
            None => scope,
        };
        scopes.insert(table.clone(), scope);
    }
    Ok(scopes)
}

/// The values `value` takes over `partner`'s rows in `scope`, or `None`
/// past the bound.
fn matched(
    partner: &Source,
    scope: &RowScope,
    value: &PartnerValue,
    parameters: &[Value],
    values: &mut ReadValues<'_>,
) -> rusqlite::Result<Option<Vec<Value>>> {
    let expression = match value {
        PartnerValue::Column(column) => {
            // Rows that must hold given values in the column hold no others.
            if let Some(found) = scope.equal_values(column) {
                return Ok(Some(found));
            }
            format!("{}.{}", partner.qualifier, quoted(column))
        }
        PartnerValue::Expression(sql) => sql.clone(),
    };
    let mut bound = parameters.to_vec();
    let condition = condition(scope, &partner.qualifier, &mut bound);
    let sql = format!(
        "SELECT DISTINCT {expression} FROM {} WHERE {condition} LIMIT {}",
        partner
            .from
            .as_ref()
            .expect("an edge's partner reads alone"),
        MATCHED_VALUES + 1
    );
    let found = values(&sql, &bound)?;
    Ok((found.len() <= MATCHED_VALUES).then_some(found))
}

/// `scope` as an SQL condition on the rows `qualifier` names, its values
/// appended to `bound` as parameters.
fn condition(scope: &RowScope, qualifier: &str, bound: &mut Vec<Value>) -> String {
    let parameter = |value: &Value, bound: &mut Vec<Value>| {
        bound.push(value.clone());
        format!("?{}", bound.len())
    };
    match scope {
        RowScope::All => "1".into(),
        RowScope::Compare {
            column,
            value,
            ordering,
            inclusive,
        } => {
            let operator = match (ordering, inclusive) {
                (Ordering::Equal, _) => "=",
                (Ordering::Less, false) => "<",
                (Ordering::Less, true) => "<=",
                (Ordering::Greater, false) => ">",
                (Ordering::Greater, true) => ">=",
            };
            format!(
                "{qualifier}.{} {operator} {}",
                quoted(column),
                parameter(value, bound)
            )
        }
        RowScope::In { values, .. } if values.is_empty() => "0".into(),
        RowScope::In { column, values } => {
            let list: Vec<String> = values.iter().map(|value| parameter(value, bound)).collect();
            format!("{qualifier}.{} IN ({})", quoted(column), list.join(","))
        }
        RowScope::And(left, right) => format!(
            "({} AND {})",
            condition(left, qualifier, bound),
            condition(right, qualifier, bound)
        ),
        RowScope::Or(left, right) => format!(
            "({} OR {})",
            condition(left, qualifier, bound),
            condition(right, qualifier, bound)
        ),
    }
}

#[cfg(test)]
#[path = "scope_values_tests.rs"]
mod tests;
