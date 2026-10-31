//! A statement's scopes for one run: its constant conditions bound to the
//! run's values, and each join as a lookup a write runs to judge whether a
//! row it changed meets a partner row in that partner's scope (§4). A run
//! reads nothing for its joins; the write that changes a joined table asks.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use crate::row_scope::{RowScope, Scopes};
use crate::scope_plan::{PartnerValue, Sources};
use crate::scope_term::Affinity;
use crate::select_syntax::quoted;
use crate::sql_value::SqlValue as Value;

/// Runs a lookup for one value of a changed row: whether some partner row
/// in its scope holds it. `Ok(true)` also when coven gives up on the
/// lookup, which then counts as a match.
pub(crate) type Lookups<'a> = dyn FnMut(&Lookup, &Value) -> rusqlite::Result<bool> + 'a;

/// The most SQLite steps one lookup runs. A lookup no index serves reads its
/// partner row by row; past this it stops and counts as a match, so the
/// query runs again and a write's cost stays bounded whatever the app's
/// indexes.
pub(crate) const LOOKUP_STEPS: i32 = 1_000;

/// The scope of each table among `sources`, for a run bound to
/// `parameters`.
pub(crate) fn scopes(sources: &Sources, parameters: &[Value]) -> Scopes {
    let parameters: Arc<[Value]> = parameters.into();
    let mut bound: Vec<Option<RowScope>> = sources.sources.iter().map(|_| None).collect();
    for &position in &sources.order {
        let source = &sources.sources[position];
        let mut scope =
            source.term.as_ref().and_then(|term| term.bind(&parameters)).unwrap_or(RowScope::All);
        for membership in &source.memberships {
            let partner = &sources.sources[membership.partner];
            let partner_scope =
                bound[membership.partner].as_ref().expect("a membership's partner comes first");
            // Rows that must hold given values in the column hold no others.
            if let PartnerValue::Column(column) = &membership.value {
                if let Some(found) = partner_scope.equal_values(column) {
                    scope = scope.and(RowScope::equal(membership.column.clone(), found));
                    continue;
                }
            }
            scope = scope.and(RowScope::Joined {
                column: membership.column.clone(),
                lookup: Arc::new(Lookup {
                    from: partner.from.clone().expect("an edge's partner reads alone"),
                    qualifier: partner.qualifier.clone(),
                    value: membership.value.clone(),
                    affinity: membership.affinity,
                    scope: partner_scope.clone(),
                    parameters: Arc::clone(&parameters),
                    sql: OnceLock::new(),
                }),
            });
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
    Scopes { tables: scopes, inlined: sources.inlined.clone() }
}

/// A join's lookup: whether a row of the partner FROM item, in the scope the
/// run gave it, holds a value equal to the changed row's join column, as the
/// statement's equality compares them.
#[derive(Debug)]
pub(crate) struct Lookup {
    /// The partner FROM item, as coven's SQL names it with its qualifier.
    from: String,
    qualifier: String,
    /// What the partner's rows give.
    value: PartnerValue,
    /// The joined column's.
    affinity: Affinity,
    /// The partner's rows in this run.
    scope: RowScope,
    /// The run's parameters, which the FROM item and an expression can name.
    parameters: Arc<[Value]>,
    sql: OnceLock<LookupSql>,
}

/// A lookup's SQL, made the first time a write runs it.
#[derive(Debug)]
struct LookupSql {
    /// The value bound as it is, its parameter last.
    plain: String,
    /// The value given the joined column's affinity, as the statement's
    /// comparison gives it: only for an expression, which has none.
    typed: Option<String>,
    /// The run's parameters, then the scope's values.
    parameters: Vec<Value>,
}

impl Lookup {
    /// The SQL that looks up `value`, and its parameters with `value` last.
    pub(crate) fn sql(&self, value: &Value) -> (&str, Vec<Value>) {
        let sql = self.sql.get_or_init(|| self.render());
        let typed = match (self.affinity, value) {
            (Affinity::Text, Value::Text(_))
            | (Affinity::Numeric, Value::Integer(_) | Value::Real(_)) => sql.typed.as_deref(),
            _ => None,
        };
        let mut parameters = sql.parameters.clone();
        parameters.push(value.clone());
        (typed.unwrap_or(&sql.plain), parameters)
    }

    fn render(&self) -> LookupSql {
        let mut parameters = self.parameters.to_vec();
        let condition = condition(&self.scope, &self.qualifier, &mut parameters);
        let value = format!("?{}", parameters.len() + 1);
        let found = |compared: &str| {
            format!(
                "SELECT 1 FROM {} WHERE {} = {compared} COLLATE BINARY AND {condition} LIMIT 1",
                self.from,
                partner_value(&self.qualifier, &self.value)
            )
        };
        // An expression has no affinity, so SQLite applies the joined
        // column's to its values. A value of that affinity's own type, cast
        // to it, carries the affinity into the lookup's comparison; a cast
        // would change a value of any other type, and no conversion makes
        // such a value equal to the expression's.
        let typed = match (&self.value, self.affinity) {
            (PartnerValue::Expression(_), Affinity::Text) => {
                Some(found(&format!("CAST({value} AS TEXT)")))
            }
            (PartnerValue::Expression(_), Affinity::Numeric) => {
                Some(found(&format!("CAST({value} AS NUMERIC)")))
            }
            _ => None,
        };
        LookupSql { plain: found(&value), typed, parameters }
    }
}

/// The value a partner's rows give, as SQL over the rows `qualifier` names.
fn partner_value(qualifier: &str, value: &PartnerValue) -> String {
    match value {
        PartnerValue::Column(column) => format!("{qualifier}.{}", quoted(column)),
        PartnerValue::Expression(sql) => format!("({sql})"),
    }
}

/// `scope` as an SQL condition on the rows `qualifier` names, its values
/// appended to `bound` as parameters. A join inside it lists its partner's
/// values in a subquery that names only the partner's rows, so no name in
/// it can mean a row around it.
fn condition(scope: &RowScope, qualifier: &str, bound: &mut Vec<Value>) -> String {
    let parameter = |value: &Value, bound: &mut Vec<Value>| {
        bound.push(value.clone());
        format!("?{}", bound.len())
    };
    match scope {
        RowScope::All => "1".into(),
        RowScope::Compare { column, value, ordering, inclusive } => {
            let operator = match (ordering, inclusive) {
                (Ordering::Equal, _) => "=",
                (Ordering::Less, false) => "<",
                (Ordering::Less, true) => "<=",
                (Ordering::Greater, false) => ">",
                (Ordering::Greater, true) => ">=",
            };
            format!("{qualifier}.{} {operator} {}", quoted(column), parameter(value, bound))
        }
        RowScope::In { values, .. } if values.is_empty() => "0".into(),
        RowScope::In { column, values } => {
            let list: Vec<String> = values.iter().map(|value| parameter(value, bound)).collect();
            format!("{qualifier}.{} IN ({})", quoted(column), list.join(","))
        }
        RowScope::Joined { column, lookup } => format!(
            "{qualifier}.{} IN (SELECT {} FROM {} WHERE {})",
            quoted(column),
            partner_value(&lookup.qualifier, &lookup.value),
            lookup.from,
            condition(&lookup.scope, &lookup.qualifier, bound)
        ),
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
#[path = "scope_lookup_tests.rs"]
mod tests;
