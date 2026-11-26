//! A statement's scopes for one run: its constant conditions bound to the run's
//! values, and each join as a lookup a write runs to judge whether a row it
//! changed meets a partner row in that partner's scope (SPEC.md section 7). A
//! run reads nothing for its joins; the write that changes a joined table asks.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::row_scope::{RowScope, Scopes};
use crate::scope_edge::MemberValue;
use crate::scope_plan::{Membership, PartnerValue, Sources};
use crate::scope_row::{RowCondition, RowTest};
use crate::scope_term::{Affinity, RunValues, ScopeTerm};
use crate::sql_value::SqlValue as Value;

/// What a write asks of SQLite about a changed row.
#[derive(Clone, Copy)]
pub enum Judging<'a> {
    /// Whether some partner row in the lookup's scope holds the value.
    Partner(&'a Lookup, &'a Value),
    /// Whether a row passes a condition: the SQL and its parameters.
    Row(&'a str, &'a [Value]),
    /// A question about a row of a table that a host statement read, which
    /// SQLite answers as the host's own SQL: the tables such a statement
    /// reads can be ones the app's SQL can't.
    Host(&'a Judging<'a>),
}

/// Runs one question about a changed row: whether SQLite found a row.
pub type Lookups<'a> = dyn FnMut(Judging<'_>) -> Result<bool, NoAnswer> + 'a;

/// Why a question about a changed row has no answer.
#[derive(Debug)]
pub enum NoAnswer {
    /// The question ran past `LOOKUP_STEPS` and stopped. sqlive can't tell
    /// whether the row passes, so the live query runs again
    /// (SPEC.md section 7).
    Stopped,
    /// SQLite failed.
    Failed(rusqlite::Error),
}

impl From<rusqlite::Error> for NoAnswer {
    fn from(error: rusqlite::Error) -> Self {
        Self::Failed(error)
    }
}

/// The most SQLite steps one question about a changed row runs. A lookup no
/// index serves reads its partner row by row; past this it stops and the
/// query runs again, so a write's cost stays bounded whatever the app's
/// indexes.
pub const LOOKUP_STEPS: i32 = 1_000;

/// The scope of each table among `sources`, for a run bound to
/// `parameters` that returned `last`, the ORDER BY's values of its last row,
/// when it returned as many rows as its LIMIT.
pub(crate) fn scopes(sources: &Sources, parameters: &[Value], last: Option<&[Value]>) -> Scopes {
    let found = source_scopes(sources, RunValues::Run(parameters), last);
    Scopes {
        tables: table_scopes(sources, found),
        inlined: sources.inlined.clone().unwrap_or_default(),
        fts5_shadows: sources.fts5_shadows.clone(),
        ..Scopes::default()
    }
}

/// The scope of each table among `sources`, whose own scopes are `found`.
pub(crate) fn table_scopes(sources: &Sources, found: Vec<RowScope>) -> BTreeMap<String, RowScope> {
    let mut scopes: BTreeMap<String, RowScope> = BTreeMap::new();
    for (source, scope) in sources.sources.iter().zip(found) {
        let Some(table) = source.table() else {
            continue;
        };
        let scope = match scopes.remove(table) {
            Some(other) => other.or(scope),
            None => scope,
        };
        scopes.insert(table.to_owned(), scope);
    }
    scopes
}

/// The scope each of `sources` gives its rows, for a run bound to `values`
/// that returned `last`, as `scopes` does. The sources placed after a sorted
/// one look its rows up within its bound too.
pub(crate) fn source_scopes(
    sources: &Sources,
    values: RunValues<'_>,
    last: Option<&[Value]>,
) -> Vec<RowScope> {
    let parameters = values.parameters();
    let mut bound: Vec<Option<RowScope>> = sources.sources.iter().map(|_| None).collect();
    for &position in &sources.order {
        let source = &sources.sources[position];
        let bind = |term: &Option<ScopeTerm>| {
            term.as_ref().and_then(|term| term.bind(values)).unwrap_or(RowScope::All)
        };
        let mut scope = bind(&source.term).and(bind(&source.carried));
        let partner_scope = |membership: &Membership| {
            bound[membership.partner].as_ref().expect("a membership's partner comes first")
        };
        // The rows whose membership finds a row of its partner in
        // `partner_scope`.
        let joined = |membership: &Membership, partner_scope: RowScope| {
            let partner = &sources.sources[membership.partner];
            let lookup = Arc::new(Lookup::new(
                partner.from.clone().expect("an edge's partner reads alone"),
                partner.qualifier.clone(),
                membership.value.clone(),
                membership.affinity,
                membership.nulls,
                partner_scope,
                parameters,
            ));
            match &membership.member {
                MemberValue::Column(column) => RowScope::Joined { column: column.clone(), lookup },
                // A write asks SQLite for the expression's value in the
                // changed row and whether a partner row gives it, compared
                // as the statement compares them (`scope_row.rs`).
                MemberValue::Expression(term) => RowScope::Row(Arc::new(RowCondition {
                    term: Arc::clone(term),
                    parameters: parameters.to_vec(),
                    test: RowTest::Joined(lookup),
                })),
            }
        };
        for membership in &source.memberships {
            // Rows that must hold given values in the column hold no others.
            if let (MemberValue::Column(member), PartnerValue::Column(column)) =
                (&membership.member, &membership.value)
            {
                if let Some(found) = partner_scope(membership).equal_values(column) {
                    scope = scope.and(RowScope::equal(member.clone(), found));
                    continue;
                }
            }
            scope = scope.and(joined(membership, partner_scope(membership).clone()));
        }
        // An OR's scope passes the rows any of its sides' scopes do.
        for sides in &source.either {
            let either = sides.iter().map(|side| {
                side.memberships.iter().fold(bind(&side.term), |scope, membership| {
                    scope.and(joined(membership, partner_scope(membership).clone()))
                })
            });
            if let Some(either) = either.reduce(RowScope::or) {
                scope = scope.and(either);
            }
        }
        if let (Some(sorted), Some(last)) = (&sources.sorted, last) {
            let found = sorted.bound(
                position,
                last,
                parameters,
                &source.memberships,
                |membership, before| {
                    joined(membership, partner_scope(membership).clone().and(before))
                },
            );
            if let Some(found) = found {
                scope = scope.and(found);
            }
        }
        bound[position] = Some(scope);
    }
    bound
        .into_iter()
        .enumerate()
        .map(|(position, scope)| {
            let scope = scope.expect("the planner places every source");
            // A subquery's window narrows its table's own scope, and no
            // lookup of another source (`scope_window.rs`).
            let window = sources.windows.iter().find(|window| window.item == position);
            match window.and_then(|window| window.bind(values)) {
                Some(window) => scope.and(window),
                None => scope,
            }
        })
        .collect()
}

/// A join's lookup: whether a row of the partner FROM item, in the scope the
/// run gave it, holds a value equal to the changed row's join column, as the
/// statement's equality compares them.
#[derive(Debug)]
pub struct Lookup {
    /// The partner FROM item, as sqlive's SQL names it with its qualifier.
    from: String,
    qualifier: String,
    /// What the partner's rows give.
    value: PartnerValue,
    /// The joined column's.
    affinity: Affinity,
    /// Whether the join is on `IS`, which matches a NULL with a NULL.
    nulls: bool,
    /// The partner's rows in this run.
    scope: RowScope,
    sql: LookupSql,
}

/// A lookup's SQL, made with the lookup.
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
    /// The lookup of the partner `from` as `qualifier` names it, in its
    /// `scope` for a run bound to `parameters`, which the FROM item and an
    /// expression can name.
    fn new(
        from: String,
        qualifier: String,
        value: PartnerValue,
        affinity: Affinity,
        nulls: bool,
        scope: RowScope,
        parameters: &[Value],
    ) -> Self {
        let sql = render(&from, &qualifier, &value, affinity, nulls, &scope, parameters);
        Self { from, qualifier, value, affinity, nulls, scope, sql }
    }

    /// Whether the join is on `IS`, so a NULL looks up a partner's NULL.
    pub(crate) fn nulls(&self) -> bool {
        self.nulls
    }

    /// The SQL that looks up `value`, and its parameters with `value` last.
    pub fn sql(&self, value: &Value) -> (&str, Vec<Value>) {
        let sql = &self.sql;
        let typed = match (self.affinity, value) {
            (Affinity::Text, Value::Text(_))
            | (Affinity::Numeric, Value::Integer(_) | Value::Real(_)) => sql.typed.as_deref(),
            _ => None,
        };
        let mut parameters = sql.parameters.clone();
        parameters.push(value.clone());
        (typed.unwrap_or(&sql.plain), parameters)
    }
}

/// A lookup's SQL: the partner's rows in `scope` holding the looked-up value,
/// compared by `IS` when `nulls`.
fn render(
    from: &str,
    qualifier: &str,
    partner: &PartnerValue,
    affinity: Affinity,
    nulls: bool,
    scope: &RowScope,
    parameters: &[Value],
) -> LookupSql {
    let mut parameters = parameters.to_vec();
    let condition = condition(scope, qualifier, &mut parameters);
    let value = format!("?{}", parameters.len() + 1);
    let operator = if nulls { "IS" } else { "=" };
    let found = |compared: &str| {
        format!(
            "SELECT 1 FROM {from} WHERE {} {operator} {compared} COLLATE BINARY AND {condition} LIMIT 1",
            partner_value(qualifier, partner)
        )
    };
    // An expression has no affinity, so SQLite applies the joined
    // column's to its values. A value of that affinity's own type, cast
    // to it, carries the affinity into the lookup's comparison; a cast
    // would change a value of any other type, and no conversion makes
    // such a value equal to the expression's.
    let typed = match (partner, affinity) {
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

/// The value a partner's rows give, as SQL over the rows `qualifier` names.
fn partner_value(qualifier: &str, value: &PartnerValue) -> String {
    match value {
        PartnerValue::Column(column) => format!("{qualifier}.{}", crate::sql::identifier(column)),
        PartnerValue::Expression(sql) => format!("({sql})"),
    }
}

/// `scope` as an SQL condition on the rows `qualifier` names, its values
/// appended to `bound` as parameters. A join inside it lists its partner's
/// values in a subquery that names only the partner's rows, so no name in
/// it can mean a row around it; a join on `IS` also passes a NULL when some
/// partner row in its scope gives NULL.
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
            format!(
                "{qualifier}.{} {operator} {}",
                crate::sql::identifier(column),
                parameter(value, bound)
            )
        }
        RowScope::Null { column, not } => format!(
            "{qualifier}.{} IS {}NULL",
            crate::sql::identifier(column),
            if *not { "NOT " } else { "" }
        ),
        RowScope::In { values, .. } if values.is_empty() => "0".into(),
        RowScope::In { column, values } => {
            let list: Vec<String> = values.iter().map(|value| parameter(value, bound)).collect();
            format!("{qualifier}.{} IN ({})", crate::sql::identifier(column), list.join(","))
        }
        // Its parameters are numbered as the statement's, which the lookup
        // binds first. A comparison with a value no cast keeps as it is
        // passes every row, as a write's question counts it a match.
        RowScope::Row(row) => row.sql(bound).unwrap_or_else(|| "1".into()),
        // A window narrows only its own table's scope, which no lookup
        // reads (`scopes`); passing every row keeps a lookup's rows whole.
        RowScope::Window(_) => "1".into(),
        RowScope::Joined { column, lookup } => {
            joined(&format!("{qualifier}.{}", crate::sql::identifier(column)), lookup, bound)
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

/// Whether `compared`, SQL over a changed row, equals a value some row of
/// `lookup`'s partner in its scope gives, as SQL whose values go to
/// `bound`. A join inside it lists its partner's values in a subquery that
/// names only the partner's rows, so no name in it can mean a row around
/// it; a join on `IS` also passes a NULL when some partner row in its scope
/// gives NULL.
pub(crate) fn joined(compared: &str, lookup: &Lookup, bound: &mut Vec<Value>) -> String {
    let value = partner_value(&lookup.qualifier, &lookup.value);
    let listed = format!(
        "{compared} IN (SELECT {value} FROM {} WHERE {})",
        lookup.from,
        condition(&lookup.scope, &lookup.qualifier, bound)
    );
    if lookup.nulls {
        format!(
            "({listed} OR {compared} IS NULL AND EXISTS (SELECT 1 FROM {} WHERE {value} IS NULL AND {}))",
            lookup.from,
            condition(&lookup.scope, &lookup.qualifier, bound)
        )
    } else {
        listed
    }
}
