//! A statement's scopes for one run: its constant conditions bound to the run's
//! values, and each join as a lookup a write runs to judge whether a row it
//! changed meets a partner row in that partner's scope (SPEC.md section 7). A
//! run reads nothing for its joins; the write that changes a joined table asks.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::row_scope::{Reference, RowScope, Scopes};
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

/// What each reference to a table among `sources` depends on, for a run
/// bound to
/// `parameters` that returned `last`, the ORDER BY's values of its last row,
/// when it returned as many rows as its LIMIT.
pub(crate) fn scopes(sources: &Sources, parameters: &[Value], last: Option<&[Value]>) -> Scopes {
    let found = source_scopes(sources, RunValues::Run(parameters), last);
    let mut tables: BTreeMap<String, Vec<Reference>> = BTreeMap::new();
    for (source, scope) in sources.sources.iter().zip(found) {
        let Some(table) = source.table().filter(|_| !source.unread) else {
            continue;
        };
        let reference = Reference { scope, columns: Arc::clone(&source.columns.read) };
        tables.entry(table.to_owned()).or_default().push(reference);
    }
    let unread = crate::unread_columns::unread_reads(&sources.sources, &tables);
    let unread_columns = crate::unread_columns::unread_columns(&sources.sources, &tables);
    // A read through a view read whole in a column nothing reads gives the
    // statement no row, so it widens no table's scope.
    let mut inlined = sources.inlined.clone().unwrap_or_default();
    inlined.extend(unread.inside().cloned());
    Scopes {
        tables,
        unread,
        unread_columns,
        inlined,
        fts5_shadows: sources.fts5_shadows.clone(),
        ..Scopes::default()
    }
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
            MemberValue::Expression { term, .. } => RowScope::Row(Arc::new(RowCondition {
                term: Arc::clone(term),
                parameters: parameters.to_vec(),
                test: RowTest::Joined(lookup),
            })),
        }
    };
    for &position in &sources.order {
        let source = &sources.sources[position];
        let bind = |term: &Option<ScopeTerm>| {
            term.as_ref().and_then(|term| term.bind(values)).unwrap_or(RowScope::All)
        };
        let mut scope = bind(&source.term).and(bind(&source.carried));
        let partner_scope = |membership: &Membership| {
            bound[membership.partner].as_ref().expect("a membership's partner comes first")
        };
        // The memberships that compare columns of this source with one
        // partner's by `=` are one join: one row of the partner gives all
        // their values, so the partner's rows are looked up once for them.
        let listed = |membership: &Membership| {
            matches!(membership.member, MemberValue::Column(_))
                && !membership.nulls
                && !membership.nested
                && !membership.pushed
                && !matches!(
                    (&membership.member, &membership.value),
                    (MemberValue::Column(_), PartnerValue::Column(column))
                        if partner_scope(membership).equal_values(column).is_some()
                )
        };
        let mut groups: Vec<(usize, Vec<&Membership>)> = Vec::new();
        for membership in source.memberships.iter().filter(|membership| listed(membership)) {
            match groups.iter_mut().find(|(partner, _)| *partner == membership.partner) {
                Some((_, group)) => group.push(membership),
                None => groups.push((membership.partner, vec![membership])),
            }
        }
        groups.retain(|(_, group)| group.len() > 1);
        let grouped = |membership: &Membership| {
            listed(membership) && groups.iter().any(|(partner, _)| *partner == membership.partner)
        };
        for membership in source.memberships.iter().filter(|membership| !grouped(membership)) {
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
        for (partner, group) in &groups {
            let partner = &sources.sources[*partner];
            let items = group
                .iter()
                .map(|membership| {
                    let MemberValue::Column(column) = &membership.member else {
                        unreachable!("a group lists memberships of columns")
                    };
                    GroupItem {
                        member: column.clone(),
                        value: membership.value.clone(),
                        affinity: membership.affinity,
                    }
                })
                .collect();
            let partner_scope = partner_scope(group[0]).clone();
            scope = scope.and(RowScope::JoinedAll(Arc::new(GroupLookup::new(
                partner.from.clone().expect("an edge's partner reads alone"),
                partner.qualifier.clone(),
                items,
                partner_scope,
                parameters,
            ))));
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
            let found =
                sorted.bound(position, last, parameters, &sources.sources, |membership, before| {
                    joined(membership, partner_scope(membership).clone().and(before))
                });
            if let Some(found) = found {
                scope = scope.and(found);
            }
        }
        bound[position] = Some(scope);
    }
    // A source inside a sorted derived source takes its bound once every
    // source has its scope: it narrows the source's own table, and no lookup
    // of another source.
    let inner: Vec<Option<RowScope>> = (0..sources.sources.len())
        .map(|position| {
            let (sorted, last) = (sources.sorted.as_ref()?, last?);
            sorted.inner_bound(
                position,
                last,
                parameters,
                &sources.sources,
                |membership, before| {
                    let partner = bound[membership.partner]
                        .as_ref()
                        .expect("the planner places every source");
                    joined(membership, partner.clone().and(before))
                },
            )
        })
        .collect();
    bound
        .into_iter()
        .zip(inner)
        .enumerate()
        .map(|(position, (scope, inner))| {
            let mut scope = scope.expect("the planner places every source");
            if let Some(inner) = inner {
                scope = scope.and(inner);
            }
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
        let typed =
            if carries_affinity(self.affinity, value) { sql.typed.as_deref() } else { None };
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
    let typed = match partner {
        PartnerValue::Expression(_) => cast_to_affinity(&value, affinity).map(|cast| found(&cast)),
        PartnerValue::Column(_) => None,
    };
    LookupSql { plain: found(&value), typed, parameters }
}

/// `value`, SQL, cast to `affinity`'s type, which carries the affinity into a
/// comparison with an expression, which has none; `None` for an affinity
/// that converts nothing.
fn cast_to_affinity(value: &str, affinity: Affinity) -> Option<String> {
    match affinity {
        Affinity::Text => Some(format!("CAST({value} AS TEXT)")),
        Affinity::Numeric => Some(format!("CAST({value} AS NUMERIC)")),
        _ => None,
    }
}

/// Whether casting `value` to `affinity`'s type keeps it as it is: a cast would
/// change a value of any other type, and no conversion makes such a value equal
/// to an expression's.
fn carries_affinity(affinity: Affinity, value: &Value) -> bool {
    matches!(
        (affinity, value),
        (Affinity::Text, Value::Text(_)) | (Affinity::Numeric, Value::Integer(_) | Value::Real(_))
    )
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
        RowScope::JoinedAll(group) => group.listed(qualifier, bound),
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

/// One equality of a join to a partner: a column of the joined source, and
/// the value of the partner's rows it equals.
#[derive(Debug)]
pub(crate) struct GroupItem {
    /// The joined source's column, lowercase.
    pub(crate) member: String,
    /// What the partner's rows give.
    pub(crate) value: PartnerValue,
    /// The joined column's affinity.
    pub(crate) affinity: Affinity,
}

/// A join's equalities to one partner, as one question about a changed row:
/// whether one row of the partner, in the scope the run gave it, holds the
/// values of all the changed row's columns (SPEC.md section 7).
#[derive(Debug)]
pub struct GroupLookup {
    /// The partner FROM item, as sqlive's SQL names it with its qualifier.
    from: String,
    qualifier: String,
    items: Vec<GroupItem>,
    /// The partner's rows in this run.
    scope: RowScope,
    /// The question's SQL up to the changed row's values: the partner's scope
    /// as a condition, and the run's parameters it binds, which the values
    /// follow.
    condition: String,
    parameters: Vec<Value>,
}

/// What a write asks of SQLite about a changed row of a joined group.
pub(crate) enum GroupQuestion {
    /// The answer, with no question to ask.
    Answered(bool),
    /// The SQL, and the parameters to bind to it.
    Ask(String, Vec<Value>),
}

impl GroupLookup {
    /// The group of the equalities `items` to the partner `from` as
    /// `qualifier` names it, in its `scope` for a run bound to `parameters`.
    pub(crate) fn new(
        from: String,
        qualifier: String,
        items: Vec<GroupItem>,
        scope: RowScope,
        parameters: &[Value],
    ) -> Self {
        let mut parameters = parameters.to_vec();
        let condition = condition(&scope, &qualifier, &mut parameters);
        Self { from, qualifier, items, scope, condition, parameters }
    }

    /// The columns of the joined source the group compares, lowercase.
    pub(crate) fn columns(&self) -> impl Iterator<Item = &String> {
        self.items.iter().map(|item| &item.member)
    }

    /// What a write asks of the row it changed. NULL equals nothing, so a
    /// row with a NULL in a column the group compares passes none. A column
    /// the image doesn't hold passes: the write that made it didn't know this
    /// scope would test it.
    pub(crate) fn question(&self, row: &crate::row_scope::RowImage) -> GroupQuestion {
        let values: Vec<Option<&Value>> =
            self.items.iter().map(|item| row.get(&item.member)).collect();
        if values.iter().any(|value| matches!(value, Some(Value::Null))) {
            return GroupQuestion::Answered(false);
        }
        if values.iter().any(Option::is_none) {
            return GroupQuestion::Answered(true);
        }
        let mut parameters = self.parameters.clone();
        let mut compared = Vec::with_capacity(self.items.len());
        for (item, value) in self.items.iter().zip(values.into_iter().flatten()) {
            parameters.push(value.clone());
            let placeholder = format!("?{}", parameters.len());
            let value = match &item.value {
                PartnerValue::Expression(_) if carries_affinity(item.affinity, value) => {
                    cast_to_affinity(&placeholder, item.affinity).unwrap_or(placeholder)
                }
                _ => placeholder,
            };
            compared.push(format!(
                "{} = {value} COLLATE BINARY",
                partner_value(&self.qualifier, &item.value)
            ));
        }
        GroupQuestion::Ask(
            format!(
                "SELECT 1 FROM {} WHERE {} AND {} LIMIT 1",
                self.from,
                compared.join(" AND "),
                self.condition
            ),
            parameters,
        )
    }

    /// Whether the columns of the row `qualifier` names equal the values of
    /// one row of the partner in its scope, as SQL whose values go to
    /// `bound`: a row value compared with a subquery that names only the
    /// partner's rows, so no name in it can mean a row around it.
    fn listed(&self, qualifier: &str, bound: &mut Vec<Value>) -> String {
        let members: Vec<String> = self
            .items
            .iter()
            .map(|item| format!("{qualifier}.{}", crate::sql::identifier(&item.member)))
            .collect();
        let values: Vec<String> =
            self.items.iter().map(|item| partner_value(&self.qualifier, &item.value)).collect();
        format!(
            "({}) IN (SELECT {} FROM {} WHERE {})",
            members.join(", "),
            values.join(", "),
            self.from,
            condition(&self.scope, &self.qualifier, bound)
        )
    }
}
