//! What one app statement's SQL lets a live query narrow, read from SQLite's
//! parsed SELECT syntax once per statement and schema (§4). A shape this
//! doesn't understand depends on whole tables rather than guessing.
//!
//! A statement's sources are the tables and other FROM items it joins. Each
//! table's rows are narrowed by the conditions SQL forces on the rows that
//! can reach the result: constants compared with its columns, and equalities
//! with earlier sources, whose values each run finds (`scope_values.rs`).

use std::collections::BTreeMap;

use crate::row_scope::RowScope;
use crate::scope_term::{term, Affinity, ScopeTerm};
use crate::scope_values::ReadValues;
use crate::select_syntax::{conjuncts, names, nests, number_parameters, quoted, text};
use crate::sql_value::SqlValue as Value;
use rusqlite::{Connection, Statement};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, JoinConstraint, JoinOperator, JoinType, Name, OneSelect, Operator,
        QualifiedName, SelectTable, Stmt,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// The rows of each table a statement can depend on, before its values are
/// bound.
pub(crate) struct ScopePlan {
    /// `None` when the statement's shape keeps every table it reads whole.
    sources: Option<Sources>,
}

impl ScopePlan {
    /// The plan for `sql`, prepared as `statement`.
    pub(crate) fn build(
        connection: &Connection,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Self> {
        Ok(Self {
            sources: Sources::read(connection, sql, statement)?,
        })
    }

    /// The scope of each table the statement reads as one of its sources,
    /// for a run bound to `parameters`; `values` runs a statement of coven's
    /// on the run's snapshot and returns its first column. A table the plan
    /// doesn't name depends on every row.
    pub(crate) fn scopes(
        &self,
        parameters: &[Value],
        values: &mut ReadValues<'_>,
    ) -> rusqlite::Result<BTreeMap<String, RowScope>> {
        match &self.sources {
            Some(sources) => crate::scope_values::scopes(sources, parameters, values),
            None => Ok(BTreeMap::new()),
        }
    }
}

/// A statement's sources, in the order their scopes are found.
pub(crate) struct Sources {
    pub(crate) sources: Vec<Source>,
    /// Positions in `sources`, each after the sources its memberships use.
    pub(crate) order: Vec<usize>,
}

pub(crate) struct Source {
    /// The table whose rows this source reads, lowercase; `None` for a FROM
    /// item coven reads only for its values, as a view or table function.
    pub(crate) table: Option<String>,
    /// The FROM item as coven's own SQL names it, with its qualifier.
    pub(crate) from: String,
    /// How the statement names this source's columns.
    pub(crate) qualifier: String,
    /// Conditions on its rows' columns alone.
    pub(crate) term: Option<ScopeTerm>,
    /// Columns that must equal a value an earlier source holds.
    pub(crate) memberships: Vec<Membership>,
}

/// `column` equals `value` over some row of `partner` in its scope.
pub(crate) struct Membership {
    pub(crate) column: String,
    pub(crate) affinity: Affinity,
    pub(crate) partner: usize,
    pub(crate) value: PartnerValue,
}

/// What a membership compares with.
#[derive(Clone)]
pub(crate) enum PartnerValue {
    /// One of the partner's columns, lowercase, of the same affinity.
    Column(String),
    /// An expression over the partner's columns, as SQL; it has no affinity,
    /// so each of its values must compare as it is with the member column.
    Expression(String),
}

/// A source's columns, lowercase: each one's affinity, or `None` when a
/// condition on it can't narrow, as a generated column, one whose collation
/// isn't BINARY, or a view's.
type Columns = BTreeMap<String, Option<Affinity>>;

/// What the planner knows of a FROM item while it reads the conditions.
struct Item {
    source: Source,
    /// Its columns; a table function's aren't known.
    columns: Option<Columns>,
    /// The right side of a LEFT JOIN: only its own ON conditions narrow it.
    nullable: bool,
}

/// Where a condition was written: the WHERE or an inner join's ON, or the
/// ON of the LEFT JOIN that adds the item at this position.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    Inner,
    LeftOn(usize),
}

impl Sources {
    fn read(
        connection: &Connection,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Option<Self>> {
        let Some(sql) = number_parameters(sql, statement)? else {
            return Ok(None);
        };
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, sql.as_bytes());
        // SQLite and the parser can accept different language versions. A
        // SQL form this parser cannot represent depends on whole tables.
        let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
            return Ok(None);
        };
        if !matches!(parser.next(), Ok(None))
            || select.with.is_some()
            || select.body.compounds.is_some()
            || nests(select)
        {
            return Ok(None);
        }
        let OneSelect::Select {
            from: Some(from),
            where_clause,
            ..
        } = &select.body.select
        else {
            return Ok(None);
        };
        let Some(first) = from.select else {
            return Ok(None);
        };
        let Some(item) = Item::read(connection, first, false)? else {
            return Ok(None);
        };
        let mut items = vec![item];
        let mut conditions: Vec<(&Expr<'_>, Place)> = Vec::new();
        for join in from.joins.iter().flatten() {
            let nullable = match join.operator {
                JoinOperator::Comma | JoinOperator::TypedJoin(None) => false,
                JoinOperator::TypedJoin(Some(kind)) => {
                    if kind.intersects(JoinType::NATURAL | JoinType::RIGHT) {
                        return Ok(None);
                    }
                    kind.contains(JoinType::LEFT)
                }
            };
            let Some(item) = Item::read(connection, &join.table, nullable)? else {
                return Ok(None);
            };
            let place = if nullable {
                Place::LeftOn(items.len())
            } else {
                Place::Inner
            };
            items.push(item);
            match &join.constraint {
                None => {}
                Some(JoinConstraint::On(expr)) => {
                    let mut found = Vec::new();
                    conjuncts(expr, &mut found);
                    conditions.extend(found.into_iter().map(|expr| (expr, place)));
                }
                Some(JoinConstraint::Using(_)) => return Ok(None),
            }
        }
        if let Some(predicate) = where_clause {
            let mut found = Vec::new();
            conjuncts(predicate, &mut found);
            conditions.extend(found.into_iter().map(|expr| (expr, Place::Inner)));
        }
        let mut edges = Vec::new();
        for (condition, place) in conditions {
            condition_into(connection, &mut items, &mut edges, condition, place)?;
        }
        Ok(Some(Self::ordered(items, edges)))
    }

    /// Order the items so each comes after the sources it takes values
    /// from: those with conditions of their own first, then views and table
    /// functions, which nothing narrows but whose values others can take,
    /// then those an ordered one reaches, in FROM order.
    fn ordered(mut items: Vec<Item>, edges: Vec<Edge>) -> Self {
        let mut order: Vec<usize> = Vec::new();
        while order.len() < items.len() {
            let remaining: Vec<usize> = (0..items.len())
                .filter(|item| !order.contains(item))
                .collect();
            let next = remaining
                .iter()
                .copied()
                .find(|item| items[*item].source.term.is_some())
                .or_else(|| {
                    remaining
                        .iter()
                        .copied()
                        .find(|item| items[*item].source.table.is_none())
                })
                .or_else(|| {
                    remaining.iter().copied().find(|item| {
                        edges
                            .iter()
                            .any(|edge| edge.member == *item && order.contains(&edge.partner))
                    })
                })
                .unwrap_or(remaining[0]);
            for edge in edges
                .iter()
                .filter(|edge| edge.member == next && order.contains(&edge.partner))
            {
                items[next].source.memberships.push(Membership {
                    column: edge.column.clone(),
                    affinity: edge.affinity,
                    partner: edge.partner,
                    value: edge.value.clone(),
                });
            }
            order.push(next);
        }
        Self {
            sources: items.into_iter().map(|item| item.source).collect(),
            order,
        }
    }
}

/// Read one condition into the terms and edges of the items it narrows.
fn condition_into(
    connection: &Connection,
    items: &mut [Item],
    edges: &mut Vec<Edge>,
    condition: &Expr<'_>,
    place: Place,
) -> rusqlite::Result<()> {
    let mut found = Vec::new();
    names(condition, &mut found);
    let mut referenced = Vec::new();
    for name in found {
        let Some((item, _)) = resolve(items, name) else {
            // A name coven can't place keeps the condition from narrowing.
            return Ok(());
        };
        if !referenced.contains(&item) {
            referenced.push(item);
        }
    }
    let narrows = |item: usize| match place {
        Place::Inner => !items[item].nullable,
        Place::LeftOn(position) => position == item,
    };
    if let [item] = referenced[..] {
        if !narrows(item) {
            return Ok(());
        }
        let column = |expr: &Expr<'_>| {
            let (found, column) = resolve(items, expr)?;
            let affinity = (found == item)
                .then(|| *items[item].columns.as_ref()?.get(&column)?)
                .flatten()?;
            Some((column, affinity))
        };
        if let Some(term) = term(connection, &column, condition)? {
            let source = &mut items[item].source;
            source.term = Some(match source.term.take() {
                Some(existing) => existing.and(term),
                None => term,
            });
        }
        return Ok(());
    }
    let Expr::Binary(left, Operator::Equals, right) = condition else {
        return Ok(());
    };
    for (member, other) in [(left, right), (right, left)] {
        if let Some(edge) = Edge::read(items, member, other) {
            if narrows(edge.member) {
                edges.push(edge);
            }
        }
    }
    Ok(())
}

/// `member`'s column equals a value of `partner`'s rows.
struct Edge {
    member: usize,
    column: String,
    affinity: Affinity,
    partner: usize,
    value: PartnerValue,
}

impl Edge {
    /// The edge `member_expr = other` gives, when `member_expr` is a column
    /// of one source and `other` reads columns of one other source only.
    fn read(items: &[Item], member_expr: &Expr<'_>, other: &Expr<'_>) -> Option<Self> {
        if !is_name(member_expr) {
            return None;
        }
        let (member, column) = resolve(items, member_expr)?;
        items[member].source.table.as_ref()?;
        let affinity = (*items[member].columns.as_ref()?.get(&column)?)?;
        let mut found = Vec::new();
        names(other, &mut found);
        let mut partner = None;
        for name in found {
            let (item, _) = resolve(items, name)?;
            if item == member || partner.is_some_and(|partner| partner != item) {
                return None;
            }
            partner = Some(item);
        }
        let partner = partner?;
        let value = if is_name(other) {
            let (_, partner_column) = resolve(items, other)?;
            match items[partner]
                .columns
                .as_ref()
                .and_then(|columns| columns.get(&partner_column))
            {
                Some(Some(partner_affinity)) => {
                    if *partner_affinity != affinity {
                        return None;
                    }
                    PartnerValue::Column(partner_column)
                }
                // A view's or function's column has an affinity coven
                // doesn't know: `+` drops it, and each value is compared
                // as it is instead.
                _ => PartnerValue::Expression(format!("+{other}")),
            }
        } else {
            match unparenthesized(other) {
                // A cast or COLLATE gives the expression an affinity or a
                // collation of its own, which SQLite's comparison would use.
                Expr::Cast { .. } | Expr::Collate(..) => return None,
                _ => PartnerValue::Expression(other.to_string()),
            }
        };
        Some(Self {
            member,
            column,
            affinity,
            partner,
            value,
        })
    }
}

fn unparenthesized<'a>(expr: &'a Expr<'a>) -> &'a Expr<'a> {
    match expr {
        Expr::Parenthesized(exprs) if exprs.len() == 1 => unparenthesized(&exprs[0]),
        expr => expr,
    }
}

fn is_name(expr: &Expr<'_>) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..)
    )
}

/// The item a column name in the statement belongs to, and the column,
/// lowercase, as SQLite resolves it; `None` when coven can't be sure.
fn resolve(items: &[Item], expr: &Expr<'_>) -> Option<(usize, String)> {
    let (qualifier, name) = match unparenthesized(expr) {
        Expr::Id(name) => (None, Name(name.0)),
        Expr::Name(name) => (None, name.clone()),
        Expr::Qualified(table, name) => (Some(table), name.clone()),
        Expr::DoublyQualified(db, table, name) if *db == "main" => (Some(table), name.clone()),
        _ => return None,
    };
    let column = |item: &Item| match &item.columns {
        Some(columns) => columns
            .keys()
            .find(|column| name == column.as_str())
            .cloned(),
        // A function's columns aren't known; the name is its own.
        None => Some(unquoted(name.0).to_ascii_lowercase()),
    };
    match qualifier {
        Some(qualifier) => {
            let item = items
                .iter()
                .position(|item| *qualifier == Name(&item.source.qualifier))?;
            Some((item, column(&items[item])?))
        }
        None => {
            // A function's columns are unknown: one of them could be it.
            if items.iter().any(|item| item.columns.is_none()) {
                return None;
            }
            let mut found = items
                .iter()
                .enumerate()
                .filter_map(|(position, item)| Some((position, column(item)?)));
            let first = found.next()?;
            found.next().is_none().then_some(first)
        }
    }
}

/// An identifier without the quotes SQL allows around it.
fn unquoted(name: &str) -> String {
    let bytes = name.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(b'"'), Some(b'"')) if name.len() > 1 => name[1..name.len() - 1].replace("\"\"", "\""),
        (Some(b'`'), Some(b'`')) if name.len() > 1 => name[1..name.len() - 1].replace("``", "`"),
        (Some(b'['), Some(b']')) => name[1..name.len() - 1].to_owned(),
        _ => name.to_owned(),
    }
}

impl Item {
    /// The FROM item `table`, or `None` when coven can't read its rows.
    fn read(
        connection: &Connection,
        table: &SelectTable<'_>,
        nullable: bool,
    ) -> rusqlite::Result<Option<Self>> {
        let alias_name = |alias: &Option<As<'_>>, name: &Name<'_>| match alias {
            Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
            None => name.0.to_owned(),
        };
        match table {
            SelectTable::Table(name, alias, _) => {
                if !in_main(name) {
                    return Ok(None);
                }
                let Some((stored, kind)) = object(connection, &name.name)? else {
                    return Ok(None);
                };
                let qualifier = alias_name(alias, &name.name);
                let ordinary = kind == "table";
                Ok(Some(Self {
                    columns: Some(columns(connection, &stored, ordinary)?),
                    source: Source {
                        table: ordinary.then(|| stored.to_ascii_lowercase()),
                        from: format!("main.{} AS {qualifier}", quoted(&stored)),
                        qualifier,
                        term: None,
                        memberships: Vec::new(),
                    },
                    nullable,
                }))
            }
            SelectTable::TableCall(name, _, alias) => {
                if !in_main(name) {
                    return Ok(None);
                }
                let qualifier = alias_name(alias, &name.name);
                let mut call = text(table);
                if alias.is_none() {
                    call = format!("{call} AS {qualifier}");
                }
                Ok(Some(Self {
                    columns: None,
                    source: Source {
                        table: None,
                        from: call,
                        qualifier,
                        term: None,
                        memberships: Vec::new(),
                    },
                    nullable,
                }))
            }
            SelectTable::Select(..) | SelectTable::Sub(..) => Ok(None),
        }
    }
}

fn in_main(name: &QualifiedName<'_>) -> bool {
    name.db_name.as_ref().is_none_or(|name| *name == "main")
}

/// The stored name and type of the table or view `name` names: an ordinary
/// table's rows are read and narrowed, a view's only read for its values.
/// Anything else, as a virtual table, keeps what it reads whole.
fn object(connection: &Connection, name: &Name<'_>) -> rusqlite::Result<Option<(String, String)>> {
    let mut objects = connection.prepare_cached(
        "SELECT name,type FROM pragma_table_list WHERE schema='main' AND type IN ('table','view')",
    )?;
    let found = objects
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .find(|(table, _)| *name == table.as_str());
    Ok(found)
}

/// A table's or view's columns. A view's comparisons happen inside SQLite,
/// so coven gives none of them an affinity to narrow by.
fn columns(connection: &Connection, table: &str, ordinary: bool) -> rusqlite::Result<Columns> {
    let mut metadata =
        connection.prepare_cached("SELECT name,type,hidden FROM pragma_table_xinfo(?1,'main')")?;
    let declared = metadata
        .query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut columns = Columns::new();
    for (column, declared, hidden) in declared {
        let affinity = if ordinary && !hidden {
            let (_, collation, _, _, _) =
                connection.column_metadata(Some("main"), table, column.as_str())?;
            collation
                .is_some_and(|collation| collation.to_bytes().eq_ignore_ascii_case(b"BINARY"))
                .then(|| Affinity::declared(&declared))
        } else {
            None
        };
        columns.insert(column.to_ascii_lowercase(), affinity);
    }
    Ok(columns)
}

#[cfg(test)]
#[path = "scope_plan_tests.rs"]
mod tests;
