//! What one app statement's SQL lets a live query narrow, read from SQLite's
//! parsed SELECT syntax once per statement and schema (§4). A shape this
//! doesn't understand depends on whole tables rather than guessing.
//!
//! A statement is a tree of SELECT cores: the statement itself, each arm of
//! a compound, and each subquery, in any clause, sitting in the core whose
//! clause holds it. A core's sources are the tables and other FROM items it
//! joins. Each table's rows are narrowed by the conditions its own core
//! forces on the rows that can reach the result: constants compared with its
//! columns, and equalities with earlier sources of the core or of a core it
//! sits in, which a write that changes the table looks up
//! (`scope_lookup.rs`). Values pass
//! into a subquery: whether a subquery finds a row doesn't narrow the core
//! around it, and only the column a subquery returns as it is does come out
//! of one, as an equality with a column around it (`scope_subquery.rs`). A
//! condition on one table's row that none of these reads, a function of a
//! column or a CASE, narrows that table by asking SQLite about each changed
//! row (`scope_row.rs`). A subquery in FROM, a CTE and one of the app's
//! views are read as cores too, and conditions on their rows pass into them
//! (`derived.rs`).

use std::collections::BTreeSet;

use crate::derived::Ctes;
use crate::from_item::{read_item, Columns, FromItem};
use crate::row_scope::Scopes;
use crate::scope_row::row_term;
use crate::scope_subquery::compared_subquery;
use crate::scope_term::{term, Affinity, ScopeTerm};
use crate::select_syntax::{conjuncts, number_parameters, parts, repeatable, window_parts, Part};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use rusqlite::{Connection, Statement};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, JoinConstraint, JoinOperator, JoinType, Name, OneSelect, Operator,
        ResultColumn, Select, SelectTable, Stmt,
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
        Ok(Self { sources: Sources::read(connection, sql, statement)? })
    }

    /// The scope of each table the statement reads as one of its sources,
    /// for a run bound to `parameters`. A table the plan doesn't name
    /// depends on every row.
    pub(crate) fn scopes(&self, parameters: &[Value]) -> Scopes {
        match &self.sources {
            Some(sources) => crate::scope_lookup::scopes(sources, parameters),
            None => Scopes::default(),
        }
    }
}

/// A statement's sources, in the order their scopes are found.
pub(crate) struct Sources {
    pub(crate) sources: Vec<Source>,
    /// Positions in `sources`, each after the sources its memberships use.
    pub(crate) order: Vec<usize>,
    /// The app's views and the CTEs read as sources of their own, by the
    /// names, lowercase, SQLite reports reads through them by.
    pub(crate) inlined: BTreeSet<String>,
}

pub(crate) struct Source {
    /// The table whose rows this source reads, lowercase; `None` for a FROM
    /// item coven reads only for its values, as a table function, one of
    /// coven's views, or a subquery, CTE or view whose own tables are read
    /// as sources of their own.
    pub(crate) table: Option<String>,
    /// The FROM item as coven's own SQL names it, with its qualifier;
    /// `None` for a table function of another source's columns, or a
    /// subquery or CTE inside another, whose rows exist only beside that
    /// source's and so can't be read alone.
    pub(crate) from: Option<String>,
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
#[derive(Clone, Debug)]
pub(crate) enum PartnerValue {
    /// One of the partner's columns, lowercase, of the same affinity.
    Column(String),
    /// An expression over the partner's columns, as SQL, of no affinity
    /// coven knows: a lookup gives the member column's to the value it
    /// compares.
    Expression(String),
}

/// One SELECT core: the statement itself, an arm of a compound, or a
/// subquery anywhere in either.
pub(crate) struct Core {
    /// The core a subquery sits in, whose names its SQL can name too.
    parent: Option<usize>,
    /// Its FROM items, positions in the planner's items.
    pub(crate) items: Vec<usize>,
    /// The names its result columns are given, which SQLite resolves a bare
    /// name to when no FROM item has that column.
    aliases: Vec<String>,
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
        if !matches!(parser.next(), Ok(None)) {
            return Ok(None);
        }
        let mut planner = Planner {
            items: Vec::new(),
            cores: Vec::new(),
            edges: Vec::new(),
            aggregates: aggregates(connection)?,
            inlined: Vec::new(),
        };
        Ok(read_select(connection, &mut planner, select, None, &Ctes::default())?
            .map(|_| planner.ordered()))
    }
}

/// A statement's cores, their items and the edges between them, as the
/// `read_` functions find them; each of those gives `None` for a shape that
/// keeps every table whole.
pub(crate) struct Planner {
    pub(crate) items: Vec<FromItem>,
    /// Each core after the core it sits in.
    pub(crate) cores: Vec<Core>,
    pub(crate) edges: Vec<Edge>,
    /// The functions that can make one row of many, lowercase: aggregate
    /// and window functions.
    pub(crate) aggregates: BTreeSet<String>,
    /// The app's views and the CTEs read as sources of their own, by the
    /// names, lowercase, SQLite reports reads through them by.
    pub(crate) inlined: Vec<String>,
}

/// The names of the connection's aggregate and window functions.
fn aggregates(connection: &Connection) -> rusqlite::Result<BTreeSet<String>> {
    let mut functions = connection
        .prepare_cached("SELECT DISTINCT name FROM pragma_function_list WHERE type IN ('a','w')")?;
    let names = functions
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|name| name.map(|name| name.to_ascii_lowercase()))
        .collect();
    names
}

impl Planner {
    /// Order the items so each comes after the sources it takes values
    /// from: core by core, each after the core it sits in; within a core,
    /// those an ordered one reaches first, so each takes the values of the
    /// sources the core's narrowing started from, then those with
    /// conditions of their own, then views and table functions, which
    /// nothing narrows but whose values others can take, in FROM order.
    fn ordered(mut self) -> Sources {
        let mut order: Vec<usize> = Vec::new();
        let all: Vec<usize> =
            self.cores.iter().flat_map(|core| core.items.iter().copied()).collect();
        // The items that have waited, which go as soon as they stop.
        let mut waited: BTreeSet<usize> = BTreeSet::new();
        while order.len() < all.len() {
            let remaining: Vec<usize> =
                all.iter().copied().filter(|item| !order.contains(item)).collect();
            let waiting = self.waiting(&order, &remaining);
            waited.extend(waiting.iter().copied());
            let released = remaining
                .iter()
                .copied()
                .find(|item| waited.contains(item) && !waiting.contains(item));
            let free: Vec<usize> =
                remaining.iter().copied().filter(|item| !waiting.contains(item)).collect();
            let next = match (released, free.first()) {
                (Some(released), _) => released,
                // The first core with an item that may go, as every core
                // goes in turn when none waits.
                (None, Some(first)) => {
                    let core = self.items[*first].core;
                    let candidates: Vec<usize> = free
                        .iter()
                        .copied()
                        .filter(|item| self.items[*item].core == core)
                        .collect();
                    let items = &self.items;
                    candidates
                        .iter()
                        .copied()
                        .find(|item| {
                            self.edges
                                .iter()
                                .any(|edge| edge.member == *item && order.contains(&edge.partner))
                        })
                        .or_else(|| {
                            // An item that takes values from a FROM item
                            // nothing narrows, a table function whose
                            // columns no equality joins to another source,
                            // goes after it, as the values are all of its
                            // narrowing.
                            let takes_unplaced_values = |item: usize| {
                                self.edges.iter().any(|edge| {
                                    edge.member == item
                                        && candidates.contains(&edge.partner)
                                        && items[edge.partner].source.table.is_none()
                                        && items[edge.partner].source.term.is_none()
                                        && !self
                                            .edges
                                            .iter()
                                            .any(|other| other.member == edge.partner)
                                })
                            };
                            candidates.iter().copied().find(|item| {
                                items[*item].source.term.is_some() && !takes_unplaced_values(*item)
                            })
                        })
                        .or_else(|| {
                            candidates
                                .iter()
                                .copied()
                                .find(|item| items[*item].source.table.is_none())
                        })
                        .unwrap_or(candidates[0])
                }
                // Every item left waits, each on one that waits: the first
                // goes without the values of partners not placed yet.
                (None, None) => remaining[0],
            };
            let taken: Vec<Edge> = self
                .edges
                .iter()
                .filter(|edge| edge.member == next && order.contains(&edge.partner))
                .cloned()
                .collect();
            for edge in &taken {
                self.items[next].source.memberships.push(Membership {
                    column: edge.column.clone(),
                    affinity: edge.affinity,
                    partner: edge.partner,
                    value: edge.value.clone(),
                });
            }
            // A subquery's, CTE's or view's cores come later, and take
            // the same values.
            crate::derived::push_down(&mut self, next, None, taken);
            order.push(next);
        }
        Sources {
            sources: self.items.into_iter().map(|item| item.source).collect(),
            order,
            inlined: self.inlined.into_iter().collect(),
        }
    }

    /// The items of `remaining` that wait: those whose value comes from a
    /// subquery's source not placed yet, which a column compared with a
    /// subquery's value takes (`scope_subquery.rs`), and those whose every
    /// partner waits, which couldn't take their values from a partner that
    /// is placed after them.
    fn waiting(&self, order: &[usize], remaining: &[usize]) -> BTreeSet<usize> {
        let mut waiting: BTreeSet<usize> = remaining
            .iter()
            .copied()
            .filter(|item| {
                self.edges.iter().any(|edge| {
                    edge.nested && edge.member == *item && !order.contains(&edge.partner)
                })
            })
            .collect();
        loop {
            let starved = remaining.iter().copied().find(|item| {
                let mut partners =
                    self.edges.iter().filter(|edge| edge.member == *item).map(|edge| edge.partner);
                !waiting.contains(item)
                    && partners.clone().next().is_some()
                    && partners.all(|partner| waiting.contains(&partner))
            });
            match starved {
                Some(item) => waiting.insert(item),
                None => break,
            };
        }
        waiting
    }

    /// The edge `member_expr = other` gives, written in `core` with `other`
    /// first when `first`, when `member_expr` is a column of one of the
    /// core's sources and `other` reads columns of one other source only,
    /// of this core or one it sits in, which coven can read alone; and when
    /// SQLite compares them by BINARY, as coven does: the comparison takes
    /// the collation of a COLLATE in either side, else of the first side
    /// when that is a column. `other` holds no clock and no function the
    /// connection doesn't flag deterministic: a lookup evaluates it again, and
    /// would get another answer than the query did.
    fn edge(
        &self,
        connection: &Connection,
        core: usize,
        member_expr: &Expr<'_>,
        other: &Expr<'_>,
        first: bool,
    ) -> rusqlite::Result<Option<Edge>> {
        let Some(edge) = self.edge_between(core, member_expr, other, first) else {
            return Ok(None);
        };
        let mut found = Vec::new();
        parts(other, &mut found);
        Ok(repeatable(connection, &found)?.then_some(edge))
    }

    fn edge_between(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
        other: &Expr<'_>,
        first: bool,
    ) -> Option<Edge> {
        if !is_name(member_expr) {
            return None;
        }
        let items = &self.items;
        let (member, column) = self.resolve(core, member_expr)?;
        if items[member].core != core {
            return None;
        }
        if items[member].source.table.is_none() && items[member].derived.is_none() {
            return None;
        }
        let affinity = items[member].columns.affinity(&column)?;
        let mut found = Vec::new();
        parts(other, &mut found);
        let mut partner = None;
        for part in found {
            let name = match part {
                Part::Name(name) => name,
                Part::Function(..) | Part::Clock => continue,
                Part::Select(_) | Part::Table | Part::Collation => return None,
            };
            let (item, _) = self.resolve(core, name)?;
            if item == member || partner.is_some_and(|partner| partner != item) {
                return None;
            }
            partner = Some(item);
        }
        let partner = partner?;
        items[partner].source.from.as_ref()?;
        if first && is_name(stripped(other)) {
            let (_, partner_column) = self.resolve(core, stripped(other))?;
            // A column coven gives an affinity compares by BINARY: a
            // table's, or a subquery's that passes such a column on.
            items[partner].columns.affinity(&partner_column)?;
        }
        let value = if is_name(other) {
            let (_, partner_column) = self.resolve(core, other)?;
            match items[partner].columns.affinity(&partner_column) {
                Some(partner_affinity) => {
                    if partner_affinity != affinity {
                        return None;
                    }
                    PartnerValue::Column(partner_column)
                }
                // A column coven gives no affinity, as a view's or a
                // function's: a lookup compares it as the statement does,
                // with whatever affinity SQLite gives it.
                None => PartnerValue::Expression(other.to_string()),
            }
        } else {
            match unparenthesized(other) {
                // A cast or COLLATE gives the expression an affinity or a
                // collation of its own, which SQLite's comparison would use.
                Expr::Cast { .. } | Expr::Collate(..) => return None,
                _ => PartnerValue::Expression(other.to_string()),
            }
        };
        Some(Edge { member, column, affinity, partner, value, nested: false })
    }

    /// The item a column name written in `core` belongs to, and the column,
    /// lowercase, as SQLite resolves it: among the core's own items first,
    /// then those of each core it sits in. `None` when coven can't be sure.
    pub(crate) fn resolve(&self, core: usize, expr: &Expr<'_>) -> Option<(usize, String)> {
        let (qualifier, name) = match unparenthesized(expr) {
            Expr::Id(name) => (None, Name(name.0)),
            Expr::Name(name) => (None, name.clone()),
            Expr::Qualified(table, name) => (Some(table), name.clone()),
            Expr::DoublyQualified(db, table, name) if *db == "main" => (Some(table), name.clone()),
            _ => return None,
        };
        let mut scope = Some(core);
        while let Some(current) = scope {
            let items = self.cores[current]
                .items
                .iter()
                .map(|position| (*position, &self.items[*position]));
            match qualifier {
                Some(qualifier) => {
                    // The first item of that name decides: SQLite looks
                    // further out only when it lacks the column.
                    if let Some((position, item)) =
                        items.clone().find(|(_, item)| *qualifier == Name(&item.source.qualifier))
                    {
                        return Some((position, item.columns.named(&name)?));
                    }
                }
                None => {
                    // A function's columns are unknown: one of them could be it.
                    if items.clone().any(|(_, item)| matches!(item.columns, Columns::Unknown)) {
                        return None;
                    }
                    let mut found = items.clone().filter_map(|(position, item)| {
                        Some((position, item.columns.named(&name)?))
                    });
                    if let Some(first) = found.next() {
                        return found.next().is_none().then_some(first);
                    }
                    // A column whose name coven doesn't know, a result
                    // column's name, or a rowid, which SQLite finds here
                    // before looking further out.
                    if items.clone().any(|(_, item)| !item.columns.complete())
                        || self.cores[current].aliases.iter().any(|alias| name == Name(alias))
                        || ["rowid", "oid", "_rowid_"].iter().any(|rowid| name == *rowid)
                    {
                        return None;
                    }
                }
            }
            scope = self.cores[current].parent;
        }
        None
    }

    /// Narrow the sources by `found`, which conditions on the joined rows
    /// give. A LEFT JOIN's right side it narrows is an inner join's then: a
    /// left row with NULLs in its place fails the condition, so no row the
    /// LEFT JOIN alone adds reaches the result. A term a row of NULLs can
    /// pass, as a NULL test, narrows nothing on a right side that is still a
    /// LEFT JOIN's and doesn't make it inner: the NULLs of a left row no
    /// right row joins pass it, and a right row added for that left row takes
    /// it out.
    pub(crate) fn narrow(&mut self, mut found: Narrowing) {
        found.terms.retain(|(item, term)| {
            !(term.may_hold_on_nulls() && self.items[*item].left_on.is_some())
        });
        for item in self.add(found) {
            self.join_inner(item);
        }
    }

    /// `found`, which the ON of the LEFT JOIN adding `item` gives: what it
    /// says of `item` narrows it, and the rest narrows the other sources
    /// only once the join is an inner join's. Until then a left row appears
    /// whatever its ON says of the left side.
    fn narrow_on(&mut self, item: usize, found: Narrowing) {
        let (own, others) = found.split(item);
        self.add(own);
        match &mut self.items[item].left_on {
            Some(held) => held.extend(others),
            None => self.narrow(others),
        }
    }

    /// Make `item` an inner join's right side, if it was a LEFT JOIN's: its
    /// ON narrows the other sources as an inner join's does.
    pub(crate) fn join_inner(&mut self, item: usize) {
        if let Some(held) = self.items[item].left_on.take() {
            self.narrow(held);
        }
    }

    /// Add `found` to the sources' terms and the edges, and say which
    /// sources it narrows.
    fn add(&mut self, found: Narrowing) -> Vec<usize> {
        let mut narrowed = Vec::new();
        for (item, term) in found.terms {
            let source = &mut self.items[item].source;
            source.term = Some(match source.term.take() {
                Some(existing) => existing.and(term),
                None => term,
            });
            narrowed.push(item);
        }
        for edge in found.edges {
            narrowed.push(edge.member);
            self.edges.push(edge);
        }
        narrowed
    }
}

/// Read `select`, which sits in the core `parent`, if any, where its FROM
/// items can name `ctes`: the cores of its arms.
pub(crate) fn read_select<'a>(
    connection: &Connection,
    planner: &mut Planner,
    select: &'a Select<'a>,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<Vec<usize>>> {
    let inner;
    let ctes = match &select.with {
        Some(with) => {
            let Some(with) = ctes.with(with) else {
                return Ok(None);
            };
            inner = with;
            &inner
        }
        None => ctes,
    };
    let arms = std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
    let mut cores = Vec::new();
    for arm in arms {
        let Some(core) = read_core(connection, planner, arm, parent, ctes)? else {
            return Ok(None);
        };
        cores.push(core);
    }
    let mut found = Vec::new();
    for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
        parts(&term.expr, &mut found);
    }
    // A single core's ORDER BY can name its sources. A compound's matches
    // its terms to result columns instead, whose SELECTs are read where
    // they are.
    let order = match cores[..] {
        [core] => Some(core),
        _ if found.iter().any(|part| matches!(part, Part::Select(_) | Part::Table)) => {
            return Ok(None)
        }
        _ => None,
    };
    if read_nested(connection, planner, &found, order, ctes)?.is_none() {
        return Ok(None);
    }
    // SQLite resolves LIMIT and OFFSET with no names around them.
    let mut found = Vec::new();
    for limit in select.limit.iter() {
        parts(&limit.expr, &mut found);
        if let Some(offset) = &limit.offset {
            parts(offset, &mut found);
        }
    }
    Ok(read_nested(connection, planner, &found, None, ctes)?.map(|_| cores))
}

/// The SELECTs read in one core's clauses, each with its arms' cores.
type Subqueries<'a> = Vec<(&'a Select<'a>, Vec<usize>)>;

/// Read the SELECTs among `found`, each sitting in `parent`.
fn read_nested<'a>(
    connection: &Connection,
    planner: &mut Planner,
    found: &[Part<'a>],
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<Subqueries<'a>>> {
    let mut read = Vec::new();
    for part in found {
        match part {
            Part::Name(_) | Part::Collation | Part::Function(..) | Part::Clock => {}
            Part::Select(select) => {
                let Some(cores) = read_select(connection, planner, select, parent, ctes)? else {
                    return Ok(None);
                };
                read.push((*select, cores));
            }
            // Its table is read outside any FROM, and so in every row.
            Part::Table => return Ok(None),
        }
    }
    Ok(Some(read))
}

/// Read one core, which sits in `parent`: its items, then the SELECTs in any
/// of its clauses, then its conditions, which a comparison with a subquery's
/// value reads the subquery's cores for.
fn read_core<'a>(
    connection: &Connection,
    planner: &mut Planner,
    select: &'a OneSelect<'a>,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<usize>> {
    let core = planner.cores.len();
    planner.cores.push(Core { parent, items: Vec::new(), aliases: Vec::new() });
    let mut found = Vec::new();
    let (columns, from, where_clause, group_by, having, window_clause) = match select {
        OneSelect::Values(rows) => {
            for expr in rows.iter().flat_map(|row| row.iter()) {
                parts(expr, &mut found);
            }
            return Ok(read_nested(connection, planner, &found, Some(core), ctes)?.map(|_| core));
        }
        OneSelect::Select {
            columns, from, where_clause, group_by, having, window_clause, ..
        } => (columns, from, where_clause, group_by, having, window_clause),
    };
    for column in columns.iter() {
        if let ResultColumn::Expr(expr, alias) = column {
            parts(expr, &mut found);
            if let Some(As::As(alias) | As::Elided(alias)) = alias {
                planner.cores[core].aliases.push(alias.0.to_owned());
            }
        }
    }
    let mut conditions: Vec<(&Expr<'_>, Place)> = Vec::new();
    if let Some(from) = from {
        let tables = from
            .select
            .iter()
            .map(|table| (*table, None))
            .chain(from.joins.iter().flatten().map(|join| (&join.table, Some(join))));
        for (table, join) in tables {
            let left = match join.map(|join| join.operator) {
                None | Some(JoinOperator::Comma | JoinOperator::TypedJoin(None)) => false,
                Some(JoinOperator::TypedJoin(Some(kind))) => {
                    if kind.intersects(JoinType::NATURAL | JoinType::RIGHT) {
                        return Ok(None);
                    }
                    kind.contains(JoinType::LEFT)
                }
            };
            if let SelectTable::TableCall(_, Some(args), _) = table {
                for arg in args.iter() {
                    parts(arg, &mut found);
                }
            }
            let Some(item) = read_item(connection, planner, table, left, core, parent, ctes)?
            else {
                return Ok(None);
            };
            let place = if left { Place::LeftOn(planner.items.len()) } else { Place::Inner };
            planner.cores[core].items.push(planner.items.len());
            planner.items.push(item);
            match join.and_then(|join| join.constraint.as_ref()) {
                None => {}
                Some(JoinConstraint::On(expr)) => {
                    parts(expr, &mut found);
                    let mut joined = Vec::new();
                    conjuncts(expr, &mut joined);
                    conditions.extend(joined.into_iter().map(|expr| (expr, place)));
                }
                Some(JoinConstraint::Using(_)) => return Ok(None),
            }
        }
    }
    if let Some(predicate) = where_clause {
        parts(predicate, &mut found);
        let mut joined = Vec::new();
        conjuncts(predicate, &mut joined);
        conditions.extend(joined.into_iter().map(|expr| (expr, Place::Inner)));
    }
    for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
        parts(expr, &mut found);
    }
    if let Some(having) = having {
        parts(having, &mut found);
    }
    for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
        window_parts(&definition.window, &mut found);
    }
    let Some(subqueries) = read_nested(connection, planner, &found, Some(core), ctes)? else {
        return Ok(None);
    };
    let mut read = Vec::new();
    for (condition, place) in conditions {
        read.push((read_condition(connection, planner, core, condition, &subqueries)?, place));
    }
    // The conditions a row of NULLs fails come first, as they make LEFT JOINs
    // inner, which the others see.
    let (others, rejecting): (Vec<_>, Vec<_>) =
        read.into_iter().partition(|(found, _)| found.may_hold_on_nulls());
    for (found, place) in rejecting.into_iter().chain(others) {
        match place {
            Place::Inner => planner.narrow(found),
            Place::LeftOn(item) => planner.narrow_on(item, found),
        }
    }
    // What its conditions say of a subquery's, CTE's or view's rows alone
    // narrows that one's own cores. Its equalities with other sources do
    // once the order says which it takes values from.
    for item in planner.cores[core].items.clone() {
        let term = planner.items[item].source.term.clone();
        crate::derived::push_down(planner, item, term, Vec::new());
    }
    Ok(Some(core))
}

/// Read one condition of `core` into the terms and edges of the items
/// it narrows: only the core's own. A condition holding a SELECT doesn't
/// narrow, except a column compared with the value a subquery returns,
/// `subqueries` being the SELECTs the core's clauses hold.
fn read_condition<'a>(
    connection: &Connection,
    planner: &mut Planner,
    core: usize,
    condition: &'a Expr<'a>,
    subqueries: &Subqueries<'a>,
) -> rusqlite::Result<Narrowing> {
    if let Some((member, select)) = compared_subquery(condition) {
        let cores = subqueries.iter().find(|(read, _)| std::ptr::eq(*read, select));
        if let Some(edge) =
            cores.and_then(|(_, cores)| planner.subquery_edge(core, member, select, cores))
        {
            return Ok(Narrowing { terms: Vec::new(), edges: vec![edge] });
        }
    }
    let mut read = Vec::new();
    parts(condition, &mut read);
    let mut referenced = Vec::new();
    for part in &read {
        let name = match part {
            Part::Name(name) => *name,
            Part::Function(..) | Part::Clock => continue,
            // A COLLATE changes only how a comparison compares: `term` and
            // `edge` refuse the comparisons it reaches, and a NULL test
            // looks through it.
            Part::Collation => continue,
            Part::Select(_) | Part::Table => return Ok(Narrowing::default()),
        };
        let Some((item, _)) = planner.resolve(core, name) else {
            // A name coven can't place keeps the condition from narrowing.
            return Ok(Narrowing::default());
        };
        if !referenced.contains(&item) {
            referenced.push(item);
        }
    }
    let mut found = Narrowing::default();
    if let [item] = referenced[..] {
        if planner.items[item].core != core {
            return Ok(Narrowing::default());
        }
        let column = |expr: &Expr<'_>| {
            let (found, column) = planner.resolve(core, expr)?;
            let affinity =
                (found == item).then(|| planner.items[item].columns.affinity(&column)).flatten()?;
            Some((column, affinity))
        };
        // What `term` can't read, SQLite judges row by row (`scope_row.rs`).
        let narrowed = match term(connection, &column, condition)? {
            Some(term) => Some(term),
            None => row_term(connection, planner, core, item, condition, &read)?,
        };
        found.terms.extend(narrowed.map(|term| (item, term)));
    } else if let Expr::Binary(left, Operator::Equals, right) = condition {
        for (member, other, first) in [(left, right, false), (right, left, true)] {
            found.edges.extend(planner.edge(connection, core, member, other, first)?);
        }
    }
    Ok(found)
}

/// What conditions narrow: terms on one source's rows each, and edges.
/// Each is false where the column it tests is NULL, so it rejects a row
/// with NULLs in its source's place, except a term a NULL test can make
/// true (`ScopeTerm::may_hold_on_nulls`).
#[derive(Default)]
pub(crate) struct Narrowing {
    terms: Vec<(usize, ScopeTerm)>,
    edges: Vec<Edge>,
}

impl Narrowing {
    /// Whether a term of it can pass a row of NULLs.
    fn may_hold_on_nulls(&self) -> bool {
        self.terms.iter().any(|(_, term)| term.may_hold_on_nulls())
    }

    /// What of this narrows `item`, and the rest.
    fn split(self, item: usize) -> (Self, Self) {
        let (own, terms) = self.terms.into_iter().partition(|(narrowed, _)| *narrowed == item);
        let (own_edges, edges) = self.edges.into_iter().partition(|edge| edge.member == item);
        (Self { terms: own, edges: own_edges }, Self { terms, edges })
    }

    fn extend(&mut self, other: Self) {
        self.terms.extend(other.terms);
        self.edges.extend(other.edges);
    }
}

/// `member`'s column equals a value of `partner`'s rows.
#[derive(Clone)]
pub(crate) struct Edge {
    pub(crate) member: usize,
    pub(crate) column: String,
    pub(crate) affinity: Affinity,
    pub(crate) partner: usize,
    pub(crate) value: PartnerValue,
    /// Whether `partner` is the source of a subquery's value that the
    /// member's column is compared with, which sits in a core inside the
    /// member's: the member waits for it.
    pub(crate) nested: bool,
}

/// `expr` without the `+` and CAST SQLite still reads as the column under
/// them, when it chooses a comparison's collation.
fn stripped<'a>(expr: &'a Expr<'a>) -> &'a Expr<'a> {
    match unparenthesized(expr) {
        Expr::Unary(sqlite3_parser::ast::UnaryOperator::Positive, expr)
        | Expr::Cast { expr, .. } => stripped(expr),
        expr => expr,
    }
}

pub(crate) fn is_name(expr: &Expr<'_>) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..)
    )
}

#[cfg(test)]
#[path = "scope_plan_tests.rs"]
mod tests;
