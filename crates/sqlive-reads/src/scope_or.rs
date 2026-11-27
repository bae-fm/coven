//! An OR of conditions that each narrow one source, read as a disjunction
//! that narrows that source to the rows either side allows (SPEC.md section 7).
//!
//! `a.x = t.y OR a.id = t.z + 1` narrows `a` to the rows whose `x` some row
//! of `t` holds in `y`, or whose `id` some row of `t` gives as `z + 1`. A row
//! the OR keeps passes one of its sides, and each side's narrowing holds of
//! every row that side keeps, so the OR of the narrowings holds of every row
//! the OR keeps. Each side is read as its conjuncts, each as a condition of
//! its own; a source gets a disjunction when every side narrows it, by a
//! term on its rows or an edge from it. The edges of a side take no part in
//! the order the sources are placed in: a source keeps a disjunction when
//! the partners of every side's edges are placed before it, and its scope
//! then holds the OR of its sides' scopes (`scope_lookup.rs`). The model
//! states the rule as `Or.readCondition` in `crates/sqlive/proofs`.

use crate::scope_condition::read_condition;
use crate::scope_edge::Edge;
use crate::scope_narrowing::Narrowing;
use crate::scope_plan::{Membership, Planner, Subqueries};
use crate::scope_term::ScopeTerm;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Operator};

/// An OR that narrows `item`: wherever the OR holds, one of its sides does.
pub(crate) struct Disjunction {
    pub(crate) item: usize,
    pub(crate) sides: Vec<Side>,
}

/// One side of an OR as it narrows one source: terms on its rows and edges
/// from it, which all hold where the side does.
#[derive(Clone)]
pub(crate) struct Side {
    pub(crate) terms: Vec<ScopeTerm>,
    pub(crate) edges: Vec<Edge>,
}

impl Side {
    /// The columns of its source that its terms test and its edges compare.
    pub(crate) fn columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        for term in &self.terms {
            term.columns(&mut columns);
        }
        for column in self.edges.iter().flat_map(|edge| edge.member.columns()) {
            if !columns.iter().any(|found| found == column) {
                columns.push(column.to_owned());
            }
        }
        columns
    }

    /// What this side, of an OR on a subquery's, CTE's or view's rows, says
    /// of `target`, whose rows the statement names `rows` and whose column
    /// `column` gives for each of the side's columns it passes on; `None`
    /// when it says nothing of `target`. No LEFT JOIN adds `target`.
    pub(crate) fn over(
        &self,
        target: usize,
        rows: &str,
        column: &dyn Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        let terms: Vec<ScopeTerm> =
            self.terms.iter().filter_map(|term| term.renamed(&column, Some(rows))).collect();
        let edges: Vec<Edge> = self
            .edges
            .iter()
            .filter_map(|edge| {
                let member = edge.member.over(target, rows, false, column)?;
                Some(Edge { member, pushed: true, ..edge.clone() })
            })
            .collect();
        (!terms.is_empty() || !edges.is_empty()).then_some(Self { terms, edges })
    }

    /// Whether a row of NULLs in the source's place can pass the side.
    fn may_hold_on_nulls(&self) -> bool {
        self.terms.iter().any(ScopeTerm::may_hold_on_nulls) || self.edges.iter().any(|e| e.nulls)
    }
}

impl Disjunction {
    /// Whether a row of NULLs in the source's place can pass the OR.
    pub(crate) fn may_hold_on_nulls(&self) -> bool {
        self.sides.iter().any(Side::may_hold_on_nulls)
    }
}

/// One side of an OR that narrows a placed source: its terms joined with
/// AND, and its edges as memberships of partners placed before the source.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct EitherSide {
    pub(crate) term: Option<ScopeTerm>,
    pub(crate) memberships: Vec<Membership>,
}

impl EitherSide {
    pub(crate) fn of(side: &Side) -> Self {
        let mut term = None;
        for found in &side.terms {
            ScopeTerm::add_to(&mut term, found.clone());
        }
        Self { term, memberships: side.edges.iter().map(Membership::of).collect() }
    }
}

/// Whether `condition` is an OR.
pub(crate) fn is_or(condition: &Expr<'_>) -> bool {
    matches!(crate::sql::unparenthesized(condition), Expr::Binary(_, Operator::Or, _))
}

/// The disjunctions the OR `condition` of `core` gives: one for each source
/// its first side narrows and every other side narrows too.
pub(crate) fn read_or<'a>(
    connection: &Connection,
    planner: &mut Planner,
    core: usize,
    condition: &'a Expr<'a>,
    subqueries: &Subqueries<'a>,
) -> rusqlite::Result<Narrowing> {
    let mut sides = Vec::new();
    disjuncts(condition, &mut sides);
    let mut found = Vec::new();
    for side in sides {
        let mut joined = Vec::new();
        crate::select_syntax::conjuncts(side, &mut joined);
        let mut read = Narrowing::default();
        for conjunct in joined {
            read.extend(read_condition(connection, planner, core, conjunct, subqueries)?);
        }
        found.push(read);
    }
    let mut items: Vec<usize> = Vec::new();
    if let Some(first) = found.first() {
        let named = first.terms.iter().map(|(item, _)| *item);
        for item in named.chain(first.edges.iter().map(|edge| edge.member.item)) {
            if !items.contains(&item) {
                items.push(item);
            }
        }
    }
    let mut ors = Vec::new();
    for item in items {
        let sides: Vec<Side> = found
            .iter()
            .map(|read| Side {
                terms: read
                    .terms
                    .iter()
                    .filter(|(narrowed, _)| *narrowed == item)
                    .map(|(_, term)| term.clone())
                    .collect(),
                edges: read.edges.iter().filter(|edge| edge.member.item == item).cloned().collect(),
            })
            .collect();
        if sides.iter().all(|side| !side.terms.is_empty() || !side.edges.is_empty()) {
            ors.push(Disjunction { item, sides });
        }
    }
    Ok(Narrowing { ors, ..Narrowing::default() })
}

/// The sides of an OR, its nested ORs flattened.
fn disjuncts<'a>(expr: &'a Expr<'a>, into: &mut Vec<&'a Expr<'a>>) {
    match crate::sql::unparenthesized(expr) {
        Expr::Binary(left, Operator::Or, right) => {
            disjuncts(left, into);
            disjuncts(right, into);
        }
        expr => into.push(expr),
    }
}

impl Planner<'_, '_> {
    /// Whether `item` waits for an OR's partner: a side compares a column of
    /// it with a subquery's value whose source isn't placed yet.
    pub(crate) fn waits_for_or(&self, item: usize, placed: &[bool]) -> bool {
        self.items[item]
            .ors
            .iter()
            .flatten()
            .any(|side| side.edges.iter().any(|edge| edge.nesting.nested() && !edge.placed(placed)))
    }

    /// The ORs on `item` whose sides take values only from sources placed
    /// before it.
    pub(crate) fn placed_ors(&self, item: usize, placed: &[bool]) -> Vec<Vec<Side>> {
        self.items[item]
            .ors
            .iter()
            .filter(|sides| {
                sides.iter().all(|side| side.edges.iter().all(|edge| edge.placed(placed)))
            })
            .cloned()
            .collect()
    }
}
