//! A column equal to a value that is one of several partners' values, read
//! as an OR of what each value gives (§4).
//!
//! `m.c = COALESCE(a, b, ...)` returns one of its arguments, and `m.c IN
//! (SELECT ... UNION SELECT ...)`, or `=` with such a subquery, compares with
//! a row one of the compound's arms returns, so a kept row's column holds the
//! value of one of them, and the OR of their narrowings holds of it
//! (`scope_or.rs`). An argument narrows as a value of one other source,
//! which a lookup compares as an expression, since SQLite applies the member
//! column's affinity to COALESCE's value; as a subquery's returned column,
//! as a column compared with a subquery's value does (`scope_subquery.rs`);
//! or as a constant. An arm narrows as a bare column of one of its own
//! sources, of the member column's affinity, or as a constant. Any other
//! argument or arm makes the comparison narrow nothing. The model states the
//! rule as `OneOf.readCondition` in `crates/sqlive/proofs`.

use crate::scope_edge::Member;
use crate::scope_or::{Disjunction, Side};
use crate::scope_plan::{Narrowing, PartnerValue, Planner, Subqueries};
use crate::scope_term::{operand, ScopeTerm};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, OneSelect, ResultColumn, Select, UnaryOperator};
use std::cmp::Ordering;

/// Whether `name` is a function that returns one of its arguments that isn't
/// NULL, so a value of it can be other than NULL where every column it names
/// is NULL.
pub(crate) fn picks_an_argument(name: &str) -> bool {
    name.eq_ignore_ascii_case("coalesce") || name.eq_ignore_ascii_case("ifnull")
}

/// COALESCE's or IFNULL's arguments, nested calls flattened, when `expr` is
/// one.
fn arguments<'a>(expr: &'a Expr<'a>) -> Option<Vec<&'a Expr<'a>>> {
    let Expr::FunctionCall {
        name,
        distinctness: None,
        args: Some(args),
        order_by: None,
        filter_over: None,
    } = unparenthesized(expr)
    else {
        return None;
    };
    if !picks_an_argument(name.0) {
        return None;
    }
    let mut found = Vec::new();
    for arg in args.iter() {
        match arguments(arg) {
            Some(nested) => found.extend(nested),
            None => found.push(arg),
        }
    }
    Some(found)
}

/// Whether `expr` is a constant or a parameter, which `operand` reads.
fn constant(expr: &Expr<'_>) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Literal(_)
            | Expr::Variable(_)
            | Expr::Unary(UnaryOperator::Negative | UnaryOperator::Positive, Expr::Literal(_))
    )
}

impl Planner<'_> {
    /// The side a constant gives: the member column equal to it.
    fn constant_side(
        connection: &Connection,
        member: &Member,
        expr: &Expr<'_>,
    ) -> rusqlite::Result<Option<Side>> {
        Ok(operand(connection, expr, member.affinity)?.map(|operand| Side {
            terms: vec![ScopeTerm::Compare {
                column: member.column.clone(),
                affinity: member.affinity,
                operand,
                ordering: Ordering::Equal,
                inclusive: false,
            }],
            edges: Vec::new(),
        }))
    }

    /// The disjunction `member_expr = other` gives, written in `core`, when
    /// `other` is COALESCE or IFNULL; `None` when it isn't, or narrows
    /// nothing.
    pub(crate) fn coalesced(
        &self,
        connection: &Connection,
        core: usize,
        member_expr: &Expr<'_>,
        other: &Expr<'_>,
        subqueries: &Subqueries<'_>,
    ) -> rusqlite::Result<Option<Narrowing>> {
        let Some(arguments) = arguments(other) else {
            return Ok(None);
        };
        let Some(member) = self.member_column(core, member_expr) else {
            return Ok(None);
        };
        let mut sides = Vec::new();
        for argument in arguments {
            let side = match unparenthesized(argument) {
                Expr::Subquery(select) => subqueries
                    .iter()
                    .find(|(read, _)| std::ptr::eq(*read, *select))
                    .and_then(|(_, cores)| {
                        self.subquery_edge(core, member_expr, (0, 1), select, cores)
                    })
                    .map(|edge| Side { terms: Vec::new(), edges: vec![edge] }),
                argument if constant(argument) => {
                    Self::constant_side(connection, &member, argument)?
                }
                // SQLite compares the member column with COALESCE's value,
                // which has no affinity, by the column's affinity.
                argument => self
                    .partner(core, member.item, argument)
                    .and_then(|partner| {
                        let value = PartnerValue::Expression(argument.to_string());
                        self.edge_to(member.clone(), partner, value)
                    })
                    .map(|edge| Side { terms: Vec::new(), edges: vec![edge] }),
            };
            let Some(side) = side else {
                return Ok(None);
            };
            sides.push(side);
        }
        Ok(Some(Narrowing {
            ors: vec![Disjunction { item: member.item, sides }],
            ..Narrowing::default()
        }))
    }

    /// The disjunction the column `member_expr`, written in `core`, compared
    /// with the compound `select` gives, `cores` being its arms' cores: one
    /// side for each arm.
    pub(crate) fn compound(
        &self,
        connection: &Connection,
        core: usize,
        member_expr: &Expr<'_>,
        select: &Select<'_>,
        cores: &[usize],
    ) -> rusqlite::Result<Narrowing> {
        let Some(member) = self.member_column(core, member_expr) else {
            return Ok(Narrowing::default());
        };
        let bodies = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
        let mut sides = Vec::new();
        for (&arm, body) in cores.iter().zip(bodies) {
            let OneSelect::Select { columns, .. } = body else {
                return Ok(Narrowing::default());
            };
            let [ResultColumn::Expr(returned, _)] = &columns[..] else {
                return Ok(Narrowing::default());
            };
            let side = if constant(returned) {
                Self::constant_side(connection, &member, returned)?
            } else {
                self.arm_edge(arm, member.clone(), returned)
                    .map(|edge| Side { terms: Vec::new(), edges: vec![edge] })
            };
            let Some(side) = side else {
                return Ok(Narrowing::default());
            };
            sides.push(side);
        }
        Ok(Narrowing {
            ors: vec![Disjunction { item: member.item, sides }],
            ..Narrowing::default()
        })
    }
}
