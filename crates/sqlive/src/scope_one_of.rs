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

use crate::scope_or::{Disjunction, Side};
use crate::scope_plan::{is_name, Edge, Narrowing, PartnerValue, Planner, Subqueries};
use crate::scope_term::{operand, Affinity, ScopeTerm};
use crate::select_syntax::{parts, Part};
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
    /// The source of `core` and its column `member_expr` names, with the
    /// column's affinity, when sqlive narrows by it.
    fn member_column(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
    ) -> Option<(usize, String, Affinity)> {
        if !is_name(member_expr) {
            return None;
        }
        let (member, column) = self.resolve(core, member_expr)?;
        let item = &self.items[member];
        if item.core != core || (item.source.table.is_none() && item.derived.is_none()) {
            return None;
        }
        let affinity = item.columns.affinity(&column)?;
        Some((member, column, affinity))
    }

    /// The side a constant gives: the member column equal to it.
    fn constant_side(
        connection: &Connection,
        column: &str,
        affinity: Affinity,
        expr: &Expr<'_>,
    ) -> rusqlite::Result<Option<Side>> {
        Ok(operand(connection, expr, affinity)?.map(|operand| Side {
            terms: vec![ScopeTerm::Compare {
                column: column.to_owned(),
                affinity,
                operand,
                ordering: Ordering::Equal,
                inclusive: false,
            }],
            edges: Vec::new(),
        }))
    }

    /// The edge from `member`'s `column` to the one other source `value`
    /// names, compared as an expression: `value` holds no subquery, no
    /// COLLATE, no clock, no function the connection doesn't flag
    /// deterministic and none that picks an argument, and sqlive reads the
    /// source alone.
    fn value_edge(
        &self,
        core: usize,
        (member, column, affinity): (usize, &str, Affinity),
        value: &Expr<'_>,
    ) -> Option<Edge> {
        let mut found = Vec::new();
        parts(value, &mut found);
        let mut partner = None;
        for part in &found {
            match part {
                Part::Name(name) => {
                    let (item, _) = self.resolve(core, name)?;
                    if item == member || partner.is_some_and(|partner| partner != item) {
                        return None;
                    }
                    partner = Some(item);
                }
                Part::Function(name, ..) if !picks_an_argument(name) => {}
                _ => return None,
            }
        }
        let partner = partner?;
        if !self.functions.repeatable(&found) {
            return None;
        }
        self.items[partner].source.from.as_ref()?;
        Some(Edge {
            member,
            column: column.to_owned(),
            affinity,
            partner,
            value: PartnerValue::Expression(value.to_string()),
            nested: false,
            nulls: false,
        })
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
        let Some((member, column, affinity)) = self.member_column(core, member_expr) else {
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
                    Self::constant_side(connection, &column, affinity, argument)?
                }
                argument => self
                    .value_edge(core, (member, &column, affinity), argument)
                    .map(|edge| Side { terms: Vec::new(), edges: vec![edge] }),
            };
            let Some(side) = side else {
                return Ok(None);
            };
            sides.push(side);
        }
        Ok(Some(Narrowing {
            ors: vec![Disjunction { item: member, sides }],
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
        let Some((member, column, affinity)) = self.member_column(core, member_expr) else {
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
                Self::constant_side(connection, &column, affinity, returned)?
            } else {
                self.arm_edge(arm, (member, &column, affinity), returned)
                    .map(|edge| Side { terms: Vec::new(), edges: vec![edge] })
            };
            let Some(side) = side else {
                return Ok(Narrowing::default());
            };
            sides.push(side);
        }
        Ok(Narrowing { ors: vec![Disjunction { item: member, sides }], ..Narrowing::default() })
    }

    /// The edge from `member`'s `column` to the source of the arm `arm` whose
    /// column `returned` names, of the column's affinity, which sqlive reads
    /// alone.
    fn arm_edge(
        &self,
        arm: usize,
        (member, column, affinity): (usize, &str, Affinity),
        returned: &Expr<'_>,
    ) -> Option<Edge> {
        if !is_name(returned) {
            return None;
        }
        let (partner, partner_column) = self.resolve(arm, returned)?;
        let item = &self.items[partner];
        if item.core != arm || item.source.from.is_none() {
            return None;
        }
        (item.columns.affinity(&partner_column)? == affinity).then(|| Edge {
            member,
            column: column.to_owned(),
            affinity,
            partner,
            value: PartnerValue::Column(partner_column),
            nested: true,
            nulls: false,
        })
    }
}
