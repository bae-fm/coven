//! A column, or each column of a row value, compared with the value a
//! subquery returns, read as an edge to the subquery's source (§4).
//!
//! `col = (SELECT x.c …)`, `(SELECT x.c …) = col` and `col IN (SELECT x.c
//! …)`, where the subquery is one SELECT whose single result column is a bare
//! column of one of its own sources `x`: when its value isn't NULL it is
//! `x.c` in a row the subquery reads, and every such row is inside `x`'s
//! scope. So the comparison gives the edge `JOIN x ON x.c = col` would, and a
//! write that changes `col`'s table asks `x`. The edge goes one way: `col`
//! takes values from `x`, never the reverse, and `x` is placed before `col`'s
//! source (`Planner::ordered`).

use crate::scope_plan::{is_name, Edge, PartnerValue, Planner};
use crate::sql::unparenthesized;
use sqlite3_parser::ast::{Expr, OneSelect, Operator, ResultColumn, Select};

/// The values a comparison with a subquery tests, each compared with the
/// subquery's result column of its position, and the subquery, when
/// `condition` has one of the forms above or is a row value compared with
/// a subquery's row the same ways: `(a, b) = (SELECT x.c, x.d …)`, either
/// way round, or `(a, b) IN (SELECT x.c, x.d …)`. A row value comparison
/// holds only where each pair is equal, so each pair narrows as it would
/// alone.
pub(crate) fn compared_subquery<'a>(
    condition: &'a Expr<'a>,
) -> Option<(Vec<&'a Expr<'a>>, &'a Select<'a>)> {
    match unparenthesized(condition) {
        Expr::Binary(left, Operator::Equals, right) => {
            match (unparenthesized(left), unparenthesized(right)) {
                (Expr::Subquery(_), Expr::Subquery(_)) => None,
                (compared, Expr::Subquery(select)) | (Expr::Subquery(select), compared) => {
                    Some((values(compared)?, select))
                }
                _ => None,
            }
        }
        Expr::InSelect { lhs, not: false, rhs } => Some((values(lhs)?, rhs)),
        _ => None,
    }
}

/// The values of a row value, or the one bare column compared alone.
fn values<'a>(compared: &'a Expr<'a>) -> Option<Vec<&'a Expr<'a>>> {
    match unparenthesized(compared) {
        Expr::Parenthesized(values) => Some(values.iter().collect()),
        compared => is_name(compared).then(|| vec![compared]),
    }
}

impl Planner<'_> {
    /// The edge from the column `member_expr` names, written in `core`, to
    /// the source of the column `select` returns at `position` of `width`,
    /// when `cores` are the cores `select` was read as. The column and the
    /// source's column compare by BINARY with no conversion, as a join's do.
    pub(crate) fn subquery_edge(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
        (position, width): (usize, usize),
        select: &Select<'_>,
        cores: &[usize],
    ) -> Option<Edge> {
        let [subquery] = cores[..] else {
            return None;
        };
        if select.body.compounds.is_some() {
            return None;
        }
        let OneSelect::Select { columns, .. } = &select.body.select else {
            return None;
        };
        let returned: Vec<&Expr<'_>> = columns
            .iter()
            .map(|column| match column {
                ResultColumn::Expr(returned, _) => Some(returned),
                ResultColumn::Star | ResultColumn::TableStar(_) => None,
            })
            .collect::<Option<_>>()?;
        if returned.len() != width || !is_name(returned[position]) || !is_name(member_expr) {
            return None;
        }
        let returned = returned[position];
        let (partner, partner_column) = self.resolve(subquery, returned)?;
        let items = &self.items;
        // A source of the subquery's own, which sqlive can read alone.
        if items[partner].core != subquery || items[partner].source.from.is_none() {
            return None;
        }
        let partner_affinity = items[partner].columns.affinity(&partner_column)?;
        let (member, column) = self.resolve(core, member_expr)?;
        if items[member].core != core
            || (items[member].source.table.is_none() && items[member].derived.is_none())
        {
            return None;
        }
        let affinity = items[member].columns.affinity(&column)?;
        if affinity != partner_affinity {
            return None;
        }
        Some(Edge {
            member,
            column,
            affinity,
            partner,
            value: PartnerValue::Column(partner_column),
            nested: true,
        })
    }
}
