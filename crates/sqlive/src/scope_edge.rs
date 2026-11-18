//! An equality between a column of one of a core's sources and a value of
//! one other source, read as an edge the first takes values by (§4): from a
//! condition, or from the ON a USING join stands for.

use crate::from_item::Columns;
use crate::scope_plan::{is_name, Edge, PartnerValue, Planner};
use crate::scope_term::Affinity;
use crate::select_syntax::{parts, repeatable, Part};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Name};

impl Planner<'_> {
    /// The edge `member_expr = other` gives, written in `core` with `other`
    /// first when `first`, when `member_expr` is a column of one of the
    /// core's sources and `other` reads columns of one other source only,
    /// of this core or one it sits in, which sqlive can read alone; and when
    /// SQLite compares them by BINARY, as sqlive does: the comparison takes
    /// the collation of a COLLATE in either side, else of the first side
    /// when that is a column. `other` holds no clock and no function the
    /// connection doesn't flag deterministic: a lookup evaluates it again, and
    /// would get another answer than the query did.
    pub(crate) fn edge(
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
        let (member, column) = self.resolve(core, member_expr)?;
        if self.items[member].core != core {
            return None;
        }
        let mut found = Vec::new();
        parts(other, &mut found);
        let mut partner = None;
        for part in found {
            let name = match part {
                Part::Name(name) => name,
                Part::Function(..) | Part::Clock => continue,
                Part::Select(_) | Part::Subquery(_) | Part::Table | Part::Collation => return None,
            };
            let (item, _) = self.resolve(core, name)?;
            if item == member || partner.is_some_and(|partner| partner != item) {
                return None;
            }
            partner = Some(item);
        }
        let partner = partner?;
        if is_name(other) {
            let (_, partner_column) = self.resolve(core, other)?;
            return self.column_edge((member, column), (partner, partner_column), first);
        }
        if first && is_name(stripped(other)) {
            let (_, partner_column) = self.resolve(core, stripped(other))?;
            // A column sqlive gives an affinity compares by BINARY: a
            // table's, or a subquery's that passes such a column on.
            self.items[partner].columns.affinity(&partner_column)?;
        }
        match unparenthesized(other) {
            // A cast or COLLATE gives the expression an affinity or a
            // collation of its own, which SQLite's comparison would use.
            Expr::Cast { .. } | Expr::Collate(..) => None,
            _ => self.edge_to(member, column, partner, |_| {
                Some(PartnerValue::Expression(other.to_string()))
            }),
        }
    }

    /// The edge `member.column = partner.column` gives, with `partner`'s
    /// column first when `first`: a column first in an equality gives it its
    /// collation, which is BINARY when sqlive gives the column an affinity.
    pub(crate) fn column_edge(
        &self,
        (member, column): (usize, String),
        (partner, partner_column): (usize, String),
        first: bool,
    ) -> Option<Edge> {
        let partner_affinity = self.items[partner].columns.affinity(&partner_column);
        if first && partner_affinity.is_none() {
            return None;
        }
        self.edge_to(member, column, partner, |affinity| match partner_affinity {
            Some(partner_affinity) => {
                (partner_affinity == affinity).then_some(PartnerValue::Column(partner_column))
            }
            // A column sqlive gives no affinity, as a view's or a
            // function's: a lookup compares it as the statement does, with
            // whatever affinity SQLite gives it.
            None => Some(PartnerValue::Expression(format!(
                "{}.{}",
                self.items[partner].source.qualifier,
                crate::sql::identifier(&partner_column)
            ))),
        })
    }

    /// The edge from `member`'s `column` to `partner`, whose rows give the
    /// value `value` makes for the column's affinity: when sqlive narrows by
    /// the column, and can read the partner alone.
    fn edge_to(
        &self,
        member: usize,
        column: String,
        partner: usize,
        value: impl FnOnce(Affinity) -> Option<PartnerValue>,
    ) -> Option<Edge> {
        let items = &self.items;
        if partner == member
            || (items[member].source.table.is_none() && items[member].derived.is_none())
        {
            return None;
        }
        let affinity = items[member].columns.affinity(&column)?;
        items[partner].source.from.as_ref()?;
        let value = value(affinity)?;
        Some(Edge { member, column, affinity, partner, value, nested: false })
    }

    /// The leftmost item of `core` before `right` that has a column named
    /// `column`, and that column, which SQLite compares `right`'s with;
    /// `None` when an item before it may have the column under a name sqlive
    /// doesn't know.
    pub(crate) fn using_left(
        &self,
        core: usize,
        right: usize,
        column: &str,
    ) -> Option<(usize, String)> {
        for &item in &self.cores[core].items {
            if item == right {
                break;
            }
            let columns = &self.items[item].columns;
            if matches!(columns, Columns::Unknown) {
                return None;
            }
            if let Some(found) = columns.named(&Name(column)) {
                return Some((item, found));
            }
            if !columns.complete() {
                return None;
            }
        }
        None
    }
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
