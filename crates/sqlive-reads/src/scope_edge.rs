//! An equality between a column of one of a core's sources, or an expression
//! over a table's own row, and a value of one other source, read as an edge the
//! first takes values by (SPEC.md section 7): from a condition, or from the ON
//! a USING join stands for. `IS` reads the same way, with a NULL matching a
//! NULL. An edge also carries what the other source's terms say of its column
//! to the first.

use crate::from_item::Columns;
use std::sync::Arc;

use crate::scope_plan::{is_name, Edge, PartnerValue, Planner};
use crate::scope_row::{item_row_term, RowTerm};
use crate::scope_term::{Affinity, ScopeTerm};
use crate::select_syntax::{parts, strict, Part};
use crate::sql::unparenthesized;
use sqlite3_parser::ast::{Expr, Name};

impl Planner<'_> {
    /// The member column `member_expr`, written in `core`, names: a bare
    /// column of one of the core's own sources that sqlive narrows by.
    pub(crate) fn member_column(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
    ) -> Option<MemberColumn> {
        if !is_name(member_expr) {
            return None;
        }
        let (item, column) = self.resolve(core, member_expr)?;
        if self.items[item].core != core {
            return None;
        }
        self.narrowed_column(item, column)
    }

    /// `item`'s `column` as a member column, when sqlive narrows by it.
    pub(crate) fn narrowed_column(&self, item: usize, column: String) -> Option<MemberColumn> {
        let found = &self.items[item];
        if found.source.table().is_none() && found.derived.is_none() {
            return None;
        }
        let affinity = found.columns.affinity(&column)?;
        Some(MemberColumn { item, column, affinity })
    }

    /// The member `member_expr`, written in `core`, gives when it is an
    /// expression over the row of one table of the core's own
    /// (`source_of`), that SQLite can evaluate row by row and whose every
    /// name is a column of that table that sqlive compares
    /// (`scope_row::item_row_term`). A write asks SQLite for its value in
    /// each row it changes.
    fn member_expression(&self, core: usize, member_expr: &Expr<'_>) -> Option<Member> {
        let item = self.source_of(core, member_expr)?;
        if self.items[item].core != core {
            return None;
        }
        let mut found = Vec::new();
        parts(member_expr, &mut found);
        let term = item_row_term(self, core, item, member_expr, &found)?;
        Some(Member { item, value: MemberValue::Expression(term), affinity: Affinity::Blob })
    }

    /// The edge `member_expr = other` gives, written in `core` with `other`
    /// first when `first`, when `member_expr` is a member column, if it is a
    /// bare name, or else an expression over one table's row
    /// (`member_expression`), and `other` is
    /// a value of one other source (`partner`); and when SQLite compares
    /// them by BINARY, as sqlive does: the comparison takes the collation of
    /// a COLLATE in either side, else of the first side when that is a
    /// column.
    pub(crate) fn edge(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
        other: &Expr<'_>,
        first: bool,
    ) -> Option<Edge> {
        let member = if is_name(member_expr) {
            self.member_column(core, member_expr)?.into()
        } else {
            self.member_expression(core, member_expr)?
        };
        let partner = self.partner(core, member.item, other)?;
        if is_name(other) {
            let (_, partner_column) = self.resolve(core, other)?;
            return self.column_edge(member, (partner, partner_column), first);
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
            _ => self.edge_to(member, partner, PartnerValue::Expression(other.to_string())),
        }
    }

    /// The one source other than `member` whose columns `other`, written in
    /// `core`, reads (`source_of`).
    pub(crate) fn partner(&self, core: usize, member: usize, other: &Expr<'_>) -> Option<usize> {
        self.source_of(core, other).filter(|partner| *partner != member)
    }

    /// The one source whose columns `expr`, written in `core`, reads, of
    /// this core or one it sits in, and its parts. `expr` holds no subquery,
    /// no COLLATE, no clock and no function the connection doesn't flag
    /// deterministic: a write evaluates it again, and would get another
    /// answer than the query did. And every binding holds a row of the
    /// source, or `expr` is NULL wherever a column it names is
    /// (`select_syntax::strict`).
    fn source_of(&self, core: usize, expr: &Expr<'_>) -> Option<usize> {
        let mut found = Vec::new();
        parts(expr, &mut found);
        let mut source = None;
        for part in &found {
            let name = match part {
                Part::Name(name) => name,
                Part::Function(..) | Part::Clock => continue,
                Part::Select(_) | Part::Subquery(_) | Part::Table(_) | Part::Collation => {
                    return None
                }
            };
            let (item, _) = self.resolve(core, name)?;
            if source.is_some_and(|source| source != item) {
                return None;
            }
            source = Some(item);
        }
        let source = source?;
        // Over a LEFT JOIN's missing row, an expression that can give a
        // value where the source's columns are NULL gives it in a binding
        // that holds no row of the source: a partner's no lookup finds, and
        // a member's makes the equality true of the NULLs, so it can't make
        // the LEFT JOIN inner (`scope_one_of.rs` reads a column equal to
        // COALESCE).
        let bound = self.items[source].always_bound() || strict(expr, self.functions);
        // A view read whole narrows the tables beside it, so a join on its
        // column does.
        (!self.volatile_by(core, &found, false) && bound).then_some(source)
    }

    /// Whether a lookup that evaluates the expression `found`, written in
    /// `core`, again at a write could get another answer than the query did:
    /// it reads the clock, calls a function the connection doesn't flag
    /// deterministic (an aggregate or a window function takes its rows'
    /// answer), names a column of a derived item that does (any such column
    /// in scope, for a name sqlive can't place), or holds a volatile subquery
    /// (`select_volatile`), or names a column of a view read whole
    /// (`FromItem::column_unjudged`).
    pub(crate) fn volatile(&self, core: usize, found: &[Part<'_>]) -> bool {
        self.volatile_by(core, found, true)
    }

    /// `volatile`, counting the columns of a view read whole when `views`.
    fn volatile_by(&self, core: usize, found: &[Part<'_>], views: bool) -> bool {
        let judged = |item: &crate::from_item::FromItem, column: &str| {
            item.columns.volatile(column) || (views && item.column_unjudged(column))
        };
        found.iter().any(|part| match part {
            Part::Clock => true,
            Part::Function(..) => !part.answers_again(self.functions),
            Part::Name(name) => match self.resolve(core, name) {
                Some((item, column)) => judged(&self.items[item], &column),
                None => self.scope_of(core).any(|scope| {
                    let items = &self.cores[scope].items;
                    items.iter().any(|item| {
                        let item = &self.items[*item];
                        item.columns.any_volatile() || (views && item.is_view_read_whole())
                    })
                }),
            },
            Part::Select(select) | Part::Subquery(select) => self.select_volatile(select),
            Part::Table(_) | Part::Collation => false,
        })
    }

    /// The edge `member = partner.column` gives, with `partner`'s column
    /// first when `first`: a column first in an equality gives it its
    /// collation, which is BINARY when sqlive gives the column an affinity.
    pub(crate) fn column_edge(
        &self,
        member: Member,
        (partner, partner_column): (usize, String),
        first: bool,
    ) -> Option<Edge> {
        // A USING join reaches here without `source_of`.
        if self.items[partner].columns.volatile(&partner_column) {
            return None;
        }
        let partner_affinity = self.items[partner].columns.affinity(&partner_column);
        if first && partner_affinity.is_none() {
            return None;
        }
        let value = match (partner_affinity, &member.value) {
            (Some(partner_affinity), MemberValue::Column(_)) => {
                self.partner_column(member.affinity, partner, partner_column, partner_affinity)?
            }
            // SQLite gives an expression's value the column's affinity, and
            // so does the question that judges a changed row (`scope_row.rs`).
            (Some(_), MemberValue::Expression(_)) => PartnerValue::Column(partner_column),
            // A column sqlive gives no affinity, as a view's or a
            // function's: a lookup compares it as the statement does, with
            // whatever affinity SQLite gives it.
            (None, _) => PartnerValue::Expression(format!(
                "{}.{}",
                self.items[partner].source.qualifier,
                crate::sql::identifier(&partner_column)
            )),
        };
        self.edge_to(member, partner, value)
    }

    /// What `partner`'s column `column`, of affinity `partner_affinity`,
    /// gives a member column of `affinity` to compare with: the column, when
    /// the two share an affinity, and the column as an expression when the
    /// partner's has none (BLOB), as a column declared with no type: SQLite
    /// gives its values the member's affinity, as a lookup gives an
    /// expression's. Columns of two other affinities give nothing: SQLite
    /// converts their values in ways sqlive doesn't follow. The model states
    /// the rule as `columnValue` in `crates/sqlive/proofs`.
    pub(crate) fn partner_column(
        &self,
        affinity: Affinity,
        partner: usize,
        column: String,
        partner_affinity: Affinity,
    ) -> Option<PartnerValue> {
        if partner_affinity == affinity {
            Some(PartnerValue::Column(column))
        } else if partner_affinity == Affinity::Blob {
            Some(PartnerValue::Expression(format!(
                "{}.{}",
                self.items[partner].source.qualifier,
                crate::sql::identifier(&column)
            )))
        } else {
            None
        }
    }

    /// The edge from `member` to `partner`, whose rows give `value`, when
    /// sqlive can read the partner alone. A lookup compares an expression
    /// `value` with the member column's affinity, as SQLite compares a
    /// column with an expression.
    pub(crate) fn edge_to(
        &self,
        member: Member,
        partner: usize,
        value: PartnerValue,
    ) -> Option<Edge> {
        self.items[partner].source.from.as_ref()?;
        Some(Edge { member, partner, value, nested: false, nulls: false })
    }

    /// The edge `member_expr IS other` gives: the equality's, matching a NULL
    /// with a NULL, when no LEFT JOIN adds the partner. A left row no right
    /// row joins holds NULLs in the right side's place with no row a lookup
    /// could find.
    pub(crate) fn is_edge(
        &self,
        core: usize,
        member_expr: &Expr<'_>,
        other: &Expr<'_>,
        first: bool,
    ) -> Option<Edge> {
        let edge = self.edge(core, member_expr, other, first)?;
        self.items[edge.partner].always_bound().then_some(Edge { nulls: true, ..edge })
    }

    /// Carry, across each edge between two items of `core` that compares
    /// with a column of the partner, the partner's own term on that column,
    /// and what a derived partner's arms' own terms say of it, to the
    /// member's column. In each row the core keeps, the member's column holds
    /// the partner's column's value, so a term true of one is true of the
    /// other: `f JOIN c ON f.p = c.p WHERE c.p = ?1` narrows `f` as
    /// `f.p = ?1` would, whichever of them goes first. A carried term only
    /// narrows the member's scope: it passes into no subquery, carries
    /// nothing further, and leaves the member to take values from the sources
    /// its edges join it to. A LEFT JOIN's right side stays one: the term
    /// holds of each of its rows that joins, and says nothing of its row of
    /// NULLs. An `IS` edge carries no derived partner's arms' terms: a
    /// derived row whose column is NULL says nothing of its arm's sources.
    /// The model states the rule as `Transfer.carry` in
    /// `crates/sqlive/proofs`.
    pub(crate) fn carry_terms(&mut self, core: usize) {
        let mut carried: Vec<(usize, ScopeTerm)> = Vec::new();
        for edge in &self.edges {
            if edge.nested
                || self.items[edge.member.item].core != core
                || self.items[edge.partner].core != core
            {
                continue;
            }
            let (PartnerValue::Column(compared), Some(member)) =
                (&edge.value, edge.member.column())
            else {
                continue;
            };
            let rename = |tested: &str| (tested == compared).then(|| member.to_owned());
            let own = self.items[edge.partner]
                .source
                .term
                .as_ref()
                .and_then(|term| term.renamed(&rename));
            let pulled = (!edge.nulls)
                .then(|| crate::derived::pulled(self, edge.partner, compared))
                .flatten()
                .and_then(|term| term.renamed(&rename));
            carried.extend(own.into_iter().chain(pulled).map(|term| (edge.member.item, term)));
        }
        for (item, term) in carried {
            ScopeTerm::add_to(&mut self.items[item].source.carried, term);
        }
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
            if matches!(columns, Columns::Unknown { .. }) {
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

/// What sqlive narrows one of a core's sources by: a table's or a derived
/// item's column that sqlive gives an affinity, or an expression over a
/// table's own columns.
#[derive(Clone)]
pub(crate) struct Member {
    pub(crate) item: usize,
    pub(crate) value: MemberValue,
    /// The affinity a lookup gives a value it compares with the member's:
    /// its column's, or BLOB, which converts nothing, for an expression,
    /// whose comparison the question that judges a row leaves to SQLite.
    pub(crate) affinity: Affinity,
}

/// A member that is one of its source's columns: a table's or a derived
/// item's column that sqlive gives an affinity.
#[derive(Clone)]
pub(crate) struct MemberColumn {
    pub(crate) item: usize,
    pub(crate) column: String,
    pub(crate) affinity: Affinity,
}

impl From<MemberColumn> for Member {
    fn from(MemberColumn { item, column, affinity }: MemberColumn) -> Self {
        Self { item, value: MemberValue::Column(column), affinity }
    }
}

/// What an edge compares of its member's rows.
#[derive(Clone)]
pub(crate) enum MemberValue {
    /// One of its columns, lowercase.
    Column(String),
    /// An expression over a table's own columns, whose value in a changed
    /// row a write asks SQLite for (`scope_row.rs`).
    Expression(Arc<RowTerm>),
}

impl Member {
    /// The member's column, unless it is an expression.
    pub(crate) fn column(&self) -> Option<&str> {
        match &self.value {
            MemberValue::Column(column) => Some(column),
            MemberValue::Expression(_) => None,
        }
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
