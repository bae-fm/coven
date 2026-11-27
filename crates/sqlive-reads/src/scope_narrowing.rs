//! What conditions narrow, and how the planner narrows the sources by it, a
//! LEFT JOIN's right side by its ON alone until a condition outside it makes
//! the join an inner join's (SPEC.md section 7).

use crate::scope_edge::Edge;
use crate::scope_plan::Planner;
use crate::scope_term::ScopeTerm;

/// What conditions narrow: terms on one source's rows each, and edges.
/// Each is false where the column it tests is NULL, so it rejects a row
/// with NULLs in its source's place, except a term a NULL test can make
/// true (`ScopeTerm::may_hold_on_nulls`) and an `IS` edge.
#[derive(Default)]
pub(crate) struct Narrowing {
    pub(crate) terms: Vec<(usize, ScopeTerm)>,
    pub(crate) edges: Vec<Edge>,
    /// ORs whose sides each narrow one source (`scope_or.rs`).
    pub(crate) ors: Vec<crate::scope_or::Disjunction>,
}

impl Narrowing {
    /// Whether a term, an edge or an OR of it can pass a row of NULLs.
    pub(crate) fn may_hold_on_nulls(&self) -> bool {
        self.terms.iter().any(|(_, term)| term.may_hold_on_nulls())
            || self.edges.iter().any(|edge| edge.nulls)
            || self.ors.iter().any(crate::scope_or::Disjunction::may_hold_on_nulls)
    }

    /// What of this, the ON of the LEFT JOIN adding `item`, narrows `item`
    /// while the join is a LEFT JOIN's, and the rest, which waits for the
    /// join to be an inner join's. An edge from `item` through a derived
    /// item, and an OR of `item` with a side that holds one, wait with the
    /// rest: the model proves such an edge only of a binding its core keeps
    /// (`Narrowing.split` in `crates/sqlive/proofs`).
    fn split(self, item: usize) -> (Self, Self) {
        let (own, terms) = self.terms.into_iter().partition(|(narrowed, _)| *narrowed == item);
        let (own_edges, edges) = self
            .edges
            .into_iter()
            .partition(|edge| edge.member.item == item && edge.nesting.through().is_none());
        let (own_ors, ors) = self.ors.into_iter().partition(|or| {
            or.item == item
                && or
                    .sides
                    .iter()
                    .all(|side| side.edges.iter().all(|edge| edge.nesting.through().is_none()))
        });
        (Self { terms: own, edges: own_edges, ors: own_ors }, Self { terms, edges, ors })
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.terms.extend(other.terms);
        self.edges.extend(other.edges);
        self.ors.extend(other.ors);
    }
}

impl Planner<'_, '_> {
    /// Narrow the sources by `found`, which conditions on the joined rows
    /// give. A LEFT JOIN's right side it narrows is an inner join's then: a
    /// left row with NULLs in its place fails the condition, so no row the
    /// LEFT JOIN alone adds reaches the result. A term a row of NULLs can
    /// pass, as a NULL test, narrows nothing on a right side that is still a
    /// LEFT JOIN's and doesn't make it inner: the NULLs of a left row no
    /// right row joins pass it, and a right row added for that left row takes
    /// it out. An `IS` edge is the same: a NULL in its member's place matches
    /// a NULL.
    pub(crate) fn narrow(&mut self, mut found: Narrowing) {
        found.terms.retain(|(item, term)| {
            !(term.may_hold_on_nulls() && self.items[*item].left_on.is_some())
        });
        found.edges.retain(|edge| !(edge.nulls && self.items[edge.member.item].left_on.is_some()));
        found.ors.retain(|or| !(or.may_hold_on_nulls() && self.items[or.item].left_on.is_some()));
        for item in self.add(found) {
            self.join_inner(item);
        }
    }

    /// `found`, which the ON of the LEFT JOIN adding `item` gives: what it
    /// says of `item` narrows it, and the rest narrows the other sources
    /// only once the join is an inner join's. Until then a left row appears
    /// whatever its ON says of the left side.
    pub(crate) fn narrow_on(&mut self, item: usize, found: Narrowing) {
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

    /// Add `edge` to the edges, each once. An equality can reach a column
    /// through several columns of a derived item, which passes an edge into
    /// the column for each: a copy of an edge passes no row the edge doesn't,
    /// and a lookup that read it would write the partner's scope once more.
    pub(crate) fn add_edge(&mut self, edge: Edge) {
        if !self.edges.contains(&edge) {
            self.edges.push(edge);
        }
    }

    /// Add `found` to the sources' terms, the edges and the sources' ORs,
    /// and say which sources it narrows.
    fn add(&mut self, found: Narrowing) -> Vec<usize> {
        let mut narrowed = Vec::new();
        for (item, term) in found.terms {
            ScopeTerm::add_to(&mut self.items[item].source.term, term);
            narrowed.push(item);
        }
        for edge in found.edges {
            narrowed.push(edge.member.item);
            self.add_edge(edge);
        }
        for or in found.ors {
            narrowed.push(or.item);
            self.items[or.item].ors.push(or.sides);
        }
        narrowed
    }
}
