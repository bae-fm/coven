//! The order a statement's sources take values in: each after the sources
//! whose values its memberships compare with (§4).

use std::collections::BTreeSet;

use crate::scope_order::SortedLimit;
use crate::scope_plan::{Edge, Membership, Planner, Sources};

impl Planner<'_> {
    /// Order the items so each comes after the sources it takes values
    /// from: core by core, each after the core it sits in; within a core,
    /// those an ordered one reaches first, so each takes the values of the
    /// sources the core's narrowing started from, then those with
    /// conditions of their own, then views and table functions, which
    /// nothing narrows but whose values others can take, in FROM order.
    pub(crate) fn ordered(mut self, sorted: Option<SortedLimit>) -> Sources {
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
                            self.edges.iter().any(|edge| {
                                edge.member.item == *item && order.contains(&edge.partner)
                            })
                        })
                        .or_else(|| {
                            // An item that takes values from a FROM item
                            // nothing narrows, a table function whose
                            // columns no equality joins to another source,
                            // goes after it, as the values are all of its
                            // narrowing.
                            let takes_unplaced_values = |item: usize| {
                                self.edges.iter().any(|edge| {
                                    edge.member.item == item
                                        && candidates.contains(&edge.partner)
                                        && items[edge.partner].source.table.is_none()
                                        && items[edge.partner].source.term.is_none()
                                        && !self
                                            .edges
                                            .iter()
                                            .any(|other| other.member.item == edge.partner)
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
                .filter(|edge| edge.member.item == next && order.contains(&edge.partner))
                .cloned()
                .collect();
            for edge in &taken {
                self.items[next].source.memberships.push(Membership::of(edge));
            }
            self.items[next].source.either = self.placed_ors(next, &order);
            // A subquery's, CTE's or view's cores come later, and take
            // the same values.
            crate::derived::push_down(&mut self, next, None, taken);
            order.push(next);
        }
        let windows = self.cores.iter_mut().filter_map(|core| core.window.take()).collect();
        let whole_names = self.whole_names;
        Sources {
            sources: self.items.into_iter().map(|item| item.source).collect(),
            order,
            inlined: self
                .inlined
                .into_iter()
                .filter(|name| whole_names.as_ref().is_some_and(|whole| !whole.contains(name)))
                .collect(),
            fts5_shadows: self.fts5_shadows.into_iter().collect(),
            sorted,
            windows,
        }
    }

    /// The items of `remaining` that wait: those whose value comes from a
    /// subquery's source not placed yet, which a column compared with a
    /// subquery's value takes (`scope_subquery.rs`), alone or in a side of
    /// an OR, and those whose every partner waits, which couldn't take their
    /// values from a partner that is placed after them.
    fn waiting(&self, order: &[usize], remaining: &[usize]) -> BTreeSet<usize> {
        let mut waiting: BTreeSet<usize> = remaining
            .iter()
            .copied()
            .filter(|item| {
                self.edges.iter().any(|edge| {
                    edge.nested && edge.member.item == *item && !order.contains(&edge.partner)
                }) || self.waits_for_or(*item, order)
            })
            .collect();
        loop {
            let starved = remaining.iter().copied().find(|item| {
                let mut partners = self
                    .edges
                    .iter()
                    .filter(|edge| edge.member.item == *item)
                    .map(|edge| edge.partner);
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
}
