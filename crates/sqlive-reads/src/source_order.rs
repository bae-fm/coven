//! The order a statement's sources take values in: each after the sources
//! whose values its memberships compare with (§4).

use crate::scope_order::SortedLimit;
use crate::scope_plan::{Edge, Membership, Planner, Sources};

/// A planner's edges by the items they join, each as its position in the
/// planner's edges, in order.
struct EdgesOfItems {
    /// The edges each item is the member of.
    by_member: Vec<Vec<usize>>,
    /// The edges each item is the partner of.
    by_partner: Vec<Vec<usize>>,
    /// How many of the planner's edges these hold.
    held: usize,
}

impl EdgesOfItems {
    /// `edges`, which join `items` items.
    fn of(items: usize, edges: &[Edge]) -> Self {
        let mut of_items = Self {
            by_member: vec![Vec::new(); items],
            by_partner: vec![Vec::new(); items],
            held: 0,
        };
        of_items.add_new(edges);
        of_items
    }

    /// Add those of `edges` added since these were last read from them.
    fn add_new(&mut self, edges: &[Edge]) {
        for (position, edge) in edges.iter().enumerate().skip(self.held) {
            self.by_member[edge.member.item].push(position);
            self.by_partner[edge.partner].push(position);
        }
        self.held = edges.len();
    }
}

impl Planner<'_> {
    /// Order the items so each comes after the sources it takes values
    /// from: core by core, each after the core it sits in; within a core,
    /// those an ordered one reaches first, so each takes the values of the
    /// sources the core's narrowing started from, then those with
    /// conditions of their own, then views and table functions, which
    /// nothing narrows but whose values others can take, in FROM order.
    pub(crate) fn ordered(mut self, sorted: Option<SortedLimit>) -> Sources {
        let mut order: Vec<usize> = Vec::new();
        let mut remaining: Vec<usize> =
            self.cores.iter().flat_map(|core| core.items.iter().copied()).collect();
        // Whether each item is in `order`.
        let mut placed = vec![false; self.items.len()];
        // Whether each item has waited: it goes as soon as it stops.
        let mut waited = vec![false; self.items.len()];
        let mut edges = EdgesOfItems::of(self.items.len(), &self.edges);
        while !remaining.is_empty() {
            let waiting = self.waiting(&edges, &placed, &remaining);
            for &item in &remaining {
                waited[item] |= waiting[item];
            }
            let released = remaining.iter().copied().find(|item| waited[*item] && !waiting[*item]);
            let free = remaining.iter().copied().find(|item| !waiting[*item]);
            let next = match (released, free) {
                (Some(released), _) => released,
                (None, Some(free)) => self.next_of_core(&edges, &placed, &waiting, free),
                // Every item left waits, each on one that waits: the first
                // goes without the values of partners not placed yet.
                (None, None) => remaining[0],
            };
            let taken: Vec<Edge> = edges.by_member[next]
                .iter()
                .map(|edge| &self.edges[*edge])
                .filter(|edge| placed[edge.partner])
                .cloned()
                .collect();
            for edge in &taken {
                self.items[next].source.memberships.push(Membership::of(edge));
            }
            self.items[next].source.either = self.placed_ors(next, &placed);
            // A subquery's, CTE's or view's cores come later, and take
            // the same values.
            crate::derived::push_down(&mut self, next, None, taken);
            edges.add_new(&self.edges);
            remaining.retain(|item| *item != next);
            placed[next] = true;
            order.push(next);
        }
        let windows = self.cores.iter_mut().filter_map(|core| core.window.take()).collect();
        let inlined = self.inlined;
        Sources {
            sources: self.items.into_iter().map(|item| item.source).collect(),
            order,
            inlined: self
                .whole_names
                .map(|whole| inlined.into_iter().filter(|name| !whole.contains(name)).collect()),
            fts5_shadows: self.fts5_shadows.into_iter().collect(),
            sorted,
            windows,
        }
    }

    /// The item to place next when none that waited has stopped: one of the
    /// items of `free`'s core that don't wait. `free` is the first item that
    /// doesn't wait, whose core goes next, as every core goes in turn when
    /// none waits.
    fn next_of_core(
        &self,
        edges: &EdgesOfItems,
        placed: &[bool],
        waiting: &[bool],
        free: usize,
    ) -> usize {
        let items = &self.items;
        let core = items[free].core;
        let is_candidate =
            |item: usize| items[item].core == core && !placed[item] && !waiting[item];
        let candidates: Vec<usize> =
            self.cores[core].items.iter().copied().filter(|item| is_candidate(*item)).collect();
        let partners =
            |item: usize| edges.by_member[item].iter().map(|edge| self.edges[*edge].partner);
        // An item that takes values from a FROM item nothing narrows, a
        // table function whose columns no equality joins to another source,
        // goes after it, as the values are all of its narrowing.
        let takes_unplaced_values = |item: usize| {
            partners(item).any(|partner| {
                is_candidate(partner)
                    && items[partner].source.table().is_none()
                    && items[partner].source.term.is_none()
                    && edges.by_member[partner].is_empty()
            })
        };
        let takes_placed_values = |item: &usize| partners(*item).any(|partner| placed[partner]);
        candidates
            .iter()
            .copied()
            .find(takes_placed_values)
            .or_else(|| {
                candidates.iter().copied().find(|item| {
                    items[*item].source.term.is_some() && !takes_unplaced_values(*item)
                })
            })
            .or_else(|| {
                candidates.iter().copied().find(|item| items[*item].source.table().is_none())
            })
            .unwrap_or(free)
    }

    /// Which items wait, of those `remaining`: those whose value comes from
    /// a subquery's source not placed yet, which a column compared with a
    /// subquery's value takes (`scope_subquery.rs`), alone or in a side of
    /// an OR, and those whose every partner waits, which couldn't take their
    /// values from a partner that is placed after them.
    fn waiting(&self, edges: &EdgesOfItems, placed: &[bool], remaining: &[usize]) -> Vec<bool> {
        let mut waiting = vec![false; self.items.len()];
        // The items that wait whose edges from other items aren't counted
        // yet.
        let mut uncounted: Vec<usize> = Vec::new();
        for &item in remaining {
            let nested = edges.by_member[item]
                .iter()
                .map(|edge| &self.edges[*edge])
                .any(|edge| edge.nested && !placed[edge.partner]);
            if nested || self.waits_for_or(item, placed) {
                waiting[item] = true;
                uncounted.push(item);
            }
        }
        // How many of each item's edges have a partner that doesn't wait.
        let mut not_waiting: Vec<usize> = edges.by_member.iter().map(Vec::len).collect();
        while let Some(partner) = uncounted.pop() {
            for edge in &edges.by_partner[partner] {
                let member = self.edges[*edge].member.item;
                not_waiting[member] -= 1;
                if not_waiting[member] == 0 && !placed[member] && !waiting[member] {
                    waiting[member] = true;
                    uncounted.push(member);
                }
            }
        }
        waiting
    }
}

#[cfg(test)]
#[path = "source_order_tests.rs"]
mod tests;
