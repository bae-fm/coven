//! The order a statement's sources take values in: each after the sources
//! whose values its memberships compare with (SPEC.md section 7).

use crate::scope_edge::{Edge, Nesting};
use crate::scope_or::{EitherSide, Side};
use crate::scope_order::SortedLimit;
use crate::scope_plan::{Membership, Planner, Sources};
use crate::scope_term::ScopeTerm;

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

impl Planner<'_, '_> {
    /// Order the items so each comes after the sources it takes values
    /// from: core by core, each after the core it sits in; within a core,
    /// those an ordered one reaches first, so each takes the values of the
    /// sources the core's narrowing started from, then those whose own
    /// conditions compare a column with a value (`gives_alone`), then views
    /// read whole and table functions, which nothing narrows but whose
    /// values others can take, then those whose only conditions SQLite
    /// judges, and then any, each in FROM order.
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
            // Of the items that waited and stopped, one that takes values
            // from an item placed already goes first: placed before it, the
            // others would take none of them.
            let stopped =
                || remaining.iter().copied().filter(|item| waited[*item] && !waiting[*item]);
            let released = stopped()
                .find(|item| {
                    edges.by_member[*item].iter().any(|edge| self.edges[*edge].placed(&placed))
                })
                .or_else(|| stopped().next());
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
                .filter(|edge| edge.placed(&placed))
                .cloned()
                .collect();
            for edge in &taken {
                self.items[next].source.memberships.push(Membership::of(edge));
            }
            let either = self.placed_ors(next, &placed);
            self.items[next].source.either =
                either.iter().map(|sides| sides.iter().map(EitherSide::of).collect()).collect();
            // A subquery's, CTE's or view's cores come later, and take
            // the same values.
            crate::derived::push_down(&mut self, next, None, taken);
            crate::derived::push_ors(&mut self, next, &either);
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
        // A FROM item that conditions don't pass through, as a table function
        // or a subquery with a LIMIT. A subquery, CTE or view that they do
        // pass through goes as the tables it reads would, written out.
        let read_whole = |item: usize| {
            items[item].source.table().is_none()
                && items[item].derived.as_ref().is_none_or(|derived| derived.arms.is_empty())
        };
        // An item that takes values from a FROM item nothing narrows, a
        // table function whose columns no equality joins to another source,
        // goes after it, as the values are all of its narrowing.
        let takes_unplaced_values = |item: usize| {
            partners(item).any(|partner| {
                is_candidate(partner)
                    && read_whole(partner)
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
                    let compared =
                        items[*item].source.term.as_ref().is_some_and(ScopeTerm::compares);
                    (compared || !read_whole(*item))
                        && self.gives_alone(*item, ScopeTerm::compares)
                        && !takes_unplaced_values(*item)
                })
            })
            .or_else(|| candidates.iter().copied().find(|item| read_whole(*item)))
            .or_else(|| candidates.iter().copied().find(|item| self.gives_alone(*item, |_| true)))
            .unwrap_or(free)
    }

    /// Which items wait, of those `remaining`: those whose value comes from
    /// a subquery's source not placed yet, which a column compared with a
    /// subquery's value takes (`scope_subquery.rs`), alone or in a side of
    /// an OR, or from a source a derived item's arm passes on, while it or
    /// the derived item isn't placed (`Planner::passed_edge`), or from an
    /// item of a core around its own not placed yet, those whose
    /// every partner waits, which couldn't take their values from a partner
    /// that is placed after them, and those a waiting derived item could
    /// pass edges into (`receiving`).
    fn waiting(&self, edges: &EdgesOfItems, placed: &[bool], remaining: &[usize]) -> Vec<bool> {
        let mut waiting = vec![false; self.items.len()];
        // The items that wait whose edges from other items aren't counted
        // yet.
        let mut uncounted: Vec<usize> = Vec::new();
        for &item in remaining {
            let core = self.items[item].core;
            let nested = edges.by_member[item].iter().map(|edge| &self.edges[*edge]).any(|edge| {
                !edge.placed(placed)
                    && (edge.nesting.nested()
                        || self
                            .scope_of(core)
                            .skip(1)
                            .any(|around| around == self.items[edge.partner].core))
            });
            if nested || self.waits_for_or(item, placed) {
                waiting[item] = true;
                uncounted.push(item);
            }
        }
        // A derived item that a partner reads another source through gives
        // that partner no values, so the partner's wait for it is no reason
        // for it to wait for the partner.
        let counted = |edge: &Edge| {
            edges.by_member[edge.partner]
                .iter()
                .all(|back| self.edges[*back].nesting != Nesting::Through(edge.member.item))
        };
        // How many of each item's counted edges have a partner that doesn't
        // wait.
        let mut not_waiting: Vec<usize> = edges
            .by_member
            .iter()
            .map(|of_member| of_member.iter().filter(|edge| counted(&self.edges[**edge])).count())
            .collect();
        while let Some(partner) = uncounted.pop() {
            for item in self.receiving(edges, partner) {
                if !placed[item] && !waiting[item] {
                    waiting[item] = true;
                    uncounted.push(item);
                }
            }
            for edge in &edges.by_partner[partner] {
                if !counted(&self.edges[*edge]) {
                    continue;
                }
                let member = self.edges[*edge].member.item;
                not_waiting[member] -= 1;
                if not_waiting[member] == 0 && !placed[member] && !waiting[member] {
                    waiting[member] = true;
                    uncounted.push(member);
                }
            }
        }
        self.wait_with_group(edges, placed, remaining, &mut waiting);
        waiting
    }

    /// Make the items wait that only a waiting item can narrow. Items that
    /// join one another and no source that gives them values, a source with
    /// conditions of its own or one with a partner already placed, give none
    /// to each other: a chain of joins whose first item waits for a
    /// subquery's source holds such a group after it, and placed before it,
    /// the group's items would take no values.
    fn wait_with_group(
        &self,
        edges: &EdgesOfItems,
        placed: &[bool],
        remaining: &[usize],
        waiting: &mut [bool],
    ) {
        let neighbors = |item: usize| {
            edges.by_member[item]
                .iter()
                .map(|edge| self.edges[*edge].partner)
                .chain(edges.by_partner[item].iter().map(|edge| self.edges[*edge].member.item))
        };
        let gives_values = |item: usize| {
            !waiting[item]
                && (self.gives_alone(item, ScopeTerm::compares)
                    || edges.by_member[item].iter().any(|edge| placed[self.edges[*edge].partner]))
        };
        let mut reached = vec![false; self.items.len()];
        let mut stack: Vec<usize> =
            remaining.iter().copied().filter(|item| gives_values(*item)).collect();
        for item in &stack {
            reached[*item] = true;
        }
        while let Some(item) = stack.pop() {
            for next in neighbors(item) {
                if !placed[next] && !waiting[next] && !reached[next] {
                    reached[next] = true;
                    stack.push(next);
                }
            }
        }
        let mut stack: Vec<usize> =
            remaining.iter().copied().filter(|item| waiting[*item]).collect();
        // Only an item that takes values from a waiting one waits for it: the
        // table a waiting item takes its values from goes first. So does a
        // waiting derived item before the items it could pass edges into.
        while let Some(item) = stack.pop() {
            let takers = edges.by_partner[item]
                .iter()
                .map(|edge| self.edges[*edge].member.item)
                .filter(|taker| !reached[*taker]);
            let found: Vec<usize> = takers.chain(self.receiving(edges, item)).collect();
            for next in found {
                if !placed[next] && !waiting[next] {
                    waiting[next] = true;
                    stack.push(next);
                }
            }
        }
    }

    /// Whether `item` has values to give before any item is placed: it has a
    /// condition of its own that `counts` takes, or it is a view read whole
    /// or a table function,
    /// which nothing narrows, or it is a subquery, CTE or view read as its
    /// own that no condition passes into, or one with an arm that reads no
    /// FROM item, as a relation of a parameter (`SELECT ?1 AS key`) does,
    /// whose row is the values it selects, or one that reads an item that
    /// has. A subquery that only passes a table's rows on has none, as the
    /// table has none. The model states the rule as `givesAlone` in
    /// `crates/sqlive/proofs`.
    fn gives_alone(&self, item: usize, counts: fn(&ScopeTerm) -> bool) -> bool {
        let found = &self.items[item];
        if found.source.term.as_ref().is_some_and(counts) {
            return true;
        }
        match &found.derived {
            Some(derived) => {
                derived.arms.is_empty()
                    || derived.cores.iter().any(|core| {
                        let items = &self.cores[*core].items;
                        items.is_empty() || items.iter().any(|item| self.gives_alone(*item, counts))
                    })
            }
            None => found.source.table().is_none(),
        }
    }

    /// The items the derived item `item` could pass edges or ORs into
    /// (`derived::receivers`). Placed before it, one of them would take none
    /// of the values the item takes, as a table that a FROM subquery reads
    /// takes none of the values of a subquery that the subquery's column is
    /// compared with.
    fn receiving(&self, edges: &EdgesOfItems, item: usize) -> Vec<usize> {
        let compared =
            edges.by_member[item].iter().flat_map(|edge| self.edges[*edge].member.columns());
        let named: Vec<String> =
            self.items[item].ors.iter().flatten().flat_map(Side::columns).collect();
        crate::derived::receivers(self, item, compared.chain(named.iter().map(String::as_str)))
    }
}

#[cfg(test)]
#[path = "source_order_tests.rs"]
mod tests;
