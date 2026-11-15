//! This module tells where an argument goes (`read_code.rs`): to a read
//! place, which makes a closure or function that code passes there read
//! code, or to a live query, which keeps a closure that code passes there.

use super::{Call, Walker, Why};

impl Walker<'_, '_> {
    /// Whether the argument at `index` goes where a read closure does.
    pub(super) fn read_place(
        &self,
        call: &Call<'_>,
        targets: &[usize],
        index: usize,
    ) -> Option<Why> {
        let index_of = &self.context.index.callables;
        let shapes = self.context.shapes;
        match call {
            Call::Method { receiver_read: true, .. } => Some(Why::PassedToReadReceiver),
            Call::Method { known: false, candidates, .. } => {
                let callbacks = candidates
                    .iter()
                    .map(|&id| &index_of[id])
                    .filter(|callable| callable.takes_callback(index, false))
                    .collect::<Vec<_>>();
                (!callbacks.is_empty()
                    && callbacks
                        .iter()
                        .all(|callable| callable.takes_read_callback(index, false, shapes)))
                .then_some(Why::PassedToReadCallee)
            }
            Call::Method { .. } => self.callee_place(targets, index, false),
            Call::Path => self.callee_place(targets, index, true),
        }
    }

    /// Whether the argument at `index` goes to a live query, or to a callee
    /// that takes one beside it: the closure there runs on each value the
    /// query reads, after the call returns.
    pub(super) fn live_query_place(
        &self,
        call: &Call<'_>,
        targets: &[usize],
        index: usize,
    ) -> bool {
        let live = &self.context.shapes.live_query_types;
        let callables = &self.context.index.callables;
        match call {
            Call::Method { receiver_live: true, .. } => true,
            Call::Method { known: false, .. } => false,
            Call::Method { .. } => {
                targets.iter().any(|&id| callables[id].takes_callback_beside(index, false, &[live]))
            }
            Call::Path => {
                targets.iter().any(|&id| callables[id].takes_callback_beside(index, true, &[live]))
            }
        }
    }

    /// Whether a resolved callee takes a read closure at `index`, or a
    /// callback there beside a read value, which it runs on what the value
    /// reads.
    fn callee_place(&self, targets: &[usize], index: usize, path_call: bool) -> Option<Why> {
        let callables = &self.context.index.callables;
        let shapes = self.context.shapes;
        if targets.iter().any(|&id| callables[id].takes_read_callback(index, path_call, shapes)) {
            return Some(Why::PassedToReadCallee);
        }
        let read = [&shapes.read_types, &shapes.live_query_types];
        targets
            .iter()
            .any(|&id| callables[id].takes_callback_beside(index, path_call, &read))
            .then_some(Why::PassedBesideRead)
    }
}
