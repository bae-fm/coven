//! Where an argument goes (`read_code.rs`): to a read place, which makes a
//! closure or function passed there read code.

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
            Call::Method { .. } => targets
                .iter()
                .any(|&id| index_of[id].takes_read_callback(index, false, shapes))
                .then_some(Why::PassedToReadCallee),
            Call::Path => targets
                .iter()
                .any(|&id| index_of[id].takes_read_callback(index, true, shapes))
                .then_some(Why::PassedToReadCallee),
        }
    }
}
