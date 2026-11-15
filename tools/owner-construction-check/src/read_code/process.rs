//! The read code only a live query's `process` runs (`read_code.rs`): the
//! closures and functions passed to `process`, and what they call, less what
//! any other read code reaches. The read rule allows such code to shape how
//! one result's rows display, and a policy's `allowed` row says so
//! (`read_shapes.rs`).

use std::collections::BTreeSet;

use super::units::{ancestors, Reach};
use super::Walked;
use crate::read_index::Index;

/// The items, by file and name, whose read code only `process` runs.
pub(super) fn process_only(
    index: &Index<'_>,
    walked: &Walked,
    takes_read: &BTreeSet<usize>,
    holds: &BTreeSet<usize>,
) -> BTreeSet<(String, String)> {
    let item = |id: usize| {
        let callable = &index.callables[id];
        (callable.file.relative_path.clone(), callable.item())
    };
    let process =
        |position: usize| walked.closures[position].passed.is_some_and(|mark| mark.process);
    let in_process = |closure: Option<usize>| ancestors(&walked.closures, closure).any(process);
    let mut process_closures = Vec::new();
    let mut other_closures = Vec::new();
    for (position, closure) in walked.closures.iter().enumerate() {
        if process(position) {
            process_closures.push(position);
        } else if closure.typed_read || closure.passed.is_some() {
            other_closures.push(position);
        }
    }
    let reach = Reach::new(index, walked);
    let processed = reach.from(
        walked.passed_to_process.iter().copied(),
        process_closures.iter().copied(),
        false,
    );
    let other_roots = takes_read.iter().chain(holds).chain(&walked.passed_elsewhere).copied();
    let elsewhere = reach.from(other_roots, other_closures.iter().copied(), true);
    let mut items = processed.iter().map(|&id| item(id)).collect::<BTreeSet<_>>();
    for &position in &process_closures {
        let closure = &walked.closures[position];
        if !in_process(closure.parent) {
            items.insert(item(closure.callable));
        }
    }
    for &id in &elsewhere {
        items.remove(&item(id));
    }
    for &position in &other_closures {
        if !in_process(Some(position)) {
            items.remove(&item(walked.closures[position].callable));
        }
    }
    items
}
