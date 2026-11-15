//! The read code only a live query's `process` runs (`read_code.rs`): the
//! closures and functions passed to `process`, and what they call, less what
//! any other read code reaches. The read rule allows such code to shape how
//! one result's rows display, and a policy's `allowed` row says so
//! (`read_shapes.rs`).

use std::collections::BTreeSet;

use super::units::ancestors;
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
    let in_process = |closure: Option<usize>| {
        ancestors(&walked.closures, closure).any(|enclosing| walked.closures[enclosing].process)
    };
    let mut process_closures = Vec::new();
    let mut other_closures = Vec::new();
    for (position, closure) in walked.closures.iter().enumerate() {
        if closure.process {
            process_closures.push(position);
        } else if closure.typed_read || closure.passed.is_some() {
            other_closures.push(position);
        }
    }
    let mut calls_of = vec![Vec::new(); index.callables.len()];
    for (position, call) in walked.calls.iter().enumerate() {
        calls_of[call.callable].push(position);
    }
    let processed = Reach { index, walked, calls_of: &calls_of, skip_process: false }
        .from(walked.passed_to_process.iter().copied(), process_closures.iter().copied());
    let other_roots =
        takes_read.iter().chain(holds).chain(&walked.passed_elsewhere).copied().collect::<Vec<_>>();
    let elsewhere = Reach { index, walked, calls_of: &calls_of, skip_process: true }
        .from(other_roots.iter().copied(), other_closures.iter().copied());
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

/// A walk through what read code calls.
struct Reach<'w, 'i, 'a> {
    index: &'i Index<'a>,
    walked: &'w Walked,
    /// The calls in each function, by their place in `walked.calls`.
    calls_of: &'w [Vec<usize>],
    /// Whether the walk leaves out the calls inside a `process` callback.
    skip_process: bool,
}

impl Reach<'_, '_, '_> {
    /// The functions the given functions and closures are or call,
    /// transitively.
    fn from(
        &self,
        functions: impl Iterator<Item = usize>,
        closures: impl Iterator<Item = usize>,
    ) -> BTreeSet<usize> {
        let walked = self.walked;
        let readable = |id: usize| {
            let callable = &self.index.callables[id];
            !callable.plumbing && callable.body.is_some()
        };
        let in_process = |closure: Option<usize>| {
            ancestors(&walked.closures, closure).any(|enclosing| walked.closures[enclosing].process)
        };
        let mut reached = BTreeSet::new();
        let mut queue = Vec::new();
        for id in functions.filter(|&id| readable(id)) {
            if reached.insert(id) {
                queue.push((id, None));
            }
        }
        queue.extend(closures.map(|position| (walked.closures[position].callable, Some(position))));
        while let Some((callable, closure)) = queue.pop() {
            for call in self.calls_of[callable].iter().map(|&position| &walked.calls[position]) {
                if closure.is_some_and(|closure| !walked.inside(call.closure, closure)) {
                    continue;
                }
                if self.skip_process && in_process(call.closure) {
                    continue;
                }
                for &target in &call.targets {
                    if readable(target) && reached.insert(target) {
                        queue.push((target, None));
                    }
                }
            }
        }
        reached
    }
}
