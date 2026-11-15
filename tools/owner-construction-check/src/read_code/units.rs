//! This module finds the units of read code (`read_code.rs`): it starts at
//! the roots and at what code passes to a read place, then follows what they
//! call.

use std::collections::{BTreeMap, BTreeSet};

use super::{Body, ClosureInfo, ReadClosure, Unit, Walked, Why};
use crate::read_index::{Callable, Index};

/// The units of read code: the roots (what takes a read value, and the
/// methods of a type that holds a live query), what code passes to a read
/// place, and what they call, transitively.
pub(super) fn units<'a>(
    index: &Index<'a>,
    walked: &Walked,
    takes_read: &BTreeSet<usize>,
    holds: &BTreeSet<usize>,
) -> (Vec<Unit<'a>>, Vec<ReadClosure<'a>>) {
    let reach = Reach::new(index, walked);
    let readable = |id: usize| reach.readable(id);
    let mut callables: BTreeMap<usize, Why> = BTreeMap::new();
    let mut closures: BTreeMap<usize, Why> = BTreeMap::new();
    for &id in takes_read {
        if readable(id) {
            callables.insert(id, Why::Root);
        }
    }
    for &id in holds {
        if readable(id) {
            callables.entry(id).or_insert(Why::Holds);
        }
    }
    for (&id, &why) in &walked.passed_paths {
        if readable(id) {
            callables.entry(id).or_insert(why);
        }
    }
    for (position, closure) in walked.closures.iter().enumerate() {
        let why =
            if closure.typed_read { Some(Why::Root) } else { closure.passed.map(|mark| mark.why) };
        if let Some(why) = why {
            closures.insert(position, why);
        }
    }
    let reached = reach.from(callables.keys().copied(), closures.keys().copied(), false);
    for id in reached {
        callables.entry(id).or_insert(Why::Reached);
    }
    let mut units = Vec::new();
    for (&id, &why) in &callables {
        let callable = &index.callables[id];
        let Some(block) = callable.body else { continue };
        units.push(unit(callable, Body::Function { block, signature: callable.signature }, why));
    }
    for (&position, &why) in &closures {
        let closure = &walked.closures[position];
        let covered = callables.contains_key(&closure.callable)
            || ancestors(&walked.closures, closure.parent)
                .any(|above| closures.contains_key(&above));
        if covered {
            continue;
        }
        let callable = &index.callables[closure.callable];
        units.push(unit(callable, Body::Closure(closure.expr.clone()), why));
    }
    let read_closures = walked
        .closures
        .iter()
        .enumerate()
        .filter(|(position, closure)| {
            closures.contains_key(position)
                || callables.contains_key(&closure.callable)
                || ancestors(&walked.closures, closure.parent)
                    .any(|above| closures.contains_key(&above))
        })
        .map(|(position, closure)| {
            let callable = &index.callables[closure.callable];
            ReadClosure {
                file: callable.file,
                item: callable.item(),
                owner: callable.owner.clone(),
                expr: closure.expr.clone(),
                captures: closure.captures.clone(),
                outlives_its_function: closures.contains_key(&position)
                    && (closure.passed.is_some_and(|mark| mark.kept_by_live_query)
                        || (!callables.contains_key(&closure.callable)
                            && !ancestors(&walked.closures, closure.parent)
                                .any(|above| closures.contains_key(&above)))),
            }
        })
        .collect();
    (units, read_closures)
}

/// Builds a unit. In test builds the unit also records why it is read code,
/// so the tests can tell the causes apart.
fn unit<'a>(callable: &Callable<'a>, body: Body<'a>, _why: Why) -> Unit<'a> {
    Unit {
        file: callable.file,
        item: callable.item(),
        owner: callable.owner.clone(),
        body,
        #[cfg(test)]
        why: _why,
    }
}

/// A walk through what read code calls.
pub(super) struct Reach<'w, 'i, 'a> {
    index: &'i Index<'a>,
    walked: &'w Walked,
    /// The calls in each function, by their place in `walked.calls`.
    calls_of: Vec<Vec<usize>>,
}

impl<'w, 'i, 'a> Reach<'w, 'i, 'a> {
    pub(super) fn new(index: &'i Index<'a>, walked: &'w Walked) -> Self {
        let mut calls_of = vec![Vec::new(); index.callables.len()];
        for (position, call) in walked.calls.iter().enumerate() {
            calls_of[call.callable].push(position);
        }
        Self { index, walked, calls_of }
    }

    /// Whether a function is read code when something reads it: it has a
    /// body, and the policy's `plumbing` list does not hold its file.
    pub(super) fn readable(&self, id: usize) -> bool {
        let callable = &self.index.callables[id];
        !callable.plumbing && callable.body.is_some()
    }

    /// The functions the given functions and closures are or call,
    /// transitively. With `skip_process`, the walk leaves out the calls
    /// inside a live query's `process` callback.
    pub(super) fn from(
        &self,
        functions: impl Iterator<Item = usize>,
        closures: impl Iterator<Item = usize>,
        skip_process: bool,
    ) -> BTreeSet<usize> {
        let walked = self.walked;
        let in_process = |closure: Option<usize>| {
            ancestors(&walked.closures, closure)
                .any(|enclosing| walked.closures[enclosing].passed.is_some_and(|mark| mark.process))
        };
        let mut reached = BTreeSet::new();
        let mut queue = Vec::new();
        for id in functions.filter(|&id| self.readable(id)) {
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
                if skip_process && in_process(call.closure) {
                    continue;
                }
                for &target in &call.targets {
                    if self.readable(target) && reached.insert(target) {
                        queue.push((target, None));
                    }
                }
            }
        }
        reached
    }
}

/// The closures enclosing one, innermost first, starting at `from`.
pub(super) fn ancestors(
    closures: &[ClosureInfo],
    from: Option<usize>,
) -> impl Iterator<Item = usize> + '_ {
    std::iter::successors(from, |&position| closures[position].parent)
}
