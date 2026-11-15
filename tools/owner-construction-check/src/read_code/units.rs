//! The units of read code (`read_code.rs`): from the roots and what is
//! passed to a read place, through what they call.

use std::collections::{BTreeMap, BTreeSet};

use super::{Body, ClosureInfo, ReadClosure, Unit, Walked, Why};
use crate::read_index::{Callable, Index};

/// The units of read code: the roots, what is passed to a read place, and
/// what they call, transitively.
pub(super) fn units<'a>(
    index: &Index<'a>,
    walked: &Walked,
    takes_read: &BTreeSet<usize>,
) -> (Vec<Unit<'a>>, Vec<ReadClosure<'a>>) {
    let readable = |id: usize| {
        let callable = &index.callables[id];
        !callable.plumbing && callable.body.is_some()
    };
    let mut callables: BTreeMap<usize, Why> = BTreeMap::new();
    let mut closures: BTreeMap<usize, Why> = BTreeMap::new();
    let mut queue = Vec::new();
    for &id in takes_read {
        if readable(id) {
            callables.insert(id, Why::Root);
            queue.push(Work::Callable(id));
        }
    }
    for (&id, &why) in &walked.passed_paths {
        if readable(id) && !callables.contains_key(&id) {
            callables.insert(id, why);
            queue.push(Work::Callable(id));
        }
    }
    for (position, closure) in walked.closures.iter().enumerate() {
        let why = if closure.typed_read { Some(Why::Root) } else { closure.passed };
        if let Some(why) = why {
            closures.insert(position, why);
            queue.push(Work::Closure(position));
        }
    }
    let mut calls_of = vec![Vec::new(); index.callables.len()];
    for (position, call) in walked.calls.iter().enumerate() {
        calls_of[call.callable].push(position);
    }
    while let Some(work) = queue.pop() {
        let (callable, closure) = match work {
            Work::Callable(id) => (id, None),
            Work::Closure(position) => (walked.closures[position].callable, Some(position)),
        };
        for &position in &calls_of[callable] {
            let call = &walked.calls[position];
            if closure.is_some_and(|closure| !walked.inside(call.closure, closure)) {
                continue;
            }
            for &target in &call.targets {
                if readable(target) && !callables.contains_key(&target) {
                    callables.insert(target, Why::Reached);
                    queue.push(Work::Callable(target));
                }
            }
        }
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
                    && !callables.contains_key(&closure.callable)
                    && !ancestors(&walked.closures, closure.parent)
                        .any(|above| closures.contains_key(&above)),
            }
        })
        .collect();
    (units, read_closures)
}

/// A unit; why it is read code is kept for the tests, which tell the causes
/// apart.
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

/// The closures enclosing one, innermost first, starting at `from`.
pub(super) fn ancestors(
    closures: &[ClosureInfo],
    from: Option<usize>,
) -> impl Iterator<Item = usize> + '_ {
    std::iter::successors(from, |&position| closures[position].parent)
}

enum Work {
    Callable(usize),
    Closure(usize),
}
