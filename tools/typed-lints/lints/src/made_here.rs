//! Whether a method hands out a value it made or one it took from its owner
//! (Appendix F.3), read from the method's MIR: an owner may return, or give
//! its caller's callback, a new value of a type it also holds, such as a
//! fresh cancellation token, but not the one it holds.
//!
//! A value reads the owner when it is computed from `self`: a place that
//! holds what an assignment or a call computed from a place that reads it,
//! by itself or through what reads it, reads it too. A method with no body
//! here, as a trait's required method, makes nothing. An async method's work
//! is its coroutine's, which holds the method's parameters as its captures:
//! the analysis reads the coroutine, where `self` is the capture the method
//! gave it.
//!
//! A callback is given an argument where the method calls it, as a function
//! pointer or through the `Fn` trait it is; a callback the method does
//! anything else with, as pass on or capture in a closure, may be given
//! anything.

use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def_id::DefId;
use rustc_middle::mir::visit::{PlaceContext, Visitor};
use rustc_middle::mir::{
    AggregateKind, Body, CoroutineLayout, CoroutineSavedLocal, Local, Location, Operand, Place,
    ProjectionElem, RETURN_PLACE, Rvalue, Statement, StatementKind, Terminator, TerminatorKind,
};
use rustc_middle::ty::TyCtxt;

/// What a method's MIR says it hands out: whether each value it returns is
/// made in it, and, for each of its parameters that is a callback, whether
/// each argument it gives the callback is.
pub(crate) struct MadeHere<'tcx> {
    returns: bool,
    /// The flow of the body that does the method's work, none for a method
    /// with no body here, and the slot each parameter is in there.
    flow: Option<Flow<'tcx>>,
    parameters: Vec<Option<Slot>>,
}

/// Where a value is in a body: a local, or, in a coroutine, one of its
/// captures or one of the locals it keeps across a suspension.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Slot {
    Local(Local),
    Capture(usize),
    Kept(CoroutineSavedLocal),
}

/// A body, and which of its slots read the owner.
struct Flow<'tcx> {
    tcx: TyCtxt<'tcx>,
    body: &'tcx Body<'tcx>,
    layout: Option<&'tcx CoroutineLayout<'tcx>>,
    /// The locals that point at the coroutine whose body this is.
    coroutine: FxHashSet<Local>,
    reads_owner: FxHashSet<Slot>,
}

impl<'tcx> MadeHere<'tcx> {
    /// What `callable`'s MIR says, `self` being its first parameter when it
    /// has one.
    pub(crate) fn of(tcx: TyCtxt<'tcx>, callable: DefId) -> Self {
        if !tcx.is_mir_available(callable) {
            return Self { returns: false, flow: None, parameters: Vec::new() };
        }
        let body = tcx.optimized_mir(callable);
        let mut flow = Flow::new(tcx, body);
        if tcx.opt_associated_item(callable).is_some_and(|item| item.is_method()) {
            flow.reads_owner.insert(Slot::Local(Local::from_usize(1)));
        }
        flow.spread();
        let locals = (1..=body.arg_count).map(Local::from_usize);
        // An async method's body only builds its coroutine, from its
        // parameters.
        if let Some((coroutine, captures)) = flow.async_coroutine() {
            let mut inner = Flow::new(tcx, tcx.optimized_mir(coroutine));
            for (capture, operand) in captures.iter().enumerate() {
                if flow.operand_reads_owner(operand) {
                    inner.reads_owner.insert(Slot::Capture(capture));
                }
            }
            inner.spread();
            let parameters = locals
                .map(|local| {
                    captures
                        .iter()
                        .position(|operand| {
                            operand.place().is_some_and(|place| place.as_local() == Some(local))
                        })
                        .map(Slot::Capture)
                })
                .collect();
            let returns = !inner.reads_owner.contains(&Slot::Local(RETURN_PLACE));
            return Self { returns, flow: Some(inner), parameters };
        }
        let returns = !flow.reads_owner.contains(&Slot::Local(RETURN_PLACE));
        Self {
            returns,
            flow: Some(flow),
            parameters: locals.map(|local| Some(Slot::Local(local))).collect(),
        }
    }

    /// Whether every value the method returns is made in it.
    pub(crate) fn returns(&self) -> bool {
        self.returns
    }

    /// Whether every value the method gives as argument `index` of the
    /// callback its parameter `parameter` holds, counting from its first,
    /// is made in it: false for a callback it does anything else with.
    pub(crate) fn callback_argument(&self, parameter: usize, index: usize) -> bool {
        match (&self.flow, self.parameters.get(parameter)) {
            (Some(flow), Some(Some(slot))) => flow.callback_argument(*slot, index),
            _ => false,
        }
    }
}

impl<'tcx> Flow<'tcx> {
    fn new(tcx: TyCtxt<'tcx>, body: &'tcx Body<'tcx>) -> Self {
        let layout = body.coroutine_layout_raw();
        let mut coroutine = FxHashSet::default();
        if layout.is_some() {
            // A coroutine's body reaches itself through its first argument,
            // a pinned pointer, and the locals copied from that pointer.
            let first = Local::from_usize(1);
            for statement in body.basic_blocks.iter().flat_map(|block| &block.statements) {
                if let StatementKind::Assign(assign) = &statement.kind
                    && let Rvalue::Use(operand, ..) = &assign.1
                    && let Some(source) = operand.place()
                    && source.local == first
                    && matches!(source.projection.as_slice(), [ProjectionElem::Field(..)])
                    && let Some(local) = assign.0.as_local()
                {
                    coroutine.insert(local);
                }
            }
        }
        Self { tcx, body, layout, coroutine, reads_owner: FxHashSet::default() }
    }

    /// The slot `place` is in: the coroutine's capture or kept local it
    /// names, or its local.
    fn slot(&self, place: Place<'tcx>) -> Slot {
        let projection = place.projection.as_slice();
        let inside = if self.coroutine.contains(&place.local) {
            match projection {
                [ProjectionElem::Deref, rest @ ..] => Some(rest),
                _ => None,
            }
        } else if self.layout.is_some() && place.local == Local::from_usize(1) {
            match projection {
                [ProjectionElem::Field(..), ProjectionElem::Deref, rest @ ..] => Some(rest),
                _ => None,
            }
        } else {
            None
        };
        match (inside, self.layout) {
            (Some([ProjectionElem::Field(capture, _), ..]), _) => Slot::Capture(capture.as_usize()),
            (
                Some([ProjectionElem::Downcast(_, variant), ProjectionElem::Field(field, _), ..]),
                Some(layout),
            ) => layout
                .variant_fields
                .get(*variant)
                .and_then(|fields| fields.get(*field))
                .map_or(Slot::Local(place.local), |&kept| Slot::Kept(kept)),
            _ => Slot::Local(place.local),
        }
    }

    fn reads(&self, place: Place<'tcx>) -> bool {
        self.reads_owner.contains(&self.slot(place))
    }

    fn operand_reads_owner(&self, operand: &Operand<'tcx>) -> bool {
        operand.place().is_some_and(|place| self.reads(place))
    }

    /// Whether any place `rvalue` reads reads the owner.
    fn rvalue_reads_owner(&self, rvalue: &Rvalue<'tcx>) -> bool {
        let mut places = Places(Vec::new());
        places.visit_rvalue(rvalue, Location::START);
        places.0.into_iter().any(|place| self.reads(place))
    }

    /// Marks each slot written from what reads the owner, until no more do.
    fn spread(&mut self) {
        loop {
            let before = self.reads_owner.len();
            for block in self.body.basic_blocks.iter() {
                for statement in &block.statements {
                    if let StatementKind::Assign(assign) = &statement.kind
                        && self.rvalue_reads_owner(&assign.1)
                    {
                        self.reads_owner.insert(self.slot(assign.0));
                    }
                }
                if let Some(Terminator {
                    kind: TerminatorKind::Call { func, args, destination, .. },
                    ..
                }) = &block.terminator
                    && (self.operand_reads_owner(func)
                        || args.iter().any(|argument| self.operand_reads_owner(&argument.node)))
                {
                    self.reads_owner.insert(self.slot(*destination));
                }
            }
            if self.reads_owner.len() == before {
                return;
            }
        }
    }

    /// The coroutine the body returns, when it does nothing else, as an
    /// async function's body does, with the operands its captures take.
    fn async_coroutine(&self) -> Option<(DefId, Vec<Operand<'tcx>>)> {
        self.body.basic_blocks.iter().flat_map(|block| &block.statements).find_map(|statement| {
            let StatementKind::Assign(assign) = &statement.kind else {
                return None;
            };
            let (place, Rvalue::Aggregate(kind, operands)) = &**assign else {
                return None;
            };
            match **kind {
                AggregateKind::Coroutine(coroutine, _)
                    if place.as_local() == Some(RETURN_PLACE)
                        && self.tcx.coroutine_is_async(coroutine) =>
                {
                    Some((coroutine, operands.iter().cloned().collect()))
                }
                _ => None,
            }
        })
    }

    /// Whether every value the body gives as argument `index` of the
    /// callback in `callback` is made in it, and the body does nothing with
    /// the callback but move it between places and call it.
    fn callback_argument(&self, callback: Slot, index: usize) -> bool {
        let body = self.body;
        let mut callbacks = FxHashSet::from_iter([callback]);
        // The places the callback moves to, or that point at it.
        loop {
            let before = callbacks.len();
            for statement in body.basic_blocks.iter().flat_map(|block| &block.statements) {
                if let Some((place, source)) = self.alias(statement)
                    && callbacks.contains(&self.slot(source))
                {
                    callbacks.insert(self.slot(place));
                }
            }
            if callbacks.len() == before {
                break;
            }
        }
        let is_callback = |place: Place<'tcx>| callbacks.contains(&self.slot(place));
        let mut arguments = Vec::new();
        for block in body.basic_blocks.iter() {
            for statement in &block.statements {
                if let Some((place, source)) = self.alias(statement)
                    && is_callback(source)
                    && is_callback(place)
                {
                    continue;
                }
                if self.mentions(statement, &is_callback) {
                    return false;
                }
            }
            let Some(terminator) = &block.terminator else {
                continue;
            };
            match &terminator.kind {
                TerminatorKind::Drop { place, .. } if is_callback(*place) => {}
                TerminatorKind::Call { func, args, .. } => {
                    let mut places = args.iter().filter_map(|argument| argument.node.place());
                    let through_trait = func.const_fn_def().is_some_and(|(callee, _)| {
                        self.tcx.trait_of_assoc(callee).is_some_and(|called| {
                            self.tcx.fn_trait_kind_from_def_id(called).is_some()
                        })
                    }) && args
                        .first()
                        .and_then(|first| first.node.place())
                        .is_some_and(is_callback);
                    if through_trait {
                        // The trait's method takes the callback, then a
                        // tuple of the callback's arguments.
                        if places.skip(1).any(is_callback) {
                            return false;
                        }
                        let Some(tuple) = args.get(1).and_then(|tuple| tuple.node.place()) else {
                            return false;
                        };
                        arguments.extend(self.tuple_operands(tuple));
                    } else if func.place().is_some_and(is_callback) {
                        if places.any(is_callback) {
                            return false;
                        }
                        arguments.push(args.iter().map(|argument| argument.node.clone()).collect());
                    } else if places.any(is_callback) {
                        return false;
                    }
                }
                _ => {
                    if self.terminator_mentions(terminator, &is_callback) {
                        return false;
                    }
                }
            }
        }
        arguments.iter().all(|operands| {
            operands.get(index).is_some_and(|operand| !self.operand_reads_owner(operand))
        })
    }

    /// The place a statement writes and the place it moves, copies or
    /// borrows into it, when it does only that.
    fn alias(&self, statement: &Statement<'tcx>) -> Option<(Place<'tcx>, Place<'tcx>)> {
        let StatementKind::Assign(assign) = &statement.kind else {
            return None;
        };
        let source = match &assign.1 {
            Rvalue::Use(operand, ..) => operand.place()?,
            Rvalue::Ref(_, _, place) | Rvalue::CopyForDeref(place) => *place,
            _ => return None,
        };
        Some((assign.0, source))
    }

    /// The operand lists that the statements writing the tuple `tuple` build
    /// it from; one list that can't be known when another statement writes
    /// it.
    fn tuple_operands(&self, tuple: Place<'tcx>) -> Vec<Vec<Operand<'tcx>>> {
        let slot = self.slot(tuple);
        let mut lists = Vec::new();
        for statement in self.body.basic_blocks.iter().flat_map(|block| &block.statements) {
            let StatementKind::Assign(assign) = &statement.kind else {
                continue;
            };
            if self.slot(assign.0) != slot {
                continue;
            }
            match &assign.1 {
                Rvalue::Aggregate(kind, operands) if matches!(**kind, AggregateKind::Tuple) => {
                    lists.push(operands.iter().cloned().collect());
                }
                _ => lists.push(Vec::new()),
            }
        }
        if lists.is_empty() {
            lists.push(Vec::new());
        }
        lists
    }

    /// Whether `statement` names a place `is_callback` accepts.
    fn mentions(
        &self,
        statement: &Statement<'tcx>,
        is_callback: &dyn Fn(Place<'tcx>) -> bool,
    ) -> bool {
        let mut places = Places(Vec::new());
        places.visit_statement(statement, Location::START);
        places.0.into_iter().any(is_callback)
    }

    /// Whether `terminator` names a place `is_callback` accepts.
    fn terminator_mentions(
        &self,
        terminator: &Terminator<'tcx>,
        is_callback: &dyn Fn(Place<'tcx>) -> bool,
    ) -> bool {
        let mut places = Places(Vec::new());
        places.visit_terminator(terminator, Location::START);
        places.0.into_iter().any(is_callback)
    }
}

/// The places a part of a body names, but for storage markers.
struct Places<'tcx>(Vec<Place<'tcx>>);

impl<'tcx> Visitor<'tcx> for Places<'tcx> {
    fn visit_place(&mut self, place: &Place<'tcx>, context: PlaceContext, _: Location) {
        if !matches!(context, PlaceContext::NonUse(_)) {
            self.0.push(*place);
        }
    }
}
