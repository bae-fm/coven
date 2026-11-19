//! What a callable's callbacks can keep of what it gives them (Appendix
//! F.3): each argument of a callback its parameters' types or its own
//! generics' bounds name, as `impl FnOnce(&'a Connection)` or `fn(Permit)`,
//! except one lent only for the callback's call. An argument is lent when it
//! has a lifetime and every lifetime in it is the callback's own, bound by
//! its `for<…>` or elided in it: `impl FnOnce(&Connection)` lets the
//! callback use the connection while it runs and keep none of it. An
//! argument the callback can keep is handed out, as a returned value is.
//!
//! A callback is a function pointer, a `dyn Fn`, `dyn FnMut` or
//! `dyn FnOnce`, or a generic one of the callable's own bounds makes one of
//! those. What a callback's own arguments and result carry flows between the
//! callback and its caller, so the search stops at each callback.

use rustc_hir::def_id::DefId;
use rustc_middle::ty::{
    self, BoundVarIndexKind, Region, Ty, TyCtxt, TypeSuperVisitable, TypeVisitable, TypeVisitor,
};

/// One argument a callback can keep.
pub(crate) struct KeptArgument<'tcx> {
    /// The callable's parameter that holds the callback, counting from its
    /// first, `self` included: the one whose type holds it, or the one
    /// parameter whose type is the generic, or a reference to it, that a
    /// bound makes a callback. None when no one parameter is.
    pub(crate) parameter: Option<usize>,
    /// The argument's place among the callback's.
    pub(crate) index: usize,
    pub(crate) ty: Ty<'tcx>,
}

/// What `callable`'s callbacks can keep of what it gives them.
pub(crate) fn kept_arguments<'tcx>(tcx: TyCtxt<'tcx>, callable: DefId) -> Vec<KeptArgument<'tcx>> {
    let signature = tcx.liberate_late_bound_regions(
        callable,
        tcx.fn_sig(callable).instantiate_identity().skip_norm_wip(),
    );
    let method = tcx.opt_associated_item(callable).is_some_and(|item| item.is_method());
    let mut kept = Vec::new();
    for (parameter, &input) in signature.inputs().iter().enumerate() {
        if method && parameter == 0 {
            continue;
        }
        callbacks_in(tcx, input, &mut |index, ty| {
            kept.push(KeptArgument { parameter: Some(parameter), index, ty });
        });
    }
    for &(clause, _) in tcx.explicit_clauses_of(callable).clauses {
        let ty::ClauseKind::Trait(bound) = clause.kind().skip_binder() else {
            continue;
        };
        if tcx.fn_trait_kind_from_def_id(bound.def_id()).is_none() {
            continue;
        }
        let ty::Param(generic) = *bound.self_ty().kind() else {
            continue;
        };
        let mut holders = signature.inputs().iter().enumerate().filter(
            |&(_, &input)| matches!(*input.peel_refs().kind(), ty::Param(held) if held == generic),
        );
        let parameter = match (holders.next(), holders.next()) {
            (Some((parameter, _)), None) => Some(parameter),
            _ => None,
        };
        let ty::Tuple(arguments) = *bound.trait_ref.args.type_at(1).kind() else {
            continue;
        };
        for (index, argument) in arguments.iter().enumerate() {
            if !lent(argument) {
                kept.push(KeptArgument { parameter, index, ty: argument });
            }
        }
    }
    kept
}

/// Calls `found` with each argument, by its place, that a callback `ty`
/// holds can keep.
fn callbacks_in<'tcx>(tcx: TyCtxt<'tcx>, ty: Ty<'tcx>, found: &mut dyn FnMut(usize, Ty<'tcx>)) {
    let arguments = |arguments: &[Ty<'tcx>], found: &mut dyn FnMut(usize, Ty<'tcx>)| {
        for (index, &argument) in arguments.iter().enumerate() {
            if !lent(argument) {
                found(index, argument);
            }
        }
    };
    match *ty.kind() {
        ty::FnPtr(signature, _) => arguments(signature.inputs().skip_binder(), found),
        ty::Dynamic(bounds, _) => {
            if let Some(principal) = bounds.principal()
                && tcx.fn_trait_kind_from_def_id(principal.def_id()).is_some()
                && let ty::Tuple(inputs) = *principal.skip_binder().args.type_at(0).kind()
            {
                arguments(inputs, found);
            }
        }
        ty::Adt(_, args) => {
            for arg in args.types() {
                callbacks_in(tcx, arg, found);
            }
        }
        ty::Ref(_, inner, _)
        | ty::RawPtr(inner, _)
        | ty::Array(inner, _)
        | ty::Slice(inner)
        | ty::Pat(inner, _) => callbacks_in(tcx, inner, found),
        ty::Tuple(parts) => {
            for part in parts {
                callbacks_in(tcx, part, found);
            }
        }
        _ => {}
    }
}

/// Whether a callback's argument `ty`, under the callback's binder, is lent
/// for the call: it has a lifetime, and each is the callback's own.
fn lent<'tcx>(ty: Ty<'tcx>) -> bool {
    let mut lifetimes = Lifetimes { depth: ty::INNERMOST, any: false, foreign: false };
    ty.visit_with(&mut lifetimes);
    lifetimes.any && !lifetimes.foreign
}

/// The lifetimes a callback's argument names outside the binders inside it:
/// whether it names any, and whether any is other than the callback's own,
/// which its binder, just outside the argument, binds.
struct Lifetimes {
    /// The callback's binder, as the regions at the current depth name it.
    depth: ty::DebruijnIndex,
    any: bool,
    foreign: bool,
}

impl<'tcx> TypeVisitor<TyCtxt<'tcx>> for Lifetimes {
    fn visit_binder<T: TypeVisitable<TyCtxt<'tcx>>>(&mut self, inner: &ty::Binder<'tcx, T>) {
        self.depth.shift_in(1);
        inner.super_visit_with(self);
        self.depth.shift_out(1);
    }

    fn visit_region(&mut self, region: Region<'tcx>) {
        match region.kind() {
            ty::ReBound(BoundVarIndexKind::Bound(binder), _) if binder < self.depth => {}
            ty::ReBound(BoundVarIndexKind::Bound(binder), _) if binder == self.depth => {
                self.any = true;
            }
            _ => {
                self.any = true;
                self.foreign = true;
            }
        }
    }
}
