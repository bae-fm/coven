//! `clippy::method_takes_capability` (the policy's
//! `retained-capability-parameters`, Appendix F.2 and F.3): a method takes no
//! construction-only capability and no handle. A capability is fixed when
//! what uses it is built, an owner for the store's life or a task for one
//! operation, and a method uses the one its value holds; a handle flows the
//! same way, into what holds it. So in an impl, only a constructor, a
//! function whose result holds the impl's own type, and a composition root
//! take one, and a free function may.
//!
//! A parameter takes what its type holds (`Owners::walk`), up to the
//! workspace's own types: one that holds a capability or a handle is an owner
//! or a task, which a method may take. A callback's arguments are its
//! caller's to give, so a callback that takes a capability hands the method
//! none, and one that returns a capability does. A generic parameter holds
//! what its bounds' associated types name, as `F: FnOnce() -> StoreDir`
//! gives a store directory. Test code answers to none of this.

use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def_id::DefId;
use rustc_hir::{ImplItem, ImplItemKind};
use rustc_lint::{LateContext, declare_tool_lint};
use rustc_middle::ty::{self, ParamEnv, Ty, TyCtxt};

use crate::finding::Finding;
use crate::owner_rules::{impl_type, is_root};
use crate::owners::{Held, Owners};

declare_tool_lint! {
    /// A function of an impl, no constructor or composition root, that takes
    /// a construction-only capability or a handle.
    pub clippy::METHOD_TAKES_CAPABILITY,
    Warn,
    "a method that is no constructor takes a construction-only capability or a handle",
    report_in_external_macro: true
}

/// Reports each construction-only capability and handle a parameter of
/// `item` takes, when `item` is a function that is no constructor or root.
pub(crate) fn check<'tcx>(owners: &Owners<'tcx>, cx: &LateContext<'tcx>, item: &ImplItem<'tcx>) {
    let ImplItemKind::Fn(signature, _) = item.kind else {
        return;
    };
    let tcx = cx.tcx;
    let callable = item.owner_id.to_def_id();
    if owners.workspace.is_test_code(tcx, item.hir_id()) || is_root(owners, tcx, callable) {
        return;
    }
    let declared = tcx.fn_sig(callable).instantiate_identity().skip_norm_wip().skip_binder();
    if let Some(own) = impl_type(tcx, callable) {
        let mut constructs = false;
        owners.walk(declared.output(), false, false, &mut |held| {
            constructs |= matches!(held, Held::Part(part) if part.id == own);
        });
        if constructs {
            return;
        }
    }
    let rows = &owners.rows;
    let bounds = tcx.param_env(callable);
    let method = tcx.opt_associated_item(callable).is_some_and(|item| item.is_method());
    for (index, (&input, written)) in
        declared.inputs().iter().zip(signature.decl.inputs).enumerate()
    {
        if method && index == 0 {
            continue;
        }
        let mut taken = Vec::new();
        parameter_holds(owners, tcx, bounds, input, &mut |id| {
            let kind = if rows.handles.contains_key(&id) {
                "handle"
            } else {
                "construction-only capability"
            };
            if !taken.iter().any(|&(seen, _)| seen == id) {
                taken.push((id, kind));
            }
        });
        for (id, kind) in taken {
            tcx.emit_node_span_lint(
                METHOD_TAKES_CAPABILITY,
                item.hir_id(),
                written.span.source_callsite(),
                Finding {
                    message: format!(
                        "`{}` takes {kind} `{}`, and is no constructor",
                        tcx.def_path_str(callable),
                        tcx.def_path_str(id)
                    ),
                    help: "build the value with the capability or handle it uses, or let a free \
                           function take it",
                },
            );
        }
    }
}

/// Each construction-only capability and handle a parameter of type `ty`
/// holds outside the workspace's types, with the associated types its bounds
/// give a generic parameter.
fn parameter_holds<'tcx>(
    owners: &Owners<'tcx>,
    tcx: TyCtxt<'tcx>,
    bounds: ParamEnv<'tcx>,
    ty: Ty<'tcx>,
    found: &mut dyn FnMut(DefId),
) {
    let rows = &owners.rows;
    let mut pending = vec![ty];
    let mut parameters = FxHashSet::default();
    while let Some(ty) = pending.pop() {
        let inside = |id| !owners.is_workspace_item(id);
        owners.walk_outside(ty, &inside, &mut |held| match held {
            Held::Part(part) => {
                if rows.construction_only.holds(tcx, &part) || rows.handles.contains_key(&part.id) {
                    found(part.id);
                }
            }
            Held::Parameter(index) => {
                if !parameters.insert(index) {
                    return;
                }
                for clause in bounds.caller_bounds() {
                    if let ty::ClauseKind::Projection(projection) = clause.kind().skip_binder()
                        && let ty::Param(parameter) = projection.projection_term.self_ty().kind()
                        && parameter.index as usize == index
                        && let Some(given) = projection.term.as_type()
                    {
                        pending.push(given);
                    }
                }
            }
        });
    }
}
