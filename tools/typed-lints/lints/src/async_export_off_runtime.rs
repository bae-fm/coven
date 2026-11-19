//! `clippy::async_export_off_runtime`: an async function that uniffi exports
//! and that runs its work on the executor of the language that called it.
//!
//! uniffi polls an exported async function's future on the caller's own
//! executor: Swift's or Kotlin's, which runs no tokio. Work that needs the
//! runtime its owner owns, its I/O and its timers, runs there only when the
//! function hands the work to that runtime, and a call the caller cancels
//! stops it only when the function's way of running it says so. The policy's
//! `async_exports.runs` lists the functions that do both
//! (`owner_construction_policy::AsyncExports`), and each async export calls
//! one of them, by the definition the call resolves to, whatever the value it
//! is called on is named.
//!
//! The exported async functions are those that uniffi's scaffolding calls
//! inside the future it hands to `uniffi::rust_future_new`, the one way uniffi
//! runs a Rust future for a foreign caller: methods, constructors and free
//! functions alike.

use std::ops::ControlFlow;

use clippy_utils::paths::{PathNS, lookup_path_str};
use clippy_utils::visitors::for_each_expr;
use rustc_data_structures::fx::{FxHashSet, FxIndexSet};
use rustc_hir::def::DefKind;
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass, declare_tool_lint, impl_lint_pass};
use rustc_middle::ty::{TyCtxt, TypeckResults};

use crate::finding::Finding;
use crate::workspace_policy::{WorkspacePolicy, resolve_row};

declare_tool_lint! {
    /// An async function that uniffi exports and that calls none of the
    /// policy's `async_exports.runs`.
    pub clippy::ASYNC_EXPORT_OFF_RUNTIME,
    Warn,
    "an async function uniffi exports runs its work on its caller's executor",
    report_in_external_macro: true
}

/// The lint's pass: the functions that run work on an owned runtime, and the
/// exported async functions it finds in the crate.
pub(crate) struct AsyncExportOffRuntime {
    runs: FxHashSet<DefId>,
    rust_future_new: Vec<DefId>,
    exports: FxIndexSet<LocalDefId>,
}

impl_lint_pass!(AsyncExportOffRuntime => [ASYNC_EXPORT_OFF_RUNTIME]);

impl AsyncExportOffRuntime {
    pub(crate) fn new(tcx: TyCtxt<'_>, workspace: Option<&WorkspacePolicy>) -> Self {
        let runs = workspace
            .and_then(|workspace| workspace.policy.async_exports.as_ref())
            .map(|exports| {
                exports
                    .runs
                    .iter()
                    .flat_map(|run| resolve_row(tcx, "async_exports.runs", run, PathNS::Value))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            runs,
            rust_future_new: lookup_path_str(tcx, PathNS::Value, "uniffi::rust_future_new"),
            exports: FxIndexSet::default(),
        }
    }
}

/// The function a call, a method call or a path names, by its definition.
fn named_function(typeck: &TypeckResults<'_>, expr: &Expr<'_>) -> Option<DefId> {
    match expr.kind {
        ExprKind::Path(ref qpath) => typeck.qpath_res(qpath, expr.hir_id).opt_def_id(),
        ExprKind::MethodCall(..) => typeck.type_dependent_def_id(expr.hir_id),
        _ => None,
    }
}

impl<'tcx> LateLintPass<'tcx> for AsyncExportOffRuntime {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if self.runs.is_empty() {
            return;
        }
        let ExprKind::Call(callee, [future, ..]) = expr.kind else {
            return;
        };
        let typeck = cx.typeck_results();
        if !named_function(typeck, callee).is_some_and(|id| self.rust_future_new.contains(&id)) {
            return;
        }
        for_each_expr(cx.tcx, future, |inner| {
            if let Some(id) = named_function(typeck, inner)
                && let Some(local) = id.as_local()
                && matches!(cx.tcx.def_kind(id), DefKind::Fn | DefKind::AssocFn)
                && cx.tcx.asyncness(id).is_async()
            {
                self.exports.insert(local);
            }
            ControlFlow::<()>::Continue(())
        });
    }

    fn check_crate_post(&mut self, cx: &LateContext<'tcx>) {
        for &export in &self.exports {
            // A trait's method has no body of its own: nothing shows what it
            // runs, so it runs on no owned runtime.
            let runs_on_owned_runtime =
                cx.tcx.hir_maybe_body_owned_by(export).is_some_and(|body| {
                    let typeck = cx.tcx.typeck(export);
                    for_each_expr(cx.tcx, body.value, |inner| match named_function(typeck, inner) {
                        Some(id) if self.runs.contains(&id) => ControlFlow::Break(()),
                        _ => ControlFlow::Continue(()),
                    })
                    .is_some()
                });
            if !runs_on_owned_runtime {
                cx.tcx.emit_node_span_lint(
                    ASYNC_EXPORT_OFF_RUNTIME,
                    cx.tcx.local_def_id_to_hir_id(export),
                    cx.tcx.def_span(export),
                    Finding {
                        message: format!(
                            "`{}` is an async export whose body calls none of the policy's \
                             async_exports.runs",
                            cx.tcx.def_path_str(export)
                        ),
                        help: "run the work through one of the functions the policy's \
                               async_exports.runs lists, which run it on a runtime its owner owns",
                    },
                );
            }
        }
    }
}
