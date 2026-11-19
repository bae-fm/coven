//! The statements read code runs, and two shapes of them. A statement is a
//! call of a function that takes a read value (an input names one of the
//! policy's read types, a live query aside) and returns none: a read
//! type's method that hands out another read value runs no SQL.
//!
//! - `query-per-row`: a statement in a loop's body, or in a closure that runs
//!   once for each item an iterator yields (an argument of a library method
//!   called on an iterator) or for each row a statement reads (its row
//!   mapper);
//! - `keyed-statement`: any other statement given, outside a closure, a
//!   value an earlier statement returned, or a statement's own call: one
//!   statement would read both. A closure argument maps the rows a
//!   statement reads and does not choose them, and a read value is the
//!   handle a statement runs on, never its key.

use clippy_utils::ty::implements_trait_with_env;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{ParamEnv, TyCtxt, TypeckResults, TypingEnv};
use rustc_span::sym;

use super::flow::{Flow, Spread};
use super::rows::Mappers;
use super::{READ_KEYED_STATEMENT, READ_QUERY_PER_ROW, Search, callback, library_method};
use crate::finding::Finding;

/// Reports the statements of the read code `unit` that run once for each
/// row or are keyed by what an earlier one returned, with the closures
/// inside it, but those only a live query's `process` runs.
pub(super) fn check<'tcx>(
    search: &Search<'_, 'tcx>,
    help: &'static str,
    mappers: &Mappers<'_, 'tcx>,
    unit: LocalDefId,
    process_only: &FxHashSet<LocalDefId>,
) {
    let tcx = search.tcx;
    let typeck = tcx.typeck(unit);
    let root = tcx.typeck_root_def_id(unit.to_def_id());
    let body = tcx.hir_body_owned_by(unit).value;
    let kinds = Kinds { search, typeck, bounds: tcx.param_env(root) };
    let is_statement = |expr: &Expr<'tcx>| kinds.statement(expr).is_some();
    let returned = Flow::of(tcx, typeck, body, Spread::Mention, &is_statement);
    let mut statements = Statements {
        tcx,
        help,
        kinds: &kinds,
        mappers,
        returned: &returned,
        typing_env: TypingEnv::post_analysis(tcx, root),
        process_only,
        item: tcx.def_path_str(root),
        per_row: 0,
    };
    statements.visit_expr(body);
}

/// What tells a statement from other calls in one body.
struct Kinds<'a, 's, 'tcx> {
    search: &'a Search<'s, 'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    /// The bounds of the generics the body sees.
    bounds: ParamEnv<'tcx>,
}

impl<'tcx> Kinds<'_, '_, 'tcx> {
    /// The function a call runs, when it is a statement.
    fn statement(&self, expr: &Expr<'tcx>) -> Option<DefId> {
        let tcx = self.search.tcx;
        let callee = match expr.kind {
            ExprKind::MethodCall(..) => self.typeck.type_dependent_def_id(expr.hir_id)?,
            ExprKind::Call(function, _) => match function.kind {
                ExprKind::Path(ref qpath) => match self.typeck.qpath_res(qpath, function.hir_id) {
                    Res::Def(DefKind::Fn | DefKind::AssocFn, callee) => callee,
                    _ => return None,
                },
                _ => return None,
            },
            _ => return None,
        };
        let bounds = tcx.param_env(callee);
        let takes_read = tcx
            .fn_sig(callee)
            .instantiate_identity()
            .skip_norm_wip()
            .skip_binder()
            .inputs()
            .iter()
            .any(|&input| self.search.names_read(input, bounds).0);
        (takes_read && !self.names_read(self.typeck.expr_ty(expr))).then_some(callee)
    }

    /// Whether a value of the type is or holds a read value or a live query.
    fn names_read(&self, ty: rustc_middle::ty::Ty<'tcx>) -> bool {
        self.search.names_read_value(ty, self.bounds)
    }
}

struct Statements<'a, 's, 'tcx> {
    tcx: TyCtxt<'tcx>,
    /// The help every finding gives: the rule, and what to do instead.
    help: &'static str,
    kinds: &'a Kinds<'a, 's, 'tcx>,
    mappers: &'a Mappers<'a, 'tcx>,
    /// What the body's statements returned.
    returned: &'a Flow<'a, 'tcx>,
    typing_env: TypingEnv<'tcx>,
    process_only: &'a FxHashSet<LocalDefId>,
    /// How findings name the unit: its function's path.
    item: String,
    /// How many loop bodies and closures that run once for each row enclose
    /// the code.
    per_row: usize,
}

impl<'tcx> Statements<'_, '_, 'tcx> {
    /// Reports the statement `expr`, which calls `callee`, when it runs once
    /// for each row or a value an earlier statement returned keys it.
    fn statement(&self, expr: &Expr<'tcx>, callee: DefId, given: &[&'tcx Expr<'tcx>]) {
        let name = self.tcx.item_name(callee);
        let (lint, what) = if self.per_row > 0 {
            (READ_QUERY_PER_ROW, format!("runs `{name}`, which runs SQL, once for each row"))
        } else if given.iter().any(|value| {
            !self.kinds.names_read(self.kinds.typeck.expr_ty(value)) && self.returned.carries(value)
        }) {
            (
                READ_KEYED_STATEMENT,
                format!(
                    "runs `{name}`, which runs SQL, keyed by what an earlier statement returned"
                ),
            )
        } else {
            return;
        };
        self.tcx.emit_node_span_lint(
            lint,
            expr.hir_id,
            expr.span.source_callsite(),
            Finding { message: format!("`{}` {what}", self.item), help: self.help },
        );
    }

    /// Whether an argument of the call `expr` is code that runs once for each
    /// row: a closure or function a library method of an iterator runs on
    /// each item, or a statement's row mapper.
    fn runs_per_row(&self, expr: &Expr<'tcx>, argument: &Expr<'tcx>) -> bool {
        let Some(code) = callback(self.kinds.typeck, argument) else { return false };
        if self.mappers.made(code).is_some() {
            return true;
        }
        let ExprKind::MethodCall(_, receiver, ..) = expr.kind else { return false };
        let tcx = self.tcx;
        library_method(tcx, self.kinds.typeck, expr).is_some()
            && tcx.get_diagnostic_item(sym::Iterator).is_some_and(|iterator| {
                let ty =
                    tcx.erase_and_anonymize_regions(self.kinds.typeck.expr_ty_adjusted(receiver));
                implements_trait_with_env(tcx, self.typing_env, ty, iterator, None, &[])
            })
    }

    /// Visits the call's arguments, each closure that runs once for each row
    /// as such.
    fn visit_arguments(&mut self, expr: &'tcx Expr<'tcx>, arguments: &'tcx [Expr<'tcx>]) {
        for argument in arguments {
            let per_row = usize::from(self.runs_per_row(expr, argument));
            self.per_row += per_row;
            self.visit_expr(argument);
            self.per_row -= per_row;
        }
    }
}

impl<'tcx> Visitor<'tcx> for Statements<'_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::Closure(closure) if self.process_only.contains(&closure.def_id) => {}
            ExprKind::Loop(..) => {
                self.per_row += 1;
                intravisit::walk_expr(self, expr);
                self.per_row -= 1;
            }
            ExprKind::MethodCall(_, receiver, arguments, _) => {
                if let Some(callee) = self.kinds.statement(expr) {
                    let given = std::iter::once(receiver).chain(arguments).collect::<Vec<_>>();
                    self.statement(expr, callee, &given);
                }
                self.visit_expr(receiver);
                self.visit_arguments(expr, arguments);
            }
            ExprKind::Call(function, arguments) => {
                if let Some(callee) = self.kinds.statement(expr) {
                    self.statement(expr, callee, &arguments.iter().collect::<Vec<_>>());
                }
                self.visit_expr(function);
                self.visit_arguments(expr, arguments);
            }
            _ => intravisit::walk_expr(self, expr),
        }
    }
}
