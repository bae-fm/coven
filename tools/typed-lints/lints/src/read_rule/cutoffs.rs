//! Cutoffs between read paths: a read has one path, so it compares nothing
//! against a limit to pick between two. `clippy::read_cutoff` finds, in read
//! code, a relational comparison (`<`, `<=`, `>`, `>=`) with an operand that
//! names an integer constant of the workspace, holds an integer literal of 2
//! or more, or calls a function of the workspace whose body names an
//! integer constant of the workspace. Comparing with 0 or 1, as emptiness
//! is, is none.

use rustc_ast::LitKind;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{BinOpKind, Expr, ExprKind};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::mir::visit::Visitor as MirVisitor;
use rustc_middle::mir::{self, Location};
use rustc_middle::ty::{TyCtxt, TypeckResults};

use super::records::Reporter;
use super::{READ_CUTOFF, Search, callee};

/// Reports the cutoffs in the read code `unit`, with the closures inside it,
/// but those only a live query's `process` runs.
pub(super) fn check<'tcx>(
    search: &Search<'_, 'tcx>,
    unit: LocalDefId,
    process_only: &FxHashSet<LocalDefId>,
    reporter: &Reporter<'_, 'tcx>,
) {
    let tcx = search.tcx;
    let mut comparisons = Comparisons {
        search,
        reporter,
        typeck: tcx.typeck(unit),
        process_only,
        item: tcx.def_path_str(tcx.typeck_root_def_id(unit.to_def_id())),
    };
    comparisons.visit_expr(tcx.hir_body_owned_by(unit).value);
}

struct Comparisons<'a, 's, 'tcx> {
    search: &'a Search<'s, 'tcx>,
    reporter: &'a Reporter<'a, 'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    process_only: &'a FxHashSet<LocalDefId>,
    item: String,
}

impl<'tcx> Comparisons<'_, '_, 'tcx> {
    /// Whether `id` is an integer constant of the workspace.
    fn is_constant(&self, id: DefId) -> bool {
        let tcx = self.search.tcx;
        matches!(tcx.def_kind(id), DefKind::Const | DefKind::AssocConst)
            && self.search.is_workspace(id)
            && tcx.type_of(id).instantiate_identity().skip_norm_wip().is_integral()
    }

    /// The first constant, literal or call of a function naming a constant
    /// that fixes an operand, as a finding names it.
    fn fixed(&self, operand: &'tcx Expr<'tcx>) -> Option<String> {
        let mut fixed = Fixed { comparisons: self, found: None };
        fixed.visit_expr(operand);
        fixed.found
    }

    /// The first integer constant of the workspace the function `id`'s body
    /// names.
    fn constant_in(&self, id: DefId) -> Option<DefId> {
        let tcx = self.search.tcx;
        if !self.search.is_workspace(id)
            || !matches!(tcx.def_kind(id), DefKind::Fn | DefKind::AssocFn)
            || !tcx.is_mir_available(id)
        {
            return None;
        }
        let mut constants = MirConstants { comparisons: self, found: None };
        constants.visit_body(tcx.optimized_mir(id));
        constants.found
    }
}

/// Finds what fixes an operand.
struct Fixed<'a, 'c, 's, 'tcx> {
    comparisons: &'a Comparisons<'c, 's, 'tcx>,
    found: Option<String>,
}

impl<'tcx> Visitor<'tcx> for Fixed<'_, '_, '_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if self.found.is_some() {
            return;
        }
        let comparisons = self.comparisons;
        let tcx = comparisons.search.tcx;
        let typeck = comparisons.typeck;
        match expr.kind {
            ExprKind::Path(ref qpath) => {
                if let Res::Def(_, id) = typeck.qpath_res(qpath, expr.hir_id)
                    && comparisons.is_constant(id)
                {
                    self.found = Some(format!("the constant `{}`", tcx.def_path_str(id)));
                }
            }
            ExprKind::Lit(literal) => {
                if let LitKind::Int(value, _) = literal.node
                    && value.get() >= 2
                {
                    self.found = Some(format!("the literal {}", value.get()));
                }
            }
            ExprKind::Call(..) | ExprKind::MethodCall(..) => {
                if let Some(callee) = callee(typeck, expr)
                    && let Some(constant) = comparisons.constant_in(callee)
                {
                    self.found = Some(format!(
                        "the constant `{}` in `{}`",
                        tcx.def_path_str(constant),
                        tcx.def_path_str(callee)
                    ));
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}

/// Finds a named integer constant of the workspace in a function's MIR.
struct MirConstants<'a, 'c, 's, 'tcx> {
    comparisons: &'a Comparisons<'c, 's, 'tcx>,
    found: Option<DefId>,
}

impl<'tcx> MirVisitor<'tcx> for MirConstants<'_, '_, '_, 'tcx> {
    fn visit_const_operand(&mut self, constant: &mir::ConstOperand<'tcx>, _: Location) {
        if self.found.is_none()
            && let mir::Const::Unevaluated(unevaluated, _) = constant.const_
            && self.comparisons.is_constant(unevaluated.def)
        {
            self.found = Some(unevaluated.def);
        }
    }
}

impl<'tcx> Visitor<'tcx> for Comparisons<'_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.search.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::Closure(closure) if self.process_only.contains(&closure.def_id) => return,
            ExprKind::Binary(op, left, right) => {
                let operator = match op.node {
                    BinOpKind::Lt => Some("<"),
                    BinOpKind::Le => Some("<="),
                    BinOpKind::Gt => Some(">"),
                    BinOpKind::Ge => Some(">="),
                    _ => None,
                };
                if let Some(operator) = operator
                    && let Some(fixed) = self.fixed(left).or_else(|| self.fixed(right))
                {
                    self.reporter.report(
                        READ_CUTOFF,
                        expr.hir_id,
                        expr.span,
                        format!("`{}` compares with `{operator}` against {fixed}", self.item),
                    );
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}
