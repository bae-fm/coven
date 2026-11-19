//! What flows from a read's result in one body: the locals that hold it, and
//! the expressions whose value carries it. A value carries what an
//! expression of `source` returned, what a local holding it holds, what a
//! method called on it returns, what a function of the standard library or
//! itertools given it returns (`?` and `for` call one), and what a block,
//! `if`, `match`, reference, field or index around it yields. A `let`, an
//! assignment, an `if let` and a `match` arm bind what their value carries.
//! What a closure or a workspace function makes of it carries nothing.

use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind, HirId, LetStmt, Pat, QPath};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{TyCtxt, TypeckResults};

use super::is_library;

/// The locals of one body that hold what flows from its sources.
pub(super) struct Flow<'s, 'tcx> {
    tcx: TyCtxt<'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    /// Whether an expression's value is a source.
    source: &'s dyn Fn(&Expr<'tcx>) -> bool,
    locals: FxHashSet<HirId>,
}

impl<'s, 'tcx> Flow<'s, 'tcx> {
    /// What flows from `source` in `body`, its closures included: every
    /// binding, to a fixed point, so a loop's later assignment counts.
    pub(super) fn of(
        tcx: TyCtxt<'tcx>,
        typeck: &'tcx TypeckResults<'tcx>,
        body: &'tcx Expr<'tcx>,
        source: &'s dyn Fn(&Expr<'tcx>) -> bool,
    ) -> Self {
        let mut flow = Self { tcx, typeck, source, locals: FxHashSet::default() };
        loop {
            let known = flow.locals.len();
            Binder { flow: &mut flow }.visit_expr(body);
            if flow.locals.len() == known {
                return flow;
            }
        }
    }

    /// Whether the expression's value carries what flows from a source.
    pub(super) fn carries(&self, expr: &Expr<'tcx>) -> bool {
        if (self.source)(expr) {
            return true;
        }
        match expr.kind {
            ExprKind::Path(QPath::Resolved(None, path)) => {
                matches!(path.res, Res::Local(local) if self.locals.contains(&local))
            }
            ExprKind::MethodCall(_, receiver, ..) => self.carries(receiver),
            ExprKind::Call(function, arguments) => {
                let library = match function.kind {
                    ExprKind::Path(ref qpath) => self
                        .typeck
                        .qpath_res(qpath, function.hir_id)
                        .opt_def_id()
                        .is_some_and(|callee| is_library(self.tcx, callee)),
                    _ => false,
                };
                library && arguments.iter().any(|argument| self.carries(argument))
            }
            ExprKind::Match(_, arms, _) => arms.iter().any(|arm| self.carries(arm.body)),
            ExprKind::If(_, then, otherwise) => {
                self.carries(then) || otherwise.is_some_and(|otherwise| self.carries(otherwise))
            }
            ExprKind::Block(block, _) => block.expr.is_some_and(|value| self.carries(value)),
            ExprKind::AddrOf(_, _, inner)
            | ExprKind::DropTemps(inner)
            | ExprKind::Field(inner, _)
            | ExprKind::Index(inner, _, _)
            | ExprKind::Unary(_, inner)
            | ExprKind::Cast(inner, _)
            | ExprKind::Type(inner, _) => self.carries(inner),
            _ => false,
        }
    }

    fn bind(&mut self, pat: &Pat<'_>) {
        pat.each_binding(|_, local, _, _| {
            self.locals.insert(local);
        });
    }
}

/// Binds what each `let`, assignment, `if let` and `match` arm takes from a
/// value that carries a source.
struct Binder<'f, 's, 'tcx> {
    flow: &'f mut Flow<'s, 'tcx>,
}

impl<'tcx> Visitor<'tcx> for Binder<'_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.flow.tcx
    }

    fn visit_local(&mut self, local: &'tcx LetStmt<'tcx>) {
        if local.init.is_some_and(|init| self.flow.carries(init)) {
            self.flow.bind(local.pat);
        }
        intravisit::walk_local(self, local);
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::Let(binding) if self.flow.carries(binding.init) => {
                self.flow.bind(binding.pat);
            }
            ExprKind::Match(scrutinee, arms, _) if self.flow.carries(scrutinee) => {
                for arm in arms {
                    self.flow.bind(arm.pat);
                }
            }
            ExprKind::Assign(target, value, _) if self.flow.carries(value) => {
                if let ExprKind::Path(QPath::Resolved(None, path)) = target.kind
                    && let Res::Local(local) = path.res
                {
                    self.flow.locals.insert(local);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}
