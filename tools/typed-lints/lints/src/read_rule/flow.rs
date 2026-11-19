//! What flows from a read's result in one body: the locals that hold it, and
//! the expressions whose value carries it. A `let`, an assignment, an
//! `if let` and a `match` arm bind what their value carries, and so do the
//! parameters of a closure given to a library method called on such a value,
//! as `head.cover.map(|cover| ..)` binds `cover`: to a fixed point over the
//! body and its closures. How a value carries it is the flow's `Spread`.

use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind, HirId, LetStmt, Pat, QPath};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{TyCtxt, TypeckResults};

use super::{is_library, library_method};

/// How a value carries what flows from a source.
#[derive(Clone, Copy)]
pub(super) enum Spread {
    /// The value is the source's own: what a source returned, what a local
    /// holding it holds, what a method called on it returns, what a
    /// function of the standard library or itertools given it returns (`?`
    /// and `for` call one), and what a block, `if`, `match`, reference,
    /// field or index around it yields. What a closure or a workspace
    /// function makes of it carries nothing.
    Value,
    /// The expression names a source or a local holding what one returned,
    /// anywhere outside a closure: a value computed from it.
    Mention,
}

/// The locals of one body that hold what flows from its sources.
pub(super) struct Flow<'s, 'tcx> {
    tcx: TyCtxt<'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    spread: Spread,
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
        spread: Spread,
        source: &'s dyn Fn(&Expr<'tcx>) -> bool,
    ) -> Self {
        let mut flow = Self { tcx, typeck, spread, source, locals: FxHashSet::default() };
        loop {
            let known = flow.locals.len();
            Binder { flow: &mut flow }.visit_expr(body);
            if flow.locals.len() == known {
                return flow;
            }
        }
    }

    /// Whether the expression's value carries what flows from a source.
    pub(super) fn carries(&self, expr: &'tcx Expr<'tcx>) -> bool {
        match self.spread {
            Spread::Value => self.carries_value(expr),
            Spread::Mention => {
                let mut mention = Mention { flow: self, found: false };
                mention.visit_expr(expr);
                mention.found
            }
        }
    }

    /// Whether the expression names a source, or a local holding what flows
    /// from one.
    fn names(&self, expr: &Expr<'tcx>) -> bool {
        (self.source)(expr)
            || matches!(expr.kind, ExprKind::Path(QPath::Resolved(None, path))
                if matches!(path.res, Res::Local(local) if self.locals.contains(&local)))
    }

    fn carries_value(&self, expr: &Expr<'tcx>) -> bool {
        if self.names(expr) {
            return true;
        }
        let carries = |inner| self.carries_value(inner);
        match expr.kind {
            ExprKind::MethodCall(_, receiver, ..) => carries(receiver),
            ExprKind::Call(function, arguments) => {
                let library = match function.kind {
                    ExprKind::Path(ref qpath) => self
                        .typeck
                        .qpath_res(qpath, function.hir_id)
                        .opt_def_id()
                        .is_some_and(|callee| is_library(self.tcx, callee)),
                    _ => false,
                };
                library && arguments.iter().any(carries)
            }
            ExprKind::Match(_, arms, _) => arms.iter().any(|arm| carries(arm.body)),
            ExprKind::If(_, then, otherwise) => carries(then) || otherwise.is_some_and(carries),
            ExprKind::Block(block, _) => block.expr.is_some_and(carries),
            ExprKind::AddrOf(_, _, inner)
            | ExprKind::DropTemps(inner)
            | ExprKind::Field(inner, _)
            | ExprKind::Index(inner, _, _)
            | ExprKind::Unary(_, inner)
            | ExprKind::Cast(inner, _)
            | ExprKind::Type(inner, _) => carries(inner),
            _ => false,
        }
    }

    fn bind(&mut self, pat: &Pat<'_>) {
        pat.each_binding(|_, local, _, _| {
            self.locals.insert(local);
        });
    }
}

/// Finds, in an expression outside its closures, a source or a local that
/// holds what flows from one.
struct Mention<'f, 's, 'tcx> {
    flow: &'f Flow<'s, 'tcx>,
    found: bool,
}

impl<'tcx> Visitor<'tcx> for Mention<'_, '_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if self.found || matches!(expr.kind, ExprKind::Closure(_)) {
            return;
        }
        if self.flow.names(expr) {
            self.found = true;
            return;
        }
        intravisit::walk_expr(self, expr);
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
            ExprKind::MethodCall(_, receiver, arguments, _)
                if library_method(self.flow.tcx, self.flow.typeck, expr).is_some()
                    && self.flow.carries(receiver) =>
            {
                for argument in arguments {
                    if let ExprKind::Closure(closure) = argument.kind {
                        for parameter in self.flow.tcx.hir_body(closure.body).params {
                            self.flow.bind(parameter.pat);
                        }
                    }
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
