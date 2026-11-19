//! Forced reruns: a live query reruns because what it read changed, so
//! nothing counts up to make it run again. `clippy::read_request_bump`
//! finds, in any production code outside the read plumbing, an integer field
//! of a request type increased by one: `x.f += 1`, `x.f = x.f + 1`,
//! `f: y.f + 1` in a literal of that type, and `x.f.wrapping_add(1)` with
//! its `saturating_` and `checked_` forms. A request type is a type of the
//! workspace in the first type argument of one of the policy's
//! `request_query_types`, wherever the workspace's code names one, inside a
//! collection or an `Option` too.

use rustc_ast::LitKind;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def_id::DefId;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{AssignOpKind, BinOpKind, Expr, ExprKind};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{self, Ty, TyCtxt, TypeckResults};

use super::{READ_REQUEST_BUMP, Search};
use crate::finding::Finding;
use crate::workspace_bodies::workspace_bodies;

/// Reports each request bump in the crate's production code outside the
/// plumbing.
pub(super) fn check<'tcx>(
    search: &Search<'_, 'tcx>,
    help: &'static str,
    queries: &FxHashSet<DefId>,
) {
    if queries.is_empty() {
        return;
    }
    let requests = requests(search, queries);
    if requests.is_empty() {
        return;
    }
    let tcx = search.tcx;
    for &body in &search.candidates {
        if tcx.def_kind(body) == rustc_hir::def::DefKind::Closure {
            continue;
        }
        let mut bumps = Bumps {
            tcx,
            help,
            requests: &requests,
            typeck: tcx.typeck(body),
            item: tcx.def_path_str(body),
        };
        bumps.visit_expr(tcx.hir_body_owned_by(body).value);
    }
}

/// The request types: the workspace's types in the first type argument of a
/// request query, in every type the crate's bodies and items name and in the
/// earlier workspace crates' bodies.
fn requests<'tcx>(search: &Search<'_, 'tcx>, queries: &FxHashSet<DefId>) -> FxHashSet<DefId> {
    let tcx = search.tcx;
    let mut requests = FxHashSet::default();
    let mut add = |ty: Ty<'tcx>| {
        for part in ty.walk() {
            if let Some(part) = part.as_type()
                && let ty::Adt(adt, args) = part.kind()
                && queries.contains(&adt.did())
                && let Some(request) = args.types().next()
            {
                for inner in request.walk() {
                    if let Some(inner) = inner.as_type()
                        && let ty::Adt(found, _) = inner.kind()
                        && search.is_workspace(found.did())
                    {
                        requests.insert(found.did());
                    }
                }
            }
        }
    };
    for owner in tcx.hir_body_owners() {
        for (_, &ty) in tcx.typeck(owner).node_types().items_in_stable_order() {
            add(ty);
        }
    }
    for item in tcx.hir_crate_items(()).definitions() {
        if matches!(
            tcx.def_kind(item),
            rustc_hir::def::DefKind::Field | rustc_hir::def::DefKind::TyAlias
        ) {
            add(tcx.type_of(item).instantiate_identity().skip_norm_wip());
        }
    }
    for body in workspace_bodies(search.setup.workspace, tcx, "clippy::read_request_bump") {
        if body.is_local() {
            continue;
        }
        for local in &tcx.optimized_mir(body).local_decls {
            add(local.ty);
        }
    }
    requests
}

struct Bumps<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    help: &'static str,
    requests: &'a FxHashSet<DefId>,
    typeck: &'tcx TypeckResults<'tcx>,
    item: String,
}

impl<'tcx> Bumps<'_, 'tcx> {
    /// The integer field of a request type the expression reads: `x.f`.
    fn request_field(&self, expr: &Expr<'tcx>) -> Option<DefId> {
        let ExprKind::Field(base, _) = expr.kind else { return None };
        let adt = self.typeck.expr_ty_adjusted(base).peel_refs().ty_adt_def()?;
        if !adt.is_struct() || !self.requests.contains(&adt.did()) {
            return None;
        }
        let field = &adt.non_enum_variant().fields[self.typeck.field_index(expr.hir_id)];
        self.typeck.expr_ty(expr).is_integral().then_some(field.did)
    }

    /// The field `x.f` of `x.f + 1` or `1 + x.f`.
    fn plus_one(&self, expr: &Expr<'tcx>) -> Option<DefId> {
        let ExprKind::Binary(op, left, right) = expr.kind else { return None };
        if op.node != BinOpKind::Add {
            return None;
        }
        if is_one(right) {
            self.request_field(left)
        } else if is_one(left) {
            self.request_field(right)
        } else {
            None
        }
    }

    fn report(&self, at: &Expr<'tcx>, field: DefId) {
        self.tcx.emit_node_span_lint(
            READ_REQUEST_BUMP,
            at.hir_id,
            at.span.source_callsite(),
            Finding {
                message: format!(
                    "`{}` adds one to the request's `{}` to run the query again",
                    self.item,
                    self.tcx.item_name(field)
                ),
                help: self.help,
            },
        );
    }
}

/// Whether the expression is the literal 1.
fn is_one(expr: &Expr<'_>) -> bool {
    matches!(expr.kind, ExprKind::Lit(literal) if matches!(literal.node, LitKind::Int(value, _) if value.get() == 1))
}

impl<'tcx> Visitor<'tcx> for Bumps<'_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::AssignOp(op, target, value)
                if op.node == AssignOpKind::AddAssign && is_one(value) =>
            {
                if let Some(field) = self.request_field(target) {
                    self.report(expr, field);
                }
            }
            ExprKind::Assign(target, value, _) => {
                if let Some(field) = self.request_field(target)
                    && self.plus_one(value) == Some(field)
                {
                    self.report(expr, field);
                }
            }
            ExprKind::Struct(_, fields, _) => {
                if let Some(adt) = self.typeck.expr_ty(expr).ty_adt_def()
                    && self.requests.contains(&adt.did())
                {
                    for value in fields {
                        let index = self.typeck.field_index(value.hir_id);
                        let field = adt.non_enum_variant().fields[index].did;
                        if self.plus_one(value.expr) == Some(field) {
                            self.report(value.expr, field);
                        }
                    }
                }
            }
            ExprKind::MethodCall(segment, receiver, [argument], _)
                if matches!(
                    segment.ident.name.as_str(),
                    "wrapping_add" | "saturating_add" | "checked_add"
                ) && is_one(argument) =>
            {
                if let Some(field) = self.request_field(receiver) {
                    self.report(expr, field);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}
