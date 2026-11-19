//! Memory kept between reads: a read starts from the database every time.
//! `clippy::read_kept_state` finds four forms:
//!
//! - a type of the crate that holds a live query or a value stream and is
//!   neither itself (a field whose type, at any depth through the
//!   workspace's types, is one of the policy's `live_query_types` or
//!   `value_stream_types`) has another field whose type retains one of
//!   `kept_state_types`: the finding is that field;
//! - such a type writes one of those other fields in a method that takes
//!   `self`: an assignment to a place in it, a compound assignment, or a call
//!   of a method that takes it by `&mut`;
//! - read code that outlives the call that made it (a closure passed to a
//!   live query, or a closure that is read code in a function that is not)
//!   captures a variable of the function around it, or a place in `self`,
//!   and changes it or holds kept state through it; and read code names a
//!   `static` whose type retains kept state;
//! - a method of read code whose `Self` type holds no live query or value
//!   stream and retains kept state.
//!
//! A type retains what its type arguments, references, slices, arrays and
//! tuples hold, and what the workspace's types hold in their fields, but
//! nothing inside a live query or a value stream: their state is how the
//! query or stream delivers its values.

use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind, HirId, QPath};
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::kw;
use std::cell::RefCell;

use super::{READ_KEPT_STATE, ReadCode, Search};
use crate::finding::Finding;
use crate::owner_rules::impl_type;
use crate::owners::TypeSet;

/// The kept-state rows, resolved, and what the types of the workspace
/// retain, as found so far.
pub(super) struct Kept<'a, 's, 'tcx> {
    search: &'a Search<'s, 'tcx>,
    help: &'static str,
    kept: &'a TypeSet<'tcx>,
    streams: &'a FxHashSet<DefId>,
    /// What each type retains: the first kept-state type, by name.
    retained: RefCell<FxHashMap<Ty<'tcx>, Option<String>>>,
    /// The types found so far to hold a live query or a value stream.
    holders: RefCell<FxHashMap<DefId, bool>>,
}

pub(super) fn check<'tcx>(
    search: &Search<'_, 'tcx>,
    help: &'static str,
    kept: &TypeSet<'tcx>,
    streams: &FxHashSet<DefId>,
    code: &ReadCode,
) {
    let kept = Kept {
        search,
        help,
        kept,
        streams,
        retained: RefCell::default(),
        holders: RefCell::default(),
    };
    kept.holders();
    kept.read_code(code);
}

impl<'tcx> Kept<'_, '_, 'tcx> {
    fn tcx(&self) -> TyCtxt<'tcx> {
        self.search.tcx
    }

    fn report(&self, id: HirId, span: rustc_span::Span, message: String) {
        self.tcx().emit_node_span_lint(
            READ_KEPT_STATE,
            id,
            span.source_callsite(),
            Finding { message, help: self.help },
        );
    }

    /// Whether `adt` is a live query or a value stream.
    fn is_request(&self, adt: DefId) -> bool {
        self.search.setup.live_types.contains(&adt) || self.streams.contains(&adt)
    }

    /// Whether a value of `ty` is or holds a live query or a value stream.
    fn holds_request(&self, ty: Ty<'tcx>) -> bool {
        super::names_adt(ty, &|adt| self.is_request(adt) || self.adt_holds_request(adt))
    }

    fn adt_holds_request(&self, adt: DefId) -> bool {
        self.search.holds(adt, &|id| self.is_request(id), &self.holders)
    }

    /// The first kept-state type a value of `ty` retains, by name.
    fn retains(&self, ty: Ty<'tcx>) -> Option<String> {
        let tcx = self.tcx();
        let ty = tcx.erase_and_anonymize_regions(ty);
        if let Some(known) = self.retained.borrow().get(&ty) {
            return known.clone();
        }
        self.retained.borrow_mut().insert(ty, None);
        let found = match ty.kind() {
            ty::Adt(adt, args) if self.kept.has(tcx, adt.did(), Some(args)) => {
                Some(tcx.item_name(adt.did()).to_string())
            }
            ty::Adt(adt, _) if self.is_request(adt.did()) => None,
            ty::Adt(adt, args) => {
                args.types().find_map(|inner| self.retains(inner)).or_else(|| {
                    (self.search.is_workspace(adt.did()))
                        .then(|| {
                            adt.all_fields()
                                .find_map(|field| self.retains(field.ty(tcx, args).skip_norm_wip()))
                        })
                        .flatten()
                })
            }
            ty::Ref(_, inner, _)
            | ty::RawPtr(inner, _)
            | ty::Slice(inner)
            | ty::Array(inner, _) => self.retains(*inner),
            ty::Tuple(items) => items.iter().find_map(|inner| self.retains(inner)),
            _ => None,
        };
        self.retained.borrow_mut().insert(ty, found.clone());
        found
    }

    /// The first form: each holder's fields beside its live query that keep
    /// state, and the second: its methods that write them.
    fn holders(&self) {
        let tcx = self.tcx();
        for item in tcx.hir_crate_items(()).definitions() {
            if !matches!(tcx.def_kind(item), DefKind::Struct | DefKind::Enum | DefKind::Union)
                || !self.search.is_production(item)
                || self.is_request(item.to_def_id())
                || !self.adt_holds_request(item.to_def_id())
            {
                continue;
            }
            let adt = tcx.adt_def(item);
            let identity = ty::GenericArgs::identity_for_item(tcx, item);
            let holds = if self.search.holds_live(item.to_def_id()) {
                "live query"
            } else {
                "value stream"
            };
            let mut others = FxHashSet::default();
            for field in adt.all_fields() {
                let ty = field.ty(tcx, identity).skip_norm_wip();
                if self.holds_request(ty) {
                    continue;
                }
                others.insert(field.did);
                if let Some(kept) = self.retains(ty) {
                    let local = field.did.expect_local();
                    self.report(
                        tcx.local_def_id_to_hir_id(local),
                        tcx.def_span(local),
                        format!(
                            "`{}` keeps `{}`, which holds `{kept}` state, beside its {holds}",
                            tcx.def_path_str(item),
                            field.name
                        ),
                    );
                }
            }
            for &implementation in tcx.inherent_impls(item.to_def_id()) {
                for &method in tcx.associated_item_def_ids(implementation) {
                    let Some(method) = method.as_local() else { continue };
                    if tcx.opt_associated_item(method.to_def_id()).is_some_and(|it| it.is_method())
                        && self.search.candidates.contains(&method)
                    {
                        self.writes(method, &others);
                    }
                }
            }
        }
    }

    /// Reports each write of a field of `others` in the method `method`.
    fn writes(&self, method: LocalDefId, others: &FxHashSet<DefId>) {
        let tcx = self.tcx();
        let body = tcx.hir_body_owned_by(method);
        let mut writes = Writes {
            kept: self,
            typeck: tcx.typeck(method),
            others,
            item: tcx.def_path_str(method),
        };
        writes.visit_expr(body.value);
    }

    /// The third form and the fourth.
    fn read_code(&self, code: &ReadCode) {
        let tcx = self.tcx();
        for &unit in &code.top {
            if tcx.def_kind(unit) == DefKind::Closure {
                self.captures(unit);
            }
            if tcx.opt_associated_item(unit.to_def_id()).is_some_and(|item| item.is_method())
                && let Some(owner) = impl_type(tcx, unit.to_def_id())
                && !self.adt_holds_request(owner)
            {
                let self_ty = tcx.type_of(owner).instantiate_identity().skip_norm_wip();
                if let Some(kept) = self.retains(self_ty)
                    && let Some(receiver) = tcx.hir_body_owned_by(unit).params.first()
                {
                    self.report(
                        tcx.local_def_id_to_hir_id(unit),
                        receiver.span,
                        format!(
                            "`{}` reads through `{}`, which keeps `{kept}` state",
                            tcx.def_path_str(unit),
                            tcx.def_path_str(owner)
                        ),
                    );
                }
            }
            let mut statics = Statics {
                kept: self,
                typeck: tcx.typeck(unit),
                item: tcx.def_path_str(tcx.typeck_root_def_id(unit.to_def_id())),
                seen: FxHashSet::default(),
            };
            statics.visit_expr(tcx.hir_body_owned_by(unit).value);
        }
        for &closure in &code.kept_by_live_queries {
            self.captures(closure);
        }
    }

    /// Reports what the closure `closure`, which outlives the call that made
    /// it, captures and changes, or captures with kept state.
    fn captures(&self, closure: LocalDefId) {
        let tcx = self.tcx();
        let item = tcx.def_path_str(tcx.typeck_root_def_id(closure.to_def_id()));
        for capture in tcx.closure_captures(closure) {
            let name = capture.to_string(tcx);
            let span = capture.get_path_span(tcx);
            let id = tcx.local_def_id_to_hir_id(closure);
            if capture.mutability.is_mut() {
                self.report(id, span, format!("`{item}` changes the captured `{name}`"));
            } else if let Some(kept) = self.retains(capture.place.ty()) {
                self.report(
                    id,
                    span,
                    format!("`{item}` captures `{name}`, which keeps `{kept}` state"),
                );
            }
        }
    }
}

/// The writes of a holder's fields beside its live query in one method.
struct Writes<'a, 'k, 's, 'tcx> {
    kept: &'a Kept<'k, 's, 'tcx>,
    typeck: &'tcx ty::TypeckResults<'tcx>,
    others: &'a FxHashSet<DefId>,
    item: String,
}

impl<'tcx> Writes<'_, '_, '_, 'tcx> {
    /// The field of `self` a place expression is in: `self.f`, `self.f.x`,
    /// `*self.f`, `self.f[i]`. `self` goes by its name, since an `async fn`
    /// binds it again inside the body it returns.
    fn self_field(&self, expr: &Expr<'tcx>) -> Option<DefId> {
        match expr.kind {
            ExprKind::Field(base, _) => {
                let on_self = matches!(base.kind, ExprKind::Path(QPath::Resolved(None, path))
                    if matches!(path.res, Res::Local(local) if self.kept.tcx().hir_name(local) == kw::SelfLower));
                if on_self {
                    let index = self.typeck.field_index(expr.hir_id);
                    let adt = self.typeck.expr_ty_adjusted(base).peel_refs().ty_adt_def()?;
                    Some(adt.non_enum_variant().fields[index].did)
                } else {
                    self.self_field(base)
                }
            }
            ExprKind::Unary(_, inner)
            | ExprKind::Index(inner, _, _)
            | ExprKind::AddrOf(_, _, inner) => self.self_field(inner),
            _ => None,
        }
    }

    fn write(&self, target: &Expr<'tcx>, at: &Expr<'tcx>) {
        if let Some(field) = self.self_field(target)
            && self.others.contains(&field)
        {
            let tcx = self.kept.tcx();
            self.kept.report(
                at.hir_id,
                at.span,
                format!(
                    "`{}` writes `self.{}` after construction",
                    self.item,
                    tcx.item_name(field)
                ),
            );
        }
    }
}

impl<'tcx> Visitor<'tcx> for Writes<'_, '_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.kept.tcx()
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::Assign(target, ..) | ExprKind::AssignOp(_, target, _) => {
                self.write(target, expr)
            }
            ExprKind::MethodCall(_, receiver, ..) => {
                let takes_mut = self.typeck.type_dependent_def_id(expr.hir_id).is_some_and(|callee| {
                    let tcx = self.kept.tcx();
                    tcx.fn_sig(callee).skip_binder().skip_binder().inputs().first().is_some_and(
                        |input| matches!(input.kind(), ty::Ref(_, _, mutability) if mutability.is_mut()),
                    )
                });
                if takes_mut {
                    self.write(receiver, expr);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}

/// The statics with kept state that one unit of read code names.
struct Statics<'a, 'k, 's, 'tcx> {
    kept: &'a Kept<'k, 's, 'tcx>,
    typeck: &'tcx ty::TypeckResults<'tcx>,
    item: String,
    seen: FxHashSet<DefId>,
}

impl<'tcx> Visitor<'tcx> for Statics<'_, '_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.kept.tcx()
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let ExprKind::Path(ref qpath) = expr.kind
            && let Res::Def(DefKind::Static { .. }, id) = self.typeck.qpath_res(qpath, expr.hir_id)
            && self.seen.insert(id)
        {
            let tcx = self.kept.tcx();
            if let Some(kept) =
                self.kept.retains(tcx.type_of(id).instantiate_identity().skip_norm_wip())
            {
                self.kept.report(
                    expr.hir_id,
                    expr.span,
                    format!(
                        "`{}` reads the static `{}`, which keeps `{kept}` state",
                        self.item,
                        tcx.def_path_str(id)
                    ),
                );
            }
        }
        intravisit::walk_expr(self, expr);
    }
}
