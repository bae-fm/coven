//! `clippy::reqwest_error_display`: a `reqwest::Error` printed with `Display`
//! or `Debug`.
//!
//! reqwest's own text is often only "error sending request for url (...)": the
//! DNS, TLS or connection failure behind it is a cause further down, which that
//! text leaves out, and the URL it names can hold a path or a query. So an
//! error is printed through a cause chain, a type that walks every cause and
//! cuts each URL to its scheme and host. The chain takes the error as a
//! `&dyn Error`, so its own printing makes no finding.
//!
//! The lint finds each place that prints a value whose type is `reqwest::Error`,
//! resolved to its definition, so an alias or a re-export names it too:
//!
//! - an argument of a format macro, through `{}` or `{:?}`, as thiserror's
//!   `#[error("{0}")]` writes one;
//! - a call of `Display::fmt`, `Debug::fmt` or `ToString::to_string`, as
//!   thiserror's `#[error(transparent)]` writes one;
//! - a reference coerced to `&dyn Display` or `&dyn Debug`, which whoever holds
//!   it prints, as a derived `Debug` does with each field.
//!
//! References, `Box`, `Rc` and `Arc` print what they point to, so a value of
//! such a type around the error is the error. For `Debug`, so are `Option`,
//! `Result`, `Vec`, slices, arrays and tuples that hold one.

use crate::finding::Finding;
use clippy_utils::paths::{PathNS, lookup_path_str};
use clippy_utils::sym;
use rustc_attr_ir::lang_items::LangItem;
use rustc_hir::def_id::DefId;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass, LintContext, declare_tool_lint, impl_lint_pass};
use rustc_middle::ty::adjustment::{Adjust, PointerCoercion};
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::Symbol;

declare_tool_lint! {
    /// A `reqwest::Error` printed with `Display` or `Debug` instead of through
    /// a cause chain.
    pub clippy::REQWEST_ERROR_DISPLAY,
    Warn,
    "prints a `reqwest::Error` with `Display` or `Debug`",
    report_in_external_macro: true
}

/// The lint's pass, holding what `reqwest::Error` resolves to in the crate it
/// checks: nothing when the crate can't reach reqwest.
pub(crate) struct ReqwestErrorDisplay {
    reqwest_error: Vec<DefId>,
}

impl_lint_pass!(ReqwestErrorDisplay => [REQWEST_ERROR_DISPLAY]);

/// The trait a value is printed through.
#[derive(Clone, Copy)]
enum Printing {
    Display,
    Debug,
}

impl Printing {
    fn name(self) -> &'static str {
        match self {
            Self::Display => "Display",
            Self::Debug => "Debug",
        }
    }

    /// The trait whose definition is `trait_id`, if it is `Display` or `Debug`.
    fn of_trait(tcx: TyCtxt<'_>, trait_id: DefId) -> Option<Self> {
        match tcx.get_diagnostic_name(trait_id)? {
            sym::Display => Some(Self::Display),
            sym::Debug => Some(Self::Debug),
            _ => None,
        }
    }
}

impl ReqwestErrorDisplay {
    pub(crate) fn new(tcx: TyCtxt<'_>) -> Self {
        Self { reqwest_error: lookup_path_str(tcx, PathNS::Type, "reqwest::Error") }
    }

    /// Whether printing a value of type `ty` through `printing` prints a
    /// `reqwest::Error`.
    fn prints_reqwest_error(&self, tcx: TyCtxt<'_>, ty: Ty<'_>, printing: Printing) -> bool {
        match *ty.kind() {
            ty::Ref(_, pointee, _) => self.prints_reqwest_error(tcx, pointee, printing),
            ty::Adt(adt, args) => {
                if self.reqwest_error.contains(&adt.did()) {
                    return true;
                }
                let name = tcx.get_diagnostic_name(adt.did());
                if adt.is_box() || matches!(name, Some(sym::Rc | sym::Arc)) {
                    return self.prints_reqwest_error(tcx, args.type_at(0), printing);
                }
                matches!(printing, Printing::Debug)
                    && matches!(name, Some(sym::Option | sym::Result | sym::Vec))
                    && args.types().any(|held| self.prints_reqwest_error(tcx, held, printing))
            }
            ty::Slice(element) | ty::Array(element, _) => {
                matches!(printing, Printing::Debug)
                    && self.prints_reqwest_error(tcx, element, printing)
            }
            ty::Tuple(elements) => {
                matches!(printing, Printing::Debug)
                    && elements
                        .iter()
                        .any(|element| self.prints_reqwest_error(tcx, element, printing))
            }
            _ => false,
        }
    }
}

fn report(cx: &LateContext<'_>, expr: &Expr<'_>, ty: Ty<'_>, printing: Printing) {
    cx.emit_span_lint(
        REQWEST_ERROR_DISPLAY,
        expr.span,
        Finding {
            message: format!("this code prints `{ty}` with `{}`", printing.name()),
            help: "print a reqwest::Error through the cause chain, which prints every cause and \
                   cuts each URL to its host",
        },
    );
}

/// The trait a call or a path to a function prints through, and the type of
/// the value it prints, when `expr` names a printing function: the format
/// macros' `Argument::new_display` and `Argument::new_debug`,
/// `Display::fmt`, `Debug::fmt` or `ToString::to_string`.
fn printed_value<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'tcx>) -> Option<(Printing, Ty<'tcx>)> {
    let typeck = cx.typeck_results();
    let def_id = match expr.kind {
        ExprKind::Path(ref qpath) => cx.qpath_res(qpath, expr.hir_id).opt_def_id()?,
        ExprKind::MethodCall(..) => typeck.type_dependent_def_id(expr.hir_id)?,
        _ => return None,
    };
    let printing = printing_function(cx.tcx, def_id)?;
    // A trait method's first type argument is `Self`; `new_display<T>` and
    // `new_debug<T>` have `T` alone.
    let printed = typeck.node_args(expr.hir_id).types().next()?;
    Some((printing, printed))
}

/// The trait the function `def_id` prints its value through, if it is one of
/// [`printed_value`]'s functions.
fn printing_function(tcx: TyCtxt<'_>, def_id: DefId) -> Option<Printing> {
    let name = tcx.opt_item_name(def_id)?;
    if let Some(trait_id) = tcx.trait_of_assoc(def_id) {
        if tcx.is_diagnostic_item(sym::ToString, trait_id) {
            return (name == sym::to_string).then_some(Printing::Display);
        }
        return Printing::of_trait(tcx, trait_id).filter(|_| name == sym::fmt);
    }
    let impl_id = tcx.inherent_impl_of_assoc(def_id)?;
    let argument = tcx.type_of(impl_id).instantiate_identity().skip_norm_wip().ty_adt_def()?;
    if !tcx.is_lang_item(argument.did(), LangItem::FormatArgument) {
        return None;
    }
    format_argument_printing(name)
}

/// The trait each of the format macros' `Argument` constructors prints through.
fn format_argument_printing(name: Symbol) -> Option<Printing> {
    match name {
        sym::new_display => Some(Printing::Display),
        sym::new_debug => Some(Printing::Debug),
        _ => None,
    }
}

impl<'tcx> LateLintPass<'tcx> for ReqwestErrorDisplay {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if self.reqwest_error.is_empty() {
            return;
        }
        if let Some((printing, printed)) = printed_value(cx, expr)
            && self.prints_reqwest_error(cx.tcx, printed, printing)
        {
            report(cx, expr, printed, printing);
        }
        // A reference coerced to `&dyn Display` or `&dyn Debug`: each
        // adjustment's source is the type the one before it left.
        let typeck = cx.typeck_results();
        let mut source = typeck.expr_ty(expr);
        for adjustment in typeck.expr_adjustments(expr) {
            if let Adjust::Pointer(PointerCoercion::Unsize) = adjustment.kind
                && let Some(target) = adjustment.target.builtin_deref(true)
                && let ty::Dynamic(predicates, ..) = target.kind()
                && let Some(printing) =
                    predicates.principal_def_id().and_then(|id| Printing::of_trait(cx.tcx, id))
                && let Some(pointee) = source.builtin_deref(true)
                && self.prints_reqwest_error(cx.tcx, pointee, printing)
            {
                report(cx, expr, pointee, printing);
            }
            source = adjustment.target;
        }
    }
}
