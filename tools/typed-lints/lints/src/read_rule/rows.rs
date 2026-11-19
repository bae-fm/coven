//! What a read's rows are. A row mapper is a function or closure of the
//! workspace whose inputs name the row a statement hands its mapper (the
//! policy's `read_shapes.statement_rows`). A mapper whose result, with
//! `Result` and `Option` taken off, implements one of the policy's
//! `read_shapes.column_values` reads one column's value, such as a number, a
//! text or a key: the rows of its read are the collections that flow from
//! what the read returns (`flow.rs`), so a count or a flag elsewhere in read
//! code is none. Any other mapper's result is a row type, and a value holds
//! rows when its type is a slice, collection or iterator whose items are or
//! hold a row type. Neither one row, one optional value, a range, a text
//! nor bytes holds rows.

use clippy_utils::ty::implements_trait_with_env;
use rustc_attr_ir::LangItem;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::DefId;
use rustc_middle::ty::{self, Ty, TyCtxt, TypingEnv};
use rustc_span::sym;

use super::names_adt;

/// What a row mapper makes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Made<'tcx> {
    /// One column's value.
    Column,
    /// A row of this type.
    Row(Ty<'tcx>),
}

/// The policy's rows that say which functions and closures are row mappers.
pub(super) struct Mappers<'s, 'tcx> {
    pub(super) tcx: TyCtxt<'tcx>,
    pub(super) statement_rows: &'s FxHashSet<DefId>,
    pub(super) column_values: &'s FxHashSet<DefId>,
}

impl<'tcx> Mappers<'_, 'tcx> {
    /// What the function or closure `id` makes, when it is a row mapper.
    pub(super) fn made(&self, id: DefId) -> Option<Made<'tcx>> {
        let tcx = self.tcx;
        let signature = tcx.instantiate_bound_regions_with_erased(signature(tcx, id)?);
        if !signature
            .inputs()
            .iter()
            .any(|&input| names_adt(input, &|adt| self.statement_rows.contains(&adt)))
        {
            return None;
        }
        let made = tcx.erase_and_anonymize_regions(peel_result(tcx, signature.output()));
        if made.is_unit() || matches!(made.kind(), ty::Param(_) | ty::Never) {
            return None;
        }
        let typing_env = TypingEnv::post_analysis(tcx, tcx.typeck_root_def_id(id));
        let column = self
            .column_values
            .iter()
            .any(|&column| implements_trait_with_env(tcx, typing_env, made, column, None, &[]));
        Some(if column { Made::Column } else { Made::Row(made) })
    }
}

/// The workspace's row types.
pub(super) struct RowTypes<'tcx> {
    tcx: TyCtxt<'tcx>,
    /// The row types that are structs or enums, by definition.
    adts: FxHashSet<DefId>,
    /// The other row types, as tuples of columns, with regions erased.
    others: FxHashSet<Ty<'tcx>>,
}

impl<'tcx> RowTypes<'tcx> {
    /// The row types the mappers among `definitions` make.
    pub(super) fn of(
        mappers: &Mappers<'_, 'tcx>,
        definitions: impl Iterator<Item = DefId>,
    ) -> Self {
        let mut rows =
            Self { tcx: mappers.tcx, adts: FxHashSet::default(), others: FxHashSet::default() };
        for id in definitions {
            match mappers.made(id) {
                Some(Made::Row(made)) => match made.kind() {
                    ty::Adt(adt, _) => {
                        rows.adts.insert(adt.did());
                    }
                    _ => {
                        rows.others.insert(made);
                    }
                },
                Some(Made::Column) | None => {}
            }
        }
        rows
    }

    /// Whether `ty` is a row type.
    pub(super) fn is_row(&self, ty: Ty<'tcx>) -> bool {
        match ty.kind() {
            ty::Adt(adt, _) => self.adts.contains(&adt.did()),
            _ => self.others.contains(&self.tcx.erase_and_anonymize_regions(ty)),
        }
    }

    /// Whether a value of `ty` holds rows: a slice, collection or iterator
    /// whose items are or hold a row type, behind references.
    pub(super) fn holds_rows(&self, ty: Ty<'tcx>) -> bool {
        let ty = ty.peel_refs();
        if self.is_row(ty) || is_single(self.tcx, ty) {
            return false;
        }
        self.parts_hold_row(ty)
    }

    /// Whether one of the types `ty` is made of, but `ty` itself, is a row
    /// type or holds one, looking through no callback.
    fn parts_hold_row(&self, ty: Ty<'tcx>) -> bool {
        let part = |inner: Ty<'tcx>| self.is_row(inner.peel_refs()) || self.parts_hold_row(inner);
        match ty.kind() {
            ty::Adt(_, args) => args.types().any(part),
            ty::Ref(_, inner, _)
            | ty::RawPtr(inner, _)
            | ty::Slice(inner)
            | ty::Array(inner, _) => part(*inner),
            ty::Tuple(items) => items.iter().any(part),
            _ => false,
        }
    }
}

/// Whether a value of `ty` is a slice, collection or iterator: what holds
/// the rows of a read of one column, when it flows from the read. Text,
/// bytes, a `Result`, an `Option` and a range are none.
pub(super) fn is_collection<'tcx>(
    tcx: TyCtxt<'tcx>,
    typing_env: TypingEnv<'tcx>,
    ty: Ty<'tcx>,
) -> bool {
    let ty = tcx.erase_and_anonymize_regions(ty.peel_refs());
    if is_text(tcx, ty) || is_single(tcx, ty) {
        return false;
    }
    match ty.kind() {
        ty::Slice(_) | ty::Array(..) => true,
        ty::Adt(adt, _) if tcx.is_diagnostic_item(sym::Result, adt.did()) => false,
        ty::Adt(..) => tcx.get_diagnostic_item(sym::IntoIterator).is_some_and(|into_iterator| {
            implements_trait_with_env(tcx, typing_env, ty, into_iterator, None, &[])
        }),
        _ => false,
    }
}

/// Whether `ty` is one value at most: an `Option`, or a range of numbers.
fn is_single<'tcx>(tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> bool {
    let ty::Adt(adt, _) = ty.kind() else {
        return false;
    };
    tcx.is_diagnostic_item(sym::Option, adt.did())
        || [
            LangItem::Range,
            LangItem::RangeFrom,
            LangItem::RangeInclusiveStruct,
            LangItem::RangeTo,
            LangItem::RangeToInclusive,
        ]
        .into_iter()
        .any(|item| tcx.lang_items().get(item) == Some(adt.did()))
}

/// The signature of a function or closure, none for anything else.
pub(super) fn signature<'tcx>(tcx: TyCtxt<'tcx>, id: DefId) -> Option<ty::PolyFnSig<'tcx>> {
    match tcx.def_kind(id) {
        DefKind::Fn | DefKind::AssocFn => {
            Some(tcx.fn_sig(id).instantiate_identity().skip_norm_wip())
        }
        DefKind::Closure => match tcx.type_of(id).instantiate_identity().skip_norm_wip().kind() {
            ty::Closure(_, args) => Some(args.as_closure().sig()),
            _ => None,
        },
        _ => None,
    }
}

/// `ty` with the `Result` or `Option` around it taken off.
pub(super) fn peel_result<'tcx>(tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> Ty<'tcx> {
    match ty.kind() {
        ty::Adt(adt, args)
            if tcx.is_diagnostic_item(sym::Result, adt.did())
                || tcx.is_diagnostic_item(sym::Option, adt.did()) =>
        {
            peel_result(tcx, args.type_at(0))
        }
        _ => ty,
    }
}

/// Whether `ty` is text or bytes: a `String`, `str`, `char` or byte, or a
/// slice or vector of bytes.
fn is_text<'tcx>(tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> bool {
    let ty = ty.peel_refs();
    match ty.kind() {
        ty::Str | ty::Char | ty::Uint(ty::UintTy::U8) => true,
        ty::Slice(inner) | ty::Array(inner, _) => is_text(tcx, *inner),
        ty::Adt(adt, args) => {
            tcx.is_diagnostic_item(sym::String, adt.did())
                || (tcx.is_diagnostic_item(sym::Vec, adt.did()) && is_text(tcx, args.type_at(0)))
        }
        _ => false,
    }
}
