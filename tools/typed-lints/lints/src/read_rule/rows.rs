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
//!
//! A function or closure of the workspace that takes a row type or a
//! converted row type, through references, and makes a struct of the
//! workspace or a tuple that is no column's value makes a converted row
//! type: read code that turns a generated statement's rows into a type of
//! its own makes one. A value of a converted row type holds a read's rows
//! only where a read's rows reach it (`row_work.rs`), so a caller's input of
//! the same type holds none.

use clippy_utils::ty::implements_trait_with_env;
use rustc_attr_ir::LangItem;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::DefId;
use rustc_middle::ty::{self, Ty, TyCtxt, TypingEnv};
use rustc_span::sym;

use super::names_adt;
use crate::workspace_policy::WorkspaceCrates;

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
        Some(if self.is_column(tcx.typeck_root_def_id(id), made) {
            Made::Column
        } else {
            Made::Row(made)
        })
    }

    /// Whether `made`, in the item `root`, is one column's value: it
    /// implements one of the policy's `column_values`.
    fn is_column(&self, root: DefId, made: Ty<'tcx>) -> bool {
        let typing_env = TypingEnv::post_analysis(self.tcx, root);
        self.column_values
            .iter()
            .any(|&column| implements_trait_with_env(self.tcx, typing_env, made, column, None, &[]))
    }
}

/// Types by definition for structs and enums, and with regions erased for
/// the others, tuples of columns.
#[derive(Default)]
struct Types<'tcx> {
    adts: FxHashSet<DefId>,
    others: FxHashSet<Ty<'tcx>>,
}

impl<'tcx> Types<'tcx> {
    fn insert(&mut self, made: Ty<'tcx>) {
        match made.kind() {
            ty::Adt(adt, _) => {
                self.adts.insert(adt.did());
            }
            _ => {
                self.others.insert(made);
            }
        }
    }

    fn contains(&self, tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> bool {
        match ty.kind() {
            ty::Adt(adt, _) => self.adts.contains(&adt.did()),
            _ => self.others.contains(&tcx.erase_and_anonymize_regions(ty)),
        }
    }
}

/// The workspace's row types, and the converted row types code makes of
/// them.
pub(super) struct RowTypes<'tcx> {
    tcx: TyCtxt<'tcx>,
    rows: Types<'tcx>,
    converted: Types<'tcx>,
}

impl<'tcx> RowTypes<'tcx> {
    /// The row types the mappers among `definitions` make, and then, until
    /// no more turn up, the converted row types made of those or of other
    /// converted row types, among the structs `workspace` declares and
    /// tuples.
    pub(super) fn of(
        mappers: &Mappers<'_, 'tcx>,
        workspace: &WorkspaceCrates<'_>,
        definitions: impl Iterator<Item = DefId>,
    ) -> Self {
        let tcx = mappers.tcx;
        let mut types = Self { tcx, rows: Types::default(), converted: Types::default() };
        let mut conversions = Vec::new();
        for id in definitions {
            match mappers.made(id) {
                Some(Made::Row(made)) => types.rows.insert(made),
                Some(Made::Column) => {}
                None => conversions.extend(conversion(mappers, workspace, id)),
            }
        }
        loop {
            let (found, rest): (Vec<_>, Vec<_>) =
                conversions.into_iter().partition(|(inputs, _): &(Vec<Ty<'tcx>>, Ty<'tcx>)| {
                    inputs.iter().any(|input| {
                        let input = input.peel_refs();
                        types.is_row(input) || types.is_converted(input)
                    })
                });
            conversions = rest;
            if found.is_empty() {
                return types;
            }
            for (_, made) in found {
                if !types.is_row(made) {
                    types.converted.insert(made);
                }
            }
        }
    }

    /// Whether `ty` is a row type.
    pub(super) fn is_row(&self, ty: Ty<'tcx>) -> bool {
        self.rows.contains(self.tcx, ty)
    }

    /// Whether `ty` is a converted row type.
    pub(super) fn is_converted(&self, ty: Ty<'tcx>) -> bool {
        self.converted.contains(self.tcx, ty)
    }

    /// Whether a value of `ty` holds rows: a slice, collection or iterator
    /// whose items are or hold a row type, behind references.
    pub(super) fn holds_rows(&self, ty: Ty<'tcx>) -> bool {
        self.holds(ty, &|ty| self.is_row(ty))
    }

    /// Whether a value of `ty` holds rows where a read's rows reach it: a
    /// slice, collection or iterator whose items are or hold a converted row
    /// type, behind references.
    pub(super) fn holds_converted(&self, ty: Ty<'tcx>) -> bool {
        self.holds(ty, &|ty| self.is_converted(ty))
    }

    /// Whether a value of `ty` is or holds a converted row type.
    pub(super) fn mentions_converted(&self, ty: Ty<'tcx>) -> bool {
        let ty = ty.peel_refs();
        self.is_converted(ty) || parts_hold(ty, &|ty| self.is_converted(ty))
    }

    /// Whether a value of `ty` holds values of the types `row` picks.
    fn holds(&self, ty: Ty<'tcx>, row: &dyn Fn(Ty<'tcx>) -> bool) -> bool {
        let ty = ty.peel_refs();
        if row(ty) || is_single(self.tcx, ty) {
            return false;
        }
        parts_hold(ty, row)
    }
}

/// Whether one of the types `ty` is made of, but `ty` itself, is one `row`
/// picks or holds one, looking through no callback.
fn parts_hold<'tcx>(ty: Ty<'tcx>, row: &dyn Fn(Ty<'tcx>) -> bool) -> bool {
    let part = |inner: Ty<'tcx>| row(inner.peel_refs()) || parts_hold(inner, row);
    match ty.kind() {
        ty::Adt(_, args) => args.types().any(part),
        ty::Ref(_, inner, _) | ty::RawPtr(inner, _) | ty::Slice(inner) | ty::Array(inner, _) => {
            part(*inner)
        }
        ty::Tuple(items) => items.iter().any(part),
        _ => false,
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

/// The inputs of the function or closure `id` and what it makes, with regions
/// erased and `Result` and `Option` taken off, when it would convert a row it
/// took: what it makes is a struct `workspace` declares or a tuple, and no
/// column's value.
fn conversion<'tcx>(
    mappers: &Mappers<'_, 'tcx>,
    workspace: &WorkspaceCrates<'_>,
    id: DefId,
) -> Option<(Vec<Ty<'tcx>>, Ty<'tcx>)> {
    let tcx = mappers.tcx;
    let signature = tcx.instantiate_bound_regions_with_erased(signature(tcx, id)?);
    let made = tcx.erase_and_anonymize_regions(peel_result(tcx, signature.output()));
    let shaped = match made.kind() {
        ty::Adt(adt, _) => adt.is_struct() && workspace.declares(tcx, adt.did()),
        ty::Tuple(items) => !items.is_empty(),
        _ => false,
    };
    if !shaped || mappers.is_column(tcx.typeck_root_def_id(id), made) {
        return None;
    }
    // A closure's signature takes its arguments as one tuple.
    let inputs = match (tcx.def_kind(id), signature.inputs()) {
        (DefKind::Closure, [arguments]) => match arguments.kind() {
            ty::Tuple(items) => items.to_vec(),
            _ => vec![*arguments],
        },
        (_, inputs) => inputs.to_vec(),
    };
    let inputs = inputs.into_iter().map(|input| tcx.erase_and_anonymize_regions(input)).collect();
    Some((inputs, made))
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
