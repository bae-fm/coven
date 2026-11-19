//! The owner rules' model of a workspace's types (Appendix F.3), from the
//! compiler's types: the policy's type rows, each resolved by its path, and
//! the owners, which hold a capability, shared state, a handle or another
//! owner.
//!
//! What a value holds is what its type is made of: each type its type names,
//! with its type arguments substituted, except what a callback takes, which
//! its caller gives it, and the type arguments of a type that never holds
//! one (`Router<AppState>` holds no `AppState`), which the type's own fields
//! tell. A `PhantomData<T>` holds its `T`: it stands for one its holder owns,
//! as a `Vec<T>` owns its items behind a raw pointer. A trait object or an opaque type holds what its associated types
//! name, such as an iterator's items or a callback's result. A projection the
//! compiler can't resolve, as `M::Open` in a type generic over `M`, holds
//! every type the impls of its trait give it. The model looks inside the
//! fields of the workspace's own types only: a dependency's type is a row of
//! the policy or a plain value.
//!
//! A workspace type is an owner when what its fields hold includes a
//! capability, shared state, a handle or another owner, or private state
//! that is shared: held through a shared pointer or a reference, or held by
//! a type that some type of the workspace holds that way. The compile sees
//! the crates the crate being compiled depends on, so a type that only a
//! later crate holds through a pointer is no owner in an earlier one.

use std::cell::RefCell;

use clippy_utils::paths::PathNS;
use owner_construction_policy::FILE_NAME;
use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def::DefKind;
use rustc_hir::def_id::{CrateNum, DefId};
use rustc_middle::ty::{
    self, AliasTy, GenericArgKind, GenericArgsRef, Ty, TyCtxt, TypeVisitableExt, TypingEnv,
    Unnormalized,
};

use crate::workspace_policy::{WorkspacePolicy, resolve_row};

/// The types one of the policy's type rows names: ADTs, whatever their type
/// arguments, and the types aliases stand for, as `Arc<dyn Clock>`, with
/// their own.
#[derive(Default)]
pub(crate) struct TypeSet<'tcx> {
    ids: FxHashSet<DefId>,
    exact: FxHashSet<Ty<'tcx>>,
}

impl<'tcx> TypeSet<'tcx> {
    /// Whether the set holds `id`, an ADT with `args` or a trait.
    pub(crate) fn has(
        &self,
        tcx: TyCtxt<'tcx>,
        id: DefId,
        args: Option<GenericArgsRef<'tcx>>,
    ) -> bool {
        self.ids.contains(&id)
            || args.is_some_and(|args| {
                !self.exact.is_empty()
                    && self.exact.contains(&tcx.erase_and_anonymize_regions(Ty::new_adt(
                        tcx,
                        tcx.adt_def(id),
                        args,
                    )))
            })
    }

    /// Whether the set holds the type of `part`.
    pub(crate) fn holds(&self, tcx: TyCtxt<'tcx>, part: &Part<'tcx>) -> bool {
        self.has(tcx, part.id, part.args)
    }

    fn insert(&mut self, row: Row<'tcx>) {
        match row {
            Row::Id(id) => {
                self.ids.insert(id);
            }
            Row::Exact(ty) => {
                self.exact.insert(ty);
            }
        }
    }
}

/// What one row's path names: a type, or what an alias stands for.
enum Row<'tcx> {
    Id(DefId),
    Exact(Ty<'tcx>),
}

/// The policy's type rows, resolved in the compile.
pub(crate) struct OwnerRows<'tcx> {
    pub(crate) capabilities: TypeSet<'tcx>,
    pub(crate) shared_state: TypeSet<'tcx>,
    pub(crate) private_state: TypeSet<'tcx>,
    pub(crate) pointers: TypeSet<'tcx>,
    /// The construction-only capabilities: the rows, and every type of the
    /// workspace that implements one of the capability traits.
    pub(crate) construction_only: TypeSet<'tcx>,
    pub(crate) non_owners: TypeSet<'tcx>,
    pub(crate) facades: TypeSet<'tcx>,
    pub(crate) tasks: TypeSet<'tcx>,
    /// Each handle, with the types that make it: none in a compile that
    /// can't reach its maker.
    pub(crate) handles: FxHashMap<DefId, Vec<DefId>>,
    /// Each capability factory's product, by the factory's row.
    pub(crate) factory_products: Vec<Vec<DefId>>,
}

/// What a type row's path names in the compile: the type, or for an alias,
/// the type it stands for. A row whose path names the crate being compiled
/// and defines nothing there stops the compile, as one that is no path does.
fn resolve_type_row<'tcx>(tcx: TyCtxt<'tcx>, row: &str, path: &str) -> Vec<Row<'tcx>> {
    resolve_type_ids(tcx, row, path)
        .into_iter()
        .map(|id| match tcx.def_kind(id) {
            DefKind::TyAlias => Row::Exact(tcx.erase_and_anonymize_regions(
                tcx.type_of(id).instantiate_identity().skip_norm_wip(),
            )),
            _ => Row::Id(id),
        })
        .collect()
}

/// The definitions a type row's path names, with the same checks.
fn resolve_type_ids(tcx: TyCtxt<'_>, row: &str, path: &str) -> Vec<DefId> {
    if !path.contains("::") {
        tcx.dcx().fatal(format!(
            "typed lints: {FILE_NAME}'s {row} names {path}, which is no path to a type"
        ));
    }
    resolve_row(tcx, row, path, PathNS::Type)
}

impl<'tcx> OwnerRows<'tcx> {
    pub(crate) fn resolve(tcx: TyCtxt<'tcx>, workspace: &WorkspacePolicy) -> Self {
        let policy = &workspace.policy;
        let set = |row: &str, paths: &std::collections::BTreeSet<String>| {
            let mut set = TypeSet::default();
            for path in paths {
                for found in resolve_type_row(tcx, row, path) {
                    set.insert(found);
                }
            }
            set
        };
        let capability_traits = policy
            .capability_traits
            .iter()
            .flat_map(|path| resolve_type_ids(tcx, "capability_traits", path))
            .collect::<Vec<_>>();
        let mut construction_only =
            set("construction_only_capability_types", &policy.construction_only_capability_types);
        for &capability_trait in &capability_traits {
            for &implementation in tcx.all_impls(capability_trait).collect::<Vec<_>>().iter() {
                if tcx.impl_polarity(implementation) != ty::ImplPolarity::Positive {
                    continue;
                }
                // A dependency's type that implements the trait, such as
                // an `Arc` of an implementation, is no capability of the
                // workspace's.
                let self_ty = tcx.type_of(implementation).instantiate_identity().skip_norm_wip();
                if let Some(adt) = self_ty.ty_adt_def()
                    && workspace.holds_file(tcx, tcx.def_span(adt.did()))
                {
                    construction_only.insert(Row::Id(adt.did()));
                }
            }
        }
        let mut handles = FxHashMap::default();
        for (handle, maker) in &policy.handles {
            let makers = resolve_type_ids(tcx, "handles", maker);
            for handle in resolve_type_ids(tcx, "handles", handle) {
                handles.entry(handle).or_insert_with(Vec::new).extend(makers.iter().copied());
            }
        }
        let factory_products = policy
            .capability_factories
            .iter()
            .map(|factory| resolve_type_ids(tcx, "capability_factories", &factory.product))
            .collect();
        Self {
            capabilities: set("capability_types", &policy.capability_types),
            shared_state: set("shared_state_types", &policy.shared_state_types),
            private_state: set("private_state_types", &policy.private_state_types),
            pointers: set("shared_pointer_types", &policy.shared_pointer_types),
            construction_only,
            non_owners: set("non_owner_types", &policy.non_owner_types),
            facades: set("borrowed_facade_types", &policy.borrowed_facade_types),
            tasks: set("task_types", &policy.task_types),
            handles,
            factory_products,
        }
    }

    /// Whether a holder of `part` is an owner by holding it: a capability,
    /// shared state or a handle.
    pub(crate) fn held_capability(&self, tcx: TyCtxt<'tcx>, part: &Part<'tcx>) -> bool {
        self.capabilities.holds(tcx, part)
            || self.shared_state.holds(tcx, part)
            || self.handles.contains_key(&part.id)
    }
}

/// One type a value holds: an ADT with its type arguments, or a trait, and
/// whether the value holds it through a shared pointer or a reference.
#[derive(Clone, Copy)]
pub(crate) struct Part<'tcx> {
    pub(crate) id: DefId,
    pub(crate) args: Option<GenericArgsRef<'tcx>>,
    pub(crate) shared: bool,
    /// Whether the value holds it through a reference: a value can lend
    /// what it holds this way, but not hand it on.
    pub(crate) borrowed: bool,
}

/// The owner rules' model of the compile's types.
pub(crate) struct Owners<'tcx> {
    tcx: TyCtxt<'tcx>,
    pub(crate) workspace: &'static WorkspacePolicy,
    pub(crate) rows: OwnerRows<'tcx>,
    workspace_crates: RefCell<FxHashMap<CrateNum, bool>>,
    /// Which type parameters of each type its fields hold.
    held_parameters: RefCell<FxHashMap<DefId, Option<Vec<bool>>>>,
    /// Whether each workspace type is an owner, `None` while the answer is
    /// being worked out.
    owners: RefCell<FxHashMap<Ty<'tcx>, Option<bool>>>,
    /// The workspace types some workspace type holds through a shared pointer
    /// or a reference, by itself or inside what is.
    shared: RefCell<Option<FxHashSet<DefId>>>,
}

impl<'tcx> Owners<'tcx> {
    pub(crate) fn new(tcx: TyCtxt<'tcx>, workspace: &'static WorkspacePolicy) -> Self {
        Self {
            tcx,
            workspace,
            rows: OwnerRows::resolve(tcx, workspace),
            workspace_crates: RefCell::default(),
            held_parameters: RefCell::default(),
            owners: RefCell::default(),
            shared: RefCell::default(),
        }
    }

    /// Whether `id` is declared in one of the workspace's crates: one whose
    /// root file is inside the workspace.
    pub(crate) fn is_workspace_item(&self, id: DefId) -> bool {
        if id.is_local() {
            return true;
        }
        *self.workspace_crates.borrow_mut().entry(id.krate).or_insert_with(|| {
            self.workspace.holds_file(self.tcx, self.tcx.def_span(id.krate.as_def_id()))
        })
    }

    /// The normalized type of a field, or of any type with its arguments.
    fn normalize(&self, ty: Unnormalized<'tcx, Ty<'tcx>>) -> Ty<'tcx> {
        let tcx = self.tcx;
        tcx.try_normalize_erasing_regions(TypingEnv::fully_monomorphized(), ty)
            .unwrap_or_else(|_| ty.skip_norm_wip())
    }

    /// Each type a value of `ty` holds (`Part`), and each of its own type
    /// parameters it holds, by index.
    pub(crate) fn walk(
        &self,
        ty: Ty<'tcx>,
        shared: bool,
        borrowed: bool,
        found: &mut dyn FnMut(Held<'tcx>),
    ) {
        let mut seen = FxHashSet::default();
        self.walk_inner(ty, shared, borrowed, &|_| true, found, &mut seen);
    }

    /// What `walk` finds in `ty`, except inside the type arguments of each
    /// ADT that `inside` refuses.
    pub(crate) fn walk_outside(
        &self,
        ty: Ty<'tcx>,
        inside: &dyn Fn(DefId) -> bool,
        found: &mut dyn FnMut(Held<'tcx>),
    ) {
        let mut seen = FxHashSet::default();
        self.walk_inner(ty, false, false, inside, found, &mut seen);
    }

    fn walk_inner(
        &self,
        ty: Ty<'tcx>,
        shared: bool,
        borrowed: bool,
        inside: &dyn Fn(DefId) -> bool,
        found: &mut dyn FnMut(Held<'tcx>),
        seen: &mut FxHashSet<(Ty<'tcx>, bool, bool)>,
    ) {
        if !seen.insert((ty, shared, borrowed)) {
            return;
        }
        let tcx = self.tcx;
        match *ty.kind() {
            ty::Adt(adt, args) => {
                found(Held::Part(Part { id: adt.did(), args: Some(args), shared, borrowed }));
                if !inside(adt.did()) {
                    return;
                }
                let pointer = self.rows.pointers.has(tcx, adt.did(), Some(args));
                let held = self.held_parameters(adt.did());
                for (index, arg) in args.iter().enumerate() {
                    let GenericArgKind::Type(arg) = arg.kind() else {
                        continue;
                    };
                    if pointer {
                        self.walk_inner(arg, true, borrowed, inside, found, seen);
                    } else if held.get(index).copied().unwrap_or(true) {
                        self.walk_inner(arg, shared, borrowed, inside, found, seen);
                    }
                }
            }
            ty::Ref(_, inner, _) => self.walk_inner(inner, true, true, inside, found, seen),
            ty::RawPtr(inner, _) | ty::Array(inner, _) | ty::Slice(inner) | ty::Pat(inner, _) => {
                self.walk_inner(inner, shared, borrowed, inside, found, seen);
            }
            ty::Tuple(parts) => {
                for part in parts {
                    self.walk_inner(part, shared, borrowed, inside, found, seen);
                }
            }
            ty::FnPtr(signature, _) => {
                let output = signature.output().skip_binder();
                self.walk_inner(output, shared, borrowed, inside, found, seen);
            }
            ty::Dynamic(bounds, _) => {
                if let Some(principal) = bounds.principal_def_id() {
                    found(Held::Part(Part { id: principal, args: None, shared, borrowed }));
                }
                for projection in bounds.projection_bounds() {
                    if let Some(term) = projection.skip_binder().term.as_type() {
                        self.walk_inner(term, shared, borrowed, inside, found, seen);
                    }
                }
            }
            ty::Alias(_, AliasTy { kind: ty::Opaque { def_id }, args, .. }) => {
                for (clause, _) in tcx
                    .explicit_item_bounds(def_id)
                    .iter_instantiated_copied(tcx, args)
                    .map(Unnormalized::skip_norm_wip)
                {
                    match clause.kind().skip_binder() {
                        ty::ClauseKind::Trait(bound) => found(Held::Part(Part {
                            id: bound.def_id(),
                            args: None,
                            shared,
                            borrowed,
                        })),
                        ty::ClauseKind::Projection(projection) => {
                            if let Some(term) = projection.term.as_type() {
                                self.walk_inner(term, shared, borrowed, inside, found, seen);
                            }
                        }
                        _ => {}
                    }
                }
            }
            ty::Alias(_, alias) if let Some(projection) = alias.try_to_projection() => {
                let normalized = self.normalize(Unnormalized::new_wip(ty));
                if normalized != ty {
                    self.walk_inner(normalized, shared, borrowed, inside, found, seen);
                    return;
                }
                let item = projection.kind;
                for implementation in tcx.all_impls(tcx.parent(item)).collect::<Vec<_>>() {
                    if let Some(&given) = tcx.impl_item_implementor_ids(implementation).get(&item)
                        && tcx.defaultness(given).has_value()
                    {
                        // What a generic impl gives names the impl's own
                        // parameters, which mean nothing here.
                        let given = tcx.type_of(given).instantiate_identity().skip_norm_wip();
                        if !given.has_param() {
                            self.walk_inner(given, shared, borrowed, inside, found, seen);
                        }
                    }
                }
            }
            ty::Param(parameter) => found(Held::Parameter(parameter.index as usize)),
            _ => {}
        }
    }

    /// Which of `id`'s type parameters its fields hold, by index: for a
    /// struct, enum or union, the parameters some field holds; for
    /// `PhantomData` and any other type, all of them.
    fn held_parameters(&self, id: DefId) -> Vec<bool> {
        if let Some(Some(held)) = self.held_parameters.borrow().get(&id) {
            return held.clone();
        }
        let tcx = self.tcx;
        let generics = tcx.generics_of(id);
        let count = generics.count();
        if !matches!(tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union)
            || tcx.adt_def(id).is_phantom_data()
        {
            return vec![true; count];
        }
        // A type that holds itself, as a list's node does, holds through
        // itself what it holds without.
        if self.held_parameters.borrow_mut().insert(id, None).is_some() {
            return vec![false; count];
        }
        let mut held = vec![false; count];
        let adt = tcx.adt_def(id);
        let identity = ty::GenericArgs::identity_for_item(tcx, id);
        for field in adt.all_fields() {
            let field = self.normalize(field.ty(tcx, identity));
            self.walk(field, false, false, &mut |part| {
                if let Held::Parameter(index) = part
                    && let Some(slot) = held.get_mut(index)
                {
                    *slot = true;
                }
            });
        }
        self.held_parameters.borrow_mut().insert(id, Some(held.clone()));
        held
    }

    /// Each type the fields of `ty` hold, for a workspace ADT.
    pub(crate) fn field_parts(&self, ty: Ty<'tcx>, found: &mut dyn FnMut(Part<'tcx>)) {
        let ty::Adt(adt, args) = *ty.kind() else {
            return;
        };
        for field in adt.all_fields() {
            let field = self.normalize(field.ty(self.tcx, args));
            self.walk(field, false, false, &mut |held| {
                if let Held::Part(part) = held {
                    found(part);
                }
            });
        }
    }

    /// Whether `ty` is an owner: a workspace ADT, no plain value or facade,
    /// whose fields hold a capability, shared state, a handle, an owner, or
    /// private state that is shared.
    pub(crate) fn is_owner(&self, ty: Ty<'tcx>) -> bool {
        let ty::Adt(adt, args) = *ty.kind() else {
            return false;
        };
        let id = adt.did();
        if !self.is_workspace_item(id)
            || self.rows.non_owners.has(self.tcx, id, Some(args))
            || self.rows.facades.has(self.tcx, id, Some(args))
        {
            return false;
        }
        if let Some(known) = self.owners.borrow().get(&ty) {
            // A type that holds itself is an owner by what else it holds.
            return known.unwrap_or(false);
        }
        self.owners.borrow_mut().insert(ty, None);
        let held_shared = self.held_shared(id);
        let mut owner = false;
        self.field_parts(ty, &mut |part| {
            if owner {
                return;
            }
            owner = self.rows.held_capability(self.tcx, &part)
                || (self.rows.private_state.holds(self.tcx, &part) && (part.shared || held_shared))
                || part.args.is_some_and(|args| {
                    part.id != id
                        && self.is_workspace_item(part.id)
                        && self.is_owner(
                            self.tcx.type_of(part.id).instantiate(self.tcx, args).skip_norm_wip(),
                        )
                });
        });
        self.owners.borrow_mut().insert(ty, Some(owner));
        owner
    }

    /// Whether some workspace type holds `id` through a shared pointer or a
    /// reference, by itself or inside what is.
    fn held_shared(&self, id: DefId) -> bool {
        if self.shared.borrow().is_none() {
            let shared = self.shared_types();
            *self.shared.borrow_mut() = Some(shared);
        }
        self.shared.borrow().as_ref().is_some_and(|shared| shared.contains(&id))
    }

    fn shared_types(&self) -> FxHashSet<DefId> {
        let tcx = self.tcx;
        let mut shared = FxHashSet::default();
        let mut pending = Vec::new();
        for id in self.workspace_types() {
            let identity = tcx.type_of(id).instantiate_identity().skip_norm_wip();
            self.field_parts(identity, &mut |part| {
                if part.shared && self.is_workspace_item(part.id) && shared.insert(part.id) {
                    pending.push(part.id);
                }
            });
        }
        while let Some(id) = pending.pop() {
            if !matches!(tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union) {
                continue;
            }
            let identity = tcx.type_of(id).instantiate_identity().skip_norm_wip();
            self.field_parts(identity, &mut |part| {
                if self.is_workspace_item(part.id) && shared.insert(part.id) {
                    pending.push(part.id);
                }
            });
        }
        shared
    }

    /// The structs, enums and unions of the crate being compiled and of the
    /// workspace's crates it depends on.
    pub(crate) fn workspace_types(&self) -> Vec<DefId> {
        let tcx = self.tcx;
        let is_type = |id: DefId| {
            matches!(tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union)
        };
        let mut types = tcx
            .hir_crate_items(())
            .definitions()
            .map(|id| id.to_def_id())
            .filter(|&id| is_type(id))
            .collect::<Vec<_>>();
        for &krate in tcx.crates(()) {
            if !self.is_workspace_item(krate.as_def_id()) {
                continue;
            }
            let mut modules = vec![krate.as_def_id()];
            let mut seen = FxHashSet::default();
            while let Some(module) = modules.pop() {
                for child in tcx.module_children(module) {
                    let Some(id) = child.res.opt_def_id() else {
                        continue;
                    };
                    if id.krate != krate || !seen.insert(id) {
                        continue;
                    }
                    match tcx.def_kind(id) {
                        DefKind::Mod => modules.push(id),
                        _ if is_type(id) => types.push(id),
                        _ => {}
                    }
                }
            }
        }
        types
    }
}

/// What a walk of a type finds: a type it holds, or one of the type
/// parameters in scope, by index.
#[derive(Clone, Copy)]
pub(crate) enum Held<'tcx> {
    Part(Part<'tcx>),
    Parameter(usize),
}
