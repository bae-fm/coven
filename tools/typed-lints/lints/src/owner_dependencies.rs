//! The hand-out rules of the policy's `owner-dependencies` (Appendix F.3), on
//! the compiler's types (`owners.rs`): an owner never hands out what it
//! holds, by returning it, through a public field, or as an argument its
//! caller's callback can keep (`kept_arguments.rs`); callers ask it to do the
//! work. A handle is the one thing an owner hands out on purpose, and shared
//! state leaves its owner only inside one.
//!
//! - `clippy::owner_hands_out_dependency`: a method returns, or gives a
//!   callback it may keep, a dependency its owner holds, by its type: an
//!   internal dependency the policy names, or a capability or an owner when
//!   the method's owner is one, unless the method makes the value rather than
//!   take it from its owner (`made_here.rs`), or takes a task whole or
//!   mutably and hands on, by value, a task it holds, as one step of an
//!   operation hands its product to the next. A public method of an owner
//!   returns no other owner, which the owner would have to build. A method
//!   that returns a value whose own methods hand out what it holds hands that
//!   out too, by what it holds of it. A public field of an owner, or one
//!   that holds an owner or a capability, hands it out as well. A raw SQLite
//!   handle never leaves a method that way, nor a callable of the database's
//!   homes, and a public one of those, unless the policy lists it, takes none,
//!   as no method that takes its owner whole or mutably does. A closed
//!   session declared at its crate's root would hand the internal
//!   dependencies it holds to every module of the crate.
//! - `clippy::handle_not_from_its_owner`: only a handle's owner, the handle
//!   itself, a composition root and what holds the owner hand the handle out:
//!   no type that holds the handle itself, other than through its owner.
//! - `clippy::shared_state_outside_handle`: no method reachable outside its
//!   module, but a composition root, hands out shared state, or private
//!   state through a shared pointer or a reference, whether it holds that
//!   state or made it.
//!
//! The rules read every method of the workspace the compile sees, so a value
//! of an earlier crate's type exposes what that type's methods hand out, and
//! report the crate's own. Test code answers to none of them.

use std::cell::RefCell;

use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def::DefKind;
use rustc_hir::def_id::{DefId, LOCAL_CRATE};
use rustc_lint::{LateContext, declare_tool_lint};
use rustc_middle::ty::{self, Ty, TyCtxt, Visibility};
use rustc_span::Span;

use crate::finding::Finding;
use crate::kept_arguments::kept_arguments;
use crate::made_here::MadeHere;
use crate::owner_rules::{is_root, type_name};
use crate::owners::{Held, Owners, Part, mentions};
use crate::workspace_bodies::workspace_definitions;

declare_tool_lint! {
    /// A method that returns, or gives a callback it may keep, a dependency
    /// its owner holds; a public field that hands one out; a raw SQLite
    /// handle that leaves or enters the database's homes' API; or a closed
    /// session at a crate's root.
    pub clippy::OWNER_HANDS_OUT_DEPENDENCY,
    Warn,
    "an owner hands out a dependency it holds",
    report_in_external_macro: true
}

declare_tool_lint! {
    /// A method that hands out a handle and is neither the handle's owner nor
    /// a method of what holds that owner.
    pub clippy::HANDLE_NOT_FROM_ITS_OWNER,
    Warn,
    "a handle is handed out by other than its owner or what holds it",
    report_in_external_macro: true
}

declare_tool_lint! {
    /// A method reachable outside its module that hands out shared state.
    pub clippy::SHARED_STATE_OUTSIDE_HANDLE,
    Warn,
    "shared state leaves its owner outside a handle",
    report_in_external_macro: true
}

const REMEDY: &str = "an owner never hands out what it holds; callers ask it to do the work";

const HANDLE_REMEDY: &str = "a handle's owner makes it, and what holds that owner asks it for a \
                             fresh one; a holder receives one when it is built and keeps it";

const SHARED_STATE_REMEDY: &str =
    "shared state leaves its owner only inside a handle the policy names with that owner";

/// How a method hands a value out: as what it returns, or as an argument its
/// caller's callback can keep.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    Return,
    Callback,
}

/// One method of an impl of a workspace type, or of a workspace trait: what
/// it hands out, and how.
struct Method<'tcx> {
    id: DefId,
    /// The impl's type, or the trait.
    owner: DefId,
    /// What its result holds, but its owner.
    output: Vec<Part<'tcx>>,
    /// What its callbacks can keep of what it gives them, but its owner,
    /// each with the parameter that holds the callback and the argument's
    /// place.
    callback_output: Vec<(Part<'tcx>, Option<usize>, usize)>,
    /// Whether it takes its owner by value or by a mutable reference.
    takes_owner_whole_or_mutably: bool,
    /// Whether its result holds a reference or a raw pointer.
    borrows_output: bool,
    /// Whether an argument its callbacks can keep holds one.
    borrows_callback_output: bool,
    /// Whether code outside its module can call it.
    public: bool,
    rooted: bool,
    made_here: RefCell<Option<MadeHere<'tcx>>>,
}

impl<'tcx> Method<'tcx> {
    /// Each type the method hands out, once, by its route: one both returned
    /// and given to a callback by its return.
    fn handed_out(&self) -> Vec<(Part<'tcx>, Route)> {
        let mut handed = Vec::new();
        let mut seen = FxHashSet::default();
        for &part in &self.output {
            if seen.insert(part.id) {
                handed.push((part, Route::Return));
            }
        }
        for &(part, ..) in &self.callback_output {
            if seen.insert(part.id) {
                handed.push((part, Route::Callback));
            }
        }
        handed
    }

    fn made_here<T>(&self, tcx: TyCtxt<'tcx>, read: impl FnOnce(&MadeHere<'tcx>) -> T) -> T {
        let mut made_here = self.made_here.borrow_mut();
        read(made_here.get_or_insert_with(|| MadeHere::of(tcx, self.id)))
    }

    /// Whether the method may hand out, by `route`, the `id` its owner holds:
    /// the owner holds that type, and the method doesn't make the value.
    fn takes(&self, tcx: TyCtxt<'tcx>, held: &FxHashSet<DefId>, id: DefId, route: Route) -> bool {
        if !held.contains(&id) {
            return false;
        }
        match route {
            Route::Return => !self.made_here(tcx, MadeHere::returns),
            Route::Callback => self.callback_output.iter().any(|&(part, parameter, index)| {
                part.id == id
                    && !parameter.is_some_and(|parameter| {
                        self.made_here(tcx, |made| made.callback_argument(parameter, index))
                    })
            }),
        }
    }

    /// Whether the method's result, or an argument its callbacks can keep,
    /// holds `id` through a shared pointer or a reference.
    fn output_or_callback_shares(&self, id: DefId) -> bool {
        self.output
            .iter()
            .chain(self.callback_output.iter().map(|(part, ..)| part))
            .any(|part| part.id == id && part.shared)
    }

    /// Whether the method hands on, by `route`, a value that is a task (`is_task`)
    /// by value, taking the task that holds it whole or mutably: the holder no
    /// longer has what it hands on. One that lends it by `&self` still has it.
    fn transfers_task(&self, is_task: bool, route: Route) -> bool {
        let borrows = match route {
            Route::Return => self.borrows_output,
            Route::Callback => self.borrows_callback_output,
        };
        self.takes_owner_whole_or_mutably && !borrows && is_task
    }
}

/// What each type's fields hold at any depth, by the type and the type they
/// are read around, whose own fields don't count.
type RetainedByType = FxHashMap<(DefId, Option<DefId>), FxHashSet<DefId>>;

/// The pass's state for one compile: the model, and what the owners it asks
/// about hold.
struct Rules<'a, 'tcx> {
    owners: &'a Owners<'tcx>,
    tcx: TyCtxt<'tcx>,
    retained: RefCell<RetainedByType>,
    /// The findings reported, each once, by the definition that holds it.
    reported: RefCell<FxHashSet<(DefId, String)>>,
}

/// Reports what the crate's methods, fields and sessions hand out.
pub(crate) fn check<'tcx>(owners: &Owners<'tcx>, cx: &LateContext<'tcx>) {
    let rules =
        Rules { owners, tcx: cx.tcx, retained: RefCell::default(), reported: RefCell::default() };
    let methods = rules.methods();
    rules.check_methods(&methods);
    rules.check_fields();
    rules.check_sessions();
    rules.check_database_callables();
}

impl<'tcx> Rules<'_, 'tcx> {
    /// Every method the compile sees of an impl of a workspace type or of a
    /// workspace trait, in production code.
    fn methods(&self) -> Vec<Method<'tcx>> {
        let tcx = self.tcx;
        workspace_definitions(self.owners, tcx, "clippy::owner_hands_out_dependency")
            .into_iter()
            .filter(|&id| tcx.def_kind(id) == DefKind::AssocFn)
            .filter(|&id| !self.owners.workspace.is_test_definition(tcx, id))
            .filter_map(|id| self.method(id))
            .collect()
    }

    fn method(&self, id: DefId) -> Option<Method<'tcx>> {
        let tcx = self.tcx;
        let parent = tcx.parent(id);
        let owner = match tcx.def_kind(parent) {
            DefKind::Impl { .. } => tcx
                .type_of(parent)
                .instantiate_identity()
                .skip_norm_wip()
                .ty_adt_def()
                .map(|adt| adt.did())?,
            DefKind::Trait => parent,
            _ => return None,
        };
        let signature = tcx
            .liberate_late_bound_regions(id, tcx.fn_sig(id).instantiate_identity().skip_norm_wip());
        let output = self.parts(signature.output(), owner);
        let mut callback_output = Vec::new();
        let mut borrows_callback_output = false;
        for kept in kept_arguments(tcx, id) {
            borrows_callback_output |= borrows(kept.ty);
            for part in self.parts(kept.ty, owner) {
                callback_output.push((part, kept.parameter, kept.index));
            }
        }
        let receiver = tcx
            .opt_associated_item(id)
            .filter(|item| item.is_method())
            .map(|_| signature.inputs()[0]);
        let takes_owner_whole_or_mutably =
            receiver.is_some_and(|receiver| match *receiver.kind() {
                ty::Ref(_, _, mutability) => mutability.is_mut(),
                _ => true,
            });
        Some(Method {
            id,
            owner,
            output,
            callback_output,
            takes_owner_whole_or_mutably,
            borrows_output: borrows(signature.output()),
            borrows_callback_output,
            public: crosses_module(tcx, id),
            rooted: is_root(self.owners, tcx, id),
            made_here: RefCell::new(None),
        })
    }

    /// What a value of `ty` holds, but `owner`, each type once.
    fn parts(&self, ty: Ty<'tcx>, owner: DefId) -> Vec<Part<'tcx>> {
        let mut parts = Vec::new();
        let mut seen = FxHashSet::default();
        self.owners.walk(ty, false, false, &mut |held| {
            if let Held::Part(part) = held
                && part.id != owner
                && seen.insert((part.id, part.args, part.shared))
            {
                parts.push(part);
            }
        });
        parts
    }

    /// What `id`'s fields hold, at any depth, but inside an `around` they
    /// hold.
    fn retained(&self, id: DefId, around: Option<DefId>) -> FxHashSet<DefId> {
        if let Some(known) = self.retained.borrow().get(&(id, around)) {
            return known.clone();
        }
        let tcx = self.tcx;
        let mut retained = FxHashSet::default();
        let mut pending = vec![id];
        let mut visited = FxHashSet::default();
        while let Some(current) = pending.pop() {
            if !visited.insert(current)
                || !matches!(
                    tcx.def_kind(current),
                    DefKind::Struct | DefKind::Enum | DefKind::Union
                )
                || !self.owners.is_workspace_item(current)
            {
                continue;
            }
            let identity = tcx.type_of(current).instantiate_identity().skip_norm_wip();
            self.owners.field_parts(identity, &mut |part| {
                if retained.insert(part.id) && around != Some(part.id) {
                    pending.push(part.id);
                }
            });
        }
        self.retained.borrow_mut().insert((id, around), retained.clone());
        retained
    }

    /// Whether the type of `part` is a service: an owner, or a capability,
    /// shared state or a handle.
    fn is_service(&self, part: &Part<'tcx>) -> bool {
        self.owners.rows.held_capability(self.tcx, part) || self.is_owner(part.id)
    }

    /// Whether the workspace type `id` is an owner by what it holds itself.
    fn is_owner(&self, id: DefId) -> bool {
        matches!(self.tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union)
            && self.owners.is_owner(self.tcx.type_of(id).instantiate_identity().skip_norm_wip())
    }

    /// `id` as a part a value holds by value: an ADT with its own
    /// parameters, or a trait.
    fn part(&self, id: DefId) -> Part<'tcx> {
        let args =
            matches!(self.tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union)
                .then(|| ty::GenericArgs::identity_for_item(self.tcx, id));
        Part { id, args, shared: false, borrowed: false }
    }

    /// Whether `id` is a raw SQLite handle.
    fn is_raw(&self, id: DefId) -> bool {
        self.owners.rows.raw_handles.has(self.tcx, id, None)
    }

    /// Whether `part` is an internal dependency: one the policy names, or a
    /// raw SQLite handle.
    fn is_internal(&self, part: &Part<'tcx>) -> bool {
        self.owners.rows.internal.holds(self.tcx, part) || self.is_raw(part.id)
    }

    /// Reports `message` at the crate's own definition `id`, once.
    fn emit(
        &self,
        lint: &'static rustc_lint::Lint,
        id: DefId,
        span: Span,
        message: String,
        help: &'static str,
    ) {
        let Some(local) = id.as_local() else {
            return;
        };
        if !self.reported.borrow_mut().insert((id, message.clone())) {
            return;
        }
        let hir_id = self.tcx.local_def_id_to_hir_id(local);
        self.tcx.emit_node_span_lint(
            lint,
            hir_id,
            span.source_callsite(),
            Finding { message, help },
        );
    }

    /// The hand-out rules over every method, reported for the crate's own.
    fn check_methods(&self, methods: &[Method<'tcx>]) {
        let tcx = self.tcx;
        let rows = &self.owners.rows;
        let report = |lint, method: &Method<'tcx>, message: String, help| {
            self.emit(lint, method.id, tcx.def_span(method.id), message, help);
        };
        let name = |method: &Method<'tcx>| tcx.def_path_str(method.id);
        // What each type's methods hand out that it holds, which a method
        // returning a value of the type hands out too.
        let mut exposed = FxHashMap::<DefId, FxHashSet<DefId>>::default();
        loop {
            let before = exposed.clone();
            for method in methods {
                let held = self.retained(method.owner, None);
                let owner_is_service = self.is_service(&self.part(method.owner));
                for (part, route) in method.handed_out() {
                    let output = part.id;
                    let what = tcx.def_path_str(output);
                    if let Some(makers) = rows.handles.get(&output) {
                        let maker = makers.first().map(|&maker| tcx.def_path_str(maker));
                        let maker = maker.unwrap_or_default();
                        let makes = method.rooted
                            || makers.contains(&method.owner)
                            || method.owner == output;
                        let message = if makes {
                            None
                        } else if makers.iter().any(|&maker| {
                            self.retained(method.owner, Some(maker)).contains(&output)
                        }) {
                            Some(format!(
                                "`{}` hands out handle `{what}`, which it holds; only `{maker}` \
                                 makes one",
                                name(method)
                            ))
                        } else if !makers.iter().any(|maker| held.contains(maker)) {
                            Some(format!(
                                "`{}` hands out handle `{what}`, which only `{maker}` and what \
                                 holds it hand out",
                                name(method)
                            ))
                        } else {
                            None
                        };
                        if let Some(message) = message {
                            report(HANDLE_NOT_FROM_ITS_OWNER, method, message, HANDLE_REMEDY);
                        }
                        continue;
                    }
                    let shared_state = rows.shared_state.holds(tcx, &part)
                        || (rows.private_state.holds(tcx, &part)
                            && method.output_or_callback_shares(output));
                    if shared_state {
                        if method.public && !method.rooted {
                            report(
                                SHARED_STATE_OUTSIDE_HANDLE,
                                method,
                                format!("`{}` hands out shared state `{what}`", name(method)),
                                SHARED_STATE_REMEDY,
                            );
                        }
                        continue;
                    }
                    let taken = method.takes(tcx, &held, output, route);
                    let retained_service = self.is_service(&part)
                        && taken
                        && !method.transfers_task(rows.tasks.holds(tcx, &part), route);
                    let new_service = method.public
                        && self.is_owner(output)
                        && !rows.tasks.holds(tcx, &part)
                        && !rows.held_capability(tcx, &part);
                    let leaks_directly = self.is_raw(output)
                        || (self.is_internal(&part) && taken)
                        || (owner_is_service
                            && !method.rooted
                            && (retained_service || new_service));
                    // What a value made in the method exposes is its own.
                    let nested = before
                        .get(&output)
                        .into_iter()
                        .flatten()
                        .copied()
                        .filter(|&exposed| method.takes(tcx, &held, exposed, route))
                        .collect::<FxHashSet<_>>();
                    if leaks_directly
                        || nested.iter().any(|&exposed| self.is_raw(exposed) || !method.rooted)
                    {
                        let message = match route {
                            Route::Return => {
                                format!("`{}` returns retained dependency `{what}`", name(method))
                            }
                            Route::Callback => format!(
                                "`{}` hands its callback retained dependency `{what}`",
                                name(method)
                            ),
                        };
                        report(OWNER_HANDS_OUT_DEPENDENCY, method, message, REMEDY);
                    }
                    let entry = exposed.entry(method.owner).or_default();
                    if leaks_directly {
                        entry.insert(output);
                    }
                    entry.extend(nested);
                }
            }
            if exposed == before {
                break;
            }
        }
        for method in methods {
            if !method.id.is_local() || !method.takes_owner_whole_or_mutably {
                continue;
            }
            for raw in self.raw_parameters(method.id) {
                report(
                    OWNER_HANDS_OUT_DEPENDENCY,
                    method,
                    format!(
                        "`{}` accepts raw dependency `{}`",
                        name(method),
                        tcx.def_path_str(raw)
                    ),
                    REMEDY,
                );
            }
        }
    }

    /// The raw SQLite handles the parameters of `callable` name, but its
    /// receiver.
    fn raw_parameters(&self, callable: DefId) -> Vec<DefId> {
        let tcx = self.tcx;
        let signature = tcx.fn_sig(callable).instantiate_identity().skip_norm_wip().skip_binder();
        let method = tcx.opt_associated_item(callable).is_some_and(|item| item.is_method());
        let mut raw = Vec::new();
        for (index, &input) in signature.inputs().iter().enumerate() {
            if method && index == 0 {
                continue;
            }
            for named in mentions(tcx, input) {
                if self.is_raw(named) && !raw.contains(&named) {
                    raw.push(named);
                }
            }
        }
        raw
    }

    /// A public field of an owner, or one that holds a service or what holds
    /// one, hands it out.
    fn check_fields(&self) {
        let tcx = self.tcx;
        for local in tcx.hir_crate_items(()).definitions() {
            let id = local.to_def_id();
            if tcx.def_kind(id) != DefKind::Struct
                || self.owners.workspace.is_test_definition(tcx, id)
            {
                continue;
            }
            let adt = tcx.adt_def(id);
            let owner = self.is_owner(id);
            let identity = ty::GenericArgs::identity_for_item(tcx, id);
            for field in adt.all_fields() {
                if !crosses_module(tcx, field.did)
                    || self.owners.workspace.is_test_definition(tcx, field.did)
                {
                    continue;
                }
                let mut exposes_service = false;
                self.owners.walk(
                    field.ty(tcx, identity).skip_norm_wip(),
                    false,
                    false,
                    &mut |held| {
                        if let Held::Part(part) = held {
                            exposes_service |= self.is_service(&part)
                                || self
                                    .retained(part.id, None)
                                    .iter()
                                    .any(|&retained| self.is_service(&self.part(retained)));
                        }
                    },
                );
                if owner || exposes_service {
                    self.emit(
                        OWNER_HANDS_OUT_DEPENDENCY,
                        field.did,
                        tcx.def_span(field.did),
                        format!(
                            "service owner `{}` exposes field `{}`",
                            tcx.def_path_str(id),
                            field.name
                        ),
                        REMEDY,
                    );
                }
            }
        }
    }

    /// A closed session declared at its crate's root holds no internal
    /// dependency: each field that holds one is reported.
    fn check_sessions(&self) {
        let tcx = self.tcx;
        let root = LOCAL_CRATE.as_def_id();
        let root_file = self.owners.workspace.relative_path(tcx, tcx.def_span(root));
        for local in tcx.hir_crate_items(()).definitions() {
            let id = local.to_def_id();
            if tcx.def_kind(id) != DefKind::Struct
                || tcx.parent(id) != root
                || self.owners.workspace.relative_path(tcx, tcx.def_span(id)) != root_file
                || !self.owners.rows.sessions.has(tcx, id, None)
                || self.owners.workspace.is_test_definition(tcx, id)
            {
                continue;
            }
            let identity = ty::GenericArgs::identity_for_item(tcx, id);
            for field in tcx.adt_def(id).all_fields() {
                let mut internal = Vec::new();
                for named in mentions(tcx, field.ty(tcx, identity).skip_norm_wip()) {
                    if (self.owners.rows.internal.has(tcx, named, None) || self.is_raw(named))
                        && !internal.contains(&named)
                    {
                        internal.push(named);
                    }
                }
                for dependency in internal {
                    self.emit(
                        OWNER_HANDS_OUT_DEPENDENCY,
                        field.did,
                        tcx.def_span(field.did),
                        format!(
                            "crate-root `{}` exposes internal dependency `{}` to every module",
                            tcx.def_path_str(id),
                            tcx.def_path_str(dependency)
                        ),
                        REMEDY,
                    );
                }
            }
        }
    }

    /// The callables of the database's homes: none returns a raw SQLite
    /// handle, and no public one the policy doesn't list takes one.
    fn check_database_callables(&self) {
        let tcx = self.tcx;
        let workspace = self.owners.workspace;
        let homes = workspace.policy.database_homes();
        for local in tcx.hir_crate_items(()).definitions() {
            let id = local.to_def_id();
            if !matches!(tcx.def_kind(id), DefKind::Fn | DefKind::AssocFn)
                || workspace.is_test_definition(tcx, id)
            {
                continue;
            }
            let Some(path) = workspace.relative_path(tcx, tcx.def_span(id)) else {
                continue;
            };
            if !homes.iter().any(|home| path.starts_with(home.as_str())) {
                continue;
            }
            let owner = type_name(tcx, id);
            let callable = if owner == "<free>" {
                tcx.item_name(id).to_string()
            } else {
                format!("{owner}::{}", tcx.item_name(id))
            };
            let output =
                tcx.fn_sig(id).instantiate_identity().skip_norm_wip().output().skip_binder();
            let mut returned = Vec::new();
            for named in mentions(tcx, output) {
                if self.is_raw(named) && !returned.contains(&named) {
                    returned.push(named);
                }
            }
            for raw in returned {
                self.emit(
                    OWNER_HANDS_OUT_DEPENDENCY,
                    id,
                    tcx.def_span(id),
                    format!(
                        "`{}` returns retained dependency `{}`",
                        tcx.def_path_str(id),
                        tcx.def_path_str(raw)
                    ),
                    REMEDY,
                );
            }
            if !tcx.visibility(id).is_public()
                || workspace.policy.allows_raw_handle_parameter(&path, &callable)
            {
                continue;
            }
            for raw in self.raw_parameters(id) {
                self.emit(
                    OWNER_HANDS_OUT_DEPENDENCY,
                    id,
                    tcx.def_span(id),
                    format!(
                        "`{}` accepts raw dependency `{}`",
                        tcx.def_path_str(id),
                        tcx.def_path_str(raw)
                    ),
                    REMEDY,
                );
            }
        }
    }
}

/// Whether `ty` holds a reference or a raw pointer anywhere in it.
fn borrows(ty: Ty<'_>) -> bool {
    ty.walk().any(|arg| {
        arg.as_type().is_some_and(|ty| matches!(ty.kind(), ty::Ref(..) | ty::RawPtr(..)))
    })
}

/// Whether code outside the module that declares `id` can name it.
fn crosses_module(tcx: TyCtxt<'_>, id: DefId) -> bool {
    match tcx.visibility(id) {
        Visibility::Public => true,
        Visibility::Restricted(module) => {
            let declared = std::iter::successors(tcx.opt_parent(id), |&item| tcx.opt_parent(item))
                .find(|&item| tcx.def_kind(item) == DefKind::Mod);
            declared != Some(module.to_def_id())
        }
    }
}
