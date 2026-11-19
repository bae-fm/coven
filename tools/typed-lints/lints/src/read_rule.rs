//! The read rule's typed lints: a read does no work SQL does. Each shape of
//! such work is a lint of its own, so a mark names the shape it keeps, and a
//! mark covers every finding of its shape in the item it sits on.
//!
//! Read code, from the policy's `read_shapes` rows:
//!
//! - every function or closure whose inputs name a read type, through
//!   references, wrappers and a generic's bounds, but not through a
//!   callback, which receives nothing: `impl Fn(&SqlReadContext)` is a
//!   callback given one. A live query counts only in a method of a type that
//!   holds one, through its fields at any depth: elsewhere code wraps or
//!   passes the query on and reads nothing;
//! - every method taking `self` of a type that holds a live query;
//! - every closure or function code passes to a method of a read type, a
//!   live query or a type that holds one, and every one it passes to a call
//!   beside a value whose type names a read type or a live query
//!   (`detail_read(query, resolve)`);
//! - every function of the crate that read code calls or passes on.
//!
//! Test code and the files the policy lists in `plumbing` are no read code.
//! A closure inside read code is part of it. What only a live query's
//! `process` runs, the closures and functions code passes to it and what
//! they reach, and no other read code reaches, may sort, filter, group,
//! count and pick among the one result `process` passes it, for display: the
//! lints find nothing there. A function other crates can call is never
//! counted so, since this compile can't see their calls.
//!
//! The shapes of row work in read code are in `read_rule/row_work.rs`, the
//! statements read code runs in `read_rule/statements.rs`, what rows are in
//! `read_rule/rows.rs`, and what flows from a read's result in
//! `read_rule/flow.rs`.

mod flow;
mod row_work;
mod rows;
mod statements;

use clippy_utils::paths::PathNS;
use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass, declare_tool_lint, impl_lint_pass};
use rustc_middle::ty::{self, Instance, InstanceKind, Ty, TyCtxt, TypingEnv};

use crate::owner_rules::impl_type;
use crate::workspace_bodies::earlier_definitions;
use crate::workspace_policy::{WorkspaceCrates, WorkspacePolicy, resolve_row};
use owner_construction_policy::ReadShapes;
use rows::{Mappers, RowTypes};

/// The crates whose functions do work SQL does, and pass on what they are
/// given.
const LIBRARIES: [&str; 5] = ["core", "alloc", "std", "itertools", "indexmap"];

/// Whether `id` is defined in the standard library, itertools or indexmap.
fn is_library(tcx: TyCtxt<'_>, id: DefId) -> bool {
    LIBRARIES.contains(&tcx.crate_name(id.krate).as_str())
}

/// Whether `ty` is, or holds through its type arguments, references,
/// pointers, slices, arrays or tuples, a struct, enum or union that `test`
/// picks: no callback, which holds none of what it is given.
fn names_adt(ty: Ty<'_>, test: &impl Fn(DefId) -> bool) -> bool {
    match ty.kind() {
        ty::Adt(adt, args) => test(adt.did()) || args.types().any(|inner| names_adt(inner, test)),
        ty::Ref(_, inner, _) | ty::RawPtr(inner, _) | ty::Slice(inner) | ty::Array(inner, _) => {
            names_adt(*inner, test)
        }
        ty::Tuple(items) => items.iter().any(|inner| names_adt(inner, test)),
        _ => false,
    }
}

/// The closure or function an argument is, when it is code a callee may run:
/// a closure, or the path of a function.
fn callback(typeck: &ty::TypeckResults<'_>, argument: &Expr<'_>) -> Option<DefId> {
    match argument.kind {
        ExprKind::Closure(closure) => Some(closure.def_id.to_def_id()),
        ExprKind::AddrOf(_, _, inner) => callback(typeck, inner),
        ExprKind::Path(ref qpath) => match typeck.qpath_res(qpath, argument.hir_id) {
            Res::Def(DefKind::Fn | DefKind::AssocFn, id) => Some(id),
            _ => None,
        },
        _ => None,
    }
}

macro_rules! shape_lints {
    ($($lint:ident, $what:literal;)*) => {
        $(
            declare_tool_lint! {
                #[doc = $what]
                pub clippy::$lint,
                Warn,
                $what,
                report_in_external_macro: true
            }
        )*

        /// The read rule's lints, one for each shape.
        pub(crate) const READ_RULE_LINTS: &[&rustc_lint::Lint] = &[$($lint),*];
    };
}

shape_lints! {
    READ_SORT, "read code sorts a read's rows";
    READ_FILTER, "read code filters a read's rows";
    READ_GROUP, "read code groups a read's rows";
    READ_AGGREGATE, "read code counts, sums or picks the greatest of a read's rows";
    READ_PICK, "read code picks among a read's rows";
    READ_JOIN, "read code joins a read's rows to another's by a key or place";
    READ_LENGTH_BRANCH, "read code branches on how many rows a read returned";
    READ_DROP_ROW, "read code drops a row when an optional column is empty";
    READ_QUERY_PER_ROW, "read code runs a statement once for each row";
    READ_KEYED_STATEMENT, "read code runs a statement keyed by what an earlier one returned";
}

/// The lint's pass: the policy's read rule, when the workspace has one.
pub(crate) struct ReadRule {
    setup: Option<Setup>,
}

impl_lint_pass!(ReadRule => [
    READ_SORT, READ_FILTER, READ_GROUP, READ_AGGREGATE, READ_PICK, READ_JOIN,
    READ_LENGTH_BRANCH, READ_DROP_ROW, READ_QUERY_PER_ROW, READ_KEYED_STATEMENT
]);

/// The read rule's rows, resolved in the compile.
struct Setup {
    workspace: &'static WorkspacePolicy,
    rows: &'static ReadShapes,
    /// The help a row work finding gives: the rule, and what to do instead.
    row_work_help: &'static str,
    /// The help a statement's finding gives.
    statement_help: &'static str,
    read_types: FxHashSet<DefId>,
    live_types: FxHashSet<DefId>,
    statement_rows: FxHashSet<DefId>,
    column_values: FxHashSet<DefId>,
}

impl ReadRule {
    pub(crate) fn new(tcx: TyCtxt<'_>, workspace: Option<&'static WorkspacePolicy>) -> Self {
        let setup = workspace.and_then(|workspace| {
            let rows = workspace.policy.read_shapes.as_ref()?;
            let resolve = |row: &str, paths: &std::collections::BTreeSet<String>| {
                paths
                    .iter()
                    .flat_map(|path| resolve_row(tcx, row, path, PathNS::Type))
                    .collect::<FxHashSet<_>>()
            };
            let live_types = resolve("read_shapes.live_query_types", &rows.live_query_types);
            let mut read_types = resolve("read_shapes.read_types", &rows.read_types);
            read_types.extend(live_types.iter().copied());
            Some(Setup {
                workspace,
                rows,
                row_work_help: Box::leak(
                    format!(
                        "{}: the statement filters, sorts, joins, groups, counts and sums, in the \
                         same query or its own live query beside it; a read returns what the \
                         screen shows",
                        rows.rule
                    )
                    .into_boxed_str(),
                ),
                statement_help: Box::leak(
                    format!(
                        "{}: one statement reads what the read needs, with a join or a \
                         subquery, instead of a statement for each row or for each key an \
                         earlier statement returned",
                        rows.rule
                    )
                    .into_boxed_str(),
                ),
                read_types,
                live_types,
                statement_rows: resolve("read_shapes.statement_rows", &rows.statement_rows),
                column_values: resolve("read_shapes.column_values", &rows.column_values),
            })
        });
        Self { setup }
    }
}

impl<'tcx> LateLintPass<'tcx> for ReadRule {
    fn check_crate(&mut self, cx: &LateContext<'tcx>) {
        let Some(setup) = &self.setup else {
            return;
        };
        let tcx = cx.tcx;
        let mappers = Mappers {
            tcx,
            statement_rows: &setup.statement_rows,
            column_values: &setup.column_values,
        };
        let rows = RowTypes::of(
            &mappers,
            tcx.hir_body_owners()
                .map(LocalDefId::to_def_id)
                .chain(earlier_definitions(setup.workspace, tcx, "clippy::read_sort"))
                .filter(|&id| !setup.workspace.is_test_definition(tcx, id)),
        );
        let search = Search::new(setup, tcx);
        let code = search.read_code();
        for &unit in &code.top {
            row_work::check(tcx, setup.row_work_help, &rows, &mappers, unit, &code.process_only);
            statements::check(&search, setup.statement_help, &mappers, unit, &code.process_only);
        }
    }
}

/// The crate's read code: the units to check, none inside another, and the
/// closures and functions only a live query's `process` runs.
struct ReadCode {
    top: Vec<LocalDefId>,
    process_only: FxHashSet<LocalDefId>,
}

/// What the search for read code knows of the crate.
struct Search<'s, 'tcx> {
    setup: &'s Setup,
    tcx: TyCtxt<'tcx>,
    /// The functions and closures that may be read code: production code
    /// outside the plumbing.
    candidates: FxHashSet<LocalDefId>,
    /// The types that hold a live query, by definition, as found so far.
    holders: std::cell::RefCell<FxHashMap<DefId, bool>>,
    workspace_crates: WorkspaceCrates<'s>,
}

impl<'s, 'tcx> Search<'s, 'tcx> {
    fn new(setup: &'s Setup, tcx: TyCtxt<'tcx>) -> Self {
        let candidates = tcx
            .hir_body_owners()
            .filter(|&id| {
                matches!(tcx.def_kind(id), DefKind::Fn | DefKind::AssocFn | DefKind::Closure)
            })
            .filter(|&id| !setup.workspace.is_test_code(tcx, tcx.local_def_id_to_hir_id(id)))
            .filter(|&id| {
                setup
                    .workspace
                    .relative_path(tcx, tcx.def_span(id))
                    .is_some_and(|path| !setup.rows.is_plumbing(&path))
            })
            .collect();
        Self {
            setup,
            tcx,
            candidates,
            holders: std::cell::RefCell::default(),
            workspace_crates: WorkspaceCrates::new(setup.workspace),
        }
    }

    fn read_code(&self) -> ReadCode {
        let tcx = self.tcx;
        let mut seeds = Vec::new();
        let mut process_seeds = Vec::new();
        let mut edges: FxHashMap<LocalDefId, Vec<LocalDefId>> = FxHashMap::default();
        for &body in &self.candidates {
            if self.takes_read(body) || self.is_holder_method(body) {
                seeds.push(body);
            }
            let found = self.walk(body);
            seeds.extend(found.passed);
            process_seeds.extend(found.processed);
            edges.insert(body, found.reached);
        }
        let mut children: FxHashMap<LocalDefId, Vec<LocalDefId>> = FxHashMap::default();
        for &body in &self.candidates {
            if tcx.def_kind(body) == DefKind::Closure {
                children.entry(tcx.local_parent(body)).or_default().push(body);
            }
        }
        let reach = |from: Vec<LocalDefId>| {
            let mut reached = FxHashSet::default();
            let mut pending = from;
            while let Some(unit) = pending.pop() {
                if !self.candidates.contains(&unit) || !reached.insert(unit) {
                    continue;
                }
                let mut inside = vec![unit];
                while let Some(body) = inside.pop() {
                    pending.extend(edges.get(&body).into_iter().flatten().copied());
                    inside.extend(children.get(&body).into_iter().flatten().copied());
                }
            }
            reached
        };
        let general = reach(seeds);
        let processed = reach(process_seeds);
        let exported = tcx.effective_visibilities(());
        let process_only = processed
            .into_iter()
            .filter(|unit| !general.contains(unit) && !exported.is_exported(*unit))
            .collect::<FxHashSet<_>>();
        let inside_unit = |unit: LocalDefId| {
            std::iter::successors(tcx.opt_local_parent(unit), |&item| tcx.opt_local_parent(item))
                .any(|item| general.contains(&item))
        };
        let mut top =
            general.iter().copied().filter(|&unit| !inside_unit(unit)).collect::<Vec<_>>();
        top.sort_by_key(|unit| tcx.def_span(*unit));
        ReadCode { top, process_only }
    }

    /// Whether a function or closure takes a read value: its inputs name a
    /// read type, or a live query in a method of a type that holds one.
    fn takes_read(&self, body: LocalDefId) -> bool {
        let tcx = self.tcx;
        let Some(signature) = rows::signature(tcx, body.to_def_id()) else {
            return false;
        };
        let inputs = signature.skip_binder().inputs().to_vec();
        let root = tcx.typeck_root_def_id(body.to_def_id());
        let bounds = tcx.param_env(root);
        let holder_method = tcx
            .opt_associated_item(root)
            .and_then(|_| impl_type(tcx, root))
            .is_some_and(|owner| self.holds_live(owner));
        inputs.into_iter().any(|input| {
            let (read, live) = self.names_read(input, bounds);
            read || (live && holder_method)
        })
    }

    /// Whether `ty` names a read type that is no live query, and whether it
    /// names a live query, through references, wrappers, trait objects and
    /// the bounds of a generic, but not through a callback.
    fn names_read(&self, ty: Ty<'tcx>, bounds: ty::ParamEnv<'tcx>) -> (bool, bool) {
        let tcx = self.tcx;
        let setup = self.setup;
        let named = |id: DefId| {
            (
                setup.read_types.contains(&id) && !setup.live_types.contains(&id),
                setup.live_types.contains(&id),
            )
        };
        let or = |a: (bool, bool), b: (bool, bool)| (a.0 || b.0, a.1 || b.1);
        match ty.kind() {
            ty::Adt(adt, args) => args
                .types()
                .map(|inner| self.names_read(inner, bounds))
                .fold(named(adt.did()), or),
            ty::Ref(_, inner, _) | ty::RawPtr(inner, _) | ty::Slice(inner) | ty::Array(inner, _) => {
                self.names_read(*inner, bounds)
            }
            ty::Tuple(items) => {
                items.iter().map(|inner| self.names_read(inner, bounds)).fold((false, false), or)
            }
            ty::Dynamic(predicates, ..) => {
                predicates.principal_def_id().map_or((false, false), |principal| {
                    if tcx.fn_trait_kind_from_def_id(principal).is_some() {
                        (false, false)
                    } else {
                        named(principal)
                    }
                })
            }
            ty::Param(param) => bounds
                .caller_bounds()
                .filter_map(|clause| clause.as_trait_clause())
                .filter(|clause| {
                    matches!(clause.self_ty().skip_binder().kind(), ty::Param(bound) if bound == param)
                })
                .map(|clause| named(clause.def_id()))
                .fold((false, false), or),
            _ => (false, false),
        }
    }

    /// Whether a function is a method taking `self` of a type that holds a
    /// live query.
    fn is_holder_method(&self, body: LocalDefId) -> bool {
        let tcx = self.tcx;
        tcx.opt_associated_item(body.to_def_id()).is_some_and(|item| item.is_method())
            && impl_type(tcx, body.to_def_id()).is_some_and(|owner| self.holds_live(owner))
    }

    /// Whether the type `adt` is a live query or holds one, through its
    /// fields at any depth through the workspace's types.
    fn holds_live(&self, adt: DefId) -> bool {
        if self.setup.live_types.contains(&adt) {
            return true;
        }
        if let Some(&known) = self.holders.borrow().get(&adt) {
            return known;
        }
        if !self.is_workspace(adt)
            || !matches!(self.tcx.def_kind(adt), DefKind::Struct | DefKind::Enum | DefKind::Union)
        {
            return false;
        }
        self.holders.borrow_mut().insert(adt, false);
        let tcx = self.tcx;
        let identity = ty::GenericArgs::identity_for_item(tcx, adt);
        let holds = tcx
            .adt_def(adt)
            .all_fields()
            .any(|field| self.type_holds_live(field.ty(tcx, identity).skip_norm_wip()));
        self.holders.borrow_mut().insert(adt, holds);
        holds
    }

    /// Whether a value of `ty` is or holds a live query.
    fn type_holds_live(&self, ty: Ty<'tcx>) -> bool {
        names_adt(ty, &|adt| self.holds_live(adt))
    }

    /// Whether a value of `ty` is a read value or holds a live query: what a
    /// method's callbacks read through.
    fn reads_through(&self, ty: Ty<'tcx>) -> bool {
        let ty = ty.peel_refs();
        match ty.kind() {
            ty::Adt(adt, args) => {
                self.setup.read_types.contains(&adt.did())
                    || self.holds_live(adt.did())
                    || (self.tcx.is_lang_item(adt.did(), rustc_attr_ir::LangItem::OwnedBox)
                        || matches!(
                            self.tcx.get_diagnostic_name(adt.did()),
                            Some(rustc_span::sym::Arc | rustc_span::sym::Rc)
                        ))
                        && self.reads_through(args.type_at(0))
            }
            ty::Dynamic(predicates, ..) => predicates
                .principal_def_id()
                .is_some_and(|principal| self.setup.read_types.contains(&principal)),
            _ => false,
        }
    }

    /// Whether the definition is the workspace's.
    fn is_workspace(&self, id: DefId) -> bool {
        self.workspace_crates.declares(self.tcx, id)
    }

    /// What one body's own code passes and calls: its nested closures are
    /// bodies of their own.
    fn walk(&self, body: LocalDefId) -> Walked {
        let tcx = self.tcx;
        let root = tcx.typeck_root_def_id(body.to_def_id());
        let mut walker = Walker {
            search: self,
            typeck: tcx.typeck(body),
            typing_env: TypingEnv::post_analysis(tcx, root),
            bounds: tcx.param_env(root),
            found: Walked::default(),
        };
        walker.visit_expr(tcx.hir_body_owned_by(body).value);
        walker.found
    }
}

/// What a body passes and calls.
#[derive(Default)]
struct Walked {
    /// The closures and functions it passes to read code's places.
    passed: Vec<LocalDefId>,
    /// The ones it passes to a live query's `process`.
    processed: Vec<LocalDefId>,
    /// The functions of the crate it calls or passes on.
    reached: Vec<LocalDefId>,
}

struct Walker<'w, 's, 'tcx> {
    search: &'w Search<'s, 'tcx>,
    typeck: &'tcx ty::TypeckResults<'tcx>,
    typing_env: TypingEnv<'tcx>,
    /// The bounds of the generics the body sees.
    bounds: ty::ParamEnv<'tcx>,
    found: Walked,
}

impl<'tcx> Walker<'_, '_, 'tcx> {
    /// The closure or crate function an argument is, when it is code a callee
    /// may run.
    fn code(&self, argument: &Expr<'tcx>) -> Option<LocalDefId> {
        callback(self.typeck, argument).and_then(DefId::as_local)
    }

    /// The crate function a call of `callee` with the expression's arguments
    /// runs: a trait method's impl when the arguments say which.
    fn resolved(&self, callee: DefId, expr: &Expr<'tcx>) -> Option<LocalDefId> {
        let tcx = self.search.tcx;
        let args = self.typeck.node_args(expr.hir_id);
        if tcx.trait_of_assoc(callee).is_some()
            && args.len() == tcx.generics_of(callee).count()
            && let Ok(Some(instance)) = Instance::try_resolve(tcx, self.typing_env, callee, args)
            && let InstanceKind::Item(implemented) = instance.def
        {
            return implemented.as_local();
        }
        callee.as_local()
    }
}

impl<'tcx> Visitor<'tcx> for Walker<'_, '_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::MethodCall(segment, receiver, arguments, _) => {
                let through = self.search.reads_through(self.typeck.expr_ty(receiver));
                let process = through
                    && segment.ident.name.as_str() == "process"
                    && self
                        .typeck
                        .expr_ty(receiver)
                        .peel_refs()
                        .ty_adt_def()
                        .is_some_and(|adt| self.search.setup.live_types.contains(&adt.did()));
                let beside = arguments
                    .iter()
                    .chain([receiver])
                    .any(|value| self.names_read_value(self.typeck.expr_ty(value)));
                for argument in arguments {
                    if let Some(code) = self.code(argument) {
                        if process {
                            self.found.processed.push(code);
                        } else if through || beside {
                            self.found.passed.push(code);
                        }
                        self.found.reached.push(code);
                    }
                }
                if let Some(callee) = self.typeck.type_dependent_def_id(expr.hir_id)
                    && let Some(local) = self.resolved(callee, expr)
                {
                    self.found.reached.push(local);
                }
            }
            ExprKind::Call(function, arguments) => {
                let beside =
                    arguments.iter().any(|value| self.names_read_value(self.typeck.expr_ty(value)));
                for argument in arguments {
                    if let Some(code) = self.code(argument) {
                        if beside {
                            self.found.passed.push(code);
                        }
                        self.found.reached.push(code);
                    }
                }
                if let ExprKind::Path(ref qpath) = function.kind
                    && let Res::Def(DefKind::Fn | DefKind::AssocFn, callee) =
                        self.typeck.qpath_res(qpath, function.hir_id)
                    && let Some(local) = self.resolved(callee, function)
                {
                    self.found.reached.push(local);
                }
            }
            ExprKind::Path(_) => {
                if let Some(code) = self.code(expr) {
                    self.found.reached.push(code);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}

impl<'tcx> Walker<'_, '_, 'tcx> {
    /// Whether a value of `ty` is a read value or a live query, which a
    /// callback passed beside it runs on.
    fn names_read_value(&self, ty: Ty<'tcx>) -> bool {
        let (read, live) = self.search.names_read(ty, self.bounds);
        read || live
    }
}
