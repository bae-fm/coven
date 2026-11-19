//! The owner rules of Appendix F.3 the workspace's policy selects, on the
//! compiler's types (`owners.rs`), in one pass that shares their model.
//!
//! `clippy::built_outside_composition_root` (the policy's
//! `capability-construction`): owners, construction-only capabilities and
//! handles are built only at the policy's composition roots. Production code
//! builds one where it writes the type's literal or constructor, and where it
//! calls, or names as a value, a function whose declared result holds one by
//! value. A trait method's call is its impl's, as `Default::default()` is the
//! `Default` impl of the type it makes, and a clone builds nothing: it is one
//! more share of what the value holds. A generic type is an owner by what it
//! holds itself: what it is given was built where that value was. Elsewhere
//! than a root:
//!
//! - a method taking `self` may give out a capability or a handle, which it
//!   derives from its owner or made under the rules here, so a call of one
//!   builds only the owners its result holds, and one that takes its value
//!   whole and gives back one of its own type, as a builder's step does,
//!   builds none;
//! - a function with no `self` whose result holds a capability or a handle
//!   is that value's factory, and so is a method the policy's
//!   `capability_factories` lists for its product: its body may build what
//!   it returns, and its callers answer for the value;
//! - a handle's owner, in its own methods, makes its handle, and a handle
//!   makes another, as a clone does;
//! - a `Clone` impl that clones each field of its own, as `derive(Clone)`
//!   writes it, shares what the value holds and builds nothing.
//!
//! A static is shared by all the code that names it, so one that holds
//! shared state or a lock, by itself or inside what it holds, is a
//! composition root the policy lists. Test code answers to none of this,
//! test-only statements and fields in production code included.

use owner_construction_policy::{FILE_NAME, Rule};
use rustc_data_structures::fx::FxHashMap;
use rustc_hir::def::{CtorKind, DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::{Expr, ExprKind, HirId, Item, ItemKind, find_attr};
use rustc_lint::{LateContext, LateLintPass, declare_tool_lint, impl_lint_pass};
use rustc_middle::ty::{self, GenericArgsRef, Instance, InstanceKind, Ty, TyCtxt};
use rustc_span::{Span, sym};

use crate::finding::Finding;
use crate::owners::{Held, Owners, Part};
use crate::workspace_policy::WorkspacePolicy;

declare_tool_lint! {
    /// An owner, a construction-only capability or a handle built outside
    /// the policy's composition roots, or a static that shares state and is
    /// no root.
    pub clippy::BUILT_OUTSIDE_COMPOSITION_ROOT,
    Warn,
    "an owner, capability or handle is built outside a composition root",
    report_in_external_macro: true
}

/// The pass: the model, and what it knows of each body's callable.
pub(crate) struct OwnerRules<'tcx> {
    owners: Option<Owners<'tcx>>,
    scopes: FxHashMap<LocalDefId, Scope>,
}

impl_lint_pass!(OwnerRules<'_> => [BUILT_OUTSIDE_COMPOSITION_ROOT]);

/// What a callable may build.
#[derive(Clone)]
enum Scope {
    /// A composition root: anything.
    Root,
    /// Any other callable: the type of its impl, for a method or an
    /// associated function; the capabilities and handles it is the factory
    /// of; and whether it is a `clone` that clones each field.
    Callable { callable: DefId, impl_type: Option<DefId>, products: Vec<DefId>, clone: bool },
    /// Test code: anything.
    Test,
}

impl<'tcx> OwnerRules<'tcx> {
    pub(crate) fn new(tcx: TyCtxt<'tcx>, workspace: Option<&'static WorkspacePolicy>) -> Self {
        let owners = workspace
            .filter(|workspace| {
                workspace.policy.selects(Rule::CapabilityConstruction)
                    && !workspace.compiles_a_tool()
            })
            .map(|workspace| Owners::new(tcx, workspace));
        Self { owners, scopes: FxHashMap::default() }
    }
}

/// The type an impl is for, when it is an ADT.
fn impl_type(tcx: TyCtxt<'_>, callable: DefId) -> Option<DefId> {
    let implementation = tcx.impl_of_assoc(callable)?;
    tcx.type_of(implementation)
        .instantiate_identity()
        .skip_norm_wip()
        .ty_adt_def()
        .map(|adt| adt.did())
}

/// How the policy names a callable's type: its impl's type, or its trait for
/// a trait's own method, or `<free>`.
fn type_name(tcx: TyCtxt<'_>, callable: DefId) -> String {
    if let Some(implemented) = impl_type(tcx, callable) {
        return tcx.item_name(implemented).to_string();
    }
    match tcx.trait_of_assoc(callable) {
        Some(declaring) => tcx.item_name(declaring).to_string(),
        None => "<free>".to_string(),
    }
}

impl<'tcx> OwnerRules<'tcx> {
    /// Whether the policy lists `callable` as a composition root.
    fn is_root(owners: &Owners<'tcx>, tcx: TyCtxt<'tcx>, callable: DefId) -> bool {
        let Some(path) = owners.workspace.relative_path(tcx, tcx.def_span(callable)) else {
            return false;
        };
        let name = tcx.item_name(callable);
        owners.workspace.policy.is_composition_root(&path, &type_name(tcx, callable), name.as_str())
    }

    /// The scope of the code in the body `owner` holds: its callable's, a
    /// closure's being the function it is written in; a const's or static's
    /// inside a root, the root's.
    fn scope(&mut self, cx: &LateContext<'tcx>, owner: LocalDefId) -> Scope {
        if let Some(scope) = self.scopes.get(&owner) {
            return scope.clone();
        }
        let tcx = cx.tcx;
        let owners = self.owners.as_ref().expect("a scope is asked for only while the rule runs");
        let callable = tcx.typeck_root_def_id(owner.to_def_id());
        let scope = if owners.workspace.is_test_code(tcx, tcx.local_def_id_to_hir_id(owner)) {
            Scope::Test
        } else if Self::is_root(owners, tcx, callable)
            || (matches!(tcx.def_kind(callable), DefKind::Const | DefKind::Static { .. })
                && tcx
                    .opt_local_parent(callable.expect_local())
                    .and_then(|parent| {
                        std::iter::successors(Some(parent), |item| tcx.opt_local_parent(*item))
                            .find(|item| {
                                matches!(tcx.def_kind(*item), DefKind::Fn | DefKind::AssocFn)
                            })
                    })
                    .is_some_and(|function| Self::is_root(owners, tcx, function.to_def_id())))
        {
            Scope::Root
        } else {
            let products = if matches!(tcx.def_kind(callable), DefKind::Fn | DefKind::AssocFn) {
                factory_products(owners, tcx, callable)
            } else {
                Vec::new()
            };
            Scope::Callable {
                callable,
                impl_type: impl_type(tcx, callable),
                products,
                clone: clones_each_field(cx, callable),
            }
        };
        self.scopes.insert(owner, scope.clone());
        scope
    }

    /// Reports `built`, a value of type `built` that the code at `id`, in the
    /// body `owner`, builds at `span`. `receiver` says it is the result of a
    /// method taking `self`.
    fn built(
        &mut self,
        cx: &LateContext<'tcx>,
        owner: LocalDefId,
        id: HirId,
        span: Span,
        built: Ty<'tcx>,
        receiver: bool,
    ) {
        let ty::Adt(adt, _) = *built.kind() else {
            return;
        };
        let what = adt.did();
        let tcx = cx.tcx;
        // What a generic type holds of what it is given is built where that
        // value is: the type is an owner by what it holds itself.
        let args = ty::GenericArgs::identity_for_item(tcx, what);
        let built = Ty::new_adt(tcx, adt, args);
        let scope = self.scope(cx, owner);
        let Scope::Callable { callable, impl_type, products, clone } = scope else {
            return;
        };
        let owners = self.owners.as_ref().expect("the rule runs");
        let rows = &owners.rows;
        let part = Part { id: what, args: Some(args), shared: false, borrowed: false };
        let is_owner = !rows.tasks.holds(tcx, &part)
            && !rows.held_capability(tcx, &part)
            && owners.is_owner(built);
        let capability =
            rows.construction_only.holds(tcx, &part) || rows.handles.contains_key(&what);
        if !is_owner && !capability {
            return;
        }
        let made_by_maker = rows
            .handles
            .get(&what)
            .is_some_and(|makers| impl_type.is_some_and(|ty| makers.contains(&ty) || ty == what));
        let cloned = clone && impl_type == Some(what);
        if made_by_maker || cloned || (!is_owner && (receiver || products.contains(&what))) {
            return;
        }
        // Code under `cfg(test)` inside production code, as a test-only
        // statement or field, is test code too.
        if owners.workspace.is_test_code(tcx, id) {
            return;
        }
        let kind = if is_owner {
            "owner"
        } else if rows.handles.contains_key(&what) {
            "handle"
        } else {
            "capability"
        };
        cx.tcx.emit_node_span_lint(
            BUILT_OUTSIDE_COMPOSITION_ROOT,
            id,
            span.source_callsite(),
            Finding {
                message: format!(
                    "`{}` builds {kind} `{}` outside a composition root",
                    cx.tcx.def_path_str(callable),
                    cx.tcx.def_path_str(what)
                ),
                help: "build owners, construction-only capabilities and handles at the policy's \
                       composition roots, and pass them in elsewhere",
            },
        );
    }

    /// The values a call of `callee` with `args` builds: what its declared
    /// result holds by value, the impl's for a trait method `args` resolve.
    fn call_builds(
        owners: &Owners<'tcx>,
        cx: &LateContext<'tcx>,
        callee: DefId,
        args: GenericArgsRef<'tcx>,
    ) -> Vec<Ty<'tcx>> {
        let tcx = cx.tcx;
        // A clone is one more share of what the value holds, and builds
        // nothing.
        if args.len() != tcx.generics_of(callee).count()
            || (tcx.trait_of_assoc(callee).is_some()
                && tcx.trait_of_assoc(callee) == tcx.get_diagnostic_item(sym::Clone))
        {
            return Vec::new();
        }
        let callee = if tcx.trait_of_assoc(callee).is_some()
            && let Ok(Some(instance)) = Instance::try_resolve(tcx, cx.typing_env(), callee, args)
            && let InstanceKind::Item(implemented) = instance.def
            && matches!(tcx.def_kind(implemented), DefKind::Fn | DefKind::AssocFn)
        {
            implemented
        } else {
            callee
        };
        let declared =
            tcx.fn_sig(callee).instantiate_identity().skip_norm_wip().output().skip_binder();
        // A method that takes its value whole and gives back one of its own
        // type, as a builder's step does, hands on what it took.
        let passed_on =
            tcx.opt_associated_item(callee).filter(|item| item.is_method()).and_then(|_| {
                let receiver = tcx.fn_sig(callee).instantiate_identity().skip_norm_wip().input(0);
                receiver.skip_binder().ty_adt_def().map(|adt| adt.did())
            });
        let mut built = Vec::new();
        owners.walk(declared, false, false, &mut |held| {
            if let Held::Part(part) = held
                && !part.borrowed
                && passed_on != Some(part.id)
                && let Some(part_args) = part.args
            {
                built.push(Ty::new_adt(tcx, tcx.adt_def(part.id), part_args));
            }
        });
        built
    }
}

/// The capabilities and handles a function or method may build as its
/// factory: those its declared result holds by value, for a function with no
/// `self`, or for a method the policy lists as their factory.
fn factory_products<'tcx>(owners: &Owners<'tcx>, tcx: TyCtxt<'tcx>, callable: DefId) -> Vec<DefId> {
    let rows = &owners.rows;
    let declared =
        tcx.fn_sig(callable).instantiate_identity().skip_norm_wip().output().skip_binder();
    let method = tcx.opt_associated_item(callable).is_some_and(|item| item.is_method());
    let listed = |product: DefId| {
        let Some(path) = owners.workspace.relative_path(tcx, tcx.def_span(callable)) else {
            return false;
        };
        let owner = type_name(tcx, callable);
        let name = tcx.item_name(callable);
        owners.workspace.policy.capability_factories.iter().zip(&rows.factory_products).any(
            |(factory, products)| {
                factory.path == path
                    && factory.owner == owner
                    && factory.method == name.as_str()
                    && products.contains(&product)
            },
        )
    };
    let mut products = Vec::new();
    owners.walk(declared, false, false, &mut |held| {
        if let Held::Part(part) = held
            && !part.borrowed
            && (rows.construction_only.holds(tcx, &part) || rows.handles.contains_key(&part.id))
            && (!method || listed(part.id))
        {
            products.push(part.id);
        }
    });
    products
}

/// Whether `callable` is the `clone` of a `Clone` impl that clones each field
/// of its own: one `derive(Clone)` writes, or one written as it would be,
/// `Self { field: self.field.clone(), .. }`.
fn clones_each_field(cx: &LateContext<'_>, callable: DefId) -> bool {
    let tcx = cx.tcx;
    let Some(implementation) = tcx.impl_of_assoc(callable) else {
        return false;
    };
    let clone_trait = tcx.get_diagnostic_item(sym::Clone);
    if tcx.impl_opt_trait_id(implementation) != clone_trait || clone_trait.is_none() {
        return false;
    }
    if find_attr!(tcx, implementation, AutomaticallyDerived) {
        return true;
    }
    let Some(local) = callable.as_local() else {
        return false;
    };
    let Some(body) = tcx.hir_maybe_body_owned_by(local) else {
        return false;
    };
    let typeck = tcx.typeck(local);
    let value = match body.value.kind {
        ExprKind::Block(block, _) if block.stmts.is_empty() => block.expr,
        _ => Some(body.value),
    };
    let Some(Expr { kind: ExprKind::Struct(_, fields, rustc_hir::StructTailExpr::None), .. }) =
        value
    else {
        return false;
    };
    fields.iter().all(|field| {
        let ExprKind::MethodCall(method, receiver, [], _) = field.expr.kind else {
            return false;
        };
        let ExprKind::Field(base, name) = receiver.kind else {
            return false;
        };
        method.ident.name == sym::clone
            && typeck
                .type_dependent_def_id(field.expr.hir_id)
                .is_some_and(|id| tcx.trait_of_assoc(id) == clone_trait)
            && name == field.ident
            && matches!(base.kind, ExprKind::Path(rustc_hir::QPath::Resolved(None, path))
                if matches!(path.res, Res::Local(_)) && path.segments.len() == 1
                    && path.segments[0].ident.name == rustc_span::symbol::kw::SelfLower)
    })
}

/// Stops the compile at each capability factory row whose file is in the
/// crate's package and declares no such method: a method of the named type,
/// taking `self`, whose result holds the product by value.
fn check_factory_rows<'tcx>(owners: &Owners<'tcx>, tcx: TyCtxt<'tcx>) {
    let policy = &owners.workspace.policy;
    for (factory, products) in policy.capability_factories.iter().zip(&owners.rows.factory_products)
    {
        if !owners.workspace.package_holds(&factory.path) {
            continue;
        }
        let declared = tcx.hir_crate_items(()).definitions().any(|local| {
            let id = local.to_def_id();
            tcx.def_kind(id) == DefKind::AssocFn
                && tcx.opt_associated_item(id).is_some_and(|item| item.is_method())
                && tcx.item_name(id).as_str() == factory.method
                && type_name(tcx, id) == factory.owner
                && owners.workspace.relative_path(tcx, tcx.def_span(id)).as_deref()
                    == Some(factory.path.as_str())
                && {
                    let output = tcx
                        .fn_sig(id)
                        .instantiate_identity()
                        .skip_norm_wip()
                        .output()
                        .skip_binder();
                    let mut holds = false;
                    owners.walk(output, false, false, &mut |held| {
                        holds |= matches!(held, Held::Part(part)
                            if !part.borrowed && products.contains(&part.id));
                    });
                    holds
                }
        });
        if !declared {
            tcx.dcx().fatal(format!(
                "typed lints: {FILE_NAME}'s capability_factories names {}: {}::{}, which is no \
                 method there that takes `self` and returns {}",
                factory.path, factory.owner, factory.method, factory.product
            ));
        }
    }
}

impl<'tcx> LateLintPass<'tcx> for OwnerRules<'tcx> {
    fn check_crate(&mut self, cx: &LateContext<'tcx>) {
        if let Some(owners) = &self.owners {
            check_factory_rows(owners, cx.tcx);
        }
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let Some(owners) = self.owners.as_ref() else {
            return;
        };
        let Some(body) = cx.enclosing_body else {
            return;
        };
        let tcx = cx.tcx;
        let owner = tcx.hir_body_owner_def_id(body);
        let typeck = cx.typeck_results();
        let mut builds = Vec::new();
        match expr.kind {
            ExprKind::Struct(..) => builds.push((typeck.expr_ty(expr), false)),
            ExprKind::Path(ref qpath) => match typeck.qpath_res(qpath, expr.hir_id) {
                Res::Def(DefKind::Ctor(_, CtorKind::Const), _) => {
                    builds.push((typeck.expr_ty(expr), false));
                }
                Res::Def(DefKind::Ctor(_, CtorKind::Fn), ctor) => {
                    let args = typeck.node_args(expr.hir_id);
                    let output = tcx.fn_sig(ctor).instantiate(tcx, args).skip_norm_wip().output();
                    builds.push((output.skip_binder(), false));
                }
                Res::Def(DefKind::Fn | DefKind::AssocFn, callee) => {
                    let receiver =
                        tcx.opt_associated_item(callee).is_some_and(|item| item.is_method());
                    let args = typeck.node_args(expr.hir_id);
                    builds.extend(
                        Self::call_builds(owners, cx, callee, args)
                            .into_iter()
                            .map(|ty| (ty, receiver)),
                    );
                }
                _ => {}
            },
            ExprKind::MethodCall(..) => {
                if let Some(callee) = typeck.type_dependent_def_id(expr.hir_id) {
                    let args = typeck.node_args(expr.hir_id);
                    builds.extend(
                        Self::call_builds(owners, cx, callee, args)
                            .into_iter()
                            .map(|ty| (ty, true)),
                    );
                }
            }
            _ => {}
        }
        for (built, receiver) in builds {
            self.built(cx, owner, expr.hir_id, expr.span, built, receiver);
        }
    }

    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        let Some(owners) = self.owners.as_ref() else {
            return;
        };
        let ItemKind::Static(_, ident, ..) = item.kind else {
            return;
        };
        let tcx = cx.tcx;
        let id = item.owner_id.to_def_id();
        if owners.workspace.is_test_code(tcx, item.hir_id()) || Self::is_root(owners, tcx, id) {
            return;
        }
        let rows = &owners.rows;
        let mut state = Vec::new();
        let mut pending = vec![tcx.type_of(id).instantiate_identity().skip_norm_wip()];
        let mut seen = rustc_data_structures::fx::FxHashSet::default();
        while let Some(held) = pending.pop() {
            if !seen.insert(held) {
                continue;
            }
            owners.walk(held, false, false, &mut |part| {
                let Held::Part(part) = part else {
                    return;
                };
                if rows.shared_state.holds(tcx, &part) || rows.private_state.holds(tcx, &part) {
                    state.push(part.id);
                } else if let Some(args) = part.args
                    && owners.is_workspace_item(part.id)
                {
                    let ty = tcx.type_of(part.id).instantiate(tcx, args).skip_norm_wip();
                    owners.field_parts(ty, &mut |field| {
                        if rows.shared_state.holds(tcx, &field)
                            || rows.private_state.holds(tcx, &field)
                        {
                            state.push(field.id);
                        } else if let Some(args) = field.args
                            && owners.is_workspace_item(field.id)
                        {
                            pending
                                .push(tcx.type_of(field.id).instantiate(tcx, args).skip_norm_wip());
                        }
                    });
                }
            });
        }
        state.sort_by_key(|id| tcx.def_path_str(*id));
        state.dedup();
        for shared in state {
            cx.tcx.emit_node_span_lint(
                BUILT_OUTSIDE_COMPOSITION_ROOT,
                item.hir_id(),
                ident.span,
                Finding {
                    message: format!(
                        "static `{ident}` shares `{}` outside a composition root",
                        tcx.def_path_str(shared)
                    ),
                    help: "a static shares what it holds with all the code that names it: list it \
                           as a composition root when that scope, the process or a thread local's \
                           thread, is the state's own, or give the state to an owner",
                },
            );
        }
    }
}
