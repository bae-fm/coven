//! `clippy::task_state_outside_task` (part of the policy's
//! `owner-dependencies`, Appendix F.3): one operation owns its state. A task
//! that puts state behind a new shared pointer in one of its fields, where
//! it is built, makes that state for its operation; any type but a task, or
//! a handle the task makes, that holds the same type through a shared
//! pointer could keep the state past the operation, as a directory clone
//! kept a bootstrap's lock for the life of the handle built from it.
//!
//! The lint knows the value by its type, a workspace type that is state: an
//! owner, or one that holds shared or private state by itself or inside what
//! it holds. A value no one changes, such as a write's list of changes, may
//! outlive the write. It reads where tasks are built in every body of the
//! workspace the compile sees (`workspace_bodies.rs`). A holder and a body
//! that builds the task are reported in one compile: the holder's crate's,
//! at the holder's field, when that crate sees the body; the body's crate's,
//! where the body builds the task, when that one sees the holder; and when
//! neither crate depends on the other, the compile of each first crate that
//! sees both, one that depends on no other crate that does
//! (`crate_graph.rs`), at the holder's field. A mark for that last finding
//! goes on that crate's root. Each compile reports a field and a task once.

use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::CRATE_HIR_ID;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::DefId;
use rustc_lint::{LateContext, declare_tool_lint};
use rustc_middle::mir::{AggregateKind, Body, Operand, Rvalue, StatementKind, TerminatorKind};
use rustc_middle::ty::{self, GenericArgKind, Ty, TyCtxt};
use rustc_span::{Span, sym};

use crate::crate_graph::CrateGraph;
use crate::finding::Finding;
use crate::owners::Owners;
use crate::workspace_bodies::workspace_bodies;

declare_tool_lint! {
    /// A type other than a task, or a handle the task makes, that holds
    /// through a shared pointer the state a task makes for its operation.
    pub clippy::TASK_STATE_OUTSIDE_TASK,
    Warn,
    "a type other than a task holds state a task makes for its operation",
    report_in_external_macro: true
}

const HELP: &str = "a task's state lives as long as its operation: keep it in the task, and \
                    give what outlives the task a plain value or a handle the task makes";

/// Where a task is built with a new shared pointer in a field: the task,
/// the body that builds it and the place in the body.
#[derive(Clone, Copy)]
struct Made {
    task: DefId,
    body: DefId,
    span: Span,
}

/// Reports each holder of the state a task makes for its operation.
pub(crate) fn check<'tcx>(owners: &Owners<'tcx>, cx: &LateContext<'tcx>) {
    let tcx = cx.tcx;
    let mut made = FxHashMap::<DefId, Vec<Made>>::default();
    for body in workspace_bodies(owners, tcx, "clippy::task_state_outside_task") {
        tasks_made_shared(owners, tcx, body, &mut |state, found| {
            made.entry(state).or_default().push(found);
        });
    }
    made.retain(|&state, _| is_state(owners, tcx, state));
    if made.is_empty() {
        return;
    }
    let mut graph = None;
    let mut reported = FxHashSet::default();
    for holder in owners.workspace_types() {
        if owners.rows.tasks.has(tcx, holder, None)
            || owners.workspace.is_test_definition(tcx, holder)
        {
            continue;
        }
        let makers = owners.rows.handles.get(&holder).map(|owner| &owner.types);
        let adt = tcx.adt_def(holder);
        let identity = ty::GenericArgs::identity_for_item(tcx, holder);
        for field in adt.all_fields() {
            for state in pointees(owners, tcx, field.ty(tcx, identity).skip_norm_wip()) {
                for found in made.get(&state).into_iter().flatten() {
                    if makers.is_some_and(|makers| makers.contains(&found.task)) {
                        continue;
                    }
                    let place = if let Some(local) = field.did.as_local() {
                        Some((tcx.local_def_id_to_hir_id(local), tcx.def_span(field.did)))
                    } else if let Some(body) = found.body.as_local() {
                        let callable = tcx.typeck_root_def_id(body.to_def_id()).expect_local();
                        Some((tcx.local_def_id_to_hir_id(callable), found.span))
                    } else {
                        let graph = graph.get_or_insert_with(|| {
                            CrateGraph::read(tcx, owners.workspace, tcx.crates(()).iter().copied())
                        });
                        first_to_see_both(owners, tcx, graph, holder, found.body)
                            .then(|| (CRATE_HIR_ID, tcx.def_span(field.did)))
                    };
                    if let Some((id, span)) = place
                        && reported.insert((field.did, found.task))
                    {
                        let message = format!(
                            "`{}` holds `{}`, which task `{}` makes for its operation",
                            tcx.def_path_str(holder),
                            tcx.def_path_str(state),
                            tcx.def_path_str(found.task)
                        );
                        tcx.emit_node_span_lint(
                            TASK_STATE_OUTSIDE_TASK,
                            id,
                            span.source_callsite(),
                            Finding { message, help: HELP },
                        );
                    }
                }
            }
        }
    }
}

/// Whether the compile is the first to see both the holder `holder` and the
/// body `body`, two other crates' definitions: neither crate sees the other,
/// so neither one's compile reports the pair, and no workspace crate the
/// compile depends on sees both.
fn first_to_see_both(
    owners: &Owners<'_>,
    tcx: TyCtxt<'_>,
    graph: &CrateGraph,
    holder: DefId,
    body: DefId,
) -> bool {
    let (holder, body) = (holder.krate, body.krate);
    if graph.sees(holder, body) || graph.sees(body, holder) {
        return false;
    }
    !tcx.crates(()).iter().any(|&earlier| {
        owners.is_workspace_item(earlier.as_def_id())
            && graph.sees(earlier, holder)
            && graph.sees(earlier, body)
    })
}

/// Whether the workspace type `id` is state: an owner, or a type that holds
/// shared or private state by itself or inside a workspace type it holds.
fn is_state<'tcx>(owners: &Owners<'tcx>, tcx: TyCtxt<'tcx>, id: DefId) -> bool {
    if !matches!(tcx.def_kind(id), DefKind::Struct | DefKind::Enum | DefKind::Union) {
        return false;
    }
    let identity = tcx.type_of(id).instantiate_identity().skip_norm_wip();
    if owners.is_owner(identity) {
        return true;
    }
    let rows = &owners.rows;
    let mut pending = vec![id];
    let mut seen = FxHashSet::default();
    let mut state = false;
    while let Some(current) = pending.pop() {
        if state || !seen.insert(current) {
            continue;
        }
        let ty = tcx.type_of(current).instantiate_identity().skip_norm_wip();
        owners.field_parts(ty, &mut |part| {
            if rows.shared_state.holds(tcx, &part) || rows.private_state.holds(tcx, &part) {
                state = true;
            } else if owners.is_workspace_item(part.id)
                && matches!(tcx.def_kind(part.id), DefKind::Struct | DefKind::Enum | DefKind::Union)
            {
                pending.push(part.id);
            }
        });
    }
    state
}

/// The workspace types `ty` holds through one of the policy's shared
/// pointers: each pointer's own type argument.
fn pointees<'tcx>(owners: &Owners<'tcx>, tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> Vec<DefId> {
    let mut found = Vec::new();
    for part in ty.walk() {
        let GenericArgKind::Type(part) = part.kind() else {
            continue;
        };
        let ty::Adt(pointer, args) = *part.kind() else {
            continue;
        };
        if !owners.rows.pointers.has(tcx, pointer.did(), Some(args)) {
            continue;
        }
        if let Some(pointee) = args.types().next()
            && let ty::Adt(pointee, _) = *pointee.kind()
            && owners.is_workspace_item(pointee.did())
        {
            found.push(pointee.did());
        }
    }
    found
}

/// Each task the body `id` builds with a new shared pointer in a field: what
/// the field points at, with the task. A new pointer is the result of a
/// pointer's `new`, `new_cyclic` or `from`, the same value moved or copied,
/// or one inside a `Some` or an `Ok`.
fn tasks_made_shared<'tcx>(
    owners: &Owners<'tcx>,
    tcx: TyCtxt<'tcx>,
    id: DefId,
    found: &mut dyn FnMut(DefId, Made),
) {
    let body: &Body<'tcx> = tcx.optimized_mir(id);
    let pointer = |ty: Ty<'tcx>| match *ty.kind() {
        ty::Adt(adt, args) => owners.rows.pointers.has(tcx, adt.did(), Some(args)),
        _ => false,
    };
    let mut fresh = FxHashSet::default();
    loop {
        let before = fresh.len();
        for block in body.basic_blocks.iter() {
            for statement in &block.statements {
                let StatementKind::Assign(assign) = &statement.kind else {
                    continue;
                };
                let (place, value) = &**assign;
                let Some(local) = place.as_local() else {
                    continue;
                };
                let carries = match value {
                    Rvalue::Use(operand, _) => is_fresh(&fresh, operand),
                    Rvalue::Aggregate(kind, operands) => match **kind {
                        AggregateKind::Adt(adt, variant, ..) if tcx.adt_def(adt).is_enum() => {
                            let name = tcx.adt_def(adt).variant(variant).name;
                            matches!(name, sym::Some | sym::Ok)
                                && operands.iter().any(|operand| is_fresh(&fresh, operand))
                        }
                        _ => false,
                    },
                    _ => false,
                };
                if carries {
                    fresh.insert(local);
                }
            }
            if let Some(terminator) = &block.terminator
                && let TerminatorKind::Call { func, destination, .. } = &terminator.kind
                && let Some((callee, _)) = func.const_fn_def()
                && matches!(tcx.item_name(callee).as_str(), "new" | "new_cyclic" | "from")
                && pointer(destination.ty(body, tcx).ty)
                && let Some(local) = destination.as_local()
            {
                fresh.insert(local);
            }
        }
        if fresh.len() == before {
            break;
        }
    }
    for block in body.basic_blocks.iter() {
        for statement in &block.statements {
            let StatementKind::Assign(assign) = &statement.kind else {
                continue;
            };
            let Rvalue::Aggregate(kind, operands) = &assign.1 else {
                continue;
            };
            let AggregateKind::Adt(task, variant, args, ..) = **kind else {
                continue;
            };
            if !owners.rows.tasks.has(tcx, task, Some(args)) {
                continue;
            }
            let fields = &tcx.adt_def(task).variant(variant).fields;
            for (field, operand) in fields.iter().zip(operands.iter()) {
                if !is_fresh(&fresh, operand) {
                    continue;
                }
                let made = Made { task, body: id, span: statement.source_info.span };
                for state in pointees(owners, tcx, field.ty(tcx, args).skip_norm_wip()) {
                    found(state, made);
                }
            }
        }
    }
}

/// Whether `operand` moves or copies a local that holds a new pointer.
fn is_fresh(fresh: &FxHashSet<rustc_middle::mir::Local>, operand: &Operand<'_>) -> bool {
    operand.place().and_then(|place| place.as_local()).is_some_and(|local| fresh.contains(&local))
}
