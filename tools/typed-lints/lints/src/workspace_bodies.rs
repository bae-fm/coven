//! The definitions and bodies of the workspace's functions and closures that
//! a compile sees: the crate's own, and those of the workspace's crates it
//! depends on, from their metadata. rustc writes every function's MIR there only under
//! `-Zalways-encode-mir`, which the driver's wrapper adds to each compile of a
//! workspace crate (`driver/src/rustc.rs`), so a lint that reads the bodies
//! stops a compile that lacks it rather than see only some of them.

use rustc_hir::Constness;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::{DefId, DefIndex};
use rustc_middle::ty::TyCtxt;

use crate::workspace_policy::WorkspacePolicy;

/// Each definition of the workspace the compile sees: the crate's own, and
/// those of the workspace's crates it depends on. `lint` reads their bodies,
/// so the compile stops when it lacks them.
pub(crate) fn workspace_definitions(
    workspace: &WorkspacePolicy,
    tcx: TyCtxt<'_>,
    lint: &str,
) -> Vec<DefId> {
    let mut definitions =
        tcx.hir_crate_items(()).definitions().map(|id| id.to_def_id()).collect::<Vec<_>>();
    definitions.extend(earlier_definitions(workspace, tcx, lint));
    definitions
}

/// Each function and closure of the workspace the compile sees whose body
/// is production code, with MIR to read (`TyCtxt::optimized_mir`), for
/// `lint`.
pub(crate) fn workspace_bodies(
    workspace: &WorkspacePolicy,
    tcx: TyCtxt<'_>,
    lint: &str,
) -> Vec<DefId> {
    let has_body = |id: DefId| {
        let callable = match tcx.def_kind(id) {
            DefKind::Fn | DefKind::AssocFn => {
                !matches!(tcx.constness(id), Constness::Const { always: true })
            }
            DefKind::Closure => true,
            _ => false,
        };
        callable && tcx.is_mir_available(id) && !workspace.is_test_definition(tcx, id)
    };
    let mut bodies = tcx
        .hir_body_owners()
        .map(|id| id.to_def_id())
        .filter(|&id| has_body(id))
        .collect::<Vec<_>>();
    bodies.extend(earlier_definitions(workspace, tcx, lint).filter(|&id| has_body(id)));
    bodies
}

/// Every definition of the workspace's crates the crate depends on. `lint`
/// reads their bodies, so the compile stops when it lacks them.
pub(crate) fn earlier_definitions<'tcx>(
    workspace: &WorkspacePolicy,
    tcx: TyCtxt<'tcx>,
    lint: &str,
) -> impl Iterator<Item = DefId> + use<'tcx> {
    if !tcx.sess.opts.unstable_opts.always_encode_mir {
        tcx.dcx().fatal(format!(
            "typed lints: {lint} reads the bodies of the workspace's earlier crates, which rustc \
             writes only under -Zalways-encode-mir: run the driver through typed-lints-rustc, as \
             tools/typed-lints/clippy.sh does"
        ));
    }
    let crates = tcx
        .crates(())
        .iter()
        .copied()
        .filter(|krate| workspace.holds_file(tcx, tcx.def_span(krate.as_def_id())))
        .collect::<Vec<_>>();
    crates.into_iter().flat_map(move |krate| {
        (0..tcx.num_extern_def_ids(krate))
            .map(move |index| DefId { krate, index: DefIndex::from_usize(index) })
    })
}
