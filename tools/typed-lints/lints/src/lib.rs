//! coven's typed lints, and clippy's beside them. The driver is clippy's own
//! driver, built with this crate in place of `clippy_lints`: it registers
//! [`declared_lints`] and calls [`register_lint_passes`], so one compile of a
//! crate runs clippy's lints and these.
//!
//! These lints are tool lints under clippy's name, such as
//! `clippy::reqwest_error_display`. A mark is
//! `#[expect(clippy::reqwest_error_display, reason = "...")]`, which rustc
//! leaves to clippy, so a plain build accepts it. clippy's own driver knows
//! no such lint and warns about the mark, so code that holds one is linted
//! with this driver.
#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_attr_ir;
extern crate rustc_data_structures;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

mod async_export_off_runtime;
mod crate_graph;
mod domain_owners;
mod finding;
mod kept_arguments;
mod made_here;
mod method_takes_capability;
mod owner_dependencies;
mod owner_rules;
mod owners;
mod read_rule;
mod reqwest_error_display;
mod sqlite_outside_its_homes;
mod task_state;
mod workspace_bodies;
mod workspace_policy;

use clippy_config::Conf;
use rustc_lint::LintStore;

/// clippy's lints, which the driver registers before it calls
/// [`register_lint_passes`].
pub use clippy_lints::declared_lints;

/// Registers clippy's lint passes, then these lints and their passes.
pub fn register_lint_passes(store: &mut LintStore, conf: &'static Conf) {
    clippy_lints::register_lint_passes(store, conf);
    // Read once for the compile, as clippy reads its `Conf`.
    let policy: &'static workspace_policy::PolicyRead =
        Box::leak(Box::new(workspace_policy::read()));
    store.register_lints(&[
        reqwest_error_display::REQWEST_ERROR_DISPLAY,
        async_export_off_runtime::ASYNC_EXPORT_OFF_RUNTIME,
        sqlite_outside_its_homes::SQLITE_OUTSIDE_ITS_HOMES,
        owner_rules::BUILT_OUTSIDE_COMPOSITION_ROOT,
        method_takes_capability::METHOD_TAKES_CAPABILITY,
        task_state::TASK_STATE_OUTSIDE_TASK,
        owner_dependencies::OWNER_HANDS_OUT_DEPENDENCY,
        owner_dependencies::HANDLE_NOT_FROM_ITS_OWNER,
        owner_dependencies::SHARED_STATE_OUTSIDE_HANDLE,
    ]);
    store.register_lints(read_rule::READ_RULE_LINTS);
    store.register_late_lint_pass(Box::new(|tcx| {
        Box::new(reqwest_error_display::ReqwestErrorDisplay::new(tcx))
    }));
    store.register_late_lint_pass(Box::new(move |tcx| {
        Box::new(async_export_off_runtime::AsyncExportOffRuntime::new(
            tcx,
            workspace_policy::for_compile(policy, tcx),
        ))
    }));
    store.register_late_lint_pass(Box::new(move |tcx| {
        Box::new(sqlite_outside_its_homes::SqliteOutsideItsHomes::new(
            tcx,
            workspace_policy::for_compile(policy, tcx),
        ))
    }));
    store.register_late_lint_pass(Box::new(move |tcx| {
        Box::new(owner_rules::OwnerRules::new(tcx, workspace_policy::for_compile(policy, tcx)))
    }));
    store.register_late_lint_pass(Box::new(move |tcx| {
        Box::new(read_rule::ReadRule::new(tcx, workspace_policy::for_compile(policy, tcx)))
    }));
}
