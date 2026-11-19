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

extern crate rustc_attr_ir;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_span;

mod finding;
mod reqwest_error_display;

use clippy_config::Conf;
use rustc_lint::LintStore;

/// clippy's lints, which the driver registers before it calls
/// [`register_lint_passes`].
pub use clippy_lints::declared_lints;

/// Registers clippy's lint passes, then these lints and their passes.
pub fn register_lint_passes(store: &mut LintStore, conf: &'static Conf) {
    clippy_lints::register_lint_passes(store, conf);
    store.register_lints(&[reqwest_error_display::REQWEST_ERROR_DISPLAY]);
    store.register_late_lint_pass(Box::new(|tcx| {
        Box::new(reqwest_error_display::ReqwestErrorDisplay::new(tcx))
    }));
}
