//! The policy file of the workspace that holds the crate the driver compiles
//! (`owner-construction-check.toml`, which `owner_construction_policy`
//! models): the rows that tell the lints that read it what the workspace's
//! types and functions are. The structural checker reads the same file.
//!
//! The workspace's root is where cargo finds it: the nearest folder, from the
//! crate's own (`CARGO_MANIFEST_DIR`) upward, whose `Cargo.toml` has a
//! `[workspace]` table. A workspace with no policy file there gives the lints
//! that read one nothing to check.
//!
//! The driver reads the file once per compile, and names it among the
//! compile's inputs, so cargo lints the crate again when the file changes.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use clippy_utils::paths::{PathNS, lookup_path_str};
use owner_construction_policy::{FILE_NAME, Policy};
use rustc_hir::def_id::{DefId, LOCAL_CRATE};
use rustc_middle::ty::TyCtxt;
use rustc_span::Symbol;

/// The workspace's policy, if it has one. A policy file that can't be read
/// stops the compile.
pub(crate) fn workspace_policy(tcx: TyCtxt<'_>) -> Option<&'static Policy> {
    static POLICY: OnceLock<Option<Policy>> = OnceLock::new();
    POLICY
        .get_or_init(|| {
            let root = workspace_root()?;
            let path = root.join(FILE_NAME);
            if !path.exists() {
                return None;
            }
            tcx.sess.file_depinfo.borrow_mut().insert(Symbol::intern(&path.to_string_lossy()));
            match Policy::read(&root) {
                Ok(policy) => Some(policy),
                Err(error) => tcx.dcx().fatal(format!("typed lints: {error}")),
            }
        })
        .as_ref()
}

/// The root of the workspace that holds the crate being compiled.
fn workspace_root() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    manifest_dir.ancestors().find(|folder| declares_workspace(folder)).map(Path::to_path_buf)
}

/// Whether the `Cargo.toml` in `folder` has a `[workspace]` table.
fn declares_workspace(folder: &Path) -> bool {
    std::fs::read_to_string(folder.join("Cargo.toml"))
        .ok()
        .and_then(|text| text.parse::<toml::Table>().ok())
        .is_some_and(|manifest| manifest.contains_key("workspace"))
}

/// What a policy row's path, such as `app_bridge::runtime::Runtime::run`,
/// defines in the crate being compiled: nothing when the crate can't reach
/// the crate the path names. A path whose crate is the one being compiled
/// and that defines nothing there is a row the workspace no longer holds,
/// which stops the compile, as the structural checker refuses such a row.
pub(crate) fn resolve_row(tcx: TyCtxt<'_>, row: &str, path: &str) -> Vec<DefId> {
    let found = lookup_path_str(tcx, PathNS::Value, path);
    let names_this_crate =
        path.split("::").next().is_some_and(|root| root == tcx.crate_name(LOCAL_CRATE).as_str());
    if found.is_empty() && names_this_crate {
        tcx.dcx()
            .fatal(format!("typed lints: {FILE_NAME}'s {row} names {path}, which defines nothing"));
    }
    found
}
