//! The policy file of the workspace that holds the crate the driver compiles
//! (`owner-construction-check.toml`, which `owner_construction_policy`
//! models): the rows that tell the lints that read it what the workspace's
//! types, functions and homes are. The structural checker reads the same file.
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

use clippy_utils::is_in_test_function;
use clippy_utils::paths::{PathNS, lookup_path_str};
use owner_construction_policy::{FILE_NAME, Policy};
use rustc_attr_ir::CfgEntry;
use rustc_hir::def_id::{DefId, LOCAL_CRATE};
use rustc_hir::{HirId, find_attr};
use rustc_middle::ty::TyCtxt;
use rustc_span::{Span, Symbol, sym};

/// The workspace's policy, and where the workspace and the crate being
/// compiled are, for the paths the policy names.
pub(crate) struct WorkspacePolicy {
    pub(crate) policy: Policy,
    /// The workspace's root, with every link in it followed.
    root: PathBuf,
    /// The folder of the crate's package, the same way.
    package: PathBuf,
}

/// The workspace's policy, if it has one. A policy file that can't be read
/// stops the compile.
pub(crate) fn workspace_policy(tcx: TyCtxt<'_>) -> Option<&'static WorkspacePolicy> {
    static POLICY: OnceLock<Option<WorkspacePolicy>> = OnceLock::new();
    POLICY
        .get_or_init(|| {
            let package = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
            let root = package.ancestors().find(|folder| declares_workspace(folder))?;
            let path = root.join(FILE_NAME);
            if !path.exists() {
                return None;
            }
            tcx.sess.file_depinfo.borrow_mut().insert(Symbol::intern(&path.to_string_lossy()));
            let policy = Policy::read(root)
                .unwrap_or_else(|error| tcx.dcx().fatal(format!("typed lints: {error}")));
            let canonical = |folder: &Path| {
                folder.canonicalize().unwrap_or_else(|error| {
                    tcx.dcx().fatal(format!("typed lints: {}: {error}", folder.display()))
                })
            };
            Some(WorkspacePolicy { policy, root: canonical(root), package: canonical(&package) })
        })
        .as_ref()
}

/// Whether the `Cargo.toml` in `folder` has a `[workspace]` table.
fn declares_workspace(folder: &Path) -> bool {
    std::fs::read_to_string(folder.join("Cargo.toml"))
        .ok()
        .and_then(|text| text.parse::<toml::Table>().ok())
        .is_some_and(|manifest| manifest.contains_key("workspace"))
}

impl WorkspacePolicy {
    /// The source file `span` is written in, relative to the workspace's
    /// root with `/` between its parts, as the policy names files: for code
    /// a macro writes, the file of the macro's call.
    pub(crate) fn relative_path(&self, tcx: TyCtxt<'_>, span: Span) -> Option<String> {
        let file = self.source_file(tcx, span)?;
        let relative = file.strip_prefix(&self.root).ok()?;
        Some(
            relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
        )
    }

    /// The source file `span` is written in, with every link followed.
    fn source_file(&self, tcx: TyCtxt<'_>, span: Span) -> Option<PathBuf> {
        let name = tcx.sess.source_map().span_to_filename(span.source_callsite());
        let local = name.into_local_path()?;
        let absolute =
            if local.is_absolute() { local } else { std::env::current_dir().ok()?.join(local) };
        absolute.canonicalize().ok()
    }

    /// Whether the code at `id` is test code, as the structural checker
    /// counts it: code under the package's `tests/` or `examples/` folder,
    /// code that compiles only under `test` or one of the policy's
    /// `test_support_features`, and a `#[test]` function's.
    pub(crate) fn is_test_code(&self, tcx: TyCtxt<'_>, id: HirId) -> bool {
        let in_test_folder = self.source_file(tcx, tcx.hir_span(id)).is_some_and(|file| {
            file.strip_prefix(&self.package)
                .is_ok_and(|inside| inside.starts_with("tests") || inside.starts_with("examples"))
        });
        let features = &self.policy.test_support_features;
        let test_only = std::iter::once(id).chain(tcx.hir_parent_id_iter(id)).any(|node| {
            find_attr!(tcx, node, CfgTrace(cfgs) => cfgs)
                .is_some_and(|cfgs| cfgs.iter().any(|(cfg, _)| requires_test(cfg, features)))
        });
        in_test_folder || test_only || is_in_test_function(tcx, id)
    }
}

/// Whether a `cfg` predicate can hold only when `test` or one of `features`
/// does.
fn requires_test(cfg: &CfgEntry, features: &[String]) -> bool {
    match cfg {
        CfgEntry::NameValue { name, value: None, .. } => *name == sym::test,
        CfgEntry::NameValue { name, value: Some(value), .. } => {
            *name == sym::feature && features.iter().any(|feature| feature == value.as_str())
        }
        CfgEntry::All(children, _) => children.iter().any(|child| requires_test(child, features)),
        CfgEntry::Any(children, _) => {
            !children.is_empty() && children.iter().all(|child| requires_test(child, features))
        }
        CfgEntry::Not(..) | CfgEntry::Bool(..) | CfgEntry::Version(..) => false,
    }
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
