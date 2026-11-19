//! Which of the workspace's crates the compile of each other one sees, from
//! the packages' manifests: a crate's compile sees the crates it depends on,
//! by itself or through what it depends on. rustc keeps each earlier crate's
//! own dependencies to itself, so a lint that must know whether two earlier
//! crates' compiles saw each other reads their `Cargo.toml` files.
//!
//! A crate is the library of the package whose manifest is the nearest one
//! above its root file. It depends on each workspace package its manifest
//! names in `[dependencies]`, or in a `[target.<platform>.dependencies]` the
//! compile's target matches, by `path` or through the workspace's own
//! `[workspace.dependencies]` row with one. An optional dependency counts as
//! none: whether a build turns it on is the dependent's features' choice,
//! which another crate's compile can't see, so the graph holds only what
//! every build of the crate depends on. A crate whose root file no package
//! holds, as a fixture's, depends on nothing the graph knows of.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use cargo_platform::{Cfg, Ident, Platform};
use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def_id::CrateNum;
use rustc_middle::ty::TyCtxt;

use crate::workspace_policy::WorkspacePolicy;

/// The package each of the compile's workspace crates is, and every package
/// each package depends on.
pub(crate) struct CrateGraph {
    packages: FxHashMap<CrateNum, PathBuf>,
    depends_on: FxHashMap<PathBuf, FxHashSet<PathBuf>>,
}

impl CrateGraph {
    /// The graph of the workspace crates among `crates`, the compile's. A
    /// manifest that can't be read stops the compile.
    pub(crate) fn read<'a>(
        tcx: TyCtxt<'a>,
        workspace: &'a WorkspacePolicy,
        crates: impl IntoIterator<Item = CrateNum>,
    ) -> Self {
        let mut manifests = Manifests::new(tcx, workspace);
        let mut packages = FxHashMap::default();
        for krate in crates {
            let root = tcx.def_span(krate.as_def_id());
            if let Some(package) = workspace.package_folder(tcx, root) {
                packages.insert(krate, package);
            }
        }
        let mut depends_on = FxHashMap::default();
        for package in packages.values() {
            let mut reached = FxHashSet::default();
            let mut pending = vec![package.clone()];
            while let Some(current) = pending.pop() {
                for dependency in manifests.dependencies(&current) {
                    if reached.insert(dependency.clone()) {
                        pending.push(dependency);
                    }
                }
            }
            depends_on.insert(package.clone(), reached);
        }
        Self { packages, depends_on }
    }

    /// Whether the compile of `krate` sees `other`: the two are one crate,
    /// or `krate` depends on `other`.
    pub(crate) fn sees(&self, krate: CrateNum, other: CrateNum) -> bool {
        if krate == other {
            return true;
        }
        let (Some(package), Some(dependency)) =
            (self.packages.get(&krate), self.packages.get(&other))
        else {
            return false;
        };
        self.depends_on.get(package).is_some_and(|reached| reached.contains(dependency))
    }
}

/// The workspace's manifests, each read once, and what the compile's target
/// is, for their `[target.<platform>]` tables.
struct Manifests<'a> {
    tcx: TyCtxt<'a>,
    root: &'a Path,
    workspace_dependencies: toml::Table,
    target: String,
    cfg: Vec<Cfg>,
    dependencies: FxHashMap<PathBuf, Vec<PathBuf>>,
}

impl<'a> Manifests<'a> {
    fn new(tcx: TyCtxt<'a>, workspace: &'a WorkspacePolicy) -> Self {
        let root = workspace.root();
        let workspace_dependencies = read_manifest(tcx, root)
            .get("workspace")
            .and_then(|table| table.get("dependencies"))
            .and_then(toml::Value::as_table)
            .cloned()
            .unwrap_or_default();
        let ident = |name: rustc_span::Symbol| Ident { name: name.to_string(), raw: false };
        let cfg = tcx
            .sess
            .config
            .iter()
            .map(|&(name, value)| match value {
                Some(value) => Cfg::KeyPair(ident(name), value.to_string()),
                None => Cfg::Name(ident(name)),
            })
            .collect();
        Self {
            tcx,
            root,
            workspace_dependencies,
            target: tcx.sess.opts.target_triple.tuple().to_string(),
            cfg,
            dependencies: FxHashMap::default(),
        }
    }

    /// The folders of the workspace packages the package in `package`
    /// depends on in every build for the compile's target.
    fn dependencies(&mut self, package: &Path) -> Vec<PathBuf> {
        if let Some(known) = self.dependencies.get(package) {
            return known.clone();
        }
        let manifest = read_manifest(self.tcx, package);
        let mut tables = vec![manifest.get("dependencies")];
        if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
            for (platform, table) in targets {
                let parsed = Platform::from_str(platform).unwrap_or_else(|error| {
                    self.tcx.dcx().fatal(format!(
                        "typed lints: {}: target {platform}: {error}",
                        package.join("Cargo.toml").display()
                    ))
                });
                if parsed.matches(&self.target, &self.cfg) {
                    tables.push(table.get("dependencies"));
                }
            }
        }
        let mut found = Vec::new();
        for (name, entry) in
            tables.into_iter().flatten().filter_map(toml::Value::as_table).flatten()
        {
            let Some(entry) = entry.as_table() else {
                continue;
            };
            if entry.get("optional").and_then(toml::Value::as_bool) == Some(true) {
                continue;
            }
            let path = if entry.get("workspace").and_then(toml::Value::as_bool) == Some(true) {
                self.workspace_dependencies
                    .get(name)
                    .and_then(|row| row.get("path"))
                    .and_then(toml::Value::as_str)
                    .map(|path| self.root.join(path))
            } else {
                entry.get("path").and_then(toml::Value::as_str).map(|path| package.join(path))
            };
            if let Some(path) = path {
                found.push(canonical(self.tcx, &path));
            }
        }
        self.dependencies.insert(package.to_path_buf(), found.clone());
        found
    }
}

/// The manifest in `folder`, parsed. One that can't be read stops the
/// compile.
fn read_manifest(tcx: TyCtxt<'_>, folder: &Path) -> toml::Table {
    let file = folder.join("Cargo.toml");
    std::fs::read_to_string(&file)
        .map_err(|error| error.to_string())
        .and_then(|text| text.parse::<toml::Table>().map_err(|error| error.to_string()))
        .unwrap_or_else(|error| {
            tcx.dcx().fatal(format!("typed lints: {}: {error}", file.display()))
        })
}

/// `folder` with every link followed, as `WorkspacePolicy::package_folder`
/// names a package's.
fn canonical(tcx: TyCtxt<'_>, folder: &Path) -> PathBuf {
    folder.canonicalize().unwrap_or_else(|error| {
        tcx.dcx().fatal(format!("typed lints: {}: {error}", folder.display()))
    })
}
