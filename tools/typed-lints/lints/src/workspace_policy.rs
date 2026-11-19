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
//! The driver reads the file once, when it registers the lints, and hands
//! what it read to each lint's pass. Each pass names the file among the
//! compile's inputs, so cargo lints the crate again when the file changes.

use std::path::{Path, PathBuf};

use clippy_utils::is_in_test_function;
use clippy_utils::paths::{PathNS, lookup_path_str};
use owner_construction_policy::{FILE_NAME, Policy, RAW_SQLITE_HANDLES};
use rustc_attr_ir::CfgEntry;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::{CRATE_DEF_ID, DefId, LOCAL_CRATE};
use rustc_hir::{HirId, ItemKind, Node, find_attr};
use rustc_middle::ty::TyCtxt;
use rustc_span::{FileName, Span, Symbol, sym};

/// The workspace's policy, and where the workspace and the crate being
/// compiled are, for the paths the policy names.
pub(crate) struct WorkspacePolicy {
    pub(crate) policy: Policy,
    /// The policy file.
    file: PathBuf,
    /// The workspace's root, with every link in it followed.
    root: PathBuf,
    /// The folder of the crate's package, the same way.
    package: PathBuf,
    /// The folder the compile runs in, which a relative source file's name
    /// starts from.
    working_folder: PathBuf,
    /// The name of the package of the crate being compiled, as cargo names it.
    package_name: Option<String>,
}

/// What the driver found when it looked for the workspace's policy: none
/// when the crate's workspace has no policy file, and why when the file is
/// there and could not be read.
pub(crate) type PolicyRead = Result<Option<WorkspacePolicy>, String>;

/// Looks for the policy of the workspace that holds the crate being compiled
/// and reads it.
pub(crate) fn read() -> PolicyRead {
    let Some(package) = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from) else {
        return Ok(None);
    };
    let Some(root) = package.ancestors().find(|folder| declares(folder, "workspace")) else {
        return Ok(None);
    };
    let file = root.join(FILE_NAME);
    if !file.exists() {
        return Ok(None);
    }
    let policy = Policy::read(root).map_err(|error| error.to_string())?;
    let canonical = |folder: &Path| {
        folder.canonicalize().map_err(|error| format!("{}: {error}", folder.display()))
    };
    let working_folder =
        std::env::current_dir().map_err(|error| format!("the current folder: {error}"))?;
    Ok(Some(WorkspacePolicy {
        policy,
        root: canonical(root)?,
        package: canonical(&package)?,
        working_folder,
        file,
        package_name: std::env::var("CARGO_PKG_NAME").ok(),
    }))
}

/// The policy a lint's pass checks the crate against, if the workspace has
/// one, named among the compile's inputs. A policy file that could not be
/// read stops the compile.
pub(crate) fn for_compile<'a>(
    read: &'a PolicyRead,
    tcx: TyCtxt<'_>,
) -> Option<&'a WorkspacePolicy> {
    let workspace = read
        .as_ref()
        .unwrap_or_else(|error| tcx.dcx().fatal(format!("typed lints: {error}")))
        .as_ref()?;
    let file = Symbol::intern(&workspace.file.to_string_lossy());
    tcx.sess.file_depinfo.borrow_mut().insert(file);
    Some(workspace)
}

/// Whether the `Cargo.toml` in `folder` has the table `table`.
fn declares(folder: &Path, table: &str) -> bool {
    std::fs::read_to_string(folder.join("Cargo.toml"))
        .ok()
        .and_then(|text| text.parse::<toml::Table>().ok())
        .is_some_and(|manifest| manifest.contains_key(table))
}

impl WorkspacePolicy {
    /// The source file `span` is written in, relative to the workspace's
    /// root with `/` between its parts, as the policy names files: for code
    /// a macro writes, the file of the macro's call.
    pub(crate) fn relative_path(&self, tcx: TyCtxt<'_>, span: Span) -> Option<String> {
        let file = self.source_file(tcx, span)?;
        self.relative(&file)
    }

    /// `file`, a path with every link followed, relative to the workspace's
    /// root as the policy names files.
    fn relative(&self, file: &Path) -> Option<String> {
        let relative = file.strip_prefix(&self.root).ok()?;
        Some(
            relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
        )
    }

    /// Whether the crate being compiled reads the policy's `path`, a source
    /// file relative to the workspace's root, as one of its own.
    pub(crate) fn compiles(&self, tcx: TyCtxt<'_>, path: &str) -> bool {
        let files = tcx.sess.source_map().files();
        files.iter().filter(|file| file.cnum == LOCAL_CRATE).any(|file| {
            self.file_on_disk(tcx, file.name.clone())
                .is_some_and(|file| self.relative(&file).as_deref() == Some(path))
        })
    }

    /// Whether the policy's `path`, a source file relative to the
    /// workspace's root, is on disk.
    pub(crate) fn has_file(&self, path: &str) -> bool {
        self.root.join(path).is_file()
    }

    /// The source file `span` is written in, with every link followed: none
    /// for code no file holds, such as what a procedural macro writes from
    /// nothing it was given. A source file the compile read that can't be
    /// found again stops the compile.
    fn source_file(&self, tcx: TyCtxt<'_>, span: Span) -> Option<PathBuf> {
        self.file_on_disk(tcx, tcx.sess.source_map().span_to_filename(span.source_callsite()))
    }

    /// The file on disk the source map's `name` stands for, as `source_file`
    /// finds it.
    fn file_on_disk(&self, tcx: TyCtxt<'_>, name: FileName) -> Option<PathBuf> {
        let local = name.into_local_path()?;
        let absolute = if local.is_absolute() { local } else { self.working_folder.join(local) };
        let canonical = absolute.canonicalize().unwrap_or_else(|error| {
            tcx.dcx().fatal(format!("typed lints: {}: {error}", absolute.display()))
        });
        Some(canonical)
    }

    /// Whether the source file `span` is written in is inside the workspace:
    /// never for code no file holds, such as the standard library's under its
    /// remapped paths. A file the compile read that can't be found again
    /// stops the compile (`source_file`).
    pub(crate) fn holds_file(&self, tcx: TyCtxt<'_>, span: Span) -> bool {
        self.source_file(tcx, span).is_some_and(|file| file.starts_with(&self.root))
    }

    /// The workspace's root, with every link followed.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// The folder of the package that holds the source file `span` is
    /// written in, with every link followed: the nearest folder above the
    /// file, inside the workspace, whose `Cargo.toml` has a `[package]`
    /// table.
    pub(crate) fn package_folder(&self, tcx: TyCtxt<'_>, span: Span) -> Option<PathBuf> {
        let file = self.source_file(tcx, span)?;
        file.ancestors()
            .skip(1)
            .take_while(|folder| folder.starts_with(&self.root))
            .find(|folder| declares(folder, "package"))
            .map(Path::to_path_buf)
    }

    /// Whether the policy's `path`, a source file relative to the
    /// workspace's root, is in the package of the crate being compiled.
    pub(crate) fn package_holds(&self, path: &str) -> bool {
        self.root.join(path).starts_with(&self.package)
    }

    /// Whether the crate being compiled is one of the policy's tools, which
    /// answer to no rule of the policy but the conventions.
    pub(crate) fn compiles_a_tool(&self) -> bool {
        self.package_name.as_deref().is_some_and(|package| self.policy.is_tool(package))
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
        in_test_folder
            || test_only
            || is_in_test_function(tcx, id)
            || self.derived_for_test(tcx, id)
    }

    /// Whether the code at `id` is in an impl a derive wrote for a type that
    /// is test code: the derive's impl keeps none of its type's `cfg`s.
    fn derived_for_test(&self, tcx: TyCtxt<'_>, id: HirId) -> bool {
        std::iter::once(id)
            .chain(tcx.hir_parent_id_iter(id))
            .filter_map(|node| match tcx.hir_node(node) {
                Node::Item(item) if matches!(item.kind, ItemKind::Impl(..)) => {
                    Some(item.owner_id.to_def_id())
                }
                _ => None,
            })
            .filter(|&implementation| tcx.is_automatically_derived(implementation))
            .filter_map(|implementation| {
                tcx.type_of(implementation).instantiate_identity().skip_norm_wip().ty_adt_def()
            })
            .filter_map(|adt| adt.did().as_local())
            .any(|adt| self.is_test_code(tcx, tcx.local_def_id_to_hir_id(adt)))
    }

    /// Whether the definition `id` is test code, as `is_test_code` counts
    /// it: for another crate's, code that compiles only under one of the
    /// policy's `test_support_features`, by the `cfg` traces its crate's
    /// metadata keeps of it and of what holds it. Another crate's build
    /// holds no `cfg(test)` code and no test folder.
    pub(crate) fn is_test_definition(&self, tcx: TyCtxt<'_>, id: DefId) -> bool {
        if let Some(local) = id.as_local() {
            return self.is_test_code(tcx, tcx.local_def_id_to_hir_id(local));
        }
        let features = &self.policy.test_support_features;
        // Only items carry attributes: a closure or an anonymous constant has
        // none of its own, and its crate's metadata keeps none for it.
        std::iter::successors(Some(id), |&item| tcx.opt_parent(item))
            .filter(|&item| {
                matches!(
                    tcx.def_kind(item),
                    DefKind::Mod
                        | DefKind::Fn
                        | DefKind::AssocFn
                        | DefKind::Impl { .. }
                        | DefKind::Trait
                        | DefKind::Struct
                        | DefKind::Enum
                        | DefKind::Union
                        | DefKind::Const
                        | DefKind::AssocConst
                        | DefKind::Static { .. }
                )
            })
            .any(|item| {
                find_attr!(tcx, item, CfgTrace(cfgs) => cfgs)
                    .is_some_and(|cfgs| cfgs.iter().any(|(cfg, _)| requires_test(cfg, features)))
            })
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

/// rusqlite's raw handles the compile can reach, each with how findings
/// name it: none in a compile that can't reach rusqlite.
pub(crate) fn raw_sqlite_handles(tcx: TyCtxt<'_>) -> Vec<(DefId, &'static str)> {
    RAW_SQLITE_HANDLES
        .iter()
        .flat_map(|&(path, kind)| {
            lookup_path_str(tcx, PathNS::Type, path).into_iter().map(move |id| (id, kind))
        })
        .collect()
}

/// What a policy row's path, such as `app_bridge::runtime::Runtime::run`,
/// defines in the crate being compiled: nothing when the crate can't reach
/// the crate the path names, or when a `cfg` leaves what it names out of
/// this compile, as a module only another platform builds. A path whose
/// crate is the one being compiled and that defines nothing there otherwise
/// is a row the workspace no longer holds, which stops the compile, as the
/// structural checker refuses such a row.
pub(crate) fn resolve_row(tcx: TyCtxt<'_>, row: &str, path: &str, ns: PathNS) -> Vec<DefId> {
    let found = lookup_path_str(tcx, ns, path);
    let names_this_crate =
        path.split("::").next().is_some_and(|root| root == tcx.crate_name(LOCAL_CRATE).as_str());
    if found.is_empty() && names_this_crate && !configured_out(tcx, path) {
        tcx.dcx()
            .fatal(format!("typed lints: {FILE_NAME}'s {row} names {path}, which defines nothing"));
    }
    found
}

/// Whether a `cfg` left out of this compile the item a path of this crate
/// names, or the module or type that holds it: the first part of the path
/// that names nothing is an item rustc stripped from the part before it,
/// which names a module or, for an associated item, a type with an impl.
fn configured_out(tcx: TyCtxt<'_>, path: &str) -> bool {
    let segments = path.split("::").collect::<Vec<_>>();
    let stripped = tcx.stripped_cfg_items(LOCAL_CRATE);
    for end in (1..segments.len()).rev() {
        let scopes = if end == 1 {
            vec![CRATE_DEF_ID.to_def_id()]
        } else {
            lookup_path_str(tcx, PathNS::Type, &segments[..end].join("::"))
        };
        if scopes.is_empty() {
            continue;
        }
        let holds = |scope: DefId| {
            scopes.contains(&scope)
                || matches!(tcx.def_kind(scope), DefKind::Impl { .. })
                    && tcx
                        .type_of(scope)
                        .instantiate_identity()
                        .skip_norm_wip()
                        .ty_adt_def()
                        .is_some_and(|adt| scopes.contains(&adt.did()))
        };
        return stripped
            .iter()
            .any(|item| item.ident.name.as_str() == segments[end] && holds(item.parent_scope));
    }
    false
}
