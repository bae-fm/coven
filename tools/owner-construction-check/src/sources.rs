//! Reading the workspace: every member the workspace manifest lists, each
//! Rust file under it parsed and tagged with its package, every member
//! manifest, and the workspace manifest's dependency table.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::crate_dependencies::{dependency_entries, workspace_package};
use crate::module_sources::test_module_files;
use crate::syntax::RustFile;

pub(crate) struct Workspace {
    pub(crate) files: Vec<RustFile>,
    pub(crate) manifests: Vec<Manifest>,
    /// The workspace manifest's `[workspace.dependencies]`.
    pub(crate) workspace_dependencies: toml::Table,
}

pub(crate) struct Manifest {
    /// Relative to the workspace root, with `/` separators.
    pub(crate) relative_path: String,
    pub(crate) table: toml::Table,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadError {
    #[error("read {}: {source}", path.display())]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {}: {source}", path.display())]
    ParseFile {
        path: PathBuf,
        #[source]
        source: syn::Error,
    },
    #[error("parse {}: {source}", path.display())]
    ParseManifest {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("relativize {} against {}: {source}", path.display(), root.display())]
    Relativize {
        path: PathBuf,
        root: PathBuf,
        #[source]
        source: std::path::StripPrefixError,
    },
    #[error("read directory {}: {source}", path.display())]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read directory entry in {}: {source}", path.display())]
    ReadDirectoryEntry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} has no [workspace] table", path.display())]
    NotAWorkspace { path: PathBuf },
    #[error("{}: workspace members are listed by directory, without globs: {member}", path.display())]
    MemberPattern { path: PathBuf, member: String },
    #[error("{} has no [package] name", path.display())]
    UnnamedPackage { path: PathBuf },
    #[error("workspace member {inner} lies inside member {outer}")]
    NestedMember { outer: String, inner: String },
    #[error("{} has no Cargo.toml", path.display())]
    MissingManifest { path: PathBuf },
    #[error("{} is a crate outside the workspace inside one of its members", path.display())]
    CrateInsideMember { path: PathBuf },
}

pub(crate) fn load(root: &Path) -> Result<Workspace, LoadError> {
    let workspace_manifest = root.join("Cargo.toml");
    let Some(toml::Value::Table(mut workspace_table)) =
        parse_manifest(&workspace_manifest)?.remove("workspace")
    else {
        return Err(LoadError::NotAWorkspace { path: workspace_manifest });
    };
    let members = match workspace_table.remove("members") {
        Some(toml::Value::Array(members)) => members
            .into_iter()
            .map(|member| match member {
                toml::Value::String(member)
                    if !member.contains(['*', '?', '[']) && !member.is_empty() =>
                {
                    Ok(member.trim_end_matches('/').to_string())
                }
                member => Err(LoadError::MemberPattern {
                    path: workspace_manifest.clone(),
                    member: member.to_string(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => Vec::new(),
    };
    // A file belongs to the one member whose directory holds it.
    for outer in &members {
        for inner in &members {
            if inner != outer && (outer == "." || inner.starts_with(&format!("{outer}/"))) {
                return Err(LoadError::NestedMember { outer: outer.clone(), inner: inner.clone() });
            }
        }
    }
    let workspace_dependencies = match workspace_table.remove("dependencies") {
        Some(toml::Value::Table(dependencies)) => dependencies,
        _ => toml::Table::new(),
    };
    let mut rust_paths = Vec::new();
    let mut manifest_paths = Vec::new();
    for member in &members {
        collect_paths(&root.join(member), &mut rust_paths, &mut manifest_paths)?;
    }
    rust_paths.sort();
    manifest_paths.sort();
    if let Some(path) = manifest_paths
        .iter()
        .find(|path| !members.iter().any(|member| **path == root.join(member).join("Cargo.toml")))
    {
        return Err(LoadError::CrateInsideMember { path: path.clone() });
    }
    let manifests = manifest_paths
        .into_iter()
        .map(|path| {
            Ok(Manifest { relative_path: relative(root, &path)?, table: parse_manifest(&path)? })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    let mut workspace = Workspace { files: Vec::new(), manifests, workspace_dependencies };
    // Each member's root directory, package name and dependency names.
    let mut packages = Vec::new();
    for member in &members {
        let relative_path = format!("{member}/Cargo.toml");
        let manifest = workspace
            .manifests
            .iter()
            .find(|manifest| manifest.relative_path == relative_path)
            .ok_or_else(|| LoadError::MissingManifest { path: root.join(&relative_path) })?;
        let name = manifest
            .package_name()
            .ok_or_else(|| LoadError::UnnamedPackage { path: root.join(&relative_path) })?;
        let names = dependency_entries(&manifest.table)
            .into_iter()
            .map(|(name, entry)| {
                let package = if entry.get("workspace").and_then(toml::Value::as_bool) == Some(true)
                {
                    workspace_package(&workspace, name)
                } else {
                    entry.get("package").and_then(toml::Value::as_str).unwrap_or(name).to_string()
                };
                (name.replace('-', "_"), package.replace('-', "_"))
            })
            .collect::<BTreeMap<_, _>>();
        packages.push((format!("{member}/"), name, names));
    }
    workspace.files = rust_paths
        .into_iter()
        .map(|path| {
            let relative_path = relative(root, &path)?;
            let (package_root, package, extern_crates) = packages
                .iter()
                .find(|(package_root, _, _)| relative_path.starts_with(package_root.as_str()))
                .expect("every source lies under the member it was collected from");
            let source = read(&path)?;
            let syntax = syn::parse_file(&source)
                .map_err(|source| LoadError::ParseFile { path: path.clone(), source })?;
            Ok(RustFile {
                relative_path,
                package_root: package_root.clone(),
                package: package.clone(),
                syntax,
                lines: source.lines().count(),
                extern_crates: extern_crates.clone(),
                test_module: false,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    let test_modules = test_module_files(&workspace.files);
    for file in &mut workspace.files {
        file.test_module = test_modules.contains(&file.relative_path);
    }
    Ok(workspace)
}

impl Manifest {
    pub(crate) fn package_name(&self) -> Option<String> {
        self.table.get("package")?.get("name")?.as_str().map(str::to_string)
    }
}

fn read(path: &Path) -> Result<String, LoadError> {
    std::fs::read_to_string(path)
        .map_err(|source| LoadError::ReadFile { path: path.to_path_buf(), source })
}

fn parse_manifest(path: &Path) -> Result<toml::Table, LoadError> {
    read(path)?
        .parse::<toml::Table>()
        .map_err(|source| LoadError::ParseManifest { path: path.to_path_buf(), source })
}

fn relative(root: &Path, path: &Path) -> Result<String, LoadError> {
    Ok(path
        .strip_prefix(root)
        .map_err(|source| LoadError::Relativize {
            path: path.to_path_buf(),
            root: root.to_path_buf(),
            source,
        })?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn collect_paths(
    directory: &Path,
    rust_paths: &mut Vec<PathBuf>,
    manifest_paths: &mut Vec<PathBuf>,
) -> Result<(), LoadError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|source| LoadError::ReadDirectory { path: directory.to_path_buf(), source })?;
    for entry in entries {
        let entry = entry.map_err(|source| LoadError::ReadDirectoryEntry {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| matches!(name.to_str(), Some(".git" | "target")))
            {
                continue;
            }
            collect_paths(&path, rust_paths, manifest_paths)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            rust_paths.push(path);
        } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            manifest_paths.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
