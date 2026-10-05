//! Reading the workspace: every Rust file under `crates/` and `tools/`, parsed,
//! and every member manifest, plus the workspace manifest's dependency table.

use std::path::{Path, PathBuf};

use crate::syntax::RustFile;

/// The directories, relative to the workspace root, whose files the checker
/// reads. A directory that does not exist yet holds nothing.
const SOURCE_ROOTS: &[&str] = &["crates", "tools"];

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
}

pub(crate) fn load(root: &Path) -> Result<Workspace, LoadError> {
    let mut rust_paths = Vec::new();
    let mut manifest_paths = Vec::new();
    for source_root in SOURCE_ROOTS {
        let directory = root.join(source_root);
        if directory.is_dir() {
            collect_paths(&directory, &mut rust_paths, &mut manifest_paths)?;
        }
    }
    rust_paths.sort();
    manifest_paths.sort();
    let files = rust_paths
        .into_iter()
        .map(|path| {
            let source = read(&path)?;
            let syntax = syn::parse_file(&source).map_err(|source| LoadError::ParseFile {
                path: path.clone(),
                source,
            })?;
            Ok(RustFile {
                relative_path: relative(root, &path)?,
                syntax,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    let manifests = manifest_paths
        .into_iter()
        .map(|path| {
            Ok(Manifest {
                relative_path: relative(root, &path)?,
                table: parse_manifest(&path)?,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    let workspace_manifest = root.join("Cargo.toml");
    let workspace_dependencies = parse_manifest(&workspace_manifest)?
        .remove("workspace")
        .and_then(|workspace| match workspace {
            toml::Value::Table(mut workspace) => Some(workspace.remove("dependencies")),
            _ => None,
        })
        .ok_or(LoadError::NotAWorkspace {
            path: workspace_manifest,
        })?
        .and_then(|dependencies| match dependencies {
            toml::Value::Table(dependencies) => Some(dependencies),
            _ => None,
        })
        .unwrap_or_default();
    Ok(Workspace {
        files,
        manifests,
        workspace_dependencies,
    })
}

fn read(path: &Path) -> Result<String, LoadError> {
    std::fs::read_to_string(path).map_err(|source| LoadError::ReadFile {
        path: path.to_path_buf(),
        source,
    })
}

fn parse_manifest(path: &Path) -> Result<toml::Table, LoadError> {
    read(path)?
        .parse::<toml::Table>()
        .map_err(|source| LoadError::ParseManifest {
            path: path.to_path_buf(),
            source,
        })
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
    let entries = std::fs::read_dir(directory).map_err(|source| LoadError::ReadDirectory {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| LoadError::ReadDirectoryEntry {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| matches!(name.to_str(), Some(".git" | "target")))
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
