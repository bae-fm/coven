//! Copies coven writes into a folder the person picked, to make cloud files
//! device-only (§17.3): the folders and files a call needs, refusing paths
//! through a symbolic link; each file written under a temporary name in its
//! final folder and renamed once synced; and the removal of what a call made,
//! which removes a copy only while it is as coven left it.

use super::atomic_file::{create_new, parent, remove, rename_new};
use super::{FileError, RelativePath};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

/// What one call makes below the folder: the folders that don't exist yet,
/// each before the folders inside it, and each file's path, in the order of
/// the relative paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyPlan {
    /// The folders to make, outermost first.
    pub folders: Vec<PathBuf>,
    /// Each file's path.
    pub files: Vec<PathBuf>,
}

/// Why a copy can't go where the app named.
#[derive(Debug, thiserror::Error)]
pub enum CopyPathError {
    /// The folder isn't an absolute path to a folder, or a path below it
    /// passes through something that isn't a folder.
    #[error("{} is not a folder", .0.display())]
    NotAFolder(PathBuf),
    /// A path below the folder passes through a symbolic link.
    #[error("{} is a symbolic link", .0.display())]
    Link(PathBuf),
    /// Something is at a file's path already, or two files have one path.
    #[error("{} is taken", .0.display())]
    Exists(PathBuf),
    /// The folder's path isn't valid Unicode, which coven records as text.
    #[error("{} is not valid Unicode", .0.display())]
    NotUnicode(PathBuf),
    /// Inspecting a path failed.
    #[error(transparent)]
    File(#[from] FileError),
}

/// Plan the copies at `paths` below `folder`. Only what lies below the
/// folder is checked for links: the folder itself is the person's choice,
/// and may be reached through one.
pub async fn plan_copies(
    folder: PathBuf,
    paths: Vec<RelativePath>,
) -> Result<CopyPlan, CopyPathError> {
    blocking(move || plan(&folder, &paths)).await
}

fn plan(folder: &Path, paths: &[RelativePath]) -> Result<CopyPlan, CopyPathError> {
    if !folder.is_absolute() {
        return Err(CopyPathError::NotAFolder(folder.to_owned()));
    }
    if folder.to_str().is_none() {
        return Err(CopyPathError::NotUnicode(folder.to_owned()));
    }
    match fs::metadata(folder) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(CopyPathError::NotAFolder(folder.to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(CopyPathError::NotAFolder(folder.to_owned()))
        }
        Err(source) => return Err(FileError::at("inspect folder", folder, source).into()),
    }
    let mut plan = CopyPlan {
        folders: Vec::new(),
        files: Vec::new(),
    };
    for relative in paths {
        let names: Vec<_> = relative.as_path().iter().collect();
        let (last, folders) = names.split_last().expect("a relative path names something");
        let mut path = folder.to_path_buf();
        for name in folders {
            path.push(name);
            if plan.files.contains(&path) {
                return Err(CopyPathError::Exists(path));
            }
            if plan.folders.contains(&path) {
                continue;
            }
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(CopyPathError::Link(path))
                }
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return Err(CopyPathError::NotAFolder(path)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    plan.folders.push(path.clone());
                }
                Err(source) => return Err(FileError::at("inspect folder", &path, source).into()),
            }
        }
        path.push(last);
        if plan.files.contains(&path) || plan.folders.contains(&path) {
            return Err(CopyPathError::Exists(path));
        }
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(CopyPathError::Exists(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(FileError::at("inspect file", &path, source).into()),
        }
        plan.files.push(path);
    }
    Ok(plan)
}

/// Make one folder a plan names, which mustn't exist yet, and sync the
/// folder holding it.
pub async fn create_folder(path: PathBuf) -> Result<(), FileError> {
    blocking(move || {
        fs::create_dir(&path).map_err(|source| FileError::at("create folder", &path, source))?;
        sync_parent(&path).map_err(|source| FileError::at("sync folder", &path, source))
    })
    .await
}

/// Start a copy's bytes at `temporary`, a new file in its final folder.
pub async fn create_copy(temporary: PathBuf) -> Result<CopyWriter, FileError> {
    let file = {
        let temporary = temporary.clone();
        blocking(move || {
            create_new(&temporary)
                .map_err(|source| FileError::at("create copy", &temporary, source))
        })
        .await?
    };
    Ok(CopyWriter {
        file: Arc::new(tokio::sync::Mutex::new(file)),
        path: temporary,
    })
}

/// A copy being written under its temporary name. Dropping it leaves the
/// bytes for the caller's recorded cleanup.
pub struct CopyWriter {
    file: Arc<tokio::sync::Mutex<fs::File>>,
    path: PathBuf,
}

impl CopyWriter {
    /// Append one bounded buffer, completing its disk write before returning.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<(), FileError> {
        let bytes = bytes.to_vec();
        self.run("write copy", move |file| file.write_all(&bytes))
            .await
    }

    /// Sync the bytes, rename them to `path`, where nothing may be, and sync
    /// the folder, so the copy is whole under its final name or not there.
    pub async fn publish(self, path: &Path) -> Result<(), FileError> {
        self.run("sync copy", |file| file.sync_all()).await?;
        let (from, to) = (self.path.clone(), path.to_owned());
        blocking(move || {
            rename_new(&from, &to).map_err(|source| FileError::at("rename copy", &to, source))?;
            sync_parent(&to).map_err(|source| FileError::AfterReplace {
                path: to.clone(),
                source: Box::new(source),
            })
        })
        .await
    }

    async fn run(
        &self,
        operation: &'static str,
        run: impl FnOnce(&mut fs::File) -> io::Result<()> + Send + 'static,
    ) -> Result<(), FileError> {
        // Acquire before spawning so later calls wait for cancelled work too.
        let mut file = self.file.clone().lock_owned().await;
        let path = self.path.clone();
        blocking(move || run(&mut file).map_err(|source| FileError::at(operation, &path, source)))
            .await
    }
}

/// Remove a copy coven wrote at `path`, if it still has the size and
/// modification time coven recorded: one that changed since is the person's,
/// and stays. An absent copy is gone already.
pub fn remove_copy(path: &Path, size: u64, modified_at: SystemTime) -> Result<(), FileError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "copy is already absent");
            return Ok(());
        }
        Err(source) => return Err(FileError::at("inspect copy", path, source)),
    };
    let modified = metadata
        .modified()
        .map_err(|source| FileError::at("read modification time", path, source))?;
    if !metadata.is_file() || metadata.len() != size || modified != modified_at {
        tracing::debug!(path = %path.display(), "leaving a copy the person changed");
        return Ok(());
    }
    remove(path)
}

/// Remove a copy's temporary bytes; absent ones are gone already.
pub fn remove_temporary(path: &Path) -> Result<(), FileError> {
    remove(path)
}

/// Remove a folder coven made, if nothing is in it: a folder holding
/// anything, the person's files included, stays. An absent one is gone.
pub fn remove_folder(path: &Path) -> Result<(), FileError> {
    match fs::remove_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "folder is already absent");
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {
            tracing::debug!(path = %path.display(), "leaving a folder that holds files");
            return Ok(());
        }
        Err(source) => return Err(FileError::at("remove folder", path, source)),
    }
    match sync_parent(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(FileError::AfterRemove {
            path: path.to_owned(),
            source: Box::new(source),
        }),
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    super::atomic_file::sync_directory(parent(path))
}

// Windows writes a folder's entries through with the file handle's flag.
#[cfg(windows)]
fn sync_parent(path: &Path) -> io::Result<()> {
    let _ = parent(path);
    Ok(())
}

async fn blocking<T: Send + 'static>(run: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(run).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("copy task was cancelled: {error}"),
    }
}

#[cfg(test)]
#[path = "folder_copy_tests.rs"]
mod tests;
