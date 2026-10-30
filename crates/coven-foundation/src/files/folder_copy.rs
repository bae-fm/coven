//! Copies coven writes into a folder the person picked, to make cloud files
//! device-only (§17.3): the folders and files a call needs, refusing paths
//! through a symbolic link; each file written under a temporary name in its
//! final folder and renamed once synced; and the removal of what a call made,
//! which removes a copy only while it is as coven left it.
//!
//! The folder is opened once. On Unix every folder, copy and rename below it
//! is made relative to that open handle, one name at a time, never following
//! a symbolic link, so a folder swapped for a link after planning can't send
//! a write elsewhere. Windows has no such calls in the standard library, and
//! its writes go by path, each checked for links just before it.

#[cfg(windows)]
use super::atomic_file::{create_new, rename_new};
use super::atomic_file::{parent, remove};
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

/// The folder the person picked, open for the copies one call makes.
pub struct CopyFolder {
    path: PathBuf,
    #[cfg(unix)]
    handle: Arc<rustix::fd::OwnedFd>,
}

impl CopyFolder {
    /// Open `folder`, an absolute path to a folder whose path is valid
    /// Unicode. The folder itself may be reached through a link: the person
    /// picked it.
    pub async fn open(folder: PathBuf) -> Result<Self, CopyPathError> {
        blocking(move || {
            if !folder.is_absolute() {
                return Err(CopyPathError::NotAFolder(folder));
            }
            if folder.to_str().is_none() {
                return Err(CopyPathError::NotUnicode(folder));
            }
            match fs::metadata(&folder) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return Err(CopyPathError::NotAFolder(folder)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(CopyPathError::NotAFolder(folder))
                }
                Err(source) => return Err(FileError::at("inspect folder", &folder, source).into()),
            }
            #[cfg(unix)]
            {
                use rustix::fs::{Mode, OFlags};
                let handle = rustix::fs::open(
                    &folder,
                    OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|errno| FileError::at("open folder", &folder, errno.into()))?;
                Ok(Self {
                    path: folder,
                    handle: Arc::new(handle),
                })
            }
            #[cfg(windows)]
            Ok(Self { path: folder })
        })
        .await
    }

    /// Plan the copies at `paths`. Only what lies below the folder is
    /// checked for links.
    pub async fn plan(&self, paths: Vec<RelativePath>) -> Result<CopyPlan, CopyPathError> {
        let folder = self.path.clone();
        blocking(move || plan(&folder, &paths)).await
    }

    /// Make the folder at `path`, a path of a plan, and sync the folder
    /// holding it. True when this call made it; false when a folder was
    /// there already, made by someone else, which the call never removes.
    pub async fn create_folder(&self, path: PathBuf) -> Result<bool, FileError> {
        let folder = self.clone_handle();
        blocking(move || folder.make_folder(&path)).await
    }

    /// Start a copy's bytes at `temporary`, a new file in its final folder.
    pub async fn create_copy(&self, temporary: PathBuf) -> Result<CopyWriter, FileError> {
        let folder = self.clone_handle();
        blocking(move || folder.make_copy(temporary)).await
    }

    fn clone_handle(&self) -> Self {
        Self {
            path: self.path.clone(),
            #[cfg(unix)]
            handle: self.handle.clone(),
        }
    }

    /// The names below the folder leading to `path`.
    fn names<'a>(&self, path: &'a Path) -> io::Result<Vec<&'a std::ffi::OsStr>> {
        let names: Vec<_> = path
            .strip_prefix(&self.path)
            .map_err(|_outside| io::Error::new(io::ErrorKind::InvalidInput, "outside the folder"))?
            .iter()
            .collect();
        if names.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the folder itself",
            ));
        }
        Ok(names)
    }

    /// The folder holding `path`, opened one name at a time from the
    /// folder's handle without following a link, and `path`'s own name.
    #[cfg(unix)]
    fn parent<'a>(
        &self,
        path: &'a Path,
    ) -> io::Result<(Option<rustix::fd::OwnedFd>, &'a std::ffi::OsStr)> {
        use rustix::fd::AsFd;
        use rustix::fs::{Mode, OFlags};
        let names = self.names(path)?;
        let (last, folders) = names
            .split_last()
            .expect("a path below the folder names something");
        let mut opened: Option<rustix::fd::OwnedFd> = None;
        for name in folders {
            let at = opened.as_ref().map_or(self.handle.as_fd(), |fd| fd.as_fd());
            opened = Some(rustix::fs::openat(
                at,
                *name,
                OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::RDONLY | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
        }
        Ok((opened, last))
    }

    #[cfg(unix)]
    fn make_folder(&self, path: &Path) -> Result<bool, FileError> {
        use rustix::fd::AsFd;
        use rustix::fs::{AtFlags, FileType, Mode};
        let at = |source: io::Error| FileError::at("create folder", path, source);
        let (opened, name) = self.parent(path).map_err(at)?;
        let parent = opened.as_ref().map_or(self.handle.as_fd(), |fd| fd.as_fd());
        match rustix::fs::mkdirat(parent, name, Mode::from(0o777)) {
            Ok(()) => {
                rustix::fs::fsync(parent).map_err(|errno| at(errno.into()))?;
                Ok(true)
            }
            Err(rustix::io::Errno::EXIST) => {
                let stat = rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|errno| at(errno.into()))?;
                if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                    Ok(false)
                } else {
                    Err(at(rustix::io::Errno::EXIST.into()))
                }
            }
            Err(errno) => Err(at(errno.into())),
        }
    }

    #[cfg(unix)]
    fn make_copy(&self, temporary: PathBuf) -> Result<CopyWriter, FileError> {
        use rustix::fd::AsFd;
        use rustix::fs::{Mode, OFlags};
        let at = |source: io::Error| FileError::at("create copy", &temporary, source);
        let (opened, name) = self.parent(&temporary).map_err(at)?;
        let parent = match opened {
            Some(opened) => opened,
            None => rustix::io::fcntl_dupfd_cloexec(self.handle.as_fd(), 0)
                .map_err(|errno| at(errno.into()))?,
        };
        let file = rustix::fs::openat(
            parent.as_fd(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from(0o666),
        )
        .map_err(|errno| at(errno.into()))?;
        let name = name.to_owned();
        Ok(CopyWriter {
            file: Arc::new(tokio::sync::Mutex::new(fs::File::from(file))),
            path: temporary,
            parent: Arc::new(parent),
            name,
        })
    }

    /// Each folder between this one and `path` must still be a folder, and
    /// not a link, just before a write by path.
    #[cfg(windows)]
    fn check_below(&self, path: &Path) -> io::Result<()> {
        let names = self.names(path)?;
        let mut below = self.path.clone();
        for name in &names[..names.len() - 1] {
            below.push(name);
            let metadata = fs::symlink_metadata(&below)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a folder"));
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn make_folder(&self, path: &Path) -> Result<bool, FileError> {
        let at = |source: io::Error| FileError::at("create folder", path, source);
        self.check_below(path).map_err(at)?;
        match fs::create_dir(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path).map_err(at)?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    Ok(false)
                } else {
                    Err(at(error))
                }
            }
            Err(error) => Err(at(error)),
        }
    }

    #[cfg(windows)]
    fn make_copy(&self, temporary: PathBuf) -> Result<CopyWriter, FileError> {
        let at = |source: io::Error| FileError::at("create copy", &temporary, source);
        self.check_below(&temporary).map_err(at)?;
        let file = create_new(&temporary).map_err(at)?;
        Ok(CopyWriter {
            file: Arc::new(tokio::sync::Mutex::new(file)),
            folder: self.clone_handle(),
            path: temporary,
        })
    }
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

/// A copy being written under its temporary name. Dropping it leaves the
/// bytes for the caller's recorded cleanup.
pub struct CopyWriter {
    file: Arc<tokio::sync::Mutex<fs::File>>,
    path: PathBuf,
    /// The open folder holding the copy, which its rename is relative to.
    #[cfg(unix)]
    parent: Arc<rustix::fd::OwnedFd>,
    #[cfg(unix)]
    name: std::ffi::OsString,
    #[cfg(windows)]
    folder: CopyFolder,
}

impl CopyWriter {
    /// Append one bounded buffer, completing its disk write before returning.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<(), FileError> {
        let bytes = bytes.to_vec();
        self.run("write copy", move |file| file.write_all(&bytes))
            .await
    }

    /// Sync the bytes, rename them to `path`, where nothing may be, in the
    /// same folder, and sync the folder, so the copy is whole under its final
    /// name or not there.
    pub async fn publish(self, path: &Path) -> Result<(), FileError> {
        self.run("sync copy", |file| file.sync_all()).await?;
        let to = path.to_owned();
        #[cfg(unix)]
        {
            let (parent, from) = (self.parent.clone(), self.name.clone());
            blocking(move || {
                use rustix::fs::RenameFlags;
                let name = to
                    .file_name()
                    .filter(|_| to.parent().is_some())
                    .ok_or_else(|| {
                        FileError::at(
                            "rename copy",
                            &to,
                            io::Error::new(io::ErrorKind::InvalidInput, "file name required"),
                        )
                    })?;
                rustix::fs::renameat_with(&*parent, &from, &*parent, name, RenameFlags::NOREPLACE)
                    .map_err(|errno| FileError::at("rename copy", &to, errno.into()))?;
                rustix::fs::fsync(&*parent).map_err(|errno| FileError::AfterReplace {
                    path: to.clone(),
                    source: Box::new(io::Error::from(errno)),
                })
            })
            .await
        }
        #[cfg(windows)]
        {
            let (folder, from) = (self.folder.clone_handle(), self.path.clone());
            blocking(move || {
                folder
                    .check_below(&to)
                    .map_err(|source| FileError::at("rename copy", &to, source))?;
                rename_new(&from, &to).map_err(|source| FileError::at("rename copy", &to, source))
            })
            .await
        }
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
