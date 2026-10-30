//! A path an app names below a folder the person picked: names only, so it
//! can't leave the folder.

use std::path::{Component, Path, PathBuf};

/// A path below a folder, one or more names joined by the platform's
/// separator. It never starts at a root or a drive and never steps up, so
/// joined to the folder it stays inside it. Each name is valid Unicode,
/// since coven records the joined path as text.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RelativePath(PathBuf);

/// Why a path can't be a `RelativePath`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelativePathError {
    /// The path names nothing.
    #[error("the path is empty")]
    Empty,
    /// The path starts at a root or a drive.
    #[error("the path is absolute")]
    Absolute,
    /// The path steps up out of a folder with `..`.
    #[error("the path steps up with ..")]
    Parent,
    /// The path starts with `.`.
    #[error("the path starts with .")]
    Current,
    /// A name isn't valid Unicode.
    #[error("the path is not valid Unicode")]
    NotUnicode,
}

impl RelativePath {
    /// Check `path`: names only, each valid Unicode.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, RelativePathError> {
        let mut names = PathBuf::new();
        for component in path.as_ref().components() {
            match component {
                Component::Prefix(_) | Component::RootDir => {
                    return Err(RelativePathError::Absolute)
                }
                Component::ParentDir => return Err(RelativePathError::Parent),
                Component::CurDir => return Err(RelativePathError::Current),
                Component::Normal(name) => {
                    if name.to_str().is_none() {
                        return Err(RelativePathError::NotUnicode);
                    }
                    names.push(name);
                }
            }
        }
        if names.as_os_str().is_empty() {
            return Err(RelativePathError::Empty);
        }
        Ok(Self(names))
    }

    /// The path, names only.
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Its names, outermost first.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|name| {
            name.to_str()
                .expect("each name is checked as Unicode when the path is made")
        })
    }
}

#[cfg(test)]
#[path = "relative_path_tests.rs"]
mod tests;
