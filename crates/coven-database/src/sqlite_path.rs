//! The path SQLite opens a store's database by (§4).
//!
//! On Windows a store's path is canonical, and a canonical Windows path is
//! verbatim: `\\?\D:\…`. SQLite takes any path that starts `\\` for a
//! network share, and then locks the shared-memory file through one handle
//! for every connection in the process, recording which connections hold
//! each lock outside the mutex that decides whether the handle takes it. Two
//! connections taking one shared lock at once both take it on the handle,
//! and only one releases it: the handle holds it for good, and no connection
//! in the process can take that lock exclusively again. A checkpoint then
//! never copies the WAL or starts it over, and readers retry their locks. So
//! a local verbatim path goes to SQLite in its plain form, where each
//! connection locks through a handle of its own.

use std::borrow::Cow;
use std::path::Path;

use crate::DbError;

/// The path SQLite opens for the file at `path`: elsewhere `path` itself.
#[cfg(not(windows))]
pub(crate) fn sqlite_path(path: &Path) -> Result<Cow<'_, Path>, DbError> {
    Ok(Cow::Borrowed(path))
}

/// The path SQLite opens for the file at `path`: a verbatim path in its
/// plain form, `\\?\D:\…` as `D:\…` and `\\?\UNC\server\share\…` as
/// `\\server\share\…`. Windows reads a plain path's names as it reads a
/// verbatim one's except for `.` and `..`, a name ending in a dot or a
/// space, and a device's name such as `NUL`; a verbatim path holding one
/// would open another file, so it is refused.
#[cfg(windows)]
pub(crate) fn sqlite_path(path: &Path) -> Result<Cow<'_, Path>, DbError> {
    use std::ffi::OsString;
    use std::path::{Component, PathBuf, Prefix};
    let mut components = path.components();
    let Some(Component::Prefix(prefix)) = components.next() else {
        return Ok(Cow::Borrowed(path));
    };
    let mut plain = match prefix.kind() {
        Prefix::VerbatimDisk(drive) => PathBuf::from(format!(r"{}:\", char::from(drive))),
        Prefix::VerbatimUNC(server, share) => {
            let mut root = OsString::from(r"\\");
            root.push(server);
            root.push(r"\");
            root.push(share);
            root.push(r"\");
            PathBuf::from(root)
        }
        _ => return Ok(Cow::Borrowed(path)),
    };
    for component in components {
        match component {
            Component::RootDir => {}
            Component::Normal(name) if reads_alike(name) => plain.push(name),
            _ => {
                return Err(DbError::Disk(coven_foundation::files::FileError::Io {
                    operation: "open database",
                    path: path.to_owned(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "a name in the path means another file without its \\\\?\\ prefix",
                    )),
                }))
            }
        }
    }
    Ok(Cow::Owned(plain))
}

/// Whether Windows reads `name` in a plain path as it does in a verbatim one.
#[cfg(windows)]
fn reads_alike(name: &std::ffi::OsStr) -> bool {
    let name = name.to_string_lossy();
    if name.ends_with(['.', ' ']) {
        return false;
    }
    let base = name.split('.').next().unwrap_or_default().trim_end_matches(' ');
    !coven_foundation::files::windows_device(base)
}

#[cfg(test)]
#[path = "sqlite_path_tests.rs"]
mod tests;
