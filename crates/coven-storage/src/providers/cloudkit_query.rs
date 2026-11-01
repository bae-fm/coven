//! Indexed directory selection for file and provider setup listings, and
//! for the upload roots a device left (§17.10).

use crate::{ObjectPath, ObjectPrefix};
use objc2::{rc::Retained, runtime::AnyObject};
use objc2_foundation::{NSArray, NSPredicate, NSString};

const DIRECTORY_FIELDS: [&str; 4] = ["directory1", "directory2", "directory3", "directory4"];

/// Each strict ancestor of a canonical object has an equality-searchable field.
pub(super) fn directories(path: &ObjectPath) -> impl Iterator<Item = (&'static str, &str)> {
    path.as_str()
        .match_indices('/')
        .enumerate()
        .map(move |(depth, (end, _))| (DIRECTORY_FIELDS[depth], &path.as_str()[..end]))
}

/// The field holding an upload root's reserved folder (D10).
pub(super) const UPLOAD_DIRECTORY: &str = "directory";

pub(super) enum Selection {
    /// The published objects in one directory.
    Objects(ObjectPrefix),
    /// The upload roots in one device's reserved folder.
    Uploads(String),
}

impl Selection {
    pub(super) fn record_type(&self) -> &'static str {
        match self {
            Self::Objects(_) => "CovenObject",
            Self::Uploads(_) => "CovenUpload",
        }
    }

    pub(super) fn keys(&self) -> &'static [&'static str] {
        match self {
            Self::Objects(_) => &["path", "size"],
            Self::Uploads(_) => &["state"],
        }
    }

    pub(super) fn predicate(&self) -> Retained<NSPredicate> {
        let (field, directory) = match self {
            Self::Objects(prefix) => {
                let directory = prefix.as_str().strip_suffix('/').expect("directory prefix");
                let depth = directory.bytes().filter(|byte| *byte == b'/').count();
                (DIRECTORY_FIELDS[depth], directory)
            }
            Self::Uploads(folder) => (UPLOAD_DIRECTORY, folder.as_str()),
        };
        let field = NSString::from_str(field);
        let value = NSString::from_str(directory);
        let args = NSArray::<AnyObject>::from_slice(&[&field, &value]);
        let format = "%K == %@";
        // Field and value are arguments, never interpolated predicate syntax.
        unsafe {
            NSPredicate::predicateWithFormat_argumentArray(&NSString::from_str(format), Some(&args))
        }
    }
}

#[cfg(test)]
#[path = "cloudkit_query_tests.rs"]
mod tests;
