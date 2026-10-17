//! Indexed directory selection for file and provider setup listings.

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

pub(super) struct Selection(pub(super) ObjectPrefix);

impl Selection {
    pub(super) fn keys(&self) -> &'static [&'static str] {
        &["path", "size"]
    }

    pub(super) fn predicate(&self) -> Retained<NSPredicate> {
        let prefix = &self.0;
        let directory = prefix.as_str().strip_suffix('/').expect("directory prefix");
        let depth = directory.bytes().filter(|byte| *byte == b'/').count();
        let field = NSString::from_str(DIRECTORY_FIELDS[depth]);
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
