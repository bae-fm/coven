//! Indexed directory selection and permanent entry-one writer discovery.

use crate::{ObjectPath, ObjectPrefix, StorageError, StorageFailure};
use coven_foundation::id_source::{DeviceId, StoreId};
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

pub(super) enum Selection {
    Objects(ObjectPrefix),
    Writers(StoreId),
}

impl Selection {
    pub(super) fn keys(&self) -> &'static [&'static str] {
        match self {
            Self::Objects(_) => &["path", "size"],
            Self::Writers(_) => &["path"],
        }
    }

    pub(super) fn predicate(&self) -> Retained<NSPredicate> {
        let prefix = match self {
            Self::Objects(prefix) => prefix.clone(),
            Self::Writers(store) => ObjectPrefix::store_logs(*store),
        };
        let directory = prefix.as_str().strip_suffix('/').expect("directory prefix");
        let depth = directory.bytes().filter(|byte| *byte == b'/').count();
        let field = NSString::from_str(DIRECTORY_FIELDS[depth]);
        let value = NSString::from_str(directory);
        let args = NSArray::<AnyObject>::from_slice(&[&field, &value]);
        let format = match self {
            Self::Objects(_) => "%K == %@",
            Self::Writers(_) => "%K == %@ AND number == '1'",
        };
        // Field and value are arguments, never interpolated predicate syntax.
        unsafe {
            NSPredicate::predicateWithFormat_argumentArray(&NSString::from_str(format), Some(&args))
        }
    }
}

pub(super) fn writer_id(name: &str, path: &str, store: StoreId) -> Result<DeviceId, StorageError> {
    let path =
        ObjectPath::parse(path).map_err(|error| StorageFailure::Protocol.with_source(error))?;
    if name != path.as_str() || path.store() != store {
        return Err(StorageFailure::Protocol.with_source("CloudKit writer identity disagrees"));
    }
    match path.store_log_position() {
        Some((writer, number)) if number.get() == 1 => Ok(writer),
        _ => Err(StorageFailure::Protocol
            .with_source("CloudKit writer query returned non-entry-one record")),
    }
}

#[cfg(test)]
#[path = "cloudkit_query_tests.rs"]
mod tests;
