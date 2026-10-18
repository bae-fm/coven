//! OneDrive file and folder facets shared by setup and object listing.

use crate::{StorageError, StorageFailure};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Folder,
    File,
    Other,
}

/// An item's facet. A facet that is not an object, or both facets at once,
/// is the provider's error.
pub(super) fn item_kind(item: &serde_json::Value) -> Result<Kind, StorageError> {
    let facet = |name: &str| match item.get(name) {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(value) if value.is_object() => Ok(true),
        Some(_) => Err(StorageFailure::Protocol.with_source("invalid OneDrive item kind")),
    };
    match (facet("folder")?, facet("file")?) {
        (true, true) => Err(StorageFailure::Protocol.with_source("invalid OneDrive item kind")),
        (true, false) => Ok(Kind::Folder),
        (false, true) => Ok(Kind::File),
        (false, false) => Ok(Kind::Other),
    }
}
