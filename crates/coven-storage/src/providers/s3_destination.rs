//! Native destinations share publication primitives. The location marker has
//! no encrypted-object path; its store id names only its copy staging
//! directory.
use crate::{location_claim::LOCATION_MARKER, ObjectPath};
use coven_foundation::id_source::StoreId;

#[derive(Clone, Copy)]
pub(super) enum Destination<'a> {
    Object(&'a ObjectPath),
    LocationMarker(StoreId),
}

impl Destination<'_> {
    /// The S3 key of this destination: an object's path, or the location
    /// marker, both from the bucket's root.
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Object(path) => path.as_str(),
            Self::LocationMarker(_) => LOCATION_MARKER,
        }
    }
    pub(super) fn store(&self) -> StoreId {
        match self {
            Self::Object(path) => path.store(),
            Self::LocationMarker(store) => *store,
        }
    }
    pub(super) fn is_replaceable(&self) -> bool {
        matches!(self, Self::Object(path) if path.is_replaceable())
    }
}

impl<'a> From<&'a ObjectPath> for Destination<'a> {
    fn from(path: &'a ObjectPath) -> Self {
        Self::Object(path)
    }
}
