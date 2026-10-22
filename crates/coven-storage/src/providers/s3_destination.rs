//! Native destinations share publication primitives. A bucket claim has no
//! encrypted-object path; its store id names only its copy staging directory.
use crate::{bucket_claim::BUCKET_CLAIM, ObjectPath};
use coven_foundation::id_source::StoreId;

#[derive(Clone, Copy)]
pub(super) enum Destination<'a> {
    Object(&'a ObjectPath),
    BucketClaim(StoreId),
}

impl Destination<'_> {
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Object(path) => path.as_str(),
            Self::BucketClaim(_) => BUCKET_CLAIM,
        }
    }
    /// The S3 key of this destination: an object's path under the base
    /// prefix, or the bucket marker at the bucket's root.
    pub(super) fn key(&self, prefix: &crate::S3Prefix) -> String {
        match self {
            Self::Object(path) => prefix.key(path),
            Self::BucketClaim(_) => BUCKET_CLAIM.into(),
        }
    }
    pub(super) fn store(&self) -> StoreId {
        match self {
            Self::Object(path) => path.store(),
            Self::BucketClaim(store) => *store,
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
