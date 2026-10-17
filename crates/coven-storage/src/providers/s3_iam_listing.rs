//! IAM listing envelopes, typed records and checked pagination.

use crate::providers::s3_admin_query::xml::{self, Element};
use crate::{StorageError, StorageFailure};

pub(super) struct Listing<T> {
    pub(super) action: &'static str,
    collection: &'static str,
    record: fn(&Element) -> Result<T, StorageError>,
}

pub(super) const USERS: Listing<String> = Listing {
    action: "ListUsers",
    collection: "Users",
    record: |entry| Ok(entry.child("UserName")?.value()?.to_owned()),
};
pub(super) const KEYS: Listing<String> = Listing {
    action: "ListAccessKeys",
    collection: "AccessKeyMetadata",
    record: |entry| Ok(entry.child("AccessKeyId")?.value()?.to_owned()),
};
pub(super) const POLICIES: Listing<(String, String)> = Listing {
    action: "ListPolicies",
    collection: "Policies",
    record: |entry| {
        Ok((
            entry.child("PolicyName")?.value()?.to_owned(),
            entry.child("Arn")?.value()?.to_owned(),
        ))
    },
};

impl<T> Listing<T> {
    pub(super) fn page(&self, bytes: &str) -> Result<Page<T>, StorageError> {
        let malformed = || StorageFailure::Protocol.with_source("malformed IAM listing");
        let root = xml::parse(bytes)?.ok_or_else(malformed)?;
        if root.name != format!("{}Response", self.action) {
            return Err(malformed());
        }
        let result = root.child(&format!("{}Result", self.action))?;
        let next = match result.child("IsTruncated")?.value()? {
            "false" => None,
            "true" => Some(result.child("Marker")?.value()?.to_owned()),
            _ => return Err(malformed()),
        };
        let collection = result.child(self.collection)?;
        let mut records = Vec::new();
        for member in collection.members()? {
            if member.name != "member" {
                return Err(malformed());
            }
            records.push((self.record)(member)?);
        }
        Ok(Page { records, next })
    }
}

pub(super) struct Page<T> {
    pub(super) records: Vec<T>,
    pub(super) next: Option<String>,
}
