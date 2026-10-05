use crate::StorageError;
use coven_crypto::{MemberId, StoredFileName};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// One validated encrypted-object path in the layout of §§6, 9, 11, 12, 15 and 16.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPath(String);

impl ObjectPath {
    /// A device's create-once write record (§6).
    pub fn device_log(device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("devices/{}/{number}", device.0))
    }
    /// A device's create-once store log entry (§9).
    pub fn store_log(device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("store-log/{}/{number}", device.0))
    }
    /// A snapshot written by a device (§15).
    pub fn snapshot(device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("snapshots/{}/{number}", device.0))
    }
    /// The positions posted by one device (§6).
    pub fn positions(device: DeviceId) -> Self {
        Self(format!("positions/{}", device.0))
    }
    /// A sealed store key for a member's lowercase hexadecimal public key (§11).
    pub fn store_key(number: NonZeroU64, member: &MemberId) -> Self {
        Self(format!("keys/store/{number}/{member}"))
    }
    /// A sealed circle key for a member (§14.3).
    pub fn circle_key(circle: CircleId, number: NonZeroU64, member: &MemberId) -> Self {
        Self(format!("keys/circles/{circle}/{number}/{member}"))
    }
    /// Encrypted file bytes named by the lower-case hexadecimal keyed hash (§16.2).
    pub fn file(name: &StoredFileName) -> Self {
        Self(format!("files/{name}"))
    }
    /// An encrypted join request under its invite id (§12.2).
    pub fn join_request(invite: InviteId) -> Self {
        Self(format!("join-requests/{invite}"))
    }
    /// Parse a listed or recorded path without permitting traversal or other layouts.
    pub fn parse(value: &str) -> Result<Self, StorageError> {
        value.to_owned().try_into()
    }
    /// The path bound into the object's encryption by the caller.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Only posted positions are replaced; logs, keys, files and snapshots are create-once.
    pub fn is_replaceable(&self) -> bool {
        self.0.starts_with("positions/")
    }
    pub(crate) fn is_first_store_entry(&self) -> bool {
        self.0.starts_with("store-log/") && self.0.ends_with("/1")
    }
    /// The device named by log, snapshot or position paths.
    pub fn device(&self) -> Option<DeviceId> {
        let mut parts = self.0.split('/');
        match parts.next()? {
            "devices" | "store-log" | "snapshots" | "positions" => {
                parts.next()?.parse().ok().map(DeviceId)
            }
            _ => None,
        }
    }
    pub(crate) fn components(&self) -> Vec<&str> {
        self.0.split('/').collect()
    }
    pub(crate) fn file_name(&self) -> &str {
        match self.0.rsplit('/').next() {
            Some(name) => name,
            None => &self.0,
        }
    }
    pub(crate) fn from_components(parent: &[String], name: &str) -> Result<Self, StorageError> {
        let mut parts = parent.to_vec();
        parts.push(name.to_owned());
        Self::parse(&parts.join("/"))
    }
    pub(crate) fn absolute(&self) -> String {
        format!("/{}", self.0)
    }
    pub(crate) fn under(&self, root: &str) -> String {
        join_root(root, &self.0)
    }
}

impl TryFrom<String> for ObjectPath {
    type Error = StorageError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<_> = value.split('/').collect();
        let valid = match parts.as_slice() {
            ["devices" | "store-log" | "snapshots", device, n] => {
                decimal(device, false) && decimal(n, true)
            }
            ["positions", device] => decimal(device, false),
            ["keys", "store", n, member] => decimal(n, true) && member.parse::<MemberId>().is_ok(),
            ["keys", "circles", circle, n, member] => {
                canonical_uuid(circle) && decimal(n, true) && member.parse::<MemberId>().is_ok()
            }
            ["files", name] => hex_name(name).is_ok(),
            ["join-requests", invite] => canonical_uuid(invite),
            _ => false,
        };
        if !valid {
            return Err(StorageError::InvalidPath);
        }
        Ok(Self(value))
    }
}
impl From<ObjectPath> for String {
    fn from(path: ObjectPath) -> Self {
        path.0
    }
}

/// A prefix of the validated object layout, used for provider listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPrefix(String);
impl ObjectPrefix {
    /// All objects in this store's location.
    pub fn all() -> Self {
        Self(String::new())
    }
    /// All writes of a device.
    pub fn device_log(device: DeviceId) -> Self {
        Self(format!("devices/{}/", device.0))
    }
    /// All entries of a device's store log.
    pub fn store_log(device: DeviceId) -> Self {
        Self(format!("store-log/{}/", device.0))
    }
    /// Every snapshot.
    pub fn snapshots() -> Self {
        Self("snapshots/".into())
    }
    /// Every stored file.
    pub fn files() -> Self {
        Self("files/".into())
    }
    /// Every waiting join request.
    pub fn join_requests() -> Self {
        Self("join-requests/".into())
    }
    /// Every sealed key.
    pub fn keys() -> Self {
        Self("keys/".into())
    }
    /// Every device's posted positions.
    pub fn positions() -> Self {
        Self("positions/".into())
    }
    /// The prefix supplied to the provider.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Whether a validated path is under this prefix.
    pub fn contains(&self, path: &ObjectPath) -> bool {
        path.0.starts_with(&self.0)
    }
    pub(crate) fn under(&self, root: &str) -> String {
        join_root(root, &self.0)
    }
}

pub(crate) fn validate_root(root: &str) -> Result<(), StorageError> {
    if !root.is_empty() {
        for part in root.split('/') {
            segment(part)?;
        }
    }
    Ok(())
}
fn segment(part: &str) -> Result<(), StorageError> {
    if part.is_empty()
        || part.len() > 255
        || !part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(StorageError::InvalidPath);
    }
    Ok(())
}
fn hex_name(name: &str) -> Result<(), StorageError> {
    if name.len() != 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(StorageError::InvalidPath);
    }
    Ok(())
}
fn decimal(part: &str, nonzero: bool) -> bool {
    match part.parse::<u64>() {
        Ok(n) => (!nonzero || n != 0) && n.to_string() == part,
        Err(_) => false,
    }
}
fn canonical_uuid(value: &str) -> bool {
    match uuid::Uuid::parse_str(value) {
        Ok(id) => id.to_string() == value,
        Err(_) => false,
    }
}
fn join_root(root: &str, value: &str) -> String {
    if root.is_empty() {
        value.to_owned()
    } else {
        format!("{root}/{value}")
    }
}

#[cfg(test)]
#[path = "path_tests.rs"]
mod tests;
