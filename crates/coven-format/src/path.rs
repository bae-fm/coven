//! Canonical object names and listing prefixes from Appendix D10.
//!
//! Crate-to-crate API: coven's format, storage and sync code build paths and
//! prefixes here. The app's CloudKit bridge only passes them through, using
//! `parse`, `as_str` and `is_replaceable` (Appendix E1).

use coven_crypto::MemberId;
use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, KeyId};
use coven_merge::Audience;
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
    /// The device and positive write number of a device-log path.
    pub fn write_position(&self) -> Option<(DeviceId, NonZeroU64)> {
        let rest = self.0.strip_prefix("devices/")?;
        let (device, number) = rest.split_once('/').expect("validated device-log path");
        Some((
            DeviceId(device.parse().expect("validated device")),
            number.parse().expect("validated write number"),
        ))
    }
    /// The identity carried by a device-log path.
    pub fn write_id(&self) -> Option<coven_merge::WriteId> {
        self.write_position()
            .map(|(device, number)| coven_merge::WriteId {
                device,
                number: number.get(),
            })
    }
    /// The identity carried by a snapshot path.
    pub fn snapshot_id(&self) -> Option<crate::store_log::SnapshotId> {
        let rest = self.0.strip_prefix("snapshots/")?;
        let (audience, rest) = rest.split_once('/').expect("validated snapshot path");
        let (device, number) = rest.split_once('/').expect("validated snapshot path");
        let audience = if audience == "store" {
            Audience::Store
        } else {
            Audience::Circle(CircleId(audience.parse().expect("validated circle")))
        };
        Some(crate::store_log::SnapshotId {
            audience,
            device: DeviceId(device.parse().expect("validated device")),
            number: number.parse().expect("validated number"),
        })
    }
    /// A device's create-once store log entry (§9).
    pub fn store_log(device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("store-log/{}/{number}", device.0))
    }
    /// The device and positive entry number of a store-log path.
    pub fn store_log_position(&self) -> Option<(DeviceId, NonZeroU64)> {
        let rest = self.0.strip_prefix("store-log/")?;
        let (device, number) = rest.split_once('/').expect("validated store-log path");
        Some((
            DeviceId(device.parse().expect("validated device")),
            number.parse().expect("validated entry number"),
        ))
    }
    /// A snapshot written by a device (§15).
    pub fn snapshot(audience: Audience, device: DeviceId, number: NonZeroU64) -> Self {
        let audience = match audience {
            Audience::Store => "store".to_owned(),
            Audience::Circle(circle) => circle.to_string(),
        };
        Self(format!("snapshots/{audience}/{}/{number}", device.0))
    }
    /// The positions posted by one device (§6).
    pub fn positions(device: DeviceId) -> Self {
        Self(format!("positions/{}", device.0))
    }
    /// A sealed store key for a member's lowercase hexadecimal public key (§11).
    pub fn store_key(key: KeyId, member: &MemberId) -> Self {
        Self(format!("keys/store/{key}/{member}"))
    }
    /// A sealed circle key for a member (§14.3).
    pub fn circle_key(circle: CircleId, key: KeyId, member: &MemberId) -> Self {
        Self(format!("keys/circles/{circle}/{key}/{member}"))
    }
    /// Encrypted file bytes named by a random UUID (§16.2).
    pub fn file(device: DeviceId, name: FileId) -> Self {
        Self(format!("files/{}/{name}", device.0))
    }
    /// An encrypted join request under its invite id (§12.2).
    pub fn join_request(invite: InviteId) -> Self {
        Self(format!("join-requests/{invite}"))
    }
    /// Parse a listed or recorded path without permitting traversal or other layouts.
    pub fn parse(value: &str) -> Result<Self, PathError> {
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
    /// Whether this is a device’s first store-log entry.
    pub fn is_first_store_entry(&self) -> bool {
        self.0.starts_with("store-log/") && self.0.ends_with("/1")
    }
    /// The device named by log, snapshot, file or position paths.
    pub fn device(&self) -> Option<DeviceId> {
        let parts: Vec<_> = self.0.split('/').collect();
        match parts.as_slice() {
            ["devices" | "store-log" | "files", device, _]
            | ["positions", device]
            | ["snapshots", _, device, _] => {
                Some(DeviceId(device.parse().expect("validated device id")))
            }
            _ => None,
        }
    }

    /// The canonical path components, for providers with folder objects.
    pub fn components(&self) -> Vec<&str> {
        self.0.split('/').collect()
    }
    /// The last path component.
    pub fn file_name(&self) -> &str {
        match self.0.rsplit('/').next() {
            Some(name) => name,
            None => &self.0,
        }
    }
    /// Validate a provider listing’s parent components and final name.
    pub fn from_components(parent: &[String], name: &str) -> Result<Self, PathError> {
        let mut parts = parent.to_vec();
        parts.push(name.to_owned());
        Self::parse(&parts.join("/"))
    }
    /// The provider path with a leading slash.
    pub fn absolute(&self) -> String {
        format!("/{}", self.0)
    }
    /// The provider path beneath a configured location prefix.
    pub fn under(&self, root: &str) -> String {
        join_root(root, &self.0)
    }
}

impl TryFrom<String> for ObjectPath {
    type Error = PathError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<_> = value.split('/').collect();
        let valid = match parts.as_slice() {
            ["devices" | "store-log", device, n] => decimal(device, false) && decimal(n, true),
            ["snapshots", audience, device, n] => {
                (*audience == "store" || canonical_uuid(audience))
                    && decimal(device, false)
                    && decimal(n, true)
            }
            ["positions", device] => decimal(device, false),
            ["keys", "store", key, member] => {
                canonical_uuid(key) && member.parse::<MemberId>().is_ok()
            }
            ["keys", "circles", circle, key, member] => {
                canonical_uuid(circle) && canonical_uuid(key) && member.parse::<MemberId>().is_ok()
            }
            ["files", device, name] => decimal(device, false) && canonical_uuid(name),
            ["join-requests", invite] => canonical_uuid(invite),
            _ => false,
        };
        if !valid {
            return Err(PathError);
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
    /// Every device's write log.
    pub fn device_logs() -> Self {
        Self("devices/".into())
    }
    /// Every device's store log.
    pub fn store_logs() -> Self {
        Self("store-log/".into())
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
    /// Snapshots of one audience, across all devices.
    pub fn audience_snapshots(audience: &Audience) -> Self {
        let audience = match audience {
            Audience::Store => "store".to_owned(),
            Audience::Circle(id) => id.to_string(),
        };
        Self(format!("snapshots/{audience}/"))
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
    /// The provider path beneath a configured location prefix.
    pub fn under(&self, root: &str) -> String {
        join_root(root, &self.0)
    }
}

/// Validate a provider folder as a strict ancestor of an object in the
/// layout. Empty folders left by interrupted uploads remain recognizable.
pub fn validate_directory(value: &str) -> Result<(), PathError> {
    let parts: Vec<_> = value.split('/').collect();
    let valid = match parts.as_slice() {
        ["devices" | "store-log" | "positions" | "snapshots" | "keys" | "files"
        | "join-requests"] => true,
        ["devices" | "store-log" | "files", device] => decimal(device, false),
        ["snapshots", audience] => *audience == "store" || canonical_uuid(audience),
        ["snapshots", audience, device] => {
            (*audience == "store" || canonical_uuid(audience)) && decimal(device, false)
        }
        ["keys", "store" | "circles"] => true,
        ["keys", "store", key] => canonical_uuid(key),
        ["keys", "circles", circle] => canonical_uuid(circle),
        ["keys", "circles", circle, key] => canonical_uuid(circle) && canonical_uuid(key),
        _ => false,
    };
    if !valid {
        return Err(PathError);
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

/// A path is outside the canonical object layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PathError;
impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid storage path")
    }
}
impl std::error::Error for PathError {}

#[cfg(test)]
#[path = "path_tests.rs"]
mod tests;
