//! Canonical object names and listing prefixes from Appendix D10.
//!
//! Crate-to-crate API: coven's format, storage and sync code build paths and
//! prefixes here. Native provider adapters map these validated paths into
//! their provider's object names.

use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, StoreId};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// One validated encrypted-object path in the layout of §§6, 9, 11, 12, 15 and 16.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPath(String);

impl ObjectPath {
    /// A device's create-once write record (§6).
    pub fn device_log(store: StoreId, device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("{store}/devices/{}/{number}", device.0))
    }
    /// The device and positive write number of a device-log path.
    pub fn write_position(&self) -> Option<(DeviceId, NonZeroU64)> {
        let rest = self.relative().strip_prefix("devices/")?;
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
        let rest = self.relative().strip_prefix("snapshots/")?;
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
    pub fn store_log(store: StoreId, device: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("{store}/store-log/{}/{number}", device.0))
    }
    /// The device and positive entry number of a store-log path.
    pub fn store_log_position(&self) -> Option<(DeviceId, NonZeroU64)> {
        let rest = self.relative().strip_prefix("store-log/")?;
        let (device, number) = rest.split_once('/').expect("validated store-log path");
        Some((
            DeviceId(device.parse().expect("validated device")),
            number.parse().expect("validated entry number"),
        ))
    }
    /// A snapshot written by a device (§15).
    pub fn snapshot(
        store: StoreId,
        audience: Audience,
        device: DeviceId,
        number: NonZeroU64,
    ) -> Self {
        let audience = match audience {
            Audience::Store => "store".to_owned(),
            Audience::Circle(circle) => circle.to_string(),
        };
        Self(format!(
            "{store}/snapshots/{audience}/{}/{number}",
            device.0
        ))
    }
    /// The positions posted by one device (§6).
    pub fn positions(store: StoreId, device: DeviceId) -> Self {
        Self(format!("{store}/positions/{}", device.0))
    }
    /// A device's storage-time probe, replaced when a time check is due (§9).
    pub fn clock(store: StoreId, device: DeviceId) -> Self {
        Self(format!("{store}/clock/{}", device.0))
    }
    /// A writer's sealed key copy, across all audiences and recipients (§11).
    pub fn key_copy(store: StoreId, writer: DeviceId, number: NonZeroU64) -> Self {
        Self(format!("{store}/keys/{}/{number}", writer.0))
    }
    /// The writer and positive copy number of a sealed-key path.
    pub fn key_copy_position(&self) -> Option<(DeviceId, NonZeroU64)> {
        let rest = self.relative().strip_prefix("keys/")?;
        let (writer, number) = rest.split_once('/').expect("validated key-copy path");
        Some((
            DeviceId(writer.parse().expect("validated writer")),
            number.parse().expect("validated copy number"),
        ))
    }
    /// Encrypted file bytes named by a random UUID (§16.2).
    pub fn file(store: StoreId, device: DeviceId, name: FileId) -> Self {
        Self(format!("{store}/files/{}/{name}", device.0))
    }
    /// An encrypted join request under its invite id (§12.2).
    pub fn join_request(store: StoreId, invite: InviteId) -> Self {
        Self(format!("{store}/join-requests/{invite}"))
    }
    /// A member's new S3 key, sealed to them, named by its mint attempt (§13).
    pub fn access_key(
        store: StoreId,
        member: &coven_foundation::id_source::MemberId,
        attempt: uuid::Uuid,
    ) -> Self {
        Self(format!("{store}/access-keys/{member}/{attempt}"))
    }
    /// Parse a listed or recorded path without permitting traversal or other layouts.
    pub fn parse(value: &str) -> Result<Self, PathError> {
        value.to_owned().try_into()
    }
    /// The store whose objects this path names.
    pub fn store(&self) -> StoreId {
        store_id(&self.0)
    }
    fn relative(&self) -> &str {
        self.0.split_once('/').expect("validated store path").1
    }
    /// The path bound into the object's encryption by the caller.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Only posted positions and clock observations can be replaced (D10).
    pub fn is_replaceable(&self) -> bool {
        matches!(
            self.relative().split_once('/'),
            Some(("positions" | "clock", _))
        )
    }
    /// The device named by log, key-copy, snapshot, file, position or clock paths.
    pub fn device(&self) -> Option<DeviceId> {
        let parts: Vec<_> = self.relative().split('/').collect();
        match parts.as_slice() {
            ["devices" | "store-log" | "files" | "keys", device, _]
            | ["positions" | "clock", device]
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
        let (store, relative) = value.split_once('/').ok_or(PathError)?;
        if !canonical_uuid(store) {
            return Err(PathError);
        }
        let parts: Vec<_> = relative.split('/').collect();
        let valid = match parts.as_slice() {
            ["devices" | "store-log" | "keys", device, n] => {
                decimal(device, false) && decimal(n, true)
            }
            ["snapshots", audience, device, n] => {
                (*audience == "store" || canonical_uuid(audience))
                    && decimal(device, false)
                    && decimal(n, true)
            }
            ["positions" | "clock", device] => decimal(device, false),
            ["files", device, name] => decimal(device, false) && canonical_uuid(name),
            ["join-requests", invite] => canonical_uuid(invite),
            ["access-keys", member, attempt] => member_id(member) && canonical_uuid(attempt),
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
    /// All objects belonging to this store.
    pub fn all(store: StoreId) -> Self {
        Self(format!("{store}/"))
    }
    /// Every device's write log.
    pub fn device_logs(store: StoreId) -> Self {
        Self(format!("{store}/devices/"))
    }
    /// Every device's store log.
    pub fn store_logs(store: StoreId) -> Self {
        Self(format!("{store}/store-log/"))
    }
    /// All writes of a device.
    pub fn device_log(store: StoreId, device: DeviceId) -> Self {
        Self(format!("{store}/devices/{}/", device.0))
    }
    /// All entries of a device's store log.
    pub fn store_log(store: StoreId, device: DeviceId) -> Self {
        Self(format!("{store}/store-log/{}/", device.0))
    }
    /// Every snapshot.
    pub fn snapshots(store: StoreId) -> Self {
        Self(format!("{store}/snapshots/"))
    }
    /// Snapshots of one audience, across all devices.
    pub fn audience_snapshots(store: StoreId, audience: &Audience) -> Self {
        let audience = match audience {
            Audience::Store => "store".to_owned(),
            Audience::Circle(id) => id.to_string(),
        };
        Self(format!("{store}/snapshots/{audience}/"))
    }
    /// Every stored file.
    pub fn files(store: StoreId) -> Self {
        Self(format!("{store}/files/"))
    }
    /// Every waiting join request.
    pub fn join_requests(store: StoreId) -> Self {
        Self(format!("{store}/join-requests/"))
    }
    /// Every sealed key.
    pub fn keys(store: StoreId) -> Self {
        Self(format!("{store}/keys/"))
    }
    /// Every device's posted positions.
    pub fn positions(store: StoreId) -> Self {
        Self(format!("{store}/positions/"))
    }
    /// The S3 keys delivered to one member (§13).
    pub fn access_keys(store: StoreId, member: &coven_foundation::id_source::MemberId) -> Self {
        Self(format!("{store}/access-keys/{member}/"))
    }
    /// The store whose objects this prefix selects.
    pub fn store(&self) -> StoreId {
        store_id(&self.0)
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
    let mut parts = value.split('/');
    if !parts.next().is_some_and(canonical_uuid) {
        return Err(PathError);
    }
    let parts: Vec<_> = parts.collect();
    let valid = match parts.as_slice() {
        [] => true,
        ["devices" | "store-log" | "positions" | "clock" | "snapshots" | "keys" | "files"
        | "join-requests" | "access-keys"] => true,
        ["access-keys", member] => member_id(member),
        ["devices" | "store-log" | "files" | "keys", device] => decimal(device, false),
        ["snapshots", audience] => *audience == "store" || canonical_uuid(audience),
        ["snapshots", audience, device] => {
            (*audience == "store" || canonical_uuid(audience)) && decimal(device, false)
        }
        _ => false,
    };
    if !valid {
        return Err(PathError);
    }
    Ok(())
}

fn store_id(path: &str) -> StoreId {
    StoreId(
        path.split_once('/')
            .expect("validated store path")
            .0
            .parse()
            .expect("validated store id"),
    )
}

fn decimal(part: &str, nonzero: bool) -> bool {
    match part.parse::<u64>() {
        Ok(n) => (!nonzero || n != 0) && n.to_string() == part,
        Err(_) => false,
    }
}
fn member_id(value: &str) -> bool {
    value
        .parse::<coven_foundation::id_source::MemberId>()
        .is_ok()
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
