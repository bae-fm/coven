//! Canonical object names and listing prefixes from format.md D10.
//!
//! Crate-to-crate API: coven's format, storage and sync code build paths and
//! prefixes here. Native provider adapters map these validated paths into
//! their provider's object names.

use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, StoreId};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// One validated encrypted-object path in the layout of §§5, 10, 12, 13, 16 and 17.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPath(String);

impl ObjectPath {
    /// One create-once slot in the store's shared sequence (§5).
    pub fn slot(store: StoreId, number: NonZeroU64) -> Self {
        Self(format!("{store}/seq/{number}"))
    }
    /// The positive number of a sequence slot.
    pub fn slot_number(&self) -> Option<crate::value::Slot> {
        self.relative()
            .strip_prefix("seq/")
            .map(|number| crate::value::Slot(number.parse().expect("validated slot")))
    }
    /// The store's conditionally replaced floor (§16.4).
    pub fn floor(store: StoreId) -> Self {
        Self(format!("{store}/floor"))
    }
    /// The identity carried by a snapshot path.
    pub fn snapshot_id(&self) -> Option<crate::admin::SnapshotId> {
        let rest = self.relative().strip_prefix("snapshots/")?;
        let (audience, rest) = rest.split_once('/').expect("validated snapshot path");
        let (device, number) = rest.split_once('/').expect("validated snapshot path");
        let audience = if audience == "store" {
            Audience::Store
        } else {
            Audience::Circle(CircleId(audience.parse().expect("validated circle")))
        };
        Some(crate::admin::SnapshotId {
            audience,
            device: DeviceId(device.parse().expect("validated device")),
            number: number.parse().expect("validated number"),
        })
    }
    /// A snapshot written by a device (§16).
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
    /// Encrypted file bytes named by a random UUID (§17.3).
    pub fn file(store: StoreId, device: DeviceId, name: FileId) -> Self {
        Self(format!("{store}/files/{}/{name}", device.0))
    }
    /// An encrypted join request under its invite id (§13.2).
    pub fn join_request(store: StoreId, invite: InviteId) -> Self {
        Self(format!("{store}/join-requests/{invite}"))
    }
    /// The invite named by a join request's path.
    pub fn join_request_invite(&self) -> Option<InviteId> {
        self.relative()
            .strip_prefix("join-requests/")
            .map(|invite| InviteId(invite.parse().expect("validated invite id")))
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
    /// Only the floor can be replaced (D10).
    pub fn is_replaceable(&self) -> bool {
        self.relative() == "floor"
    }
    /// The device named by a snapshot or uploaded file path.
    pub fn device(&self) -> Option<DeviceId> {
        let parts: Vec<_> = self.relative().split('/').collect();
        match parts.as_slice() {
            ["files", device, _] | ["snapshots", _, device, _] => {
                Some(DeviceId(device.parse().expect("validated device id")))
            }
            _ => None,
        }
    }

    /// The fixed id of a stored file's path.
    pub fn file_id(&self) -> Option<FileId> {
        match self.relative().split('/').collect::<Vec<_>>().as_slice() {
            ["files", _, name] => Some(FileId(name.parse().expect("validated file id"))),
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
            ["seq", n] => decimal(n, true),
            ["snapshots", audience, device, n] => {
                (*audience == "store" || canonical_uuid(audience))
                    && decimal(device, false)
                    && decimal(n, true)
            }
            ["files", device, name] => decimal(device, false) && canonical_uuid(name),
            ["join-requests", invite] => canonical_uuid(invite),
            ["floor"] => true,
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectPrefix(String);
impl ObjectPrefix {
    /// All objects belonging to this store.
    pub fn all(store: StoreId) -> Self {
        Self(format!("{store}/"))
    }
    /// Every stored file.
    pub fn files(store: StoreId) -> Self {
        Self(format!("{store}/files/"))
    }
    /// The files one device uploaded (§17.3).
    pub fn device_files(store: StoreId, device: DeviceId) -> Self {
        Self(format!("{store}/files/{}/", device.0))
    }
    /// Every waiting join request.
    pub fn join_requests(store: StoreId) -> Self {
        Self(format!("{store}/join-requests/"))
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
        ["seq" | "snapshots" | "files" | "join-requests"] => true,
        ["files", device] => decimal(device, false),
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
fn canonical_uuid(value: &str) -> bool {
    match uuid::Uuid::parse_str(value) {
        Ok(id) => id.to_string() == value,
        Err(_) => false,
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
