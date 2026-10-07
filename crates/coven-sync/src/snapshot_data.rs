//! Journal state for snapshot publication, reload and retention.

use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, UploadSession};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotTask {
    pub(crate) job: SnapshotJob,
    /// Every disk name is recorded before creation, including partial downloads.
    pub(crate) temporary: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum SnapshotJob {
    Write {
        #[serde(with = "audience")]
        audience: Audience,
        device: DeviceId,
        trigger: SnapshotTrigger,
        session: Option<UploadSession>,
    },
    Reload {
        scope: ReloadScope,
        files: Option<ReloadFiles>,
    },
    Retain,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum ReloadScope {
    All,
    Changed(#[serde(with = "audiences")] Vec<Audience>),
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ReloadFiles {
    pub(crate) snapshots: Vec<SavedSnapshot>,
    #[serde(with = "audiences")]
    pub(crate) empty: Vec<Audience>,
    pub(crate) absent: Vec<(DeviceId, u64)>,
    pub(crate) writes: Vec<SavedWrite>,
    pub(crate) boundaries: Vec<SavedBoundary>,
    pub(crate) expected_entries: Vec<(DeviceId, u64)>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedSnapshot {
    pub(crate) path: ObjectPath,
    pub(crate) file: String,
    pub(crate) prefix: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedWrite {
    pub(crate) header: Vec<u8>,
    pub(crate) parts: Vec<Option<String>>,
}

pub(crate) mod audience {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &Audience,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Audience::Store => serializer.serialize_str("store"),
            Audience::Circle(id) => serializer.serialize_str(&id.to_string()),
        }
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Audience, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value == "store" {
            Ok(Audience::Store)
        } else {
            Ok(Audience::Circle(coven_foundation::id_source::CircleId(
                value.parse().map_err(serde::de::Error::custom)?,
            )))
        }
    }
}

mod audiences {
    use super::*;
    #[derive(Serialize, Deserialize)]
    struct Item(#[serde(with = "audience")] Audience);
    pub(super) fn serialize<S: serde::Serializer>(
        values: &[Audience],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        values
            .iter()
            .cloned()
            .map(Item)
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Audience>, D::Error> {
        Ok(Vec::<Item>::deserialize(deserializer)?
            .into_iter()
            .map(|item| item.0)
            .collect())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedBoundary {
    pub(crate) entry: Vec<u8>,
    pub(crate) prefix: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum SnapshotTrigger {
    Growth,
    Requested,
    Reset,
    Raise {
        version: u32,
        entry: Option<Vec<u8>>,
    },
}

/// The journal uses the canonical snapshot path to retain the format's identity.
pub(crate) mod snapshot_id {
    use super::*;
    use coven_format::store_log::SnapshotId;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &SnapshotId,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let number = std::num::NonZeroU64::new(value.number)
            .ok_or_else(|| serde::ser::Error::custom("zero snapshot number"))?;
        ObjectPath::snapshot(value.audience.clone(), value.device, number).serialize(serializer)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SnapshotId, D::Error> {
        ObjectPath::deserialize(deserializer)?
            .snapshot_id()
            .ok_or_else(|| serde::de::Error::custom("not a snapshot path"))
    }
}
