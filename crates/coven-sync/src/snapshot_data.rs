//! Journal state for snapshot publication and reload. Retention is derived
//! each pass and has no journal state (§18.1).

use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use coven_storage::{StoredObject, UploadSession};
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
        /// The prepared attempt; none before preparation and again once an
        /// expired attempt is abandoned (§15).
        attempt: Option<SnapshotAttempt>,
        /// Abandoned attempts whose request may land, or has landed too late
        /// to count: the operation deletes each landing that does not count
        /// and finishes only once each is settled (§15).
        abandoned: Vec<AbandonedAttempt>,
    },
    Reload {
        scope: ReloadScope,
        files: Option<ReloadFiles>,
    },
}

/// One preparation's publication: the path number it reserved and the
/// upload session its sealed file is sent through. Its signed `T_c` and
/// sealed bytes stay fixed on retry; a new attempt takes a new number.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotAttempt {
    pub(crate) number: std::num::NonZeroU64,
    /// Its signed `T_c`: the attempt counts only if it lands within a day.
    pub(crate) caught_at: coven_format::value::StorageTime,
    pub(crate) session: Option<UploadSession>,
    /// Saved before its first request: once set, a request may have reached
    /// storage, so abandoning it records it (§15).
    pub(crate) sent: bool,
}

/// An attempt given up before a counting landing was confirmed, recorded
/// with the abandonment. A landing at its path counts only if it is at most
/// a day after `caught_at`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AbandonedAttempt {
    pub(crate) number: std::num::NonZeroU64,
    /// The attempt's signed `T_c`.
    pub(crate) caught_at: coven_format::value::StorageTime,
    pub(crate) landing: AbandonedLanding,
}

/// What the device knew of an abandoned attempt's landing (§15).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AbandonedLanding {
    /// Its landing was confirmed too late to count: once its path is absent
    /// the deletion is done.
    Late,
    /// Abandoned before any landing was confirmed, at this observed storage
    /// time: a request may still land. An absent path is settled once an
    /// observed storage time is more than the listing margin past it.
    Unconfirmed {
        abandoned_at: coven_format::value::StorageTime,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum ReloadScope {
    All,
    /// Audiences whose reset or schema version replay changed, while the
    /// database still requires their reload.
    Changed(#[serde(with = "audiences")] Vec<Audience>),
    /// Audiences whose newest snapshot passes a returning device's positions.
    Returned(#[serde(with = "audiences")] Vec<Audience>),
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
    /// The authenticated observation whose plaintext `file` holds; a body
    /// the reload transaction rejects is refused under it (§19.1).
    pub(crate) object: StoredObject,
    pub(crate) file: String,
    pub(crate) prefix: Vec<u8>,
    /// The member whose device signed it, in its signed view (§15).
    pub(crate) author: coven_foundation::id_source::MemberId,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedWrite {
    pub(crate) header: Vec<u8>,
    pub(crate) parts: Vec<Option<String>>,
    /// First complete publication time, for the retirement window (§10).
    pub(crate) stored_at: coven_format::value::StorageTime,
    /// The signing key that authenticated the write in its recorded view,
    /// for the old-key window (§10).
    pub(crate) signer: [u8; 32],
    /// The write's signed header and part roots, which the loaded parts must
    /// reproduce and later snapshots carry (D9).
    pub(crate) evidence: SavedEvidence,
}

/// A write's header evidence, as the journal keeps it.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedEvidence {
    prefix: Vec<u8>,
    header: Vec<u8>,
    roots: Vec<[u8; 32]>,
    signature: Vec<u8>,
}

impl SavedEvidence {
    pub(crate) fn new(evidence: coven_database::HeaderEvidence) -> Self {
        Self {
            prefix: evidence.prefix,
            header: evidence.header,
            roots: evidence.roots,
            signature: evidence.signature.to_vec(),
        }
    }

    pub(crate) fn evidence(
        &self,
    ) -> Result<coven_database::HeaderEvidence, coven_database::DbError> {
        Ok(coven_database::HeaderEvidence {
            prefix: self.prefix.clone(),
            header: self.header.clone(),
            roots: self.roots.clone(),
            signature: self
                .signature
                .as_slice()
                .try_into()
                .map_err(|_| coven_database::DbError::DamagedDatabase)?,
        })
    }
}

pub(crate) mod audience {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &Audience,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Audience::Store => serializer.serialize_str("store"),
            Audience::Circle(id) => serializer.serialize_str(&id.to_string()),
        }
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
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
    pub(super) struct Item(#[serde(with = "audience")] pub(super) Audience);
    pub(crate) fn serialize<S: serde::Serializer>(
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
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
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
    pub(crate) entry: (DeviceId, u64),
    pub(crate) prefix: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum SnapshotTrigger {
    Growth,
    Requested,
    /// Retention found this final, gated write uncovered by the audience's
    /// current newest counting snapshot (§15). Preparation publishes only
    /// while that is still so.
    Coverage {
        writer: DeviceId,
        number: u64,
    },
    Reset,
    Raise {
        version: u32,
        entry: Option<Vec<u8>>,
    },
}

/// The journal retains the audience, device and positive snapshot number.
pub(crate) mod snapshot_id {
    use super::*;
    use coven_format::store_log::SnapshotId;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &SnapshotId,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let number = std::num::NonZeroU64::new(value.number)
            .ok_or_else(|| serde::ser::Error::custom("zero snapshot number"))?;
        (
            audiences::Item(value.audience.clone()),
            value.device,
            number,
        )
            .serialize(serializer)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SnapshotId, D::Error> {
        let (audience, device, number) =
            <(audiences::Item, DeviceId, std::num::NonZeroU64)>::deserialize(deserializer)?;
        Ok(SnapshotId {
            audience: audience.0,
            device,
            number: number.get(),
        })
    }
}

impl SnapshotJob {
    pub(crate) fn same_work(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Write {
                    audience: a,
                    trigger: ta,
                    ..
                },
                Self::Write {
                    audience: b,
                    trigger: tb,
                    ..
                },
            ) => {
                a == b
                    && match (ta, tb) {
                        (
                            SnapshotTrigger::Raise { version: a, .. },
                            SnapshotTrigger::Raise { version: b, .. },
                        ) => a == b,
                        (SnapshotTrigger::Growth, SnapshotTrigger::Growth)
                        | (SnapshotTrigger::Requested, SnapshotTrigger::Requested)
                        | (SnapshotTrigger::Coverage { .. }, SnapshotTrigger::Coverage { .. })
                        | (SnapshotTrigger::Reset, SnapshotTrigger::Reset) => true,
                        _ => false,
                    }
            }
            (Self::Reload { scope: a, .. }, Self::Reload { scope: b, .. }) => match (a, b) {
                (ReloadScope::All, ReloadScope::All) => true,
                (ReloadScope::Changed(a), ReloadScope::Changed(b))
                | (ReloadScope::Returned(a), ReloadScope::Returned(b)) => a == b,
                _ => false,
            },
            _ => false,
        }
    }
}
