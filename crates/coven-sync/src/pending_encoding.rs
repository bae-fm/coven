//! Local serde adapters for domain values whose owning crates define wire encodings.
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use serde::{Deserialize, Serialize};

pub(crate) mod sealing {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        v: &coven_crypto::SealingKey,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.as_bytes().serialize(s)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<coven_crypto::SealingKey, D::Error> {
        Ok(coven_crypto::SealingKey::from_bytes(
            <[u8; 32]>::deserialize(d)?,
        ))
    }
}
macro_rules! write_id {
    ($module:ident, $ty:path) => {
        pub(crate) mod $module {
            use super::*;
            pub(crate) fn serialize<S: serde::Serializer>(
                v: &$ty,
                s: S,
            ) -> Result<S::Ok, S::Error> {
                (v.device, v.number).serialize(s)
            }
            pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
                d: D,
            ) -> Result<$ty, D::Error> {
                let (device, number) = Deserialize::deserialize(d)?;
                if number == 0 {
                    return Err(serde::de::Error::custom("zero write number"));
                }
                type Id = $ty;
                Ok(Id { device, number })
            }
        }
    };
}
write_id!(write, coven_merge::WriteId);
pub(crate) mod slot {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &coven_merge::Slot,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        value.0.serialize(s)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<coven_merge::Slot, D::Error> {
        Ok(coven_merge::Slot(u64::deserialize(d)?))
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
}

pub(crate) mod snapshot_id {
    use super::*;
    use coven_format::admin::SnapshotId;
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

/// Public pairs kept as operation preconditions, independent of wire frames.
pub(crate) mod public_keys {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &coven_crypto::MemberPublicKeys,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        (value.signing.as_bytes(), value.sealing.as_bytes()).serialize(serializer)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<coven_crypto::MemberPublicKeys, D::Error> {
        let (signing, sealing) = <([u8; 32], [u8; 32])>::deserialize(deserializer)?;
        Ok(coven_crypto::MemberPublicKeys {
            signing: coven_crypto::SigningKey::from_bytes(signing)
                .map_err(serde::de::Error::custom)?,
            sealing: coven_crypto::SealingKey::from_bytes(sealing),
        })
    }
}

/// A path on this device as its recorded bytes (`path_bytes`), which hold a
/// name that isn't valid Unicode too.
pub(crate) mod path {
    use super::*;
    use std::path::{Path, PathBuf};
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &Path,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        coven_foundation::files::path_bytes(value).serialize(serializer)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PathBuf, D::Error> {
        coven_foundation::files::path_from_bytes(Vec::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
