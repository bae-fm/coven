//! Local serde adapters for domain values whose owning crates define wire encodings.
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
macro_rules! log_id {
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
                    return Err(serde::de::Error::custom("zero log number"));
                }
                type Id = $ty;
                Ok(Id { device, number })
            }
        }
    };
}
log_id!(write, coven_merge::WriteId);
log_id!(entry, coven_format::value::EntryId);

pub(crate) mod refusal {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        v: &crate::Refusal,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        u8::from(coven_format::pending::RefusalCode::from(v)).serialize(s)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<crate::Refusal, D::Error> {
        coven_format::pending::RefusalCode::try_from(u8::deserialize(d)?)
            .map(Into::into)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "coven_format::pending::RequiredUpdate")]
pub(crate) enum Update {
    AppSchema { version: u32 },
    CovenFormat { version: u16 },
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "coven_format::pending::FileSourceFailure")]
pub(crate) enum SourceFailure {
    Missing,
    Changed,
    Integrity,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "coven_database::EntryTarget")]
pub(crate) enum Target {
    Member(coven_foundation::id_source::MemberId),
    Device(coven_foundation::id_source::DeviceId),
    Circle(coven_foundation::id_source::CircleId),
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "coven_database::DropReason")]
pub(crate) enum Drop {
    LandedTooLate,
    BeatenBy(#[serde(with = "entry")] coven_format::value::EntryId),
    TargetGone(#[serde(with = "Target")] coven_database::EntryTarget),
    NoAdminLeft,
    NotAllowed,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "crate::AccessKeyToDelete")]
pub(crate) struct AccessKey {
    access_key_id: String,
    principal: coven_format::S3Principal,
    member: Option<coven_foundation::id_source::MemberId>,
}
