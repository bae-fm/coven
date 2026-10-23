//! Wire encodings for the identities their owning crates define, and SQLite values.

use crate::error::{bound, require, Error, Rule};
use crate::wire::{get_blob, put_blob, wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{SealingKey, SigningKey};
use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, KeyId, MemberId, StoreId};
pub use coven_merge::Slot;
use coven_merge::{Audience, RowId, Timestamp, WriteId};
use uuid::Uuid;

macro_rules! byte_id {
    ($($ty:ty),*) => { $(
        impl Wire for $ty {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> { self.0.put(out) }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> { Ok(Self(Wire::get(input)?)) }
        }
    )* };
}
byte_id!(DeviceId);
macro_rules! uuid_id {
    ($($ty:ty),*) => { $(
        impl Wire for $ty {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> { self.0.as_bytes().put(out) }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
                Ok(Self(Uuid::from_bytes(Wire::get(input)?)))
            }
        }
    )* };
}
uuid_id!(StoreId, CircleId, InviteId, KeyId, FileId, MemberId);

impl Wire for SigningKey {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.as_bytes().put(out)
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "coven-crypto refuses a signing key only as invalid or weak"
    )]
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::from_bytes(Wire::get(input)?).map_err(|_| Error::InvalidSigningKey)
    }
}
wire_struct!(coven_crypto::MemberPublicKeys, signing, sealing);
impl Wire for SealingKey {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.as_bytes().put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self::from_bytes(Wire::get(input)?))
    }
}
impl Wire for Timestamp {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        out.bytes(&self.milliseconds().to_be_bytes()[2..])?;
        self.counter().put(out)?;
        self.device().put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let mut ms = [0; 8];
        ms[2..].copy_from_slice(input.take(6)?);
        Self::new(u64::from_be_bytes(ms), Wire::get(input)?, Wire::get(input)?)
            .map_err(Error::Merge)
    }
}
impl Wire for Audience {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Store => 0u8.put(out),
            Self::Circle(id) => {
                1u8.put(out)?;
                id.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Store),
            1 => Ok(Self::Circle(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "audience",
                tag,
            }),
        }
    }
}
wire_struct!(WriteId, device, number);
impl Wire for Slot {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        positive(self.0)?;
        self.0.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let slot = Self(u64::get(input)?);
        positive(slot.0)?;
        Ok(slot)
    }
}

/// A provider storage time: Unix seconds and a nanosecond fraction (D2, §20.5).
/// It keeps every returned digit on every platform, unlike `SystemTime`,
/// which counts 100 ns on Windows; ordering is time order.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "StoredStorageTime")]
pub struct StorageTime {
    seconds: u64,
    nanos: u32,
}

/// A serialized `StorageTime`, checked on the way in.
#[derive(serde::Deserialize)]
struct StoredStorageTime {
    seconds: u64,
    nanos: u32,
}

impl TryFrom<StoredStorageTime> for StorageTime {
    type Error = Error;
    fn try_from(stored: StoredStorageTime) -> Result<Self, Error> {
        Self::new(stored.seconds, stored.nanos)
    }
}

impl StorageTime {
    /// The Unix epoch, the earliest storage time.
    pub const UNIX_EPOCH: Self = Self {
        seconds: 0,
        nanos: 0,
    };

    /// A sample whose fraction is below one second.
    pub fn new(seconds: u64, nanos: u32) -> Result<Self, Error> {
        require(nanos < 1_000_000_000, "storage time", Rule::Nanoseconds)?;
        Ok(Self { seconds, nanos })
    }

    /// The exact provider time, refusing one before the Unix epoch.
    #[expect(
        clippy::map_err_ignore,
        reason = "a time before the epoch is the Range rule; how far before is not part of it"
    )]
    pub fn from_system_time(time: std::time::SystemTime) -> Result<Self, Error> {
        let since = time
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Invalid {
                field: "storage time",
                rule: Rule::Range,
            })?;
        Self::new(since.as_secs(), since.subsec_nanos())
    }

    /// The time `since` the Unix epoch.
    pub fn since_epoch(since: std::time::Duration) -> Self {
        Self {
            seconds: since.as_secs(),
            nanos: since.subsec_nanos(),
        }
    }

    /// The exact time elapsed since the Unix epoch.
    pub fn elapsed_since_epoch(self) -> std::time::Duration {
        std::time::Duration::new(self.seconds, self.nanos)
    }

    /// This time plus `duration`, or `None` past the representable range.
    pub fn checked_add(self, duration: std::time::Duration) -> Option<Self> {
        self.elapsed_since_epoch()
            .checked_add(duration)
            .map(Self::since_epoch)
    }

    /// This time minus `duration`, or `None` before the Unix epoch.
    pub fn checked_sub(self, duration: std::time::Duration) -> Option<Self> {
        self.elapsed_since_epoch()
            .checked_sub(duration)
            .map(Self::since_epoch)
    }

    /// The exact time from `earlier` to this time, or `None` if `earlier` is later.
    pub fn duration_since(self, earlier: Self) -> Option<std::time::Duration> {
        self.elapsed_since_epoch()
            .checked_sub(earlier.elapsed_since_epoch())
    }

    /// Whole Unix seconds.
    pub fn seconds(self) -> u64 {
        self.seconds
    }

    /// The fraction of a second, in nanoseconds.
    pub fn nanos(self) -> u32 {
        self.nanos
    }
}

/// Panics past the representable range, as `SystemTime` does; use
/// `checked_add` where the duration is not known to fit.
impl std::ops::Add<std::time::Duration> for StorageTime {
    type Output = Self;
    fn add(self, duration: std::time::Duration) -> Self {
        self.checked_add(duration)
            .expect("storage time past the representable range")
    }
}

impl std::ops::AddAssign<std::time::Duration> for StorageTime {
    fn add_assign(&mut self, duration: std::time::Duration) {
        *self = *self + duration;
    }
}

/// Panics before the Unix epoch; use `checked_sub` where that can happen.
impl std::ops::Sub<std::time::Duration> for StorageTime {
    type Output = Self;
    fn sub(self, duration: std::time::Duration) -> Self {
        self.checked_sub(duration)
            .expect("storage time before the Unix epoch")
    }
}

impl Wire for StorageTime {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.seconds.put(out)?;
        self.nanos.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::new(Wire::get(input)?, Wire::get(input)?)
    }
}

pub(crate) fn positive(number: u64) -> Result<(), Error> {
    require(number > 0, "positive number", Rule::Required)
}

/// A SQLite value. Real numbers use IEEE 754 bits; NaNs and negative zero are
/// refused so each numeric value has one representation. Infinities are allowed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// A double's bits, encoded big-endian.
    Real(u64),
    /// UTF-8 text, without Unicode normalization.
    Text(String),
    /// Bytes, including an empty blob.
    Blob(Vec<u8>),
}
impl Value {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Self::Real(bits) = self {
            require(
                !f64::from_bits(*bits).is_nan() && *bits != (1u64 << 63),
                "real",
                Rule::Real,
            )?;
        }
        Ok(())
    }
}
impl Wire for Value {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Null => 0u8.put(out),
            Self::Integer(v) => {
                1u8.put(out)?;
                v.put(out)
            }
            Self::Real(v) => {
                2u8.put(out)?;
                v.put(out)
            }
            Self::Text(v) => {
                3u8.put(out)?;
                v.put(out)
            }
            Self::Blob(v) => {
                4u8.put(out)?;
                put_blob(v, out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Null),
            1 => Ok(Self::Integer(i64::get(input)?)),
            2 => Ok(Self::Real(u64::get(input)?)),
            3 => Ok(Self::Text(String::get(input)?)),
            4 => Ok(Self::Blob(get_blob(input)?)),
            tag => Err(Error::UnknownTag {
                field: "SQLite value",
                tag,
            }),
        }
    }
}

impl Wire for RowId {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.table.put(out)?;
        put_blob(&self.key, out)?;
        self.audience.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            table: crate::wire::get_name(input)?,
            key: get_blob(input)?,
            audience: Wire::get(input)?,
        })
    }
}
pub(crate) fn row(row: &RowId) -> Result<(), Error> {
    name(&row.table)?;
    crate::key::decode_key(&row.key)?;
    Ok(())
}

/// Check D2's `name` rule: 1 to 1,024 bytes, no NUL. Table, column, store,
/// device, circle and invited people's names follow it.
pub fn name(value: &str) -> Result<(), Error> {
    bound(value.len(), 1024, "name")?;
    require(
        !value.is_empty() && !value.contains('\0'),
        "name",
        Rule::Required,
    )
}

#[cfg(test)]
#[path = "value_tests.rs"]
mod tests;
