//! Wire encodings for the identities their owning crates define, and SQLite values.

use crate::error::{bound, require, Error, Rule};
use crate::wire::{get_blob, put_blob, wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{Fingerprint, MemberId, SealingPublicKey};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, StoreId};
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
uuid_id!(StoreId, CircleId, InviteId, KeyId);

impl Wire for MemberId {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.to_bytes().put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::from_bytes(Wire::get(input)?).map_err(|_| Error::InvalidMemberId)
    }
}
impl Wire for SealingPublicKey {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.as_bytes().put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self::from_bytes(Wire::get(input)?))
    }
}
impl Wire for Fingerprint {
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

/// One device's numbered store-log entry (§9), distinct from a merge write.
///
/// ```compile_fail
/// use coven_format::value::EntryPositions;
/// use coven_foundation::id_source::DeviceId;
/// use coven_merge::WriteId;
/// let entries = EntryPositions(vec![WriteId { device: DeviceId(1), number: 1 }]);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    /// The device whose store log contains the entry.
    pub device: DeviceId,
    /// The entry's number, starting at one.
    pub number: u64,
}
wire_struct!(EntryId, device, number);
impl EntryId {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.number)
    }
}

macro_rules! positions {
    ($ty:ident, $id:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $ty(
            /// Positive positions, strictly increasing by device, without duplicates.
            pub Vec<$id>,
        );
        impl $ty {
            pub(crate) fn validate(&self) -> Result<(), Error> {
                bound(self.0.len(), crate::wire::MAX_ITEMS, "collection")?;
                ordered(&self.0, |p| p.device, "positions")?;
                for p in &self.0 {
                    positive(p.number)?;
                }
                Ok(())
            }
            /// Whether this set covers the named position.
            pub fn covers(&self, position: $id) -> bool {
                self.0
                    .iter()
                    .any(|p| p.device == position.device && p.number >= position.number)
            }
            pub(crate) fn without_own_device(&self, device: DeviceId) -> Result<(), Error> {
                self.validate()?;
                require(
                    self.0.iter().all(|p| p.device != device),
                    "had-read own device",
                    Rule::OwnPosition,
                )
            }
        }
        byte_id!($ty);
    };
}
positions!(
    WritePositions,
    WriteId,
    "How far each device's write log has been read (§7.1)."
);
impl WritePositions {
    /// The author's causal past, including its implicit earlier own writes.
    /// These positions must name only other devices, as in a write header or
    /// applied-write record. The frontier retains their order, then the author's
    /// preceding write, without copying or expanding the positions.
    pub fn causal_past(&self, author: WriteId) -> impl coven_merge::WritePast + '_ {
        CausalPast {
            others: self,
            author,
            previous: (author.number > 1).then(|| WriteId {
                number: author.number - 1,
                ..author
            }),
        }
    }
}

struct CausalPast<'a> {
    others: &'a WritePositions,
    author: WriteId,
    previous: Option<WriteId>,
}

impl coven_merge::WritePast for CausalPast<'_> {
    fn contains(&self, write: &WriteId) -> bool {
        if write.device == self.author.device {
            write.number < self.author.number
        } else {
            self.others.covers(*write)
        }
    }

    fn frontier(&self) -> impl Iterator<Item = &WriteId> {
        self.others.0.iter().chain(self.previous.as_ref())
    }
}

impl coven_merge::WritePast for WritePositions {
    fn contains(&self, write: &WriteId) -> bool {
        self.covers(*write)
    }
    fn frontier(&self) -> impl Iterator<Item = &WriteId> {
        self.0.iter()
    }
}

positions!(
    EntryPositions,
    EntryId,
    "How far each device's store log has been read (§9)."
);

pub(crate) fn positive(number: u64) -> Result<(), Error> {
    require(number > 0, "log number", Rule::Required)
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

pub(crate) fn name(value: &str) -> Result<(), Error> {
    bound(value.len(), 1024, "name")?;
    require(
        !value.is_empty() && !value.contains('\0'),
        "name",
        Rule::Required,
    )
}

pub(crate) fn ordered<T, K: Ord>(
    items: &[T],
    key: impl Fn(&T) -> K,
    field: &'static str,
) -> Result<(), Error> {
    require(
        items.windows(2).all(|pair| key(&pair[0]) < key(&pair[1])),
        field,
        Rule::Order,
    )
}

#[cfg(test)]
#[path = "value_tests.rs"]
mod tests;
