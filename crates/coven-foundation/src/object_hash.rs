//! The 32-byte content address every stored object, commit, and blob fact is
//! identified by: SHA-256 over exact bytes, rendered as lowercase hex.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// A string that is not the lowercase-hex rendering of a 32-byte digest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid object hash: {0}")]
pub struct InvalidObjectHash(pub String);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    const ENCODED_LEN: usize = 64;

    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn from_digest(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn encoded(self) -> [u8; Self::ENCODED_LEN] {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";

        let mut encoded = [0; Self::ENCODED_LEN];
        for (index, byte) in self.0.into_iter().enumerate() {
            encoded[index * 2] = DIGITS[usize::from(byte >> 4)];
            encoded[index * 2 + 1] = DIGITS[usize::from(byte & 0x0f)];
        }
        encoded
    }

    fn decode_nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
}

impl fmt::Debug for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded = self.encoded();
        formatter.write_str(
            std::str::from_utf8(&encoded).expect("ObjectHash lowercase hex must be UTF-8"),
        )
    }
}

impl FromStr for ObjectHash {
    type Err = InvalidObjectHash;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != Self::ENCODED_LEN {
            return Err(InvalidObjectHash(value.to_string()));
        }
        let mut bytes = [0_u8; 32];
        for (output, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            let Some(high) = Self::decode_nibble(pair[0]) else {
                return Err(InvalidObjectHash(value.to_string()));
            };
            let Some(low) = Self::decode_nibble(pair[1]) else {
                return Err(InvalidObjectHash(value.to_string()));
            };
            *output = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for ObjectHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = self.encoded();
        serializer.serialize_str(
            std::str::from_utf8(&encoded).expect("ObjectHash lowercase hex must be UTF-8"),
        )
    }
}

struct ObjectHashVisitor;

impl<'de> serde::de::Visitor<'de> for ObjectHashVisitor {
    type Value = ObjectHash;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a 64-character lowercase hexadecimal SHA-256 digest")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for ObjectHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ObjectHashVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::{Error as _, Visitor};

    struct BorrowedHash<'a>(&'a str);

    impl<'de> Deserializer<'de> for BorrowedHash<'de> {
        type Error = serde::de::value::Error;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_borrowed_str(self.0)
        }

        fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_borrowed_str(self.0)
        }

        fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Err(Self::Error::custom(
                "ObjectHash requested an owned string while deserializing",
            ))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf option unit
            unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
            ignored_any
        }
    }

    #[test]
    fn deserialization_accepts_a_borrowed_hash_without_requesting_an_owned_string() {
        let encoded = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let hash = ObjectHash::deserialize(BorrowedHash(encoded)).expect("deserialize hash");
        assert_eq!(hash.to_string(), encoded);
    }
}
