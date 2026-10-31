//! The storage account carried by member additions and access updates (format.md D6).

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use serde::{Deserialize, Serialize};

const MAX_TEXT: usize = 1024;

/// An account to share with, or the id of a member's S3 key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberAccess {
    /// The member's provider account email.
    ProviderAccount(String),
    /// A member's S3 key, made in the provider's console.
    S3AccessKey {
        /// Public identifier; never the secret access key.
        access_key_id: String,
    },
}

impl MemberAccess {
    /// Whether both name the same credential: one account, compared without
    /// case or surrounding space, or one S3 key id.
    pub fn same_credential(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ProviderAccount(a), Self::ProviderAccount(b)) => {
                a.trim().eq_ignore_ascii_case(b.trim())
            }
            (Self::S3AccessKey { access_key_id: a }, Self::S3AccessKey { access_key_id: b }) => {
                a == b
            }
            _ => false,
        }
    }
}

fn get_text(input: &mut Decoder<'_>) -> Result<String, Error> {
    crate::wire::get_text(input, MAX_TEXT, "member access")
}

impl Wire for MemberAccess {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::ProviderAccount(account) => {
                crate::error::bound(account.len(), MAX_TEXT, "member access")?;
                0u8.put(out)?;
                account.put(out)
            }
            Self::S3AccessKey { access_key_id } => {
                crate::error::bound(access_key_id.len(), MAX_TEXT, "member access")?;
                1u8.put(out)?;
                access_key_id.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::ProviderAccount(get_text(input)?)),
            1 => Ok(Self::S3AccessKey { access_key_id: get_text(input)? }),
            tag => Err(Error::UnknownTag { field: "member access", tag }),
        }
    }
}

#[cfg(test)]
#[path = "member_access_tests.rs"]
mod tests;
