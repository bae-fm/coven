//! The storage account carried by member additions and access updates (Appendix D6).

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use serde::{Deserialize, Serialize};

/// An account to share with, or the public identifier of a member's S3 key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberAccess {
    /// The member's provider account email.
    ProviderAccount(String),
    /// An S3 key made in the provider console.
    S3AccessKey {
        /// Public identifier; never the secret access key.
        access_key_id: String,
    },
}

impl Wire for MemberAccess {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        let text = match self {
            Self::ProviderAccount(account) => account,
            Self::S3AccessKey { access_key_id } => access_key_id,
        };
        crate::error::bound(text.len(), 1024, "member access")?;
        match self {
            Self::ProviderAccount(account) => {
                0u8.put(out)?;
                account.put(out)
            }
            Self::S3AccessKey { access_key_id } => {
                1u8.put(out)?;
                access_key_id.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::ProviderAccount(crate::wire::get_text(
                input,
                1024,
                "member access",
            )?)),
            1 => Ok(Self::S3AccessKey {
                access_key_id: crate::wire::get_text(input, 1024, "member access")?,
            }),
            tag => Err(Error::UnknownTag {
                field: "member access",
                tag,
            }),
        }
    }
}
