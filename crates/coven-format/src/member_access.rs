//! The storage account carried by member additions and access updates (Appendix D6).

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use serde::{Deserialize, Serialize};

const MAX_TEXT: usize = 1024;

/// An account to share with, or a member's S3 key and what deleting it needs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberAccess {
    /// The member's provider account email.
    ProviderAccount(String),
    /// A member's S3 key.
    S3AccessKey {
        /// Public identifier; never the secret access key.
        access_key_id: String,
        /// The provider identity holding the key, as non-secret handles.
        principal: S3Principal,
    },
}

impl MemberAccess {
    /// Whether both name the same credential: one account, compared without
    /// case or surrounding space, or one S3 key id whatever principal it names.
    pub fn same_credential(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ProviderAccount(a), Self::ProviderAccount(b)) => {
                a.trim().eq_ignore_ascii_case(b.trim())
            }
            (
                Self::S3AccessKey {
                    access_key_id: a, ..
                },
                Self::S3AccessKey {
                    access_key_id: b, ..
                },
            ) => a == b,
            _ => false,
        }
    }
}

/// Who holds an S3 key at its provider: the handles deleting it needs (§13).
/// None is a secret; each names an identity in the storage account.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Principal {
    /// Made by hand in the provider's console. Coven holds no handle beyond
    /// the key id, so only the console deletes it.
    Console,
    /// An IAM user holding the key: AWS IAM, Ceph RGW accounts and SeaweedFS's
    /// embedded IAM. Deleting the user requires removing its policy first.
    IamUser {
        /// The user name.
        user: String,
        /// The policy granting the user the store's prefix.
        policy: IamPolicy,
    },
    /// A GCS service account whose HMAC key is the member key. Its access id is
    /// the key id; the service account is granted one managed folder.
    GcsServiceAccount {
        /// The project holding the service account and its HMAC keys.
        project: String,
        /// The service account's email.
        email: String,
        /// The managed folder granted to it, `<prefix>/<store>/`.
        managed_folder: String,
    },
    /// A Cloudflare account API token; the key id is the token id.
    R2Token {
        /// The Cloudflare account holding the token.
        account: String,
    },
    /// A RustFS service account whose access key is the key id, under the
    /// dedicated parent user coven's service accounts share.
    RustFsServiceAccount {
        /// The parent user.
        parent: String,
    },
}

/// How an IAM user receives its prefix policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IamPolicy {
    /// An inline user policy (AWS, Ceph RGW), deleted by name.
    Inline {
        /// The policy name on the user.
        name: String,
    },
    /// A managed policy attached to the user (SeaweedFS), detached and deleted by ARN.
    Managed {
        /// The policy's ARN.
        arn: String,
    },
}

fn put_text(text: &str, out: &mut Encoder) -> Result<(), Error> {
    crate::error::bound(text.len(), MAX_TEXT, "member access")?;
    text.to_owned().put(out)
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
            Self::S3AccessKey {
                access_key_id,
                principal,
            } => {
                crate::error::bound(access_key_id.len(), MAX_TEXT, "member access")?;
                1u8.put(out)?;
                access_key_id.put(out)?;
                principal.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::ProviderAccount(get_text(input)?)),
            1 => Ok(Self::S3AccessKey {
                access_key_id: get_text(input)?,
                principal: S3Principal::get(input)?,
            }),
            tag => Err(Error::UnknownTag {
                field: "member access",
                tag,
            }),
        }
    }
}

impl Wire for S3Principal {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Console => 0u8.put(out),
            Self::IamUser { user, policy } => {
                1u8.put(out)?;
                put_text(user, out)?;
                match policy {
                    IamPolicy::Inline { name } => {
                        0u8.put(out)?;
                        put_text(name, out)
                    }
                    IamPolicy::Managed { arn } => {
                        1u8.put(out)?;
                        put_text(arn, out)
                    }
                }
            }
            Self::GcsServiceAccount {
                project,
                email,
                managed_folder,
            } => {
                2u8.put(out)?;
                put_text(project, out)?;
                put_text(email, out)?;
                put_text(managed_folder, out)
            }
            Self::R2Token { account } => {
                3u8.put(out)?;
                put_text(account, out)
            }
            Self::RustFsServiceAccount { parent } => {
                4u8.put(out)?;
                put_text(parent, out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Console),
            1 => {
                let user = get_text(input)?;
                let policy = match u8::get(input)? {
                    0 => IamPolicy::Inline {
                        name: get_text(input)?,
                    },
                    1 => IamPolicy::Managed {
                        arn: get_text(input)?,
                    },
                    tag => {
                        return Err(Error::UnknownTag {
                            field: "IAM policy",
                            tag,
                        })
                    }
                };
                Ok(Self::IamUser { user, policy })
            }
            2 => Ok(Self::GcsServiceAccount {
                project: get_text(input)?,
                email: get_text(input)?,
                managed_folder: get_text(input)?,
            }),
            3 => Ok(Self::R2Token {
                account: get_text(input)?,
            }),
            4 => Ok(Self::RustFsServiceAccount {
                parent: get_text(input)?,
            }),
            tag => Err(Error::UnknownTag {
                field: "S3 principal",
                tag,
            }),
        }
    }
}

#[cfg(test)]
#[path = "member_access_tests.rs"]
mod tests;
