//! Making and deleting members' S3 keys with the storage account's admin
//! access (§4, §13). Admin credentials live in memory for one operation; they
//! are never logged, stored, or put in the store log or invites.

use crate::{S3Credentials, StorageError};
use async_trait::async_trait;
use coven_crypto::SecretText;
use coven_format::S3Principal;
use coven_foundation::id_source::{DeviceId, StoreId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The storage account's admin access for one operation (§4). Its secrets are
/// erased on drop and never formatted.
#[derive(Clone)]
pub enum AdminCredential {
    /// An admin access key: AWS IAM (or exported short-lived credentials with
    /// their session token), a Ceph RGW account, SeaweedFS IAM, RustFS,
    /// Versity Gateway's IAM.
    AccessKey {
        /// The admin key's public id.
        access_key_id: String,
        /// Its secret.
        secret_access_key: SecretText,
        /// The session token of temporary credentials.
        session_token: Option<SecretText>,
        /// The IAM API's endpoint where the service runs IAM apart from its
        /// S3 endpoint: Versity Gateway's standalone IAM service, which the
        /// operator deploys at its own address. Every other service takes
        /// none: AWS IAM is global, and the rest serve their admin API at the
        /// location's endpoint.
        iam_endpoint: Option<url::Url>,
    },
    /// An OAuth or API bearer token: Google Cloud, or a Cloudflare API token.
    BearerToken(SecretText),
}
impl fmt::Debug for AdminCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AccessKey { .. } => "AdminCredential::AccessKey([REDACTED])",
            Self::BearerToken(_) => "AdminCredential::BearerToken([REDACTED])",
        })
    }
}

/// What a minted key may reach, and the principal names its journal fixed
/// before the first provider call (§4, §18).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintScope {
    /// The store whose prefix the key reaches.
    pub store: StoreId,
    /// The device making the key. Its name carries the device so a key no
    /// entry records is reclaimed only once that device is retired (§13).
    pub device: DeviceId,
    /// The principal's short name, for services whose names are short (GCS
    /// service accounts, RustFS service accounts); a retry with the same
    /// scope finds what an earlier attempt created.
    pub name: String,
    /// The name carrying the store, the minting device and the attempt, which
    /// a store's key listing reads back (`named_device`): an IAM user's name,
    /// and the first word of an R2 token's name or a GCS or RustFS
    /// description.
    pub key_name: String,
    /// Who the key is for, as a provider tag: a member or invite id, no email.
    pub member: String,
}

/// How many hex digits of the store id a key name carries.
const STORE_DIGITS: usize = 24;

impl MintScope {
    /// Name the principal from a journal attempt id. The short name is
    /// `coven-` and 24 hex digits, within every service's limit (GCS service
    /// accounts take 6 to 30 lowercase letters, digits and hyphens, starting
    /// with a letter). The key name is `coven-<store>-<device>-<attempt>`: 24
    /// hex digits of the store id, the device's 16 and 16 of the attempt's,
    /// 64 characters, an IAM user name's limit.
    pub fn new(store: StoreId, member: &str, device: DeviceId, attempt: uuid::Uuid) -> Self {
        let attempt = attempt.simple().to_string();
        let store_digits = store.0.simple().to_string();
        Self {
            store,
            device,
            name: format!("coven-{}", &attempt[..24]),
            key_name: format!(
                "coven-{}-{:016x}-{}",
                &store_digits[..STORE_DIGITS],
                device.0,
                &attempt[..16]
            ),
            member: member.to_owned(),
        }
    }

    /// The key name followed by the member tag: an R2 token's name, and a GCS
    /// or RustFS service account's description.
    pub fn label(&self) -> String {
        format!("{} member {}", self.key_name, self.member)
    }
}

/// The minting device a listed name or description names, when its first
/// word is a key name coven made for this store; `None` for anything else.
pub(crate) fn named_device(text: &str, store: StoreId) -> Option<DeviceId> {
    let name = text.split(' ').next()?;
    let mut parts = name.strip_prefix("coven-")?.split('-');
    let (Some(store_part), Some(device), Some(attempt), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let hex = |part: &str, digits: usize| {
        part.len() == digits
            && part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    let expected = store.0.simple().to_string();
    if !hex(store_part, STORE_DIGITS)
        || store_part != &expected[..STORE_DIGITS]
        || !hex(device, 16)
        || !hex(attempt, 16)
    {
        return None;
    }
    u64::from_str_radix(device, 16).ok().map(DeviceId)
}

/// A key the provider lists for a store, with the minting device its name
/// carries (§13).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedKey {
    /// The key's public id.
    pub access_key_id: String,
    /// The provider identity holding it, as deleting it needs.
    pub principal: S3Principal,
    /// The device whose operation made it.
    pub device: DeviceId,
}

/// A member's S3 key and who holds it. The secret is erased on drop and
/// never formatted.
#[derive(Clone, Serialize, Deserialize)]
pub struct S3MemberKey {
    /// The key itself.
    pub credentials: S3Credentials,
    /// The provider identity holding it.
    pub principal: S3Principal,
}
impl fmt::Debug for S3MemberKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3MemberKey")
            .field("access_key_id", &self.credentials.access_key_id)
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

/// A service's key administration, holding admin access for one operation.
/// Coven deletes a key and then reads whether that exact key remains; only
/// both together clear the key's pending deletion (§13).
#[async_trait]
pub trait AccessKeyAdmin: Send + Sync {
    /// Create the principal and a key limited to the scope's store. Each step
    /// tolerates what an earlier attempt with this scope created; a key whose
    /// secret that attempt lost is replaced.
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError>;
    /// Check the key from the member's side before it is delivered: a write,
    /// read, listing and deletion under the store's reserved check prefix,
    /// retried within the service's propagation bound, and a listing outside
    /// the store's prefix, which must be refused.
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError>;
    /// Remove whatever an attempt with this scope created; absence succeeds.
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError>;
    /// Delete the key and the principal coven made to hold it. A key or
    /// principal already absent succeeds, so a retry is safe.
    async fn revoke(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<(), StorageError>;
    /// Read whether the provider still has this exact key. A failed read is
    /// an error, never absence.
    async fn key_exists(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<bool, StorageError>;
    /// Every key the provider holds under a name coven made for this store,
    /// with the minting device that name carries. A failed or incomplete
    /// listing is an error, never a shorter list.
    async fn list_keys(&self, store: StoreId) -> Result<Vec<ListedKey>, StorageError>;
}

#[cfg(test)]
#[path = "access_admin_tests.rs"]
mod tests;
