//! The provider account a sign-in reaches, and the account an invite shared
//! the store's folder with (§13.2).

use crate::{StorageError, StorageFailure};
use coven_format::MemberAccess;
use serde::{Deserialize, Serialize};

/// The account a Dropbox or OneDrive sign-in belongs to, as the provider
/// reports it: its primary address and, where an invite's share names the
/// account by the same id, that id (Dropbox's `account_id`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAccount {
    /// The provider's id for the account, which no address change alters.
    /// None on OneDrive: Microsoft's documentation does not say a
    /// permission's user id is the id `/me` returns, so OneDrive accounts are
    /// compared by address.
    pub id: Option<String>,
    /// The account's primary address, normalized as
    /// [`MemberAccess::normalized_account`] normalizes one.
    pub address: String,
}

/// The account an invite shared the store's folder with, as the inviting
/// device recorded it when the provider granted the share.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedWith {
    address: String,
    id: Option<String>,
}

impl SharedWith {
    /// The share's account: the address the invite was sent to, and the
    /// provider's id for the account when the share reports one. A share sent
    /// to an address with no account yet has no id.
    pub fn new(address: &str, id: Option<String>) -> Result<Self, StorageError> {
        let address = MemberAccess::normalized_account(address);
        if address.is_empty() || id.as_deref().is_some_and(str::is_empty) {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("an invite's shared-with account is empty"));
        }
        Ok(Self { address, id })
    }

    /// The address the invite was sent to, which names the account to the
    /// person.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// The provider's id for the account, when the share named one (Dropbox's
    /// `account_id`, which Dropbox documents as the account's unique and stable
    /// ID in `users/get_current_account` and, as `UserInfo.account_id`, in a
    /// folder's member list).
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Whether `signed_in` is the account the folder was shared with: by the
    /// provider's id when the share named one, so an alias or secondary
    /// address of the account is never read as another account; else by
    /// primary address.
    pub fn is(&self, signed_in: &ProviderAccount) -> bool {
        match &self.id {
            Some(id) => signed_in.id.as_deref() == Some(id.as_str()),
            None => MemberAccess::same_account(&self.address, &signed_in.address),
        }
    }
}

#[cfg(test)]
#[path = "provider_account_tests.rs"]
mod tests;
