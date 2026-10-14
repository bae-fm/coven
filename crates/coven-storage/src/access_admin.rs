//! Deleting members' S3 keys with the storage account's admin access (§4, §13).

use crate::StorageError;
use async_trait::async_trait;
use coven_format::S3Principal;

/// A service's key administration, holding admin access for one operation.
/// Coven deletes a key and then reads whether that exact key remains; only
/// both together clear the key's pending deletion (§13).
#[async_trait]
pub trait AccessKeyAdmin: Send + Sync {
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
}
