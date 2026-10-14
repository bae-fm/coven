//! Each catalogued profile's key minter (§4): function pointers in its const
//! entry that make, roll back, delete and read member keys with the storage
//! account's admin access. The key is checked from the member's side before
//! the caller delivers it.
use super::*;
use crate::{AccessKeyAdmin, AdminCredential, MintScope, S3MemberKey};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use std::{future::Future, pin::Pin, time::Duration};
use url::Url;

/// A minter's pending provider work, borrowing its context.
pub(crate) type Pending<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

/// One service's admin key operations. Each tolerates what an earlier attempt
/// with the same scope already created or deleted.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyMinter {
    pub(super) name: &'static str,
    /// Create the principal and a key whose secret this call returns.
    pub(super) mint: for<'a> fn(&'a AdminRequest, &'a MintScope) -> Pending<'a, S3MemberKey>,
    /// Remove whatever an attempt with this scope created.
    pub(super) roll_back: for<'a> fn(&'a AdminRequest, &'a MintScope) -> Pending<'a, ()>,
    /// Delete the key and the principal holding it; absence succeeds.
    pub(super) revoke: for<'a> fn(&'a AdminRequest, &'a str, &'a S3Principal) -> Pending<'a, ()>,
    /// Read whether the provider still has this exact key.
    pub(super) key_exists:
        for<'a> fn(&'a AdminRequest, &'a str, &'a S3Principal) -> Pending<'a, bool>,
    /// The longest a new key takes to work; the member-side check retries within it.
    pub(super) propagation: Duration,
}
// Profiles compare by their service; minters are named constants.
impl PartialEq for KeyMinter {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for KeyMinter {}

/// The provider endpoints admin calls use, apart from the location's own.
#[derive(Clone, Debug)]
pub(crate) struct AdminEndpoints {
    pub(crate) aws_iam: Url,
    pub(crate) gcs_storage: Url,
    pub(crate) gcs_iam: Url,
    pub(crate) gcs_projects: Url,
}
impl AdminEndpoints {
    pub(crate) fn production() -> Self {
        let url = |text| Url::parse(text).expect("static URL");
        Self {
            aws_iam: url("https://iam.amazonaws.com/"),
            gcs_storage: url("https://storage.googleapis.com/storage/v1/"),
            gcs_iam: url("https://iam.googleapis.com/v1/"),
            gcs_projects: url("https://cloudresourcemanager.googleapis.com/v3/"),
        }
    }
}

/// One admin call's inputs: the credential, the location, where to send and
/// the signing time.
pub(crate) struct AdminRequest {
    pub(super) http: reqwest::Client,
    pub(super) admin: AdminCredential,
    pub(super) config: StorageConfig,
    pub(super) endpoints: AdminEndpoints,
    pub(super) now: std::time::SystemTime,
}

/// A location's key administration with admin access for one operation.
pub(crate) struct ProfileKeyAdmin {
    http: reqwest::Client,
    admin: AdminCredential,
    config: StorageConfig,
    endpoints: AdminEndpoints,
    clock: ClockRef,
    ids: IdSourceRef,
    minter: &'static KeyMinter,
}

impl ProfileKeyAdmin {
    fn request(&self) -> AdminRequest {
        AdminRequest {
            http: self.http.clone(),
            admin: self.admin.clone(),
            config: self.config.clone(),
            endpoints: self.endpoints.clone(),
            now: self.clock.now(),
        }
    }
}

/// The key administration a location's profile has. Custom profiles and
/// services without a minter take guided console steps instead (§4).
pub(crate) fn key_admin(
    config: StorageConfig,
    admin: AdminCredential,
    endpoints: AdminEndpoints,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<std::sync::Arc<dyn AccessKeyAdmin>, StorageError> {
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &config
    else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let profile::Selected::Catalogued(selected) =
        profile::select(endpoint.as_ref(), profile.as_ref())?
    else {
        return Err(StorageFailure::InvalidConfiguration
            .with_source("a custom S3 profile's keys are made in the console"));
    };
    let minter = selected.minter.ok_or_else(|| {
        StorageFailure::InvalidConfiguration
            .with_source("this service's keys are made in the console")
    })?;
    Ok(std::sync::Arc::new(ProfileKeyAdmin {
        http: crate::providers::http::client()
            .map_err(|e| crate::providers::http::transport(CloudProvider::S3, e))?,
        admin,
        config,
        endpoints,
        clock,
        ids,
        minter,
    }))
}

#[async_trait]
impl AccessKeyAdmin for ProfileKeyAdmin {
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
        (self.minter.mint)(&self.request(), scope).await
    }
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError> {
        let storage = S3Storage::new(
            self.config.clone(),
            key.clone(),
            self.clock.clone(),
            self.ids.clone(),
        )?;
        let run = self.ids.new_id();
        let pause = Duration::from_secs(1);
        let mut waited = Duration::ZERO;
        loop {
            match storage.probe_member_key(scope.store, run).await {
                Ok(()) => break,
                // A new key or its policy may not have reached every server yet.
                Err(error)
                    if matches!(
                        error.failure(),
                        StorageFailure::Authentication | StorageFailure::PermissionDenied
                    ) && waited < self.minter.propagation =>
                {
                    self.clock.sleep(pause).await;
                    waited += pause;
                }
                Err(error) => return Err(error),
            }
        }
        storage.outside_store_refused(scope.store).await
    }
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError> {
        (self.minter.roll_back)(&self.request(), scope).await
    }
    async fn revoke(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<(), StorageError> {
        (self.minter.revoke)(&self.request(), access_key_id, principal).await
    }
    async fn key_exists(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<bool, StorageError> {
        (self.minter.key_exists)(&self.request(), access_key_id, principal).await
    }
}

/// The policy a prefix-scoped member key needs, also shown for console steps
/// (§4): object access under the store's prefix, and listing only there.
/// Never a `Deny` on `s3:prefix`: a request without one, such as a bucket
/// check, would match the negated condition and be denied.
pub(crate) fn member_policy(
    config: &StorageConfig,
    store: StoreId,
) -> Result<String, StorageError> {
    let StorageConfig::S3 { bucket, prefix, .. } = config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let scope = ObjectPrefix::all(store).under(prefix);
    Ok(serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
                ],
                "Resource": format!("arn:aws:s3:::{bucket}/{scope}*"),
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": {"StringLike": {"s3:prefix": format!("{scope}*")}},
            },
        ],
    })
    .to_string())
}

#[cfg(test)]
#[path = "s3_minting_tests.rs"]
mod tests;
