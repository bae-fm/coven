//! Each catalogued profile's key minter (§4): function pointers in its const
//! entry that make, roll back, delete and read member keys with the storage
//! account's admin access. The key is checked from the member's side before
//! the caller delivers it.
use super::*;
use crate::{AccessKeyAdmin, AdminCredential, ListedKey, MintScope, S3MemberKey};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use std::{future::Future, pin::Pin, time::Duration};
use url::Url;

/// A minter's pending provider work, borrowing its context.
pub(crate) type Pending<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

/// One service's admin key operations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyMinter {
    pub(super) name: &'static str,
    /// Create the principal and a key whose secret this call returns, under
    /// the scope's fresh names.
    pub(super) mint: for<'a> fn(&'a AdminRequest, &'a MintScope) -> Pending<'a, S3MemberKey>,
    /// Remove whatever an attempt with this scope created.
    pub(super) roll_back: for<'a> fn(&'a AdminRequest, &'a MintScope) -> Pending<'a, ()>,
    /// Delete the key and the principal holding it; absence succeeds.
    pub(super) revoke: for<'a> fn(&'a AdminRequest, &'a str, &'a S3Principal) -> Pending<'a, ()>,
    /// Read whether the provider still has this exact key.
    pub(super) key_exists:
        for<'a> fn(&'a AdminRequest, &'a str, &'a S3Principal) -> Pending<'a, bool>,
    /// Every key under a name coven made for the store, with its minting device.
    pub(super) list: for<'a> fn(&'a AdminRequest, StoreId) -> Pending<'a, Vec<ListedKey>>,
    /// How long the member-side check retries authentication and permission
    /// failures, and admin calls a NotFound for a principal not yet seen.
    pub(super) propagation: Duration,
    /// The service runs its IAM API apart from the S3 endpoint, at an address
    /// only its operator knows, so admin access names it (Versity Gateway).
    pub(super) separate_iam: bool,
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
    pub(crate) cloudflare: Url,
}
impl AdminEndpoints {
    pub(crate) fn production() -> Self {
        let url = |text| Url::parse(text).expect("static URL");
        Self {
            aws_iam: url("https://iam.amazonaws.com/"),
            gcs_storage: url("https://storage.googleapis.com/storage/v1/"),
            gcs_iam: url("https://iam.googleapis.com/v1/"),
            gcs_projects: url("https://cloudresourcemanager.googleapis.com/v3/"),
            cloudflare: url("https://api.cloudflare.com/client/v4/"),
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
    key_scope: S3KeyScope,
}

impl ProfileKeyAdmin {
    /// A service may not yet see a principal a call just made, and answers
    /// NotFound for it (GCS for minutes-old service accounts). The call is
    /// repeated after a second within the service's window; any other
    /// failure, or one past the window, is returned.
    async fn unseen(&self, waited: &mut Duration, error: StorageError) -> Result<(), StorageError> {
        if error.failure() != StorageFailure::NotFound || *waited >= self.minter.propagation {
            return Err(error);
        }
        let pause = Duration::from_secs(1);
        self.clock.sleep(pause).await;
        *waited += pause;
        Ok(())
    }

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
    let iam_endpoint = match &admin {
        AdminCredential::AccessKey { iam_endpoint, .. } => iam_endpoint.as_ref(),
        AdminCredential::BearerToken(_) => None,
    };
    match (minter.separate_iam, iam_endpoint) {
        (true, None) => {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("this service's admin access names its IAM endpoint"))
        }
        (false, Some(_)) => {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("this service's IAM is not at a separate endpoint"))
        }
        (true, Some(url))
            if !crate::web_url::is_web_url(url)
                || url.query().is_some()
                || url.fragment().is_some() =>
        {
            return Err(StorageFailure::InvalidConfiguration.with_source("IAM endpoint"))
        }
        _ => {}
    }
    Ok(std::sync::Arc::new(ProfileKeyAdmin {
        http: crate::providers::http::client()
            .map_err(|e| crate::providers::http::transport(CloudProvider::S3, e))?,
        admin,
        config,
        endpoints,
        clock,
        ids,
        minter,
        key_scope: selected.key_scope,
    }))
}

#[async_trait]
impl AccessKeyAdmin for ProfileKeyAdmin {
    /// Each minter makes its key last, and its earlier steps accept what an
    /// earlier attempt under the same names made, so repeating is safe.
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
        let mut waited = Duration::ZERO;
        loop {
            match (self.minter.mint)(&self.request(), scope).await {
                Err(error) => self.unseen(&mut waited, error).await?,
                minted => return minted,
            }
        }
    }
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError> {
        let storage = S3Storage::new(
            self.config.clone(),
            key.clone(),
            self.clock.clone(),
            self.ids.clone(),
            std::sync::Arc::new(crate::NoCooldown),
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
        match self.key_scope {
            S3KeyScope::Prefix => storage.outside_store_refused(scope.store).await,
            // The store has the bucket to itself; the key must reach no other.
            S3KeyScope::Bucket => storage.other_buckets_refused().await,
        }
    }
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError> {
        let mut waited = Duration::ZERO;
        loop {
            match (self.minter.roll_back)(&self.request(), scope).await {
                Err(error) => self.unseen(&mut waited, error).await?,
                done => return done,
            }
        }
    }
    async fn revoke(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<(), StorageError> {
        let mut waited = Duration::ZERO;
        loop {
            match (self.minter.revoke)(&self.request(), access_key_id, principal).await {
                Err(error) => self.unseen(&mut waited, error).await?,
                done => return done,
            }
        }
    }
    async fn key_exists(
        &self,
        access_key_id: &str,
        principal: &S3Principal,
    ) -> Result<bool, StorageError> {
        (self.minter.key_exists)(&self.request(), access_key_id, principal).await
    }
    async fn list_keys(&self, store: StoreId) -> Result<Vec<ListedKey>, StorageError> {
        let mut waited = Duration::ZERO;
        loop {
            match (self.minter.list)(&self.request(), store).await {
                Err(error) => self.unseen(&mut waited, error).await?,
                listed => return listed,
            }
        }
    }
}

/// The policy a prefix-scoped member key needs, also shown for console steps
/// (§4): object access under the store's prefix, listing only there, a
/// bucket-versioning read so setup can check the owner's setting, and the
/// multipart-upload listing a session needs to abort an initiation whose
/// reply was lost. AWS takes no prefix condition on that listing.
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
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketVersioning", "s3:ListBucketMultipartUploads"],
                "Resource": format!("arn:aws:s3:::{bucket}"),
            },
        ],
    })
    .to_string())
}

#[cfg(test)]
#[path = "s3_minting_tests.rs"]
mod tests;
