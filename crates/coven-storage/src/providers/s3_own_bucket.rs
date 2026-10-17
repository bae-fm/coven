//! A store on a service whose member keys reach a whole bucket has the bucket
//! to itself (§4): any member's key reaches every object in it, so a second
//! store there would be readable and deletable by the first store's members.
//! Validation already requires the location's base prefix to be empty. The
//! root listing admits only the store folder and the ownership marker.
use super::*;
use crate::bucket_claim::BUCKET_CLAIM;
use crate::s3_detection::SIGNERS;
use crate::OwnBucketReason;
use coven_foundation::id_source::StoreId;

/// Before a store's first setup, refuse a bucket holding anything outside the
/// store's own folder, except the ownership marker, where member keys reach
/// the whole bucket. Reads only, so it runs before detection writes its
/// probes. Locations with prefix-scoped keys are not checked.
pub(crate) async fn check_new_store(
    config: StorageConfig,
    credentials: S3Credentials,
    store: StoreId,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<(), StorageError> {
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &config
    else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let selected = profile::select(endpoint.as_ref(), profile.as_ref())?;
    if selected.key_scope() == S3KeyScope::Prefix {
        return Ok(());
    }
    // Without settings yet, try each signer with the V1 listing detection's
    // read-only signing probe uses, preferring the plain signer's refusal.
    let candidates = match selected.settings() {
        Some(settings) => vec![settings],
        None => SIGNERS
            .map(|signing| S3Settings {
                signing,
                listing: S3Listing::V1,
                ..profile::PLAIN
            })
            .to_vec(),
    };
    let mut refusal = None;
    for settings in candidates {
        let storage = S3Storage::with_settings(
            config.clone(),
            credentials.clone(),
            clock.clone(),
            ids.clone(),
            std::sync::Arc::new(crate::NoCooldown),
            settings,
        )?;
        match storage.holds_only(store).await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(StorageFailure::OwnBucketRequired {
                    reason: OwnBucketReason::Occupied,
                }
                .into())
            }
            Err(error) if refusal.is_none() || settings.signing == profile::PLAIN.signing => {
                refusal = Some(error)
            }
            Err(_) => {}
        }
    }
    Err(refusal.unwrap_or(StorageFailure::Authentication.into()))
}

impl S3Storage {
    /// Whether the bucket's root holds only this store's folder and the
    /// marker, whose bytes setup checks after provider validation. The
    /// delimiter folds each store's objects into one entry, so a page never
    /// enumerates a store's contents. Objects stored within the listing
    /// margin M may be absent (§4).
    async fn holds_only(&self, store: StoreId) -> Result<bool, StorageError> {
        if !self.prefix.is_empty() {
            return Err(StorageFailure::OwnBucketRequired {
                reason: OwnBucketReason::Prefix,
            }
            .into());
        }
        let own = ObjectPrefix::all(store);
        let mut marker = None;
        let mut seen = Pagination::new();
        loop {
            let page = self
                .list_page(self.settings.listing, "", Some("/"), marker, None)
                .await?;
            if page
                .contents
                .iter()
                .any(|object| object.key() != Some(BUCKET_CLAIM))
                || page
                    .folders
                    .iter()
                    .any(|folder| folder.prefix() != Some(own.as_str()))
            {
                return Ok(false);
            }
            let Some(next) = page.next else {
                return Ok(true);
            };
            seen.check(&next)?;
            marker = Some(next);
        }
    }
}

#[cfg(test)]
#[path = "s3_own_bucket_tests.rs"]
mod tests;
