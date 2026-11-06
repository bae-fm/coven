//! Every S3 store has its bucket to itself (§20.7): coven can't tell how far
//! a member's key reaches, so it takes every key to reach every object in
//! the bucket, and a second store or anyone's other files there would be
//! readable and deletable by the store's members. The root listing admits
//! only the store's folder and the ownership marker.
use super::*;
use crate::bucket_claim::BUCKET_CLAIM;
use crate::s3_detection::SIGNERS;
use coven_foundation::id_source::StoreId;

/// Before a store's first setup, refuse a bucket holding anything outside the
/// store's own folder, except the ownership marker. The store's folder holds
/// whatever an earlier attempt at this creation wrote, a detection run's
/// probes included, so a retried creation passes. Reads only, so it runs
/// before detection writes its probes.
pub(crate) async fn check_new_store(
    config: StorageConfig,
    credentials: S3Credentials,
    store: StoreId,
    clock: ClockRef,
    ids: IdSourceRef,
    http: HttpClient,
    waits: &crate::SetupWaits,
) -> Result<(), StorageError> {
    let StorageConfig::S3 { endpoint, profile, .. } = &config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let selected = profile::select(endpoint.as_ref(), profile.as_ref())?;
    // Without settings yet, try each signer with the V1 listing detection's
    // read-only signing probe uses, preferring the plain signer's refusal.
    let candidates = match selected.settings() {
        Some(settings) => vec![settings],
        None => SIGNERS
            .map(|signing| S3Settings { signing, listing: S3Listing::V1, ..profile::PLAIN })
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
            http.clone(),
        )?;
        // Each page is one listing request: a rate-limited one stored nothing.
        let reader = &storage;
        match waits.resend(clock.as_ref(), move || reader.holds_only(store)).await {
            Ok(true) => return Ok(()),
            Ok(false) => return Err(StorageFailure::OwnLocationRequired.into()),
            Err(error) if refusal.is_none() || settings.signing == profile::PLAIN.signing => {
                refusal = Some(error)
            }
            Err(_) => {}
        }
    }
    Err(refusal.unwrap_or(StorageFailure::Authentication.into()))
}

/// One entry of the bucket root's listing with delimiter `/`.
pub(super) enum RootEntry<'a> {
    /// An object at the root, by its key.
    Object(&'a str),
    /// A folder, by its name and closing slash: every object under it.
    Folder(&'a str),
}

impl S3Storage {
    /// Whether the bucket's root holds only this store's folder and the
    /// marker, whose bytes setup checks after provider validation.
    async fn holds_only(&self, store: StoreId) -> Result<bool, StorageError> {
        let own = ObjectPrefix::all(store);
        self.root_admits(|entry| match entry {
            RootEntry::Object(key) => key == BUCKET_CLAIM,
            RootEntry::Folder(folder) => folder == own.as_str(),
        })
        .await
    }

    /// Whether `admit` accepts every entry at the bucket's root, stopping at
    /// the first it refuses; an entry the service sends without its name is
    /// refused. The delimiter folds each folder's objects into one entry, so
    /// a page never enumerates a store's contents. Objects stored within the
    /// listing margin M may be absent (§20.7).
    pub(super) async fn root_admits(
        &self,
        admit: impl Fn(RootEntry<'_>) -> bool,
    ) -> Result<bool, StorageError> {
        let mut marker = None;
        let mut seen = Pagination::new();
        loop {
            let page = self.list_page(self.settings.listing, "", Some("/"), marker, None).await?;
            let objects = page.contents.iter().map(|object| object.key().map(RootEntry::Object));
            let folders = page.folders.iter().map(|folder| folder.prefix().map(RootEntry::Folder));
            if !objects.chain(folders).all(|entry| entry.is_some_and(&admit)) {
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
