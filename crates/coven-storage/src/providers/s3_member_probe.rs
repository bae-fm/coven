//! A minted member key checked from the member's side before delivery (§4):
//! it writes, reads, lists objects and multipart uploads, and deletes under
//! the store's reserved
//! `<store>/.coven-check/<run>/` prefix, reads bucket versioning as setup
//! does, and a listing outside the store's prefix must be refused, which
//! catches a policy reaching too far.
use super::*;
use coven_foundation::id_source::StoreId;

impl S3Storage {
    /// Write a fresh object under this key, refuse a second create, read it
    /// back, find it by listing the namespace, then delete it and see it gone.
    /// The object is deleted even after a failed check; both causes survive.
    pub(super) async fn probe_member_key(
        &self,
        store: StoreId,
        run: uuid::Uuid,
    ) -> Result<(), StorageError> {
        let namespace = format!(
            "{}.coven-check/{run}/",
            ObjectPrefix::all(store).under(&self.prefix)
        );
        let key = format!("{namespace}member-key");
        let bytes = format!("coven member key {run}").into_bytes();
        let (name, value) = self.settings.single_put.header();
        let created = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.clone()))
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert(name, value);
            })
            .send()
            .await
            .map_err(s3_error);
        match created {
            // An earlier attempt of this run wrote it before a later step was
            // refused; the read below compares the run's bytes.
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {}
            Err(error) => {
                // Nothing was written, unless a lost reply hid the write.
                let cleanup = self.delete_probe(&key).await;
                return crate::transfer::with_cleanup(Err(error), cleanup);
            }
            Ok(_) => {}
        }
        let result = self.check_probe(&namespace, &key, &bytes).await;
        let cleanup = self.delete_probe(&key).await;
        crate::transfer::with_cleanup(result, cleanup)
    }

    async fn check_probe(
        &self,
        namespace: &str,
        key: &str,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let (name, value) = self.settings.single_put.header();
        let again = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert(name, value);
            })
            .send()
            .await
            .map_err(s3_error);
        match again {
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(StorageFailure::Protocol.with_source("member key replaced an object"))
            }
        }
        let read = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?
            .body
            .collect()
            .await
            .map_err(|error| StorageFailure::Network.with_source(error))?
            .into_bytes();
        if read.as_ref() != bytes {
            return Err(StorageFailure::Protocol.with_source("member key read other bytes"));
        }
        // Listing lags publication; this proves the key may list its prefix.
        self.list_page(self.settings.listing, namespace, None, None, None)
            .await?;
        // Setup reads the bucket's versioning with this key; only a profile
        // certified with versioning enabled proceeds when it is refused (§4).
        if let Err(error) = self.bucket_versioning().await {
            let StorageConfig::S3 {
                endpoint, profile, ..
            } = &self.config
            else {
                unreachable!("S3 configuration")
            };
            if !(versioning::unknown(&error)
                && versioning::certified(endpoint.as_ref(), profile.as_ref())?)
            {
                return Err(error);
            }
        }
        // A session lists the uploads at its name to abort one whose
        // initiation reply was lost (§4).
        self.client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .prefix(namespace)
            .send()
            .await
            .map_err(s3_error)?;
        Ok(())
    }

    async fn delete_probe(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?;
        Ok(())
    }

    /// A key limited to its store's own bucket must not list the account's
    /// buckets: that would show it reaches beyond the bucket.
    pub(super) async fn other_buckets_refused(&self) -> Result<(), StorageError> {
        match self.client.list_buckets().send().await.map_err(s3_error) {
            Err(error) if error.failure() == StorageFailure::PermissionDenied => Ok(()),
            Err(error) => Err(error),
            Ok(_) => Err(StorageFailure::KeyBeyondStore
                .with_source("a member key for a store's own bucket lists the account's buckets")),
        }
    }

    /// A listing of the location's base prefix, outside every store's
    /// folder, must be refused with permission denied.
    pub(super) async fn outside_store_refused(&self, store: StoreId) -> Result<(), StorageError> {
        let outside = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        match self
            .list_page(self.settings.listing, &outside, Some("/"), None, None)
            .await
        {
            Err(error) if error.failure() == StorageFailure::PermissionDenied => Ok(()),
            Err(error) => Err(error),
            Ok(_) => Err(StorageFailure::KeyBeyondStore.with_source(format!(
                "a member key for store {store} lists outside its prefix"
            ))),
        }
    }
}
