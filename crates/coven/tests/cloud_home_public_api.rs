use async_trait::async_trait;
use bytes::Bytes;
use coven::{
    write_cloud_object_stream, AppendedListing, AppendedObject, BlobBody, BoxPartSink,
    CloudAccessOutcome, CloudAccessState, CloudFileReadError, CloudHeadCreateError,
    CloudHeadReplaceError, CloudHeadVersion, CloudHome, CloudHomeError, CloudKitAtomicCreateBatch,
    CloudKitChangeToken, CloudKitOps, CloudKitRecordChangesPage, CloudKitRecordCreate,
    CloudKitRecordVersion, CloudKitScope, CloudKitShare, CloudObjectStream, CloudVersionedHead,
    ImmutableCopyStorage, ListingCoverage,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct ExternalProvider {
    immutable_put_called: Arc<AtomicBool>,
}

struct ExternalCloudKitBridge;

impl CloudKitOps for ExternalCloudKitBridge {
    fn write_record(&self, _: &CloudKitScope, _: &str, _: Vec<u8>) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn read_record(&self, _: &CloudKitScope, _: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!()
    }
    fn list_records(&self, _: &CloudKitScope, _: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!()
    }
    fn delete_record(&self, _: &CloudKitScope, _: &str) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn record_exists(&self, _: &CloudKitScope, _: &str) -> Result<bool, CloudHomeError> {
        unimplemented!()
    }
    fn read_versioned_record(
        &self,
        _: &CloudKitScope,
        _: &str,
    ) -> Result<CloudVersionedHead, CloudHomeError> {
        unimplemented!()
    }
    fn create_record(
        &self,
        _: &CloudKitScope,
        _: &str,
        _: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        unimplemented!()
    }
    fn replace_record(
        &self,
        _: &CloudKitScope,
        _: &str,
        _: &CloudHeadVersion,
        _: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
        unimplemented!()
    }
    fn begin_atomic_create(
        &self,
        _: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        unimplemented!()
    }
    fn stage_atomic_create_record(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
        _: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn commit_atomic_create(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        unimplemented!()
    }
    fn discard_atomic_create(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn record_changes(
        &self,
        _: &CloudKitScope,
        _: Option<&CloudKitChangeToken>,
    ) -> Result<CloudKitRecordChangesPage, CloudHomeError> {
        unimplemented!()
    }
    fn delete_record_versions(
        &self,
        _: &CloudKitScope,
        _: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn share_for_member(&self, _: &str) -> Result<Option<CloudKitShare>, CloudHomeError> {
        unimplemented!()
    }
    fn grant_share(&self, _: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!()
    }
    fn revoke_share(&self, _: &str) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn accept_share(&self, _: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!()
    }
}

#[test]
fn external_cloudkit_bridge_can_name_every_signature_type() {
    fn assert_bridge<T: CloudKitOps>() {}
    assert_bridge::<ExternalCloudKitBridge>();
}

#[test]
fn appended_provider_identity_rejects_empty_components() {
    let empty_key = AppendedObject::from_provider(String::new(), "provider:id".to_string())
        .expect_err("empty logical key must fail");
    assert!(matches!(empty_key, CloudHomeError::Configuration(_)));
    assert!(!empty_key.is_retryable());

    let empty_provider = AppendedObject::from_provider("objects/copy".to_string(), String::new())
        .expect_err("empty provider id must fail");
    assert!(matches!(empty_provider, CloudHomeError::Transport(_)));
    assert!(empty_provider.is_retryable());
}

#[async_trait]
impl CloudHome for ExternalProvider {
    fn immutable_copy_storage(self: Arc<Self>) -> Option<Arc<dyn ImmutableCopyStorage>> {
        Some(self)
    }

    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "multipart unsupported in compile-test provider".to_string(),
        ))
    }

    fn multipart_threshold(&self) -> u64 {
        u64::MAX
    }

    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }

    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        Ok(false)
    }

    async fn set_access(
        &self,
        _desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "sharing unsupported in compile-test provider".to_string(),
        ))
    }
}

#[async_trait]
impl ImmutableCopyStorage for ExternalProvider {
    async fn append_object(
        &self,
        key: &str,
        body: BlobBody,
        progress: &coven::UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        let data = body.collect().await?;
        self.immutable_put_called.store(true, Ordering::SeqCst);
        progress(data.len() as u64);
        AppendedObject::from_provider(key.to_string(), "provider:created-copy".to_string())
    }
    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        Ok(AppendedListing {
            objects: vec![AppendedObject::from_provider(
                format!("{prefix}copy"),
                "provider:copy".to_string(),
            )?],
            coverage: ListingCoverage::CompleteAtScan,
        })
    }
    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        assert_eq!(object.opaque_provider_id(), "provider:copy");
        Ok(b"external provider bytes".to_vec())
    }
    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), CloudFileReadError> {
        assert_eq!(object.opaque_provider_id(), "provider:copy");
        let stream: CloudObjectStream = Box::pin(futures_util::stream::once(async {
            Ok(Bytes::from_static(b"external provider bytes"))
        }));
        write_cloud_object_stream(destination, stream).await?;
        Ok(())
    }
    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        assert_eq!(object.opaque_provider_id(), "provider:copy");
        Ok(())
    }
}

#[tokio::test]
async fn external_provider_can_name_and_implement_the_full_cloud_home_surface() {
    let immutable_put_called = Arc::new(AtomicBool::new(false));
    let provider: Arc<dyn CloudHome> = Arc::new(ExternalProvider {
        immutable_put_called: immutable_put_called.clone(),
    });
    let immutable = provider
        .clone()
        .immutable_copy_storage()
        .expect("immutable adapter");
    let appended = immutable
        .append_object(
            "objects/default-append",
            BlobBody::from_bytes(b"external create-only bytes".to_vec()),
            &|_| {},
        )
        .await
        .expect("provider append creates an exact immutable copy");
    assert_eq!(appended.logical_key(), "objects/default-append");
    assert_eq!(appended.opaque_provider_id(), "provider:created-copy");
    assert!(immutable_put_called.load(Ordering::SeqCst));
    let object =
        AppendedObject::from_provider("objects/copy".to_string(), "provider:copy".to_string())
            .expect("valid external provider identity");
    let temp = tempfile::tempdir().expect("temp dir");
    let destination = temp.path().join("object.bin");

    immutable
        .read_appended_to_file(&object, &destination)
        .await
        .expect("external provider stream");

    assert_eq!(
        tokio::fs::read(destination)
            .await
            .expect("read destination"),
        b"external provider bytes"
    );
    let listing = immutable
        .list_appended("objects/")
        .await
        .expect("external provider listing");
    assert_eq!(listing.coverage, ListingCoverage::CompleteAtScan);
    assert_eq!(listing.objects[0].opaque_provider_id(), "provider:copy");
    assert_eq!(
        immutable.read_appended(&object).await.unwrap(),
        b"external provider bytes"
    );
    immutable.delete_appended(&object).await.unwrap();
}
