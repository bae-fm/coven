use async_trait::async_trait;
use bytes::Bytes;
use coven::{
    write_cloud_object_stream, AppendedListing, AppendedObject, BlobBody, BoxPartSink,
    CloudAccessOutcome, CloudAccessState, CloudFileReadError, CloudHome, CloudHomeError,
    CloudObjectStream, ListingCoverage, UploadProgress,
};

struct ExternalProvider;

#[async_trait]
impl CloudHome for ExternalProvider {
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

    async fn append_object(
        &self,
        full_logical_key: &str,
        _body: BlobBody,
        _progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        Ok(AppendedObject::from_provider(
            full_logical_key.to_string(),
            format!("provider:{full_logical_key}"),
        ))
    }

    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        Ok(AppendedListing {
            objects: vec![AppendedObject::from_provider(
                format!("{prefix}copy"),
                "provider:copy".to_string(),
            )],
            coverage: ListingCoverage::CompleteAtScan,
        })
    }

    async fn read_appended_to_file(
        &self,
        _object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), CloudFileReadError> {
        let stream: CloudObjectStream = Box::pin(futures_util::stream::once(async {
            Ok(Bytes::from_static(b"external provider bytes"))
        }));
        write_cloud_object_stream(destination, stream).await?;
        Ok(())
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

#[tokio::test]
async fn external_provider_can_name_and_implement_the_full_cloud_home_surface() {
    let provider: Box<dyn CloudHome> = Box::new(ExternalProvider);
    let object =
        AppendedObject::from_provider("objects/copy".to_string(), "provider:copy".to_string());
    let temp = tempfile::tempdir().expect("temp dir");
    let destination = temp.path().join("object.bin");

    provider
        .read_appended_to_file(&object, &destination)
        .await
        .expect("external provider stream");

    assert_eq!(
        tokio::fs::read(destination)
            .await
            .expect("read destination"),
        b"external provider bytes"
    );
    let listing = provider
        .list_appended("objects/")
        .await
        .expect("external provider listing");
    assert_eq!(listing.coverage, ListingCoverage::CompleteAtScan);
    assert_eq!(listing.objects[0].opaque_provider_id(), "provider:copy");
}
