#![cfg(test)]

use super::*;
use aws_sdk_s3::config::{
    interceptors::BeforeDeserializationInterceptorContextRef, ConfigBag, Intercept,
    RuntimeComponents,
};
use aws_smithy_runtime_api::box_error::BoxError;

#[derive(Clone, Debug, Default)]
struct Facts {
    checksum: bool,
    unstated: bool,
    google: bool,
    generation: bool,
}
#[derive(Debug, Clone)]
struct Capture(Arc<Mutex<Facts>>);
impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "CertificationHeaderFacts"
    }
    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let h = context.response().headers();
        let mut facts = self.0.lock().expect("header facts lock");
        facts.google |= h.get("x-goog-hash").is_some();
        facts.generation |= h.get("x-goog-generation").is_some();
        let checksum = [
            "x-amz-checksum-sha256",
            "x-amz-checksum-sha1",
            "x-amz-checksum-crc32",
            "x-amz-checksum-crc32c",
            "x-amz-checksum-crc64nvme",
        ]
        .iter()
        .any(|name| h.get(*name).is_some());
        facts.checksum |= checksum || facts.google;
        facts.unstated |= checksum && h.get("x-amz-checksum-type").is_none();
        Ok(())
    }
}
pub(super) async fn detect(
    s: &S3Storage,
    store: StoreId,
    profile: &mut S3Profile,
) -> Result<(), StorageError> {
    let key = path(store, 40).under(&s.prefix);
    put(s, &key, b"checksum observation", None).await?;
    let facts = Capture(Arc::new(Mutex::new(Facts::default())));
    s.client
        .head_object()
        .bucket(&s.bucket)
        .key(&key)
        .customize()
        .interceptor(facts.clone())
        .send()
        .await
        .map_err(s3_error)?;
    let passive_checksum = facts.0.lock().expect("header facts lock").checksum;

    let requested = s
        .client
        .head_object()
        .bucket(&s.bucket)
        .key(&key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .customize()
        .interceptor(facts.clone())
        .send()
        .await
        .map_err(s3_error);
    println!("native checksum_mode={}", outcome(&requested));
    // Request the checksum extension only when the response needs it. Google
    // already supplies its whole-object hash, including on ranged responses.
    profile.request_checksums = requested.is_ok() && !passive_checksum;
    if let Err(error) = requested {
        require(
            error.failure() == StorageFailure::Refused,
            "checksum request failed unexpectedly",
        )?;
    }
    for key in [&key, &path(store, 20).under(&s.prefix)] {
        s.client
            .head_object()
            .bucket(&s.bucket)
            .key(key)
            .set_checksum_mode(
                profile
                    .request_checksums
                    .then_some(aws_sdk_s3::types::ChecksumMode::Enabled),
            )
            .customize()
            .interceptor(facts.clone())
            .send()
            .await
            .map_err(s3_error)?;
        let read = s
            .client
            .get_object()
            .bucket(&s.bucket)
            .key(key)
            .set_checksum_mode(
                profile
                    .request_checksums
                    .then_some(aws_sdk_s3::types::ChecksumMode::Enabled),
            )
            .customize()
            .interceptor(facts.clone())
            .send()
            .await
            .map_err(s3_error)?;
        read.body
            .collect()
            .await
            .map_err(|_| StorageFailure::Network)?;
    }
    let facts = facts.0.lock().expect("header facts lock").clone();
    // §4 permits a missing checksum: the complete-byte comparison replaces
    // it. No checksum on any probe, including with checksum mode, is recorded
    // as such, and the adapter stops requesting what the service never returns.
    profile.checksums = if !facts.checksum {
        profile.request_checksums = false;
        Checksums::Absent
    } else if facts.unstated {
        Checksums::UnstatedIgnored
    } else {
        Checksums::ScopeStated
    };
    profile.revision = if facts.generation {
        Revision::Generation
    } else {
        Revision::ETag
    };
    profile.response_headers = if facts.google || facts.generation {
        ResponseHeaders::Google
    } else {
        ResponseHeaders::S3
    };
    let bucket = s
        .client
        .head_bucket()
        .bucket(&s.bucket)
        .send()
        .await
        .map_err(s3_error);
    let list = s
        .list_page(
            profile.listing,
            &ObjectPrefix::all(store).under(&s.prefix),
            None,
            None,
            Some(0),
        )
        .await;
    // Keep a catalogued least-permission route when both methods work with
    // this account; extra bucket privileges do not prove they are required.
    profile.bucket_check = match (&bucket, &list, s.profile.bucket_check) {
        (_, Ok(_), BucketCheck::List) => BucketCheck::List,
        (Ok(_), _, _) => BucketCheck::Head,
        (Err(error), Ok(_), _) if error.failure() == StorageFailure::PermissionDenied => {
            BucketCheck::List
        }
        _ => {
            bucket?;
            list?;
            unreachable!()
        }
    };
    println!(
        "native bucket_head={} bucket_list={} selected={:?}",
        outcome(&bucket),
        outcome(&list),
        profile.bucket_check
    );
    println!("native checksum_present={} checksum_scope={:?} revision={:?} response_headers={:?} bucket_check={:?}", facts.checksum, profile.checksums, profile.revision, profile.response_headers, profile.bucket_check);
    Ok(())
}
