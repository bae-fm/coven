//! Unscoped checksums must reach neither metadata nor the SDK's body validator.
use aws_sdk_s3::config::{
    interceptors::BeforeDeserializationInterceptorContextMut, ConfigBag, Intercept,
    RuntimeComponents,
};
use aws_smithy_runtime_api::box_error::BoxError;

#[derive(Debug)]
pub(super) struct UnstatedChecksumHeaders;
impl Intercept for UnstatedChecksumHeaders {
    fn name(&self) -> &'static str {
        "UnstatedChecksumHeaders"
    }
    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.response_mut().headers_mut();
        if headers.get("x-amz-checksum-type").is_none() {
            for name in [
                "x-amz-checksum-sha256",
                "x-amz-checksum-sha1",
                "x-amz-checksum-crc32",
                "x-amz-checksum-crc32c",
                "x-amz-checksum-crc64nvme",
            ] {
                headers.remove(name);
            }
        }
        Ok(())
    }
}
