//! Check publication authority at the SDK's final transmit boundary.
use crate::PublicationRequest;
use aws_sdk_s3::config::{
    interceptors::BeforeTransmitInterceptorContextRef, ConfigBag, Intercept, RuntimeComponents,
};
use aws_smithy_runtime_api::box_error::BoxError;
impl Intercept for PublicationRequest {
    fn name(&self) -> &'static str {
        "PublicationRequest"
    }
    fn read_before_transmit(
        &self,
        _: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.check().map_err(|error| Box::new(error) as BoxError)
    }
}

#[cfg(test)]
#[path = "s3_publication_tests.rs"]
mod tests;
