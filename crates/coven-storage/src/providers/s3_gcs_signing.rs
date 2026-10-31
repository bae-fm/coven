//! GCS XML uses Google V4 HMAC signing, which `x-goog-if-generation-match`
//! needs, and every extension header goes out in its x-goog form. Google's
//! docs don't say whether GCS refuses mixed x-amz/x-goog headers.
//! Keep the SDK's request serialization, transport and credential replacement;
//! replace its signer and translate response metadata at the protocol boundary.

use aws_sdk_s3::{
    config::{
        http::HttpRequest, interceptors::BeforeDeserializationInterceptorContextMut, ConfigBag,
        Intercept, RuntimeComponents,
    },
    primitives::{DateTime, DateTimeFormat},
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        auth::{AuthScheme, AuthSchemeEndpointConfig, AuthSchemeId, Sign},
        identity::{Identity as SigningIdentity, SharedIdentityResolver},
        runtime_components::GetIdentityResolver,
    },
};
use coven_crypto::{gcs_request_signature, s3_request_hash};
use std::collections::BTreeMap;

#[derive(Debug)]
pub(super) struct GcsSigning {
    pub(super) region: String,
}

impl AuthScheme for GcsSigning {
    fn scheme_id(&self) -> AuthSchemeId {
        // Replace the selected S3 scheme's signer, retaining its credentials resolver.
        AuthSchemeId::new("sigv4")
    }

    fn identity_resolver(
        &self,
        resolvers: &dyn GetIdentityResolver,
    ) -> Option<SharedIdentityResolver> {
        resolvers.identity_resolver(self.scheme_id())
    }

    fn signer(&self) -> &dyn Sign {
        self
    }
}

impl Sign for GcsSigning {
    fn sign_http_request(
        &self,
        request: &mut HttpRequest,
        identity: &SigningIdentity,
        _endpoint: AuthSchemeEndpointConfig<'_>,
        components: &RuntimeComponents,
        _config: &ConfigBag,
    ) -> Result<(), BoxError> {
        let credentials = identity
            .data::<aws_credential_types::Credentials>()
            .ok_or("GCS signing requires HMAC credentials")?;
        let url = url::Url::parse(request.uri())?;
        let payload = hex::encode(s3_request_hash(
            request.body().bytes().ok_or("GCS signing requires a buffered request body")?,
        ));
        let time = DateTime::from(
            components.time_source().ok_or("GCS signing requires the configured clock")?.now(),
        );
        let stamp =
            DateTime::from_secs(time.secs()).fmt(DateTimeFormat::DateTime)?.replace(['-', ':'], "");
        let day = stamp.get(..8).ok_or("GCS signing date out of range")?;
        let headers = request.headers_mut();
        let translated: Vec<_> = headers
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("x-amz-")
                    .map(|suffix| (name.to_owned(), format!("x-goog-{suffix}"), value.to_owned()))
            })
            .collect();
        for (old, new, value) in translated {
            headers.remove(old);
            headers.try_insert(new, value)?;
        }
        headers.try_insert("x-goog-date", stamp.clone())?;
        headers.try_insert("x-goog-content-sha256", payload.clone())?;
        let mut canonical_headers = BTreeMap::<String, String>::new();
        // Sign the authority without inserting Host: the HTTP transport supplies
        // Host or HTTP/2 :authority. GCS rejects a duplicate HTTP/2 authority.
        canonical_headers.insert(
            "host".into(),
            url[url::Position::BeforeHost..url::Position::AfterPort].to_owned(),
        );
        for (name, value) in headers.iter() {
            if name == "host"
                || name.starts_with("x-goog-")
                || matches!(name, "content-type" | "content-md5")
            {
                let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
                canonical_headers
                    .entry(name.to_owned())
                    .and_modify(|values| {
                        values.push(',');
                        values.push_str(&value);
                    })
                    .or_insert(value);
            }
        }
        let signed = canonical_headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_headers = canonical_headers
            .into_iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect::<String>();
        let mut query = match url.query() {
            Some(query) => query
                .split('&')
                .map(|pair| if pair.contains('=') { pair.to_owned() } else { format!("{pair}=") })
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        query.sort();
        let canonical = format!(
            "{}\n{}\n{}\n{}\n{}\n{payload}",
            request.method(),
            url.path(),
            query.join("&"),
            canonical_headers,
            signed
        );
        let scope = format!("{day}/{}/storage/goog4_request", self.region);
        let to_sign = format!(
            "GOOG4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
            hex::encode(s3_request_hash(canonical.as_bytes()))
        );
        let signature = gcs_request_signature(
            credentials.secret_access_key(),
            day,
            &self.region,
            to_sign.as_bytes(),
        );
        request.headers_mut().try_insert(
            "authorization",
            format!(
                "GOOG4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={}",
                credentials.access_key_id(),
                hex::encode(signature)
            ),
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct GcsResponseHeaders;

impl Intercept for GcsResponseHeaders {
    fn name(&self) -> &'static str {
        "GcsResponseHeaders"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _components: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.response_mut().headers_mut();
        let metadata: Vec<_> = headers
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("x-goog-meta-")
                    .map(|suffix| (format!("x-amz-meta-{suffix}"), value.to_owned()))
            })
            .collect();
        for (name, value) in metadata {
            headers.try_insert(name, value)?;
        }
        if let Some(generation) = headers.get("x-goog-generation").map(str::to_owned) {
            headers.try_insert("x-amz-version-id", generation)?;
        }
        let hashes: Vec<_> = headers.get_all("x-goog-hash").map(str::to_owned).collect();
        for hashes in hashes {
            for hash in hashes.split(',').map(str::trim) {
                if let Some(crc32c) = hash.strip_prefix("crc32c=") {
                    headers.try_insert("x-amz-checksum-crc32c", crc32c.to_owned())?;
                    headers.try_insert("x-amz-checksum-type", "FULL_OBJECT")?;
                }
            }
        }
        Ok(())
    }
}
