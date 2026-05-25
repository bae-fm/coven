//! S3-backed `CloudHome` implementation.
//!
//! Wraps `aws-sdk-s3` to provide raw storage operations against any
//! S3-compatible endpoint.

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;

use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

/// S3-backed cloud home.
pub struct S3CloudHome {
    client: Client,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
}

impl S3CloudHome {
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
    ) -> Result<Self, CloudHomeError> {
        let credentials = Credentials::new(&access_key, &secret_key, None, None, "bae-cloud-home");

        let mut builder = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .credentials_provider(credentials);

        if let Some(ref ep) = endpoint {
            builder = builder.endpoint_url(ep.trim_end_matches('/'));
        }

        let aws_config = builder.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(S3CloudHome {
            client,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        })
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }
}

/// Prepend an optional prefix to a key. Trailing slashes on the prefix are normalized.
fn apply_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{}/{}", p.trim_end_matches('/'), key),
        None => key.to_string(),
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full)
            .body(data.into())
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("put {key}: {e}")))?;
        Ok(())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                if msg.contains("NoSuchKey") || msg.contains("not found") || msg.contains("404") {
                    CloudHomeError::NotFound(key.to_string())
                } else {
                    CloudHomeError::Storage(format!("get {key}: {e}"))
                }
            })?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read body for {key}: {e}")))?
            .into_bytes()
            .to_vec();

        Ok(bytes)
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let range = format!("bytes={start}-{}", end.saturating_sub(1));
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                if msg.contains("NoSuchKey") || msg.contains("not found") || msg.contains("404") {
                    CloudHomeError::NotFound(key.to_string())
                } else {
                    CloudHomeError::Storage(format!("get range {key}: {e}"))
                }
            })?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read range body for {key}: {e}")))?
            .into_bytes()
            .to_vec();

        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let full_prefix = self.full_key(prefix);
        let strip_prefix = self
            .key_prefix
            .as_ref()
            .map(|p| format!("{}/", p.trim_end_matches('/')));

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("list {prefix}: {e}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let stripped = match &strip_prefix {
                        Some(p) => key.strip_prefix(p.as_str()).unwrap_or(key),
                        None => key,
                    };
                    keys.push(stripped.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("delete {key}: {e}")))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let full = self.full_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("NotFound")
                    || msg.contains("not found")
                    || msg.contains("404")
                    || msg.contains("NoSuchKey")
                {
                    Ok(false)
                } else {
                    Err(CloudHomeError::Storage(format!("head {key}: {e}")))
                }
            }
        }
    }

    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        // S3 access is managed externally (IAM/pre-shared credentials).
        // Return the owner's credentials so they can be embedded in the invite code.
        Ok(CloudHomeJoinInfo::S3 {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            key_prefix: self.key_prefix.clone(),
        })
    }

    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        // S3 access is managed externally (IAM/pre-shared credentials).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_key_prepends_prefix() {
        let key = apply_prefix(Some("libs/abc"), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }

    #[test]
    fn full_key_no_prefix() {
        let key = apply_prefix(None, "heads/dev1.json");
        assert_eq!(key, "heads/dev1.json");
    }

    #[test]
    fn full_key_strips_trailing_slash() {
        let key = apply_prefix(Some("libs/abc/"), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }
}
