//! HTTP-backed `CloudHome` implementation.
//!
//! Talks to a bae-proxy's `/cloud/*` write proxy endpoints.
//! Requests are authenticated with Ed25519 signatures.

use async_trait::async_trait;
use reqwest::Client;

use crate::clock::ClockRef;
use crate::keys::UserKeypair;

use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

/// HTTP-backed cloud home that proxies through a bae-proxy.
pub struct HttpCloudHome {
    base_url: String,
    keypair: UserKeypair,
    client: Client,
    clock: ClockRef,
}

impl HttpCloudHome {
    pub fn new(base_url: String, keypair: UserKeypair, clock: ClockRef) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            keypair,
            client: Client::new(),
            clock,
        }
    }

    /// Build auth headers for a request.
    fn sign_request(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let timestamp = self.clock.now().timestamp() as u64;

        let message = format!("{}\n{}\n{}", method, path, timestamp);
        let signature = self.keypair.sign(message.as_bytes());

        [
            ("X-Bae-Pubkey", hex::encode(self.keypair.public_key)),
            ("X-Bae-Timestamp", timestamp.to_string()),
            ("X-Bae-Signature", hex::encode(signature)),
        ]
    }

    /// Map an HTTP response to a CloudHomeError for non-success status codes.
    async fn map_error(key: &str, resp: reqwest::Response) -> CloudHomeError {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));

        if status == reqwest::StatusCode::NOT_FOUND {
            CloudHomeError::NotFound(key.to_string())
        } else if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            CloudHomeError::Storage(format!("unauthorized: {body}"))
        } else {
            CloudHomeError::Storage(format!("{status}: {body}"))
        }
    }
}

#[async_trait]
impl CloudHome for HttpCloudHome {
    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let path = format!("/cloud/{key}");
        let url = format!("{}{}", self.base_url, path);
        let headers = self.sign_request("PUT", &path);

        let resp = self
            .client
            .put(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .body(data)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("write {key}: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Self::map_error(key, resp).await)
        }
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let path = format!("/cloud/{key}");
        let url = format!("{}{}", self.base_url, path);
        let headers = self.sign_request("GET", &path);

        let resp = self
            .client
            .get(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read {key}: {e}")))?;

        if resp.status().is_success() {
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("read body {key}: {e}")))?;
            Ok(bytes.to_vec())
        } else {
            Err(Self::map_error(key, resp).await)
        }
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let path = format!("/cloud/{key}");
        let url = format!("{}{}", self.base_url, path);
        let headers = self.sign_request("GET", &path);
        let range_value = format!("bytes={}-{}", start, end.saturating_sub(1));

        let resp = self
            .client
            .get(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .header("Range", &range_value)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read_range {key}: {e}")))?;

        if resp.status().is_success() {
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("read_range body {key}: {e}")))?;
            Ok(bytes.to_vec())
        } else {
            Err(Self::map_error(key, resp).await)
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let url = format!(
            "{}/cloud?prefix={}",
            self.base_url,
            urlencoding::encode(prefix)
        );
        let headers = self.sign_request("GET", "/cloud");

        let resp = self
            .client
            .get(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("list {prefix}: {e}")))?;

        if resp.status().is_success() {
            let keys: Vec<String> = resp
                .json()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("list parse {prefix}: {e}")))?;
            Ok(keys)
        } else {
            Err(Self::map_error(prefix, resp).await)
        }
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let path = format!("/cloud/{key}");
        let url = format!("{}{}", self.base_url, path);
        let headers = self.sign_request("DELETE", &path);

        let resp = self
            .client
            .delete(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("delete {key}: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Self::map_error(key, resp).await)
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let path = format!("/cloud/{key}");
        let url = format!("{}{}", self.base_url, path);
        let headers = self.sign_request("HEAD", &path);

        let resp = self
            .client
            .head(&url)
            .header(headers[0].0, &headers[0].1)
            .header(headers[1].0, &headers[1].1)
            .header(headers[2].0, &headers[2].1)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("exists {key}: {e}")))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(false)
        } else if resp.status().is_success() {
            Ok(true)
        } else {
            Err(Self::map_error(key, resp).await)
        }
    }

    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::HttpProxy {
            url: self.base_url.clone(),
        })
    }

    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FixedClock;
    use crate::keys::{verify_signature, UserKeypair};
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    fn test_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    /// Fixed instant the test clock returns, so signed timestamps are
    /// deterministic.
    fn fixed_instant() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn test_cloud_home(base_url: &str, keypair: UserKeypair) -> HttpCloudHome {
        HttpCloudHome::new(
            base_url.to_string(),
            keypair,
            Arc::new(FixedClock(fixed_instant())),
        )
    }

    #[test]
    fn sign_request_produces_three_headers() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com", kp);
        let headers = cloud_home.sign_request("PUT", "/cloud/changes/dev1/42.enc");

        assert_eq!(headers[0].0, "X-Bae-Pubkey");
        assert_eq!(headers[1].0, "X-Bae-Timestamp");
        assert_eq!(headers[2].0, "X-Bae-Signature");

        // Pubkey is hex-encoded 32-byte key = 64 hex chars
        assert_eq!(headers[0].1.len(), 64);

        // Timestamp is the injected clock's unix seconds.
        let ts: u64 = headers[1].1.parse().unwrap();
        assert_eq!(ts, fixed_instant().timestamp() as u64);

        // Signature is hex-encoded 64-byte signature = 128 hex chars
        assert_eq!(headers[2].1.len(), 128);
    }

    #[test]
    fn sign_request_signature_verifies() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com", kp.clone());
        let headers = cloud_home.sign_request("GET", "/cloud/some/key");

        let message = format!("GET\n/cloud/some/key\n{}", headers[1].1);
        let sig_bytes: [u8; crate::keys::SIGN_BYTES] =
            hex::decode(&headers[2].1).unwrap().try_into().unwrap();

        assert!(verify_signature(
            &sig_bytes,
            message.as_bytes(),
            &kp.public_key
        ));
    }

    #[test]
    fn sign_request_different_methods_produce_different_signatures() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com", kp);

        let h1 = cloud_home.sign_request("GET", "/cloud/key");
        let h2 = cloud_home.sign_request("PUT", "/cloud/key");

        // The fixed clock gives both requests the same timestamp, so the
        // signatures differ purely because the method is part of the signed
        // message.
        assert_eq!(h1[1].1, h2[1].1);
        assert_ne!(h1[2].1, h2[2].1);
    }

    #[test]
    fn base_url_trailing_slash_stripped() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com/", kp);
        assert_eq!(cloud_home.base_url, "https://example.com");
    }

    #[test]
    fn grant_access_returns_join_info() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com", kp);

        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cloud_home.grant_access("some-member"));

        assert!(result.is_ok());
        let join_info = result.unwrap();
        match join_info {
            CloudHomeJoinInfo::HttpProxy { url } => {
                assert_eq!(url, "https://example.com");
            }
            _ => panic!("expected HttpProxy join info"),
        }
    }

    #[test]
    fn revoke_access_succeeds() {
        let kp = test_keypair();
        let cloud_home = test_cloud_home("https://example.com", kp);

        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cloud_home.revoke_access("some-member"));

        assert!(result.is_ok());
    }
}
