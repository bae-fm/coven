use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use serde::{Deserialize, Serialize};

/// This member's S3 credentials, made by an admin in the provider console (§4).
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Credentials {
    /// The public access key id the admin reads or deletes in the provider console.
    pub access_key_id: String,
    /// The corresponding secret key.
    pub secret_access_key: SecretText,
}

/// Device sign-in tokens exchanged between provider code and credential custody.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthTokens {
    /// The token authorizing requests.
    pub access_token: SecretText,
    /// The token used to obtain a new access token, when issued.
    pub refresh_token: Option<SecretText>,
    /// Expiry calculated with the injected clock; absent for non-expiring tokens.
    pub expires_at: Option<std::time::SystemTime>,
}

/// Device credentials committed separately from provider settings.
#[derive(Clone, Serialize, Deserialize)]
pub enum StorageCredentials {
    /// A member's manually created S3 key.
    S3(S3Credentials),
    /// A device's own provider sign-in.
    OAuth(OAuthTokens),
    /// CloudKit uses the Apple account through the app's bridge.
    CloudKit,
}

impl StorageCredentials {
    /// Encode for the facade to seal or place in key custody, never in settings.
    pub fn encode(&self) -> Result<SecretBytes, crate::StorageError> {
        crate::secret_json::encode(self)
    }
    /// Decode credentials obtained from device custody.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::StorageError> {
        serde_json::from_slice(bytes)
            .map_err(|error| crate::StorageFailure::Encoding.with_source(error))
    }
}

#[cfg(test)]
#[path = "credentials_tests.rs"]
mod tests;
