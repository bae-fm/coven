use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use serde::{Deserialize, Serialize};

/// This member's S3 credentials, made by an admin in the provider console (§20.10).
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Credentials {
    /// The public access key id the admin reads or deletes in the provider console.
    pub access_key_id: String,
    /// The corresponding secret key.
    pub secret_access_key: SecretText,
}

impl S3Credentials {
    /// Encode the separately held or transferred S3 key, never a restore code.
    pub fn encode(&self) -> Result<SecretBytes, crate::StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Read a key supplied separately from a restore code.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::StorageError> {
        let keys: Self = serde_json::from_slice(bytes)
            .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
        keys.validate()?;
        Ok(keys)
    }

    pub(crate) fn validate(&self) -> Result<(), crate::StorageError> {
        if self.access_key_id.is_empty() || self.secret_access_key.as_str().is_empty() {
            return Err(crate::StorageFailure::InvalidConfiguration
                .with_source("S3 requires an access key id and secret"));
        }
        Ok(())
    }
}

/// Device sign-in tokens exchanged between provider code and credential custody.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthTokens {
    /// The app client that issued these tokens; refresh must use this client.
    pub client_id: String,
    /// Access confirmed by the provider's token response.
    pub access: crate::OAuthAccess,
    /// The token authorizing requests.
    pub access_token: SecretText,
    /// The token used to obtain a new access token, when issued. The access
    /// token has no local expiry: it is used until the provider rejects it (§20.2).
    pub refresh_token: Option<SecretText>,
}

/// Device credentials committed separately from provider settings.
#[derive(Clone, Serialize, Deserialize)]
pub enum StorageCredentials {
    /// A member's manually created S3 key.
    S3(S3Credentials),
    /// A device's own provider sign-in.
    OAuth(OAuthTokens),
    /// CloudKit uses the Apple account through the built-in native adapter.
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
