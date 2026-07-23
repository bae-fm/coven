use super::*;

/// Persist and activate a membership key rotation on this device.
pub(crate) fn apply_key_rotation(
    new_encryption: EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
) -> Result<String, KeyError> {
    // Merge, never replace: the fixed-mode cipher state can extend an encrypted
    // keyring but has no transition to plaintext. Re-check under its keyring lock
    // because another member operation may have adopted a newer rotation while
    // this caller was reading the cloud.
    if let Some(fingerprint) = cipher.merge_key_rotation(&new_encryption, custody)? {
        return Ok(fingerprint);
    }
    let crate::sync::cloud_storage::CloudCipher::Encrypted(live) = cipher.snapshot() else {
        return Err(KeyError::Crypto(
            "cannot rotate the key of a plaintext cloud home".to_string(),
        ));
    };
    if live.merged_with(&new_encryption).key_count() != live.key_count() {
        return Err(KeyError::Crypto(
            "live keyring changed without retaining an adopted rotation".to_string(),
        ));
    }
    Ok(live.fingerprint())
}
