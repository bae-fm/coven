use super::*;

fn classify(error: keyring_core::Error) -> KeyError {
    KeyError::from(KeychainError::from(error))
}

#[test]
fn no_storage_access_is_temporarily_unavailable() {
    let error = classify(keyring_core::Error::NoStorageAccess(Box::new(
        std::io::Error::other("locked"),
    )));
    assert!(matches!(error, KeyError::KeychainTemporarilyUnavailable(_)));
}

#[test]
fn other_platform_failures_stay_keychain_failures() {
    let error = classify(keyring_core::Error::PlatformFailure(Box::new(
        std::io::Error::other("refused"),
    )));
    assert!(matches!(error, KeyError::Keychain(_)), "{error:?}");
}

/// The Apple keychain answers errSecInteractionNotAllowed (OSStatus -25308)
/// while the device or session is locked, and errSecMissingEntitlement
/// (-34018) when the app lacks its keychain-access-groups entitlement.
/// apple-native-keyring-store reports both as a PlatformFailure boxing the
/// native status, so only the status tells them apart (§12.4).
#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn apple_statuses_classify_a_locked_keychain_and_a_missing_entitlement() {
    let apple = |status: i32| {
        classify(keyring_core::Error::PlatformFailure(Box::new(
            security_framework::base::Error::from_code(status),
        )))
    };
    let locked = apple(-25308);
    assert!(
        matches!(locked, KeyError::KeychainTemporarilyUnavailable(_)),
        "{locked:?}"
    );
    // The native cause stays in the source chain, and names the status.
    let mut cause = std::error::Error::source(&locked);
    let mut status = None;
    while let Some(error) = cause {
        if let Some(native) = error.downcast_ref::<security_framework::base::Error>() {
            status = Some(native.code());
        }
        cause = error.source();
    }
    assert_eq!(status, Some(-25308));
    let entitlement = apple(-34018);
    assert!(
        matches!(entitlement, KeyError::MissingKeychainEntitlement(_)),
        "{entitlement:?}"
    );
    assert!(entitlement.to_string().contains("-34018"), "{entitlement}");
    assert_eq!(
        format!("{locked}"),
        "keychain is unavailable in this session: PlatformFailure(OSStatus -25308)"
    );
    // Any other status keeps the generic failure and its native cause; none is
    // ever reported as a missing entry.
    for status in [-25293, -25299, -26276] {
        let other = apple(status);
        assert!(
            matches!(other, KeyError::Keychain(_)),
            "{status}: {other:?}"
        );
    }
}
