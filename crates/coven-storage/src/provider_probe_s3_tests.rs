use super::*;
use crate::cloud::{CloudHomeFactory, CloudVersionedObject};
use coven_foundation::clock::SystemClock;
use coven_foundation::config::ExactUploadVerification;

/// Uses an existing bucket and two independently authorized credentials.
/// Required: COVEN_TEST_S3_URL, COVEN_TEST_S3_BUCKET, COVEN_TEST_S3_KEY,
/// COVEN_TEST_S3_SECRET, COVEN_TEST_S3_PEER_KEY, COVEN_TEST_S3_PEER_SECRET.
/// COVEN_TEST_S3_REGION defaults to us-east-1; COVEN_TEST_S3_PREFIX is optional.
/// Run with `cargo test -p coven-storage configured_s3_principals_compete_on_one_revision -- --ignored`.
#[tokio::test]
#[ignore = "requires an S3 bucket and two independent permitted provider credentials"]
async fn configured_s3_principals_compete_on_one_revision() {
    let required = |name: &str| {
        optional_setting(name).unwrap_or_else(|| panic!("{name} must be configured for this test"))
    };
    let bucket = required("COVEN_TEST_S3_BUCKET");
    let endpoint = required("COVEN_TEST_S3_URL");
    let region = match optional_setting("COVEN_TEST_S3_REGION") {
        Some(region) => region,
        None => "us-east-1".to_string(),
    };
    let prefix = optional_setting("COVEN_TEST_S3_PREFIX");
    let credentials = [
        (
            required("COVEN_TEST_S3_KEY"),
            required("COVEN_TEST_S3_SECRET"),
        ),
        (
            required("COVEN_TEST_S3_PEER_KEY"),
            required("COVEN_TEST_S3_PEER_SECRET"),
        ),
    ];
    let factory = CloudHomeFactory::new(crate::oauth::OAuthClients::empty());
    let clock = Arc::new(SystemClock);
    let [first_credentials, second_credentials] = credentials;
    let first = factory
        .open_s3(
            bucket.clone(),
            region.clone(),
            Some(endpoint.clone()),
            first_credentials.0,
            first_credentials.1,
            prefix.clone(),
            ExactUploadVerification::MetadataHash,
            clock.clone(),
        )
        .await
        .expect("open first configured S3 principal");
    let second = factory
        .open_s3(
            bucket,
            region,
            Some(endpoint),
            second_credentials.0,
            second_credentials.1,
            prefix,
            ExactUploadVerification::MetadataHash,
            clock,
        )
        .await
        .expect("open second configured S3 principal");
    let first_binding = first
        .provider_binding()
        .await
        .expect("resolve first S3 principal");
    let second_binding = second
        .provider_binding()
        .await
        .expect("resolve second S3 principal");
    assert_eq!(first_binding.store, second_binding.store);
    assert_ne!(
        first_binding.device, second_binding.device,
        "credentials must resolve to independent provider principals"
    );
    let id = uuid::Uuid::new_v4();
    let slot = first
        .allocate_slot(&format!("__coven_probe__/cross-principal-conditional/{id}"))
        .await
        .expect("allocate isolated conditional probe slot");
    let initial = format!("initial:{id}").into_bytes();
    let replacements = [
        format!("first:{id}").into_bytes(),
        format!("second:{id}").into_bytes(),
    ];
    // Capture failures as values so exact cleanup runs before any assertion.
    let observed = async {
        create_versioned_bytes(&first, &slot, &initial).await?;
        let first_start = first
            .read_versioned_at(&slot)
            .await
            .map_err(StorageError::from)?;
        let second_start = second
            .read_versioned_at(&slot)
            .await
            .map_err(StorageError::from)?;
        let outcomes = tokio::join!(
            first.replace_at_if_version(&slot, &first_start.version, replacements[0].clone()),
            second.replace_at_if_version(&slot, &second_start.version, replacements[1].clone()),
        );
        let first_read = first
            .read_versioned_at(&slot)
            .await
            .map_err(StorageError::from)?;
        let second_read = second
            .read_versioned_at(&slot)
            .await
            .map_err(StorageError::from)?;
        Ok::<_, ProviderProbeError>((first_start, second_start, outcomes, first_read, second_read))
    }
    .await;
    let cleanup = delete_versioned_probe(&first, &slot).await;
    let peer_absence = second.read_versioned_at(&slot).await;
    cleanup.expect("remove the exact isolated probe object after success or failure");
    assert!(
        matches!(peer_absence, Err(CloudHomeError::NotFound(_))),
        "second principal must observe exact deletion: {peer_absence:?}"
    );
    let (first_start, second_start, outcomes, first_read, second_read) =
        observed.expect("run real provider conditional updates and readbacks");
    assert_eq!(
        first_start, second_start,
        "both principals must observe the same starting bytes and revision"
    );
    assert_eq!(first_start.bytes, initial);
    let winner = match outcomes {
        (
            Ok(ConditionalWriteOutcome::Replaced(version)),
            Ok(ConditionalWriteOutcome::VersionChanged),
        ) => (0, version),
        (
            Ok(ConditionalWriteOutcome::VersionChanged),
            Ok(ConditionalWriteOutcome::Replaced(version)),
        ) => (1, version),
        outcomes => panic!(
            "independent conditional contenders require one winner and one conflict: {outcomes:?}"
        ),
    };
    let expected = CloudVersionedObject {
        bytes: replacements[winner.0].clone(),
        version: winner.1,
    };
    assert_eq!(
        first_read, expected,
        "first principal must read the accepted exact revision and bytes"
    );
    assert_eq!(
        second_read, expected,
        "second principal must read the same accepted exact revision and bytes"
    );
}

fn optional_setting(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => panic!("{name} must contain UTF-8 text"),
    }
}
