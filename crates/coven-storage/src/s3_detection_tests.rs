use super::*;
use crate::test_utils::conforming_detection;
use crate::S3ProfileName;

const SERVICE: S3Settings = S3Settings {
    signing: S3Signing::AwsSigV4,
    single_put: S3SinglePut::IfNoneMatch,
    multipart: S3Multipart::ConditionalCompletion,
    multipart_timestamp: S3MultipartTimestamp::Completion,
    checksums: S3Checksums::Absent,
    listing: S3Listing::V2,
    revision: S3Revision::ETag,
    bucket_check: S3BucketCheck::Head,
    response_headers: S3ResponseHeaders::S3,
    request_checksums: false,
};

fn location(endpoint: Option<&str>, profile: Option<S3Profile>) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "region".into(),
        endpoint: endpoint.map(|url| url.parse().unwrap()),
        profile,
        prefix: "store".into(),
    }
}

fn failed(result: Result<(), StorageSetupError>) -> StorageCheck {
    match result {
        Err(StorageSetupError::ProviderCheck { check, source }) => {
            assert_eq!(source.failure(), StorageFailure::Protocol);
            check
        }
        other => panic!("expected a failed check, got {other:?}"),
    }
}

#[test]
fn derivation_keeps_each_held_behavior_and_conforming_settings_pass() {
    let detection = conforming_detection(SERVICE);
    let derived = S3Detection::derive(detection.observations.clone(), SERVICE);
    assert_eq!(derived.settings, SERVICE);
    derived.check(&SERVICE).unwrap();
}

#[test]
fn checks_run_in_order_and_name_the_first_failure() {
    let nothing_held = S3Detection {
        settings: SERVICE,
        observations: Vec::new(),
    };
    assert_eq!(
        failed(nothing_held.check(&SERVICE)),
        StorageCheck::CreateOnce
    );
    let detection = conforming_detection(SERVICE);
    for (settings, check) in [
        (
            S3Settings {
                single_put: S3SinglePut::GenerationMatch,
                multipart: S3Multipart::Copy(S3CopyCondition::IfNoneMatch),
                ..SERVICE
            },
            StorageCheck::CreateOnce,
        ),
        (
            S3Settings {
                multipart: S3Multipart::Copy(S3CopyCondition::IfNoneMatch),
                multipart_timestamp: S3MultipartTimestamp::Start,
                ..SERVICE
            },
            StorageCheck::MultipartCreateOnce,
        ),
        (
            S3Settings {
                multipart_timestamp: S3MultipartTimestamp::Start,
                request_checksums: true,
                ..SERVICE
            },
            StorageCheck::PublicationTime,
        ),
        (
            S3Settings {
                request_checksums: true,
                ..SERVICE
            },
            StorageCheck::Behavior,
        ),
        (
            S3Settings {
                revision: S3Revision::Generation,
                ..SERVICE
            },
            StorageCheck::Behavior,
        ),
        (
            S3Settings {
                bucket_check: S3BucketCheck::List,
                ..SERVICE
            },
            StorageCheck::Behavior,
        ),
    ] {
        assert_eq!(failed(detection.check(&settings)), check, "{settings:?}");
    }
    // Stated scope with nothing returned contradicts nothing; reads see none.
    detection
        .check(&S3Settings {
            checksums: S3Checksums::ScopeStated,
            ..SERVICE
        })
        .unwrap();
}

#[test]
fn a_start_timestamp_needs_an_enforced_copy_dated_by_publication() {
    let mut detection = conforming_detection(S3Settings {
        multipart: S3Multipart::Copy(S3CopyCondition::IfNoneMatch),
        multipart_timestamp: S3MultipartTimestamp::Start,
        ..SERVICE
    });
    detection.check(&detection.settings).unwrap();
    for observation in &mut detection.observations {
        if observation.probe == S3Probe::CopyTimestamp {
            observation.held = false;
        }
    }
    assert_eq!(
        failed(detection.check(&detection.settings)),
        StorageCheck::PublicationTime
    );
}

#[test]
fn resolution_records_detected_settings_only_where_the_location_needs_them() {
    let detection = conforming_detection(SERVICE);
    let uncatalogued = Some("http://localhost:9000");
    assert_eq!(
        detection.resolve(location(uncatalogued, None)).unwrap(),
        location(uncatalogued, Some(S3Profile::Custom(SERVICE)))
    );
    let named = location(uncatalogued, Some(S3Profile::Named(S3ProfileName::RustFs)));
    assert_eq!(detection.resolve(named.clone()).unwrap(), named);
    // AWS's catalogued settings need a conditional copy this service lacks.
    assert_eq!(
        match detection.resolve(location(None, None)) {
            Err(StorageSetupError::ProviderCheck { check, .. }) => check,
            other => panic!("{other:?}"),
        },
        StorageCheck::MultipartCreateOnce
    );
    assert!(matches!(
        detection.resolve(location(
            Some("https://storage.googleapis.com"),
            Some(S3Profile::Custom(SERVICE))
        )),
        Err(StorageSetupError::Storage(error))
            if error.failure() == StorageFailure::InvalidConfiguration
    ));
    assert!(detection
        .resolve(StorageConfig::GoogleDrive {
            folder_id: "folder".into()
        })
        .is_err());
}
