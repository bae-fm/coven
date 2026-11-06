use super::*;
use crate::sequence_sync::tests::Group;
use coven_storage::{S3Profile, S3ProfileName};

fn bucket(name: &str, profile: Option<S3Profile>) -> StorageConfig {
    StorageConfig::S3 {
        bucket: name.into(),
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".parse().unwrap()),
        profile,
    }
}

/// A run recorded where detection had no profile is found from the
/// location setup recorded with a profile, since its probes sit with the
/// same objects; a run in another bucket isn't. Ending a record leaves the
/// rest (§20.7).
#[tokio::test]
async fn runs_are_found_by_the_objects_their_probes_sit_with() {
    let device = Group::new().device(1).await;
    let database = device.database();
    let household = bucket("household", None);
    let first = DetectionRecord::start(database, &household, uuid::Uuid::from_bytes([1; 16]))
        .await
        .unwrap();
    DetectionRecord::start(database, &household, uuid::Uuid::from_bytes([2; 16])).await.unwrap();
    DetectionRecord::start(database, &bucket("garden", None), uuid::Uuid::from_bytes([3; 16]))
        .await
        .unwrap();
    let named = bucket("household", Some(S3Profile::Named(S3ProfileName::RustFs)));
    let runs = |records: Vec<DetectionRecord>| {
        records.iter().map(DetectionRecord::run).collect::<Vec<_>>()
    };
    assert_eq!(
        runs(DetectionRecord::at(database, &named).await.unwrap()),
        [uuid::Uuid::from_bytes([1; 16]), uuid::Uuid::from_bytes([2; 16])]
    );
    first.end(database).await.unwrap();
    assert_eq!(
        runs(DetectionRecord::at(database, &household).await.unwrap()),
        [uuid::Uuid::from_bytes([2; 16])]
    );
    // A replacement installation's journal carries each recorded run.
    let carried = DetectionRecord::carried(&database.operations().await.unwrap());
    assert_eq!(carried.len(), 2);
    assert!(carried.iter().all(|row| row.kind == S3_DETECTION));
}
