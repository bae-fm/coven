use super::*;
use crate::keys::StoreKeys;
use crate::storage::cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
};
use coven_foundation::clock::FixedClock;
use coven_foundation::config::{CloudProvider, Config, HomeStorage};
use coven_foundation::store_dir::StoreDir;
use std::sync::Mutex;

struct ScopeRecordingOps {
    seen: Mutex<Vec<CloudKitScope>>,
}

impl ScopeRecordingOps {
    fn new() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl CloudKitOps for ScopeRecordingOps {
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
        let (owner_name, zone_name) = match scope {
            CloudKitScope::Private => ("test-owner", "test-zone"),
            CloudKitScope::Shared {
                owner_name,
                zone_name,
            } => (owner_name.as_str(), zone_name.as_str()),
        };
        Ok(CloudKitProviderIdentity {
            container_id: "iCloud.test.coven".to_string(),
            environment: crate::CloudKitEnvironment::Development,
            owner_name: owner_name.to_string(),
            zone_name: zone_name.to_string(),
            current_user_record_name: "test-user".to_string(),
        })
    }

    fn accepted_read_write_share(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
        Err(CloudHomeError::NotFound(
            "accepted CloudKit share".to_string(),
        ))
    }

    fn write_record(
        &self,
        _scope: &CloudKitScope,
        _key: &str,
        _data: Vec<u8>,
    ) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn read_record(&self, _scope: &CloudKitScope, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn list_records(
        &self,
        scope: &CloudKitScope,
        _prefix: &str,
    ) -> Result<Vec<String>, CloudHomeError> {
        self.seen.lock().unwrap().push(scope.clone());
        Ok(Vec::new())
    }

    fn delete_record(&self, _scope: &CloudKitScope, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn record_exists(&self, _scope: &CloudKitScope, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn read_versioned_record(
        &self,
        _scope: &CloudKitScope,
        _key: &str,
    ) -> Result<crate::storage::cloud::CloudVersionedObject, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn begin_atomic_create(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn stage_atomic_create_record(
        &self,
        _scope: &CloudKitScope,
        _batch: &CloudKitAtomicCreateBatch,
        _record: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn commit_atomic_create(
        &self,
        _scope: &CloudKitScope,
        _batch: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn discard_atomic_create(
        &self,
        _scope: &CloudKitScope,
        _batch: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn delete_record_versions(
        &self,
        _scope: &CloudKitScope,
        _records: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn grant_share(&self, _member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn share_for_member(
        &self,
        _member_pubkey: &str,
    ) -> Result<Option<CloudKitShare>, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn revoke_share(&self, _member_pubkey: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }

    fn accept_share(&self, _share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!("not exercised by these tests")
    }
}

fn cloudkit_config(owner_zone: Option<(&str, &str)>) -> Config {
    let mut config = Config::with_defaults(
        "store-1".to_string(),
        "device-1".to_string(),
        StoreDir::new("unused-store-dir"),
        "CloudKit Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::CloudKit);
    config.cloud_home.storage = HomeStorage::Opaque;
    if let Some((owner, zone)) = owner_zone {
        config.cloud_home.cloudkit_owner_name = Some(owner.to_string());
        config.cloud_home.cloudkit_zone_name = Some(zone.to_string());
    }
    config
}

#[tokio::test]
async fn s3_without_a_bucket_is_a_non_retryable_configuration_error() {
    let mut config = Config::with_defaults(
        "s3-config-error".to_string(),
        "device-1".to_string(),
        StoreDir::new("unused-store-dir"),
        "S3 Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::S3);
    config.cloud_home.s3_bucket = None;
    let factory = CloudHomeFactory::new(
        StoreKeys::bind(config.store_id.clone()),
        crate::oauth::OAuthClients::empty(),
    );
    let clock: coven_foundation::clock::ClockRef =
        std::sync::Arc::new(FixedClock(chrono::Utc::now()));

    let error = match factory.create(&config, clock, None).await {
        Ok(_) => panic!("a provider with no bucket must not build a cloud home"),
        Err(error) => error,
    };

    assert!(matches!(error, CloudHomeError::Configuration(_)));
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn neither_owner_nor_zone_builds_a_private_home() {
    let config = cloudkit_config(None);
    let key_service = StoreKeys::bind(config.store_id.clone());
    let ops = std::sync::Arc::new(ScopeRecordingOps::new());
    let clock: coven_foundation::clock::ClockRef =
        std::sync::Arc::new(FixedClock(chrono::Utc::now()));
    let factory = CloudHomeFactory::new(key_service, crate::oauth::OAuthClients::empty());

    let home = factory
        .create(&config, clock, Some(ops.clone()))
        .await
        .expect("private CloudKit config builds a home");
    home.list("").await.expect("list against the built home");

    assert_eq!(
        ops.seen.lock().unwrap().as_slice(),
        [CloudKitScope::Private]
    );
}

#[tokio::test]
async fn both_owner_and_zone_build_a_shared_home() {
    let config = cloudkit_config(Some(("owner-name", "zone-name")));
    let key_service = StoreKeys::bind(config.store_id.clone());
    let ops = std::sync::Arc::new(ScopeRecordingOps::new());
    let clock: coven_foundation::clock::ClockRef =
        std::sync::Arc::new(FixedClock(chrono::Utc::now()));
    let factory = CloudHomeFactory::new(key_service, crate::oauth::OAuthClients::empty());

    let home = factory
        .create(&config, clock, Some(ops.clone()))
        .await
        .expect("shared CloudKit config builds a home");
    home.list("").await.expect("list against the built home");

    assert_eq!(
        ops.seen.lock().unwrap().as_slice(),
        [CloudKitScope::Shared {
            owner_name: "owner-name".to_string(),
            zone_name: "zone-name".to_string(),
        }]
    );
}

#[tokio::test]
async fn mixed_owner_zone_is_a_configuration_error() {
    let mut config = cloudkit_config(None);
    config.cloud_home.cloudkit_owner_name = Some("owner-name".to_string());
    let key_service = StoreKeys::bind(config.store_id.clone());
    let ops = std::sync::Arc::new(ScopeRecordingOps::new());
    let clock: coven_foundation::clock::ClockRef =
        std::sync::Arc::new(FixedClock(chrono::Utc::now()));
    let factory = CloudHomeFactory::new(key_service, crate::oauth::OAuthClients::empty());

    let result = factory.create(&config, clock, Some(ops)).await;
    match result {
        Ok(_) => panic!("mixed owner/zone must not build a home"),
        Err(CloudHomeError::Configuration(message)) => {
            assert!(message.contains("cloudkit_owner_name"), "{message}");
            assert!(message.contains("cloudkit_zone_name"), "{message}");
        }
        Err(other) => panic!("expected Configuration error, got {other:?}"),
    }
}
