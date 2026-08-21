use async_trait::async_trait;
use bytes::Bytes;
use coven::{
    write_cloud_object_stream, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudFileReadError, CloudHome, CloudHomeError, CloudKitAcceptedShareRecord,
    CloudKitAtomicCreateBatch, CloudKitEnvironment, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare, CloudObjectStream,
    CloudVersionedObject, CovenHandle, DeviceJoinCancellation, DeviceJoinError,
    DeviceJoinWriteRevocationExecutor, ExactCreateOutcome, ExactSlotStorage, ExactUpload,
    JoinerJoinTerminal, ObjectSlot, PhysicalObjectLocator, ProviderAccessLocator,
    ProviderAccessWithdrawal, ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
    StoreMemberProviderAccessGrantRef, StoreProviderBinding,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct ExternalProvider {
    exact_create_called: Arc<AtomicBool>,
}

struct ExternalCloudKitBridge;

struct ExternalWriteRevocationExecutor;

#[async_trait]
impl DeviceJoinWriteRevocationExecutor for ExternalWriteRevocationExecutor {
    async fn revoke_write_authority(
        &self,
        _authority: &StoreMemberProviderAccessGrantRef,
        locator: &ProviderAccessLocator,
        _protected_slots: &[ObjectSlot],
    ) -> Result<ProviderAccessWithdrawal, DeviceJoinError> {
        Ok(ProviderAccessWithdrawal::Direct {
            locator: locator.clone(),
            verified_absent: true,
        })
    }
}

async fn external_host_can_revoke_a_missing_joining_device(
    handle: &CovenHandle,
    cancellation: DeviceJoinCancellation,
) -> Result<(), coven::SyncError> {
    let executor = ExternalWriteRevocationExecutor;
    let _: JoinerJoinTerminal = handle
        .revoke_joining_device_writes(cancellation, &executor)
        .await?;
    Ok(())
}

async fn external_host_can_probe_a_proposed_cloud_home(
    handle: &CovenHandle,
    config: &coven::Config,
) -> Result<(), coven::SyncError> {
    handle.probe_cloud_home(config).await
}

async fn external_host_can_setup_s3(
    handle: &CovenHandle,
    cloud_home: coven::CloudHomeConfig,
) -> Result<coven::ConnectedCloudHome, coven::CloudHomeSetupError> {
    handle
        .setup_s3_cloud_home(cloud_home, "access".to_string(), "secret".to_string())
        .await
}

async fn external_host_can_setup_cloudkit(
    handle: &CovenHandle,
    cloud_home: coven::CloudHomeConfig,
) -> Result<coven::ConnectedCloudHome, coven::CloudHomeSetupError> {
    handle
        .setup_cloudkit_cloud_home(cloud_home, Arc::new(ExternalCloudKitBridge))
        .await
}

#[cfg(feature = "oauth-providers")]
async fn external_host_can_setup_oauth(
    handle: &CovenHandle,
    cloud_home: coven::CloudHomeConfig,
    cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<coven::ConnectedCloudHome, coven::CloudHomeSetupError> {
    handle.setup_oauth_cloud_home(cloud_home, cancel).await
}

fn external_host_can_read_cloud_home_key_state(
    handle: &CovenHandle,
    storage: coven::HomeStorage,
) -> Result<coven::CloudHomeKeyState, coven::KeyError> {
    handle.cloud_home_key_state(storage)
}

/// A host that asks for the cloud home key state has to tell a keychain that
/// refused right now — locked, display asleep, no UI session — apart from a
/// store that never held a key and from every other keyring failure, so it can
/// ask again after unlock instead of aborting its boot. That decision is a
/// match on a variant, not a search through a message.
fn external_host_can_tell_a_keychain_refusal_apart(error: &coven::KeyError) -> bool {
    matches!(error, coven::KeyError::KeychainTemporarilyUnavailable)
}

fn external_host_can_classify_cloud_home_setup(
    error: &coven::CloudHomeSetupError,
) -> coven::CloudHomeSetupFailure {
    error.failure()
}

#[test]
fn external_host_can_name_device_join_revocation_surface() {
    fn assert_executor<T: DeviceJoinWriteRevocationExecutor>() {}
    assert_executor::<ExternalWriteRevocationExecutor>();
    let _ = external_host_can_revoke_a_missing_joining_device;
}

#[test]
fn external_host_can_name_cloud_home_probe_surface() {
    let _ = external_host_can_probe_a_proposed_cloud_home;
    let _ = external_host_can_setup_s3;
    let _ = external_host_can_setup_cloudkit;
    #[cfg(feature = "oauth-providers")]
    let _ = external_host_can_setup_oauth;
    let _ = external_host_can_read_cloud_home_key_state;
    let _ = external_host_can_tell_a_keychain_refusal_apart;
    let _ = external_host_can_classify_cloud_home_setup;
}

impl CloudKitOps for ExternalCloudKitBridge {
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
        let (owner_name, zone_name) = match scope {
            CloudKitScope::Private => ("external-owner", "external-zone"),
            CloudKitScope::Shared {
                owner_name,
                zone_name,
            } => (owner_name.as_str(), zone_name.as_str()),
        };
        Ok(CloudKitProviderIdentity {
            container_id: "iCloud.external.coven".to_string(),
            environment: CloudKitEnvironment::Development,
            owner_name: owner_name.to_string(),
            zone_name: zone_name.to_string(),
            current_user_record_name: "external-user".to_string(),
        })
    }

    fn accepted_read_write_share(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
        unimplemented!()
    }

    fn write_record(&self, _: &CloudKitScope, _: &str, _: Vec<u8>) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn read_record(&self, _: &CloudKitScope, _: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!()
    }
    fn list_records(&self, _: &CloudKitScope, _: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!()
    }
    fn delete_record(&self, _: &CloudKitScope, _: &str) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn record_exists(&self, _: &CloudKitScope, _: &str) -> Result<bool, CloudHomeError> {
        unimplemented!()
    }
    fn read_versioned_record(
        &self,
        _: &CloudKitScope,
        _: &str,
    ) -> Result<CloudVersionedObject, CloudHomeError> {
        unimplemented!()
    }

    fn begin_atomic_create(
        &self,
        _: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        unimplemented!()
    }
    fn stage_atomic_create_record(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
        _: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn commit_atomic_create(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        unimplemented!()
    }
    fn discard_atomic_create(
        &self,
        _: &CloudKitScope,
        _: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn delete_record_versions(
        &self,
        _: &CloudKitScope,
        _: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn share_for_member(&self, _: &str) -> Result<Option<CloudKitShare>, CloudHomeError> {
        unimplemented!()
    }
    fn grant_share(&self, _: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!()
    }
    fn revoke_share(&self, _: &str) -> Result<(), CloudHomeError> {
        unimplemented!()
    }
    fn accept_share(&self, _: &str) -> Result<CloudKitShare, CloudHomeError> {
        unimplemented!()
    }
}

#[test]
fn external_cloudkit_bridge_can_name_every_signature_type() {
    fn assert_bridge<T: CloudKitOps>() {}
    assert_bridge::<ExternalCloudKitBridge>();
}

#[test]
fn object_slot_rejects_empty_components() {
    let empty_key = ObjectSlot::logical(String::new()).expect_err("empty logical key must fail");
    assert!(matches!(empty_key, coven::StorageError::Configuration(_)));
    assert!(!empty_key.is_transport());

    let empty_provider = ObjectSlot::opaque("objects/copy".to_string(), String::new())
        .expect_err("empty provider id must fail");
    assert!(matches!(
        empty_provider,
        coven::StorageError::Configuration(_)
    ));
    assert!(!empty_provider.is_transport());
}

#[async_trait]
impl CloudHome for ExternalProvider {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "multipart unsupported in compile-test provider".to_string(),
        ))
    }

    fn multipart_threshold(&self) -> u64 {
        u64::MAX
    }

    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        Ok(Vec::new())
    }

    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }

    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        Ok(false)
    }

    async fn set_access(
        &self,
        _desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "sharing unsupported in compile-test provider".to_string(),
        ))
    }
}

#[async_trait]
impl ExactSlotStorage for ExternalProvider {
    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, CloudHomeError> {
        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::Dropbox {
                namespace_id: "namespace:external-folder".to_string(),
            },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::Dropbox {
                    account_id: "dbid:external-account".to_string(),
                },
            },
        })
    }

    async fn allocate_slot(&self, key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ObjectSlot::opaque(key.to_string(), "provider:created-copy".to_string())
            .map_err(CloudHomeError::from)
    }

    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        CloudHome::list(self, prefix)
            .await?
            .into_iter()
            .map(|key| {
                ObjectSlot::opaque(key, "provider:created-copy".to_string())
                    .map_err(CloudHomeError::from)
            })
            .collect()
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        progress: &coven::UploadProgress,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
        let slot = upload.object().slot();
        assert_eq!(slot.logical_key(), "objects/default-create");
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:created-copy".to_string())
        );
        let data = upload.body().await?.collect().await?;
        self.exact_create_called.store(true, Ordering::SeqCst);
        progress(data.len() as u64);
        Ok(ExactCreateOutcome::Created)
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:copy".to_string())
        );
        Ok(b"external provider bytes".to_vec())
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        let bytes = self.read_at(slot).await?;
        Ok(bytes[start as usize..end as usize].to_vec())
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
        progress: coven::DownloadProgress,
    ) -> Result<(), CloudFileReadError> {
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:copy".to_string())
        );
        let stream: CloudObjectStream = Box::pin(futures_util::stream::iter([
            Ok(Bytes::from_static(b"external")),
            Ok(Bytes::from_static(b" provider bytes")),
        ]));
        write_cloud_object_stream(destination, stream, progress).await?;
        Ok(())
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:copy".to_string())
        );
        Ok(())
    }
}

#[tokio::test]
async fn external_provider_can_name_and_implement_the_full_cloud_home_surface() {
    let exact_create_called = Arc::new(AtomicBool::new(false));
    let provider: Arc<dyn coven::ExactCloudHome> = Arc::new(ExternalProvider {
        exact_create_called: exact_create_called.clone(),
    });
    assert!(matches!(
        provider.provider_binding().await.unwrap().store,
        StoreProviderBinding::Dropbox { .. }
    ));
    let created = provider
        .allocate_slot("objects/default-create")
        .await
        .expect("allocate external provider slot");
    let created_bytes = b"external create-only bytes";
    let created_object = coven::ExactObjectRef::new(
        created.clone(),
        created_bytes.len() as u64,
        coven::ObjectHash::digest(created_bytes),
    );
    let created_upload =
        ExactUpload::from_bytes(&created_object, created_bytes).expect("valid exact upload");
    provider
        .create_at(&created_upload, &coven::no_progress())
        .await
        .expect("provider creates the exact slot");
    assert_eq!(created.logical_key(), "objects/default-create");
    assert!(exact_create_called.load(Ordering::SeqCst));
    let object = ObjectSlot::opaque("objects/copy".to_string(), "provider:copy".to_string())
        .expect("valid external provider slot");
    let temp = tempfile::tempdir().expect("temp dir");
    let destination = temp.path().join("object.bin");

    let read_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&read_progress);
    provider
        .read_at_to_file(
            &object,
            &destination,
            Arc::new(move |bytes| observed.lock().expect("progress lock").push(bytes)),
        )
        .await
        .expect("external provider stream");
    assert_eq!(*read_progress.lock().expect("progress lock"), vec![8, 23]);

    assert_eq!(
        tokio::fs::read(destination)
            .await
            .expect("read destination"),
        b"external provider bytes"
    );
    assert_eq!(
        provider.read_at(&object).await.unwrap(),
        b"external provider bytes"
    );
    assert_eq!(
        provider.read_range_at(&object, 9, 17).await.unwrap(),
        b"provider"
    );
    provider.delete_at(&object).await.unwrap();
}
