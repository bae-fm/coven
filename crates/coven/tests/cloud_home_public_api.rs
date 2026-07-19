use async_trait::async_trait;
use bytes::Bytes;
use coven::{
    write_cloud_object_stream, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudFileReadError, CloudHeadCreateError, CloudHeadReplaceError, CloudHeadVersion, CloudHome,
    CloudHomeError, CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitEnvironment,
    CloudKitOps, CloudKitProviderIdentity, CloudKitRecordCreate, CloudKitRecordVersion,
    CloudKitScope, CloudKitShare, CloudObjectStream, CloudVersionedHead, CovenHandle,
    DeviceJoinCancellation, DeviceJoinError, DeviceJoinProducer, DeviceJoinWriteRevocationExecutor,
    ExactSlotStorage, JoinerJoinTerminal, ObjectSlot, PhysicalObjectLocator, ProviderAccessLocator,
    ProviderAccessWithdrawal, ProviderAdminGrantId, ProviderAdminJoinTerminal,
    ProviderDeviceBinding, ProviderPrincipalId, ProviderWriteAuthorityRef, ResolvedProviderBinding,
    StoreProviderBinding,
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
        _producer: DeviceJoinProducer,
        _authority: &ProviderWriteAuthorityRef,
        locator: &ProviderAccessLocator,
        _protected_slots: &[ObjectSlot],
    ) -> Result<ProviderAccessWithdrawal, DeviceJoinError> {
        Ok(ProviderAccessWithdrawal::Direct {
            locator: locator.clone(),
            verified_absent: true,
        })
    }
}

async fn external_host_can_revoke_missing_device_join_producers(
    handle: &CovenHandle,
    cancellation: DeviceJoinCancellation,
    executor_grant: ProviderAdminGrantId,
) -> Result<(), coven::SyncError> {
    let executor = ExternalWriteRevocationExecutor;
    let _: ProviderAdminJoinTerminal = handle
        .revoke_device_provider_admission_writes(
            cancellation.clone(),
            &executor,
            executor_grant.clone(),
        )
        .await?;
    let _: JoinerJoinTerminal = handle
        .revoke_joining_device_writes(cancellation, &executor, executor_grant)
        .await?;
    Ok(())
}

#[test]
fn external_host_can_name_device_join_revocation_surface() {
    fn assert_executor<T: DeviceJoinWriteRevocationExecutor>() {}
    assert_executor::<ExternalWriteRevocationExecutor>();
    let _ = external_host_can_revoke_missing_device_join_producers;
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
    ) -> Result<CloudVersionedHead, CloudHomeError> {
        unimplemented!()
    }
    fn create_record(
        &self,
        _: &CloudKitScope,
        _: &str,
        _: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        unimplemented!()
    }
    fn replace_record(
        &self,
        _: &CloudKitScope,
        _: &str,
        _: &CloudHeadVersion,
        _: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
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
    assert!(matches!(empty_key, CloudHomeError::Configuration(_)));
    assert!(!empty_key.is_retryable());

    let empty_provider = ObjectSlot::opaque("objects/copy".to_string(), String::new())
        .expect_err("empty provider id must fail");
    assert!(matches!(empty_provider, CloudHomeError::Configuration(_)));
    assert!(!empty_provider.is_retryable());
}

#[async_trait]
impl CloudHome for ExternalProvider {
    fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
        Some(self)
    }

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
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &coven::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        assert_eq!(slot.logical_key(), "objects/default-create");
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:created-copy".to_string())
        );
        let data = body.collect().await?;
        self.exact_create_called.store(true, Ordering::SeqCst);
        progress(data.len() as u64);
        Ok(())
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
    ) -> Result<(), CloudFileReadError> {
        assert_eq!(
            slot.physical(),
            &PhysicalObjectLocator::Opaque("provider:copy".to_string())
        );
        let stream: CloudObjectStream = Box::pin(futures_util::stream::once(async {
            Ok(Bytes::from_static(b"external provider bytes"))
        }));
        write_cloud_object_stream(destination, stream).await?;
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
    let provider: Arc<dyn CloudHome> = Arc::new(ExternalProvider {
        exact_create_called: exact_create_called.clone(),
    });
    let exact = provider
        .clone()
        .exact_slot_storage()
        .expect("exact-slot adapter");
    assert!(matches!(
        exact.provider_binding().await.unwrap().store,
        StoreProviderBinding::Dropbox { .. }
    ));
    let created = exact
        .allocate_slot("objects/default-create")
        .await
        .expect("allocate external provider slot");
    exact
        .create_at(
            &created,
            BlobBody::from_bytes(b"external create-only bytes".to_vec()),
            &|_| {},
        )
        .await
        .expect("provider creates the exact slot");
    assert_eq!(created.logical_key(), "objects/default-create");
    assert!(exact_create_called.load(Ordering::SeqCst));
    let object = ObjectSlot::opaque("objects/copy".to_string(), "provider:copy".to_string())
        .expect("valid external provider slot");
    let temp = tempfile::tempdir().expect("temp dir");
    let destination = temp.path().join("object.bin");

    exact
        .read_at_to_file(&object, &destination)
        .await
        .expect("external provider stream");

    assert_eq!(
        tokio::fs::read(destination)
            .await
            .expect("read destination"),
        b"external provider bytes"
    );
    assert_eq!(
        exact.read_at(&object).await.unwrap(),
        b"external provider bytes"
    );
    assert_eq!(
        exact.read_range_at(&object, 9, 17).await.unwrap(),
        b"provider"
    );
    exact.delete_at(&object).await.unwrap();
}
