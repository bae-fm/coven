use super::*;
use crate::device_log_sync::tests::STORE_ID;
use crate::device_log_sync::tests::{group, storage};
use crate::store_log_sync::tests::Device;
use coven_format::pending::{PendingReason, PendingReport, PendingSubject, RefusalCode};
use coven_format::sealed_single::SingleChunkPrefix;
use coven_foundation::id_source::KeyId;
use uuid::Uuid;

#[tokio::test]
async fn unchanged_positions_are_not_replaced() {
    use coven_storage::test_utils::MemoryRequest;
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    devices[0].writes.post_positions().await.unwrap();
    let before = storage.request_trace().len();
    devices[0].writes.post_positions().await.unwrap();
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(request, MemoryRequest::Replace(_))));
}

#[tokio::test]
async fn pending_reports_travel_while_own_uploads_hold_positions() {
    use crate::device_log_sync::tests::sql;
    use coven_foundation::id_source::DeviceId;
    use coven_storage::Storage;
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    devices[0].writes.post_positions().await.unwrap();
    let before = devices[0]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    devices[0]
        .db
        .record_pending(crate::PendingRecord {
            subject: crate::PendingSubject::Write(coven_merge::WriteId {
                device: DeviceId(2),
                number: 1,
            }),
            reason: crate::PendingReason::Refused(crate::Refusal::WrongIdentity { cause: None }),
            reported_by: DeviceId(1),
        })
        .await
        .unwrap();
    assert!(!devices[0].writes.post_positions().await.unwrap());
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let bytes = storage.read(&path).await.unwrap();
    let after = open(
        &bytes,
        &path,
        devices[0].custody.read().unwrap().as_ref(),
        &devices[0].db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(after.writes, before.writes);
    assert_eq!(after.store_log, before.store_log);
    assert_eq!(after.fingerprints, before.fingerprints);
    assert_eq!(after.pending.len(), 1);
}

#[tokio::test]
async fn comparable_fingerprint_disagreement_is_pending() {
    use coven_storage::Storage;
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let mut peer = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    peer.fingerprints[0].bytes = coven_crypto::Fingerprint::from_bytes([17; 32]);
    let path = ObjectPath::positions(STORE_ID, peer.device);
    storage
        .replace(&path, &signed_positions(&devices[1], peer.clone()))
        .await
        .unwrap();
    devices[0].writes.download_writes().await.unwrap();
    assert!(devices[0]
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| {
            record.subject
                == crate::PendingSubject::Agreement {
                    audience: Audience::Store,
                    peer: peer.device,
                }
                && record.reason == crate::PendingReason::Disagrees
        }));
}

pub(crate) fn signed_positions(device: &Device, positions: PostedPositions) -> Vec<u8> {
    let path = ObjectPath::positions(STORE_ID, positions.device);
    let key = KeyId(Uuid::from_u128(1));
    let prefix = SingleChunkPrefix::PostedPositions(key);
    let ring = device.custody.read().unwrap().unwrap();
    let sealed = ring
        .store_key(key)
        .unwrap()
        .derive()
        .seal_object_chunk(
            path.as_str(),
            &prefix.encode().unwrap(),
            0,
            0,
            &Object::PostedPositions(positions).encode().unwrap(),
        )
        .unwrap();
    let mut bytes = prefix.encode_chunk(&sealed).unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        device
            .member
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    bytes
}

#[tokio::test]
async fn authenticated_key_copy_reports_are_limited_to_the_posters_member() {
    let storage = storage();
    let devices = group(storage, 2).await;
    let mut positions = devices[0]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    positions.fingerprints.clear();
    let own = devices[0].member.member();
    let other = crate::store_log_sync::tests::member(0x42).member();
    let sealing = devices[0].member.keys().public_keys().sealing;
    let path = ObjectPath::positions(STORE_ID, positions.device);
    let ring = devices[1].custody.read().unwrap().unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    for member in [own, other] {
        positions.pending = vec![PendingReport {
            subject: PendingSubject::KeyCopy {
                audience: Audience::Store,
                key: KeyId(Uuid::from_u128(1)),
                member,
                sealing,
            },
            reason: PendingReason::Refused(RefusalCode::Decryption),
        }];
        let bytes = signed_positions(&devices[0], positions.clone());
        let result = crate::posted_positions::open(&bytes, &path, Some(&ring), &log);
        if member == own {
            assert_eq!(result.unwrap().unwrap(), positions);
        } else {
            assert!(matches!(
                result,
                Ok(Err(crate::PendingReason::InvalidPositions(_)))
            ));
        }
    }
}

#[tokio::test]
async fn authenticated_report_paths_are_limited_to_the_posted_store() {
    use coven_format::pending::Prerequisite;
    use coven_foundation::id_source::StoreId;
    use std::num::NonZeroU64;

    let devices = group(storage(), 2).await;
    let mut positions = devices[0]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    positions.fingerprints.clear();
    let path = ObjectPath::positions(STORE_ID, positions.device);
    let ring = devices[1].custody.read().unwrap().unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    let other = StoreId(Uuid::from_u128(2));
    assert_ne!(STORE_ID, other);
    for store in [STORE_ID, other] {
        let missing = ObjectPath::device_log(store, positions.device, NonZeroU64::new(1).unwrap());
        for reason in [
            PendingReason::Missing {
                path: missing.clone(),
            },
            PendingReason::Waits(Prerequisite::Object(missing)),
        ] {
            positions.pending = vec![PendingReport {
                subject: PendingSubject::Write(coven_merge::WriteId {
                    device: positions.device,
                    number: 1,
                }),
                reason,
            }];
            let bytes = signed_positions(&devices[0], positions.clone());
            let result = open(&bytes, &path, Some(&ring), &log);
            if store == STORE_ID {
                assert_eq!(result.unwrap().unwrap(), positions);
            } else {
                assert!(matches!(
                    result,
                    Ok(Err(crate::PendingReason::InvalidPositions(_)))
                ));
            }
        }
    }
}

#[tokio::test]
async fn oversized_reports_fail_publication_without_replacing_or_truncating_the_post() {
    use coven_database::LogObject;
    use coven_merge::WriteId;
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(1024, 256)
            .build()
            .unwrap(),
    );
    let mut devices = group(storage.clone(), 1).await;
    let device = &mut devices[0];
    device.writes.post_positions().await.unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let previous = storage.read(&path).await.unwrap();
    for writer in 2..66 {
        device
            .db
            .record_pending(crate::PendingRecord::log_refusal(
                coven_database::LogRefusal {
                    object: LogObject::Write(WriteId {
                        device: DeviceId(writer),
                        number: 1,
                    }),
                    failure: RefusalCode::Parse,
                },
                DeviceId(1),
            ))
            .await
            .unwrap();
    }
    assert!(matches!(device.writes.post_positions().await,
        Err(SyncError::Storage(error)) if matches!(error.failure(),
            coven_storage::StorageFailure::SingleRequestTooLarge { limit: 1024, .. })));
    assert_eq!(storage.read(&path).await.unwrap(), previous);
    assert_eq!(
        device
            .writes
            .current_positions()
            .await
            .unwrap()
            .unwrap()
            .pending
            .len(),
        64
    );
}

// SP step 3: positions longer than any positions object are that peer's
// invalid version, judged from the listed size without a GET.
#[tokio::test]
async fn oversize_positions_are_invalid_without_a_download() {
    use coven_foundation::id_source::DeviceId;
    use coven_storage::test_utils::{MemoryRequest, MemoryStorage};
    use coven_storage::Storage;
    let storage = std::sync::Arc::new(
        MemoryStorage::builder()
            .transfer_limits(64 * 1024 * 1024, 64 * 1024)
            .build()
            .unwrap(),
    );
    let mut devices = group(storage.clone(), 3).await;
    devices[1].writes.post_positions().await.unwrap();
    devices[2].writes.post_positions().await.unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(2));
    let oversize = coven_format::sealed_single::MAX_SINGLE_CHUNK_OBJECT + 1;
    storage.replace(&path, &vec![0; oversize]).await.unwrap();
    for _ in 0..2 {
        let before = storage.request_trace().len();
        let observed = devices[0].writes.observe_positions().await.unwrap();
        assert!(!storage.request_trace()[before..]
            .iter()
            .any(|request| matches!(request, MemoryRequest::Read(read, _) if read == &path)));
        assert!(observed.positions.contains_key(&DeviceId(3)));
        assert!(!observed.positions.contains_key(&DeviceId(2)));
    }
    assert!(devices[0]
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(
            |record| record.subject == crate::PendingSubject::Positions(DeviceId(2))
                && matches!(record.reason, crate::PendingReason::InvalidPositions(_))
        ));
}
