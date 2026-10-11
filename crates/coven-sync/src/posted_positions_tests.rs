use super::*;
use crate::device_log_sync::tests::{group, storage};
use crate::store_log_sync::tests::Device;
use coven_format::pending::{PendingReason, PendingReport, PendingSubject, RefusalCode};
use coven_format::sealed_single::SingleChunkPrefix;
use coven_foundation::id_source::KeyId;
use uuid::Uuid;

pub(crate) fn signed_positions(device: &Device, positions: PostedPositions) -> Vec<u8> {
    let path = ObjectPath::positions(positions.device);
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
    let own = devices[0].member.member_id();
    let other: coven_crypto::MemberId =
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
            .parse()
            .unwrap();
    let path = ObjectPath::positions(positions.device);
    let ring = devices[1].custody.read().unwrap().unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    for member in [own.clone(), other] {
        positions.pending = vec![PendingReport {
            subject: PendingSubject::KeyCopy {
                audience: Audience::Store,
                key: KeyId(Uuid::from_u128(1)),
                member: member.clone(),
            },
            reason: PendingReason::Refused(RefusalCode::Decryption),
        }];
        let bytes = signed_positions(&devices[0], positions.clone());
        let result = crate::posted_positions::open(&bytes, &path, Some(&ring), &log);
        if member == own {
            assert_eq!(result.unwrap(), positions);
        } else {
            assert!(matches!(result, Err(SyncError::Damaged(_))));
        }
    }
}
