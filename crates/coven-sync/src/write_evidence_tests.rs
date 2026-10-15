use super::*;
use crate::device_log_sync::tests::{group, sql, storage, STORE_ID};
use coven_database::Authorship;
use coven_format::authorship::Multiproof;
use coven_storage::test_utils::Faults;

/// Ana's first write, sealed as its first attempt would send it, and the
/// header evidence a snapshot would carry for it.
async fn sealed_write() -> (crate::store_log_sync::tests::Device, WriteEvidence) {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let mut device = devices.remove(0);
    sql(&device.db, "INSERT INTO notes VALUES('one','first','body')").await;
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(device.writes.upload_writes().await.is_err());
    let (write, bytes) = crate::device_log_sync::tests::writes::resealed(&device).await;
    let prefix_length = WriteObjectPrefix::length(&bytes).unwrap();
    let prefix = WriteObjectPrefix::decode(&bytes[..prefix_length]).unwrap();
    let header_end =
        prefix_length + WriteObjectPrefix::header_chunk_length(&bytes[prefix_length..]).unwrap();
    let tail_end = header_end + WriteObjectPrefix::tail_length(prefix.part_keys.len());
    let tail = prefix.decode_tail(&bytes[header_end..tail_end]).unwrap();
    let evidence = WriteEvidence {
        write,
        prefix: bytes[..prefix_length].to_vec(),
        header: bytes[prefix_length..header_end].to_vec(),
        roots: tail.roots,
        signature: *tail.signature.as_bytes(),
        proof: Multiproof {
            leaves: 0,
            revealed: Vec::new(),
            hidden: Vec::new(),
        },
    };
    (device, evidence)
}

async fn keys(device: &crate::store_log_sync::tests::Device) -> EvidenceKeys {
    EvidenceKeys::new(
        STORE_ID,
        device.custody.read().unwrap(),
        device.db.local_store_log().await.unwrap().log,
    )
}

fn inconsistent(result: Result<WriteHeaderFrame, DbError>) -> &'static str {
    match result {
        Err(DbError::Snapshot(SnapshotError::Inconsistent(reason))) => reason,
        other => panic!(
            "expected a refusal, got {:?}",
            other.map(|h| h.header.position)
        ),
    }
}

#[tokio::test]
async fn a_carried_header_opens_and_verifies_with_its_authors_key() {
    let (device, evidence) = sealed_write().await;
    let keys = keys(&device).await;
    let header = keys.header(&evidence).unwrap();
    assert_eq!(header.header.position, evidence.write);
    // The hasher is the one the sealed part's root was computed with.
    let prefix = WriteObjectPrefix::decode(&evidence.prefix).unwrap();
    let ring = device.custody.read().unwrap().unwrap();
    let expected = crate::write_seal::derive(&ring, &Audience::Store, prefix.part_keys[0])
        .unwrap()
        .authorship_hasher();
    let hasher = keys.hasher(&Audience::Store, prefix.part_keys[0]).unwrap();
    assert_eq!(hasher.leaf(b"leaf"), expected.leaf(b"leaf"));
}

#[tokio::test]
async fn a_changed_root_or_signature_or_identity_is_refused() {
    let (device, evidence) = sealed_write().await;
    let keys = keys(&device).await;
    let mut root = evidence.clone();
    root.roots[0][0] ^= 1;
    assert_eq!(
        inconsistent(keys.header(&root)),
        "write evidence header signature does not verify"
    );
    let mut signature = evidence.clone();
    signature.signature[0] ^= 1;
    assert_eq!(
        inconsistent(keys.header(&signature)),
        "write evidence header signature does not verify"
    );
    // The sealed header is bound to its path: another write's id cannot open it.
    let mut moved = evidence.clone();
    moved.write.number += 1;
    assert_eq!(
        inconsistent(keys.header(&moved)),
        "write evidence header does not open"
    );
    let mut extra = evidence.clone();
    extra.roots.push([0; 32]);
    assert_eq!(
        inconsistent(keys.header(&extra)),
        "write evidence does not match its prefix"
    );
    let mut cut = evidence;
    cut.header.pop();
    assert!(keys.header(&cut).is_err());
}

#[tokio::test]
async fn evidence_under_a_key_not_held_waits_for_it() {
    let (device, evidence) = sealed_write().await;
    let keys = EvidenceKeys::new(
        STORE_ID,
        None,
        device.db.local_store_log().await.unwrap().log,
    );
    let prefix = WriteObjectPrefix::decode(&evidence.prefix).unwrap();
    assert!(matches!(
        keys.header(&evidence),
        Err(DbError::Snapshot(SnapshotError::KeyUnavailable { audience: Audience::Store, key }))
            if key == prefix.store_key
    ));
    // A view without the write's recorded store-log entries cannot judge its author.
    let empty = EvidenceKeys::new(
        STORE_ID,
        device.custody.read().unwrap(),
        StoreLog::default(),
    );
    assert!(matches!(
        empty.header(&evidence),
        Err(DbError::Snapshot(SnapshotError::StoreLog { .. }))
    ));
}
