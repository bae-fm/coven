use super::*;
use crate::sequence_sync::tests::{insert, pin, Group};
use coven_format::FormatVersion;
use coven_storage::Storage;
use std::num::NonZeroU64;

#[tokio::test]
async fn inner_lengths_never_read_past_the_declared_object() {
    let group = Group::new();
    let device = group.device(1).await;
    let store_key = group.ring.store_key(group.ring.store_key_ids().next().unwrap()).unwrap();
    for (index, (parts, length, consumed)) in
        [(0, 10, 0), (1, 23, 23), (0, 27, 27)].into_iter().enumerate()
    {
        let path = ObjectPath::slot(group.store, NonZeroU64::new(index as u64 + 1).unwrap());
        let mut plaintext = WriteObjectPrefix {
            format: FormatVersion::CURRENT,
            store_key: store_key.id(),
            part_keys: vec![store_key.id(); parts],
        }
        .encode()
        .unwrap();
        plaintext.truncate(23);
        plaintext.extend_from_slice(&1024u32.to_be_bytes());
        plaintext.resize(1024, 0);
        let prefix = SlotObjectPrefix {
            format: FormatVersion::CURRENT,
            key: store_key.id(),
            kind: SlotKind::Writes,
        }
        .encode()
        .unwrap();
        let key = Arc::new(store_key.derive());
        let sealed = key.seal_object_chunk(path.as_str(), &prefix, 0, 0, &plaintext).unwrap();
        let bytes =
            coven_format::sealed_stream::StreamObjectLayout::new().encode_chunk(&sealed).unwrap();
        let input = device
            .database()
            .observe_input(path.as_str().into(), vec![index as u8], Vec::new(), false)
            .await
            .unwrap()
            .unwrap();
        let size = bytes.len() as u64;
        input.keep_range(0, bytes).await.unwrap();
        let mut plaintext = ChunkReader::new(
            input.local_bytes(size).await.unwrap(),
            0,
            plaintext.len() as u64,
            Arc::from(path.as_str()),
            Arc::from(prefix),
            key,
            0,
        );
        assert!(inner(
            &mut plaintext,
            &path,
            length,
            &group.ring,
            &History::default(),
            |audience| matches!(audience, Audience::Store)
        )
        .is_err());
        assert_eq!(
            plaintext.stream_position().unwrap(),
            consumed,
            "the declared object ends at {length}"
        );
    }
    device.database().close().await.unwrap();
}

/// Ana's store after one write: its slots, and the history of every slot
/// before the writes slot that holds the write, as Ben reads them.
async fn written(
    group: &Group,
) -> (crate::sequence_sync::tests::Device, History, ObjectPath, Vec<u8>) {
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    crate::sequence_sync::tests::insert(ana.database(), "a", "one").await;
    ana.sync.pass().await.unwrap();
    let last = ana.database().applied_slot().await.unwrap();
    ana.database().close().await.unwrap();
    let slot = |number| ObjectPath::slot(group.store, NonZeroU64::new(number).unwrap());
    let mut history = History::default();
    for number in 1..last {
        let path = slot(number);
        let bytes = coven_storage::Storage::read(&group.storage, &path).await.unwrap();
        let received = open_bounded(bytes, &path, &group.ring, &history).unwrap();
        history.consume(received, ben.identity.member());
    }
    let path = slot(last);
    let bytes = coven_storage::Storage::read(&group.storage, &path).await.unwrap();
    (ben, history, path, bytes)
}

/// A bounded slot's bytes are hashed once: the signature's digest and the
/// complete object's digest share one pass.
#[tokio::test]
async fn a_bounded_slot_is_hashed_once() {
    let group = Group::new();
    let (ben, _, _, _) = written(&group).await;
    let path = ObjectPath::slot(group.store, NonZeroU64::new(1).unwrap());
    let bytes = coven_storage::Storage::read(&group.storage, &path).await.unwrap();
    let counter = coven_foundation::test_work::Counter::new();
    counter.scope(|| open_bounded(bytes.clone(), &path, &group.ring, &History::default())).unwrap();
    assert_eq!(counter.take().object_bytes_hashed, bytes.len());
    ben.database().close().await.unwrap();
}

/// A writes slot is read from the input database once: the pass that opens
/// its chunks also hashes them for its signature. Only the clear prefix's
/// first read reaches past the prefix.
#[tokio::test]
async fn a_writes_slot_is_read_once() {
    let group = Group::new();
    let (ben, history, path, bytes) = written(&group).await;
    let size = bytes.len() as u64;
    let input = ben
        .database()
        .observe_input(path.as_str().into(), vec![1], Vec::new(), false)
        .await
        .unwrap()
        .unwrap();
    input.keep_range(0, bytes).await.unwrap();
    let retained = input.local_bytes(size).await.unwrap();
    let counter = coven_foundation::test_work::Counter::new();
    let received = counter
        .scope(|| {
            open(retained, &path, &group.ring, &history, |audience| {
                matches!(audience, Audience::Store)
            })
        })
        .unwrap();
    assert!(received.writes());
    let read = counter.take().retained_bytes_read as u64;
    assert!(
        read <= size + SlotObjectPrefix::MAX_LENGTH as u64,
        "read {read} bytes of a {size}-byte slot"
    );
    ben.database().close().await.unwrap();
}

/// Neither the provider nor an outsider reads names, values, key commitments
/// or operation contents: every object the store holds is ciphertext around
/// what the app wrote (§2.3).
#[tokio::test]
async fn stored_objects_never_hold_names_values_or_operation_contents() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Moonlit Gifts".into()).await.unwrap();
    pin(ana.database(), "00000000-0000-4000-8000-000000000001", gifts, "a distinctive pin value")
        .await;
    insert(ana.database(), "note-id-distinct", "a distinctive note title").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Circle(gifts)).await.unwrap();

    let objects = group.storage.list(&coven_storage::ObjectPrefix::all(group.store)).await.unwrap();
    assert!(objects.len() > 3, "slots, snapshots and the floor are stored: {}", objects.len());
    let key = [7u8; 32];
    let secrets: [&[u8]; 8] = [
        b"a distinctive note title",
        b"a distinctive pin value",
        b"note-id-distinct",
        b"Moonlit Gifts",
        b"notes",
        b"pins",
        b"INSERT",
        &key,
    ];
    for object in objects {
        let bytes = group.storage.read(&object.path).await.unwrap();
        for secret in secrets {
            assert!(
                !bytes.windows(secret.len()).any(|window| window == secret),
                "{:?} holds {:?}",
                object.path,
                String::from_utf8_lossy(secret)
            );
        }
    }
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Slot one carries its author's public key, and that key verifies its
/// signature: a creation signed by another key is refused (§10.3).
#[test]
fn slot_one_is_verified_with_the_key_it_carries() {
    let path = coven_format::test_utils::slot_path(1);
    let ring = StoreKeyring::new(coven_format::test_utils::store_key());
    let creation = || SlotFrame::Admin(coven_format::test_utils::creation_change());
    let signed = coven_format::test_utils::bounded_slot(1, 1, SlotKind::Admin, creation());

    let received = open_bounded(signed.clone(), &path, &ring, &History::default()).unwrap();
    assert!(matches!(received.body, SlotBody::Bounded { frame, .. } if frame == creation()));

    let mut forged = signed;
    let signed_len = forged.len() - SIGNATURE_LEN;
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&forged[..signed_len]);
    let impostor = coven_format::test_utils::keys_from_seed(0x44);
    forged[signed_len..]
        .copy_from_slice(impostor.sign_object(path.as_str(), &hash.finish()).as_bytes());
    assert!(open_bounded(forged, &path, &ring, &History::default()).is_err());
}
