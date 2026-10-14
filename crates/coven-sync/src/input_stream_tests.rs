use super::*;
use crate::store_log_sync::tests::{device, member, store};
use coven_foundation::id_source::DeviceId;
use coven_storage::{
    test_utils::{BodyFailure, Faults, MemoryRequest, MemoryStorage},
    ObjectPath,
};
use std::sync::Arc;

/// The whole retained object and its missing suffix through one stream.
async fn whole(input: &ObjectInput, storage: &dyn Storage) -> Result<Vec<u8>, SyncError> {
    let stream = input.stream(storage).await;
    input.collect(stream).await
}

#[tokio::test]
async fn split_transport_frames_share_one_response_and_bounded_received_buffers() {
    for chunk in [1, 3, 65_535, 65_536, 65_537] {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let receiver = device(storage.clone(), 9, member(1), store(1)).await;
        let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
        let bytes: Vec<_> = (0..200_003).map(|i| (i % 251) as u8).collect();
        storage.create(&path, &bytes).await.unwrap();
        let object = storage.status(&path).await.unwrap().unwrap();
        let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
        let mut faults = Faults::none();
        faults.read_chunk_size = chunk;
        storage.set_faults(faults).await;
        let before = storage.request_trace().len();
        let mut stream = input.stream(storage.as_ref()).await;
        assert_eq!(stream.read_exact(23).await.unwrap(), bytes[..23]);
        assert_eq!(stream.read_exact(65_541).await.unwrap(), bytes[23..65_564]);
        assert_eq!(
            stream.read_exact(bytes.len() - 65_564).await.unwrap(),
            bytes[65_564..]
        );
        stream.drain().await.unwrap();
        assert!(input.received().await.unwrap());
        assert_eq!(
            &storage.request_trace()[before..],
            &[MemoryRequest::Read(path, None)]
        );
        let buffers = storage.received_buffers().await;
        assert_eq!(buffers.iter().sum::<usize>(), bytes.len());
        assert!(buffers.iter().all(|n| *n <= chunk.min(BUFFER)));
    }
}

#[tokio::test]
async fn failed_transport_eof_cannot_be_recovered_from_a_complete_cached_body() {
    for failure in [
        BodyFailure::Interrupted(150_000),
        BodyFailure::Truncated(149_999),
        BodyFailure::ExtraByte,
    ] {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
        let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
        let bytes = vec![7; 150_000];
        storage.create(&path, &bytes).await.unwrap();
        let object = storage.status(&path).await.unwrap().unwrap();
        let mut faults = Faults::none();
        faults.body_failures.insert(path.clone(), failure);
        storage.set_faults(faults).await;
        let retained = if matches!(failure, BodyFailure::Truncated(_)) {
            149_999
        } else {
            2 * BUFFER as u64
        };
        {
            let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
            let result = input.stream(storage.as_ref()).await.drain().await;
            assert!(matches!(result, Err(SyncError::Storage(_))));
            assert!(!input.received().await.unwrap());
            assert_eq!(receiver.db.input_budget().await.unwrap().retained, retained);
        }
        receiver.reopen(storage.clone(), vec![], vec![]).await;
        storage.set_faults(Faults::none()).await;
        let before = storage.reads().await.len();
        let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
        assert_eq!(whole(&input, storage.as_ref()).await.unwrap(), bytes);
        let reads = storage.reads().await;
        assert_eq!(&reads[before..], &[(path, retained, 150_000 - retained)]);
        assert!(input.received().await.unwrap());
    }
}

#[tokio::test]
async fn empty_response_completion_survives_reopen_without_a_second_request() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
    storage.create(&path, &[]).await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let before = storage.reads().await.len();
    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    assert!(whole(&input, storage.as_ref()).await.unwrap().is_empty());
    assert!(input.received().await.unwrap());
    drop(input);
    receiver.reopen(storage.clone(), vec![], vec![]).await;
    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    assert!(whole(&input, storage.as_ref()).await.unwrap().is_empty());
    assert_eq!(storage.reads().await.len() - before, 1);
}

#[tokio::test]
async fn failed_input_commit_preserves_neither_completion_nor_bytes() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    receiver.db.set_input_budget(0).await.unwrap();
    let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
    storage.create(&path, b"content").await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    assert!(matches!(
        whole(&input, storage.as_ref()).await,
        Err(SyncError::Database(
            coven_database::DbError::InputBudgetExceeded { .. }
        ))
    ));
    assert!(!input.received().await.unwrap());
    assert_eq!(receiver.db.input_budget().await.unwrap().retained, 0);
}

#[tokio::test]
async fn maximum_header_vectors_use_two_ranges_and_preserve_every_overread_byte() {
    use coven_format::{sealed_snapshot::SnapshotObjectPrefix, sealed_write::WriteObjectPrefix};
    use coven_foundation::id_source::KeyId;
    let key = KeyId(uuid::Uuid::from_u128(1));
    let write = WriteObjectPrefix {
        format: coven_format::FormatVersion::CURRENT,
        store_key: key,
        part_keys: vec![key; 65_536],
    };
    let mut header = write.encode().unwrap();
    let plaintext = 16 * 1024 * 1024;
    header.extend((plaintext as u32).to_be_bytes());
    header.resize(
        header.len() + plaintext + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD,
        0,
    );
    assert_eq!(header.len(), WriteObjectPrefix::MAX_HEADER_END);
    let snapshot = SnapshotObjectPrefix {
        audience: coven_merge::Audience::Circle(coven_foundation::id_source::CircleId(
            uuid::Uuid::from_u128(2),
        )),
        key,
        caught_at: coven_format::value::StorageTime::new(0, 0).unwrap(),
        writes: coven_format::value::WritePositions(
            (0..65_536)
                .map(|i| coven_merge::WriteId {
                    device: DeviceId(i),
                    number: 1,
                })
                .collect(),
        ),
        store_log: coven_format::value::EntryPositions(
            (0..65_536)
                .map(|i| coven_format::value::EntryId {
                    device: DeviceId(i),
                    number: 1,
                })
                .collect(),
        ),
    };
    let mut signed = snapshot.encode().unwrap();
    signed.extend([0; 64]);
    assert_eq!(signed.len(), SnapshotObjectPrefix::MAX_SIGNED_PREFIX);
    type Boundary = fn(&[u8]) -> Result<usize, coven_format::Error>;
    for (expected, maximum, boundary) in [
        (
            header,
            WriteObjectPrefix::MAX_HEADER_END,
            WriteObjectPrefix::header_end as Boundary,
        ),
        (
            signed,
            SnapshotObjectPrefix::MAX_SIGNED_PREFIX,
            SnapshotObjectPrefix::signed_length as Boundary,
        ),
    ] {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let receiver = device(storage.clone(), 9, member(1), store(1)).await;
        let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
        let mut bytes = expected.clone();
        bytes.extend([7; 1000]);
        storage.create(&path, &bytes).await.unwrap();
        let object = storage.status(&path).await.unwrap().unwrap();
        let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
        let before = storage.reads().await.len();
        assert_eq!(
            input
                .prefix(storage.as_ref(), maximum, boundary)
                .await
                .unwrap(),
            expected
        );
        let header_reads = storage.reads().await;
        assert_eq!(header_reads.len() - before, 2);
        assert_eq!(header_reads[before].1, 0);
        assert_eq!(header_reads[before].2, 64);
        let mut stream = input.stream(storage.as_ref()).await;
        stream.drain().await.unwrap();
        let reads = storage.reads().await;
        let mut through = 0;
        for (_, start, length) in &reads[before..] {
            assert_eq!(*start, through);
            through += length;
        }
        assert_eq!(through, object.size);
        assert_eq!(reads.len() - header_reads.len(), 1);
    }
}

#[tokio::test]
async fn retaining_the_rest_continues_over_local_input_without_another_request() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
    let bytes: Vec<_> = (0..150_001).map(|i| (i % 253) as u8).collect();
    storage.create(&path, &bytes).await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    let mut stream = input.stream(storage.as_ref()).await;
    assert_eq!(stream.read_exact(70_000).await.unwrap(), bytes[..70_000]);
    stream.retain_rest().await.unwrap();
    let requests = storage.request_count();
    assert_eq!(stream.position(), 70_000);
    assert_eq!(
        stream.read_exact(bytes.len() - 70_000).await.unwrap(),
        bytes[70_000..]
    );
    stream.drain().await.unwrap();
    assert_eq!(storage.request_count(), requests);
    drop(stream);
    input.receive(storage.as_ref()).await.unwrap();
    assert_eq!(storage.request_count(), requests);
}

/// SP 481–482: a write whose second chunk stalls in storage must not keep an
/// app write waiting for the database writer (§3.1).
#[tokio::test]
async fn a_stalled_write_body_does_not_hold_the_database_writer() {
    use crate::device_log_sync::tests::{group, sql};
    let storage = crate::device_log_sync::tests::storage();
    let mut devices = group(storage.clone(), 2).await;
    let body = "x".repeat(150_000);
    devices[0]
        .db
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES('large','title',?1)", [&body])?;
            Ok(())
        })
        .await
        .unwrap();
    devices[0].writes.upload_writes().await.unwrap();
    let path = ObjectPath::device_log(
        crate::device_log_sync::tests::STORE_ID,
        DeviceId(1),
        1.try_into().unwrap(),
    );
    let size = storage.status(&path).await.unwrap().unwrap().size;
    assert!(size > 2 * BUFFER as u64);
    let (stalled, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_body_at(path, BUFFER as u64 + 1, stalled, held)
        .await;
    let receiver = &mut devices[1];
    let db = receiver.db.clone();
    let app = async move {
        observed.await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sql(
                &db,
                "INSERT INTO notes VALUES('local','while','downloading')",
            ),
        )
        .await
        .expect("an app write waited for network input");
        resume.send(()).unwrap();
    };
    let (downloaded, ()) = tokio::join!(receiver.writes.download_writes(), app);
    downloaded.unwrap();
    let ids: Vec<String> = receiver
        .db
        .read(|sql| Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert!(ids.contains(&"large".to_string()) && ids.contains(&"local".to_string()));
}
