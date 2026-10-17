use super::*;
use crate::sequence_sync::tests::Group;
use coven_format::FormatVersion;
use std::num::NonZeroU64;

#[tokio::test]
async fn inner_lengths_never_read_past_the_declared_object() {
    let group = Group::new();
    let device = group.device(1).await;
    let store_key = group
        .ring
        .store_key(group.ring.store_key_ids().next().unwrap())
        .unwrap();
    for (index, (parts, length, consumed)) in [(0, 10, 0), (1, 23, 23), (0, 27, 27)]
        .into_iter()
        .enumerate()
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
        let sealed = key
            .seal_object_chunk(path.as_str(), &prefix, 0, 0, &plaintext)
            .unwrap();
        let mut bytes = (plaintext.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&sealed);
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
