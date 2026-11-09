use super::*;
use std::io::{self, Cursor, SeekFrom};

struct ObservedReader {
    bytes: Cursor<Vec<u8>>,
    requested: Vec<usize>,
}

impl Read for ObservedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.requested.push(output.len());
        self.bytes.read(output)
    }
}

impl Seek for ObservedReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.bytes.seek(from)
    }
}

fn object_path() -> ObjectPath {
    ObjectPath::parse("10101010-1010-1010-1010-101010101010/snapshots/store/1/1").unwrap()
}

#[test]
fn a_header_without_its_advertised_payload_never_requests_a_payload_buffer() {
    let mut input = ObservedReader {
        bytes: Cursor::new(vec![0x05, 0x00, 0x01, 0x00, 0xff, 0xff, 0xf9]),
        requested: Vec::new(),
    };
    assert!(matches!(frame(&mut input, &object_path()), Err(SyncError::Damaged(_))));
    assert_eq!(input.requested, [coven_format::FRAME_PREFIX_LEN]);
    assert_eq!(input.bytes.position(), 7);
}

#[test]
fn bounded_reads_reject_lengths_past_the_remaining_input_before_reading() {
    for position in [0, 2, 4, 5] {
        let mut input =
            ObservedReader { bytes: Cursor::new(vec![1, 2, 3, 4]), requested: Vec::new() };
        input.bytes.set_position(position);
        assert!(read_exact(&mut input, 5, &object_path()).is_err());
        assert!(input.requested.is_empty());
        assert_eq!(input.bytes.position(), position);
    }
}

#[test]
fn frame_reads_stop_at_the_declared_boundary() {
    let first = [5, 0, 1, 0, 0, 0, 3, 10, 11, 12];
    let second = [7, 0, 1, 0, 0, 0, 0];
    let mut input = Cursor::new([first.as_slice(), second.as_slice()].concat());
    assert_eq!(frame(&mut input, &object_path()).unwrap(), first);
    assert_eq!(input.position(), first.len() as u64);
    assert_eq!(frame(&mut input, &object_path()).unwrap(), second);
}

/// Ana publishes Gifts' snapshot to the Household store on iCloud, where a
/// read moves each part it reaches whole. When she adds Ben to Gifts, his
/// device loads Gifts from that snapshot: reading its prefix, then its body,
/// moves each part once.
#[tokio::test]
async fn loading_a_snapshot_moves_each_part_once() {
    use crate::sequence_sync::tests::*;
    use coven_format::admin::StoreChange;
    use coven_storage::test_utils::MemoryStorage;
    // A part far smaller than CloudKit's 8 MiB, so a small snapshot spans
    // several.
    const PART: usize = 1024;
    let group = Group::on(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::CloudKit)
            .transfer_limits(PART as u64, PART)
            .reads_whole_parts(),
    );
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    let wishes = 40;
    ana.database()
        .write(move |sql| {
            for wish in 0..wishes {
                let id = format!("00000000-0000-4000-8000-{wish:012}");
                sql.execute(
                    "INSERT INTO pins VALUES(?1,?2,?3)",
                    (id, gifts.to_string(), format!("wish {wish} {}", "x".repeat(100))),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Circle(gifts)).await.unwrap();
    let snapshot = group
        .storage
        .list(&coven_storage::ObjectPrefix::snapshots(group.store))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .find(|path| path.snapshot_id().is_some())
        .expect("the snapshot is uploaded");
    let size = group.storage.status(&snapshot).await.unwrap().expect("the snapshot").size;
    assert!(size > 2 * PART as u64, "the snapshot spans several parts: {size}");
    ben.sync.pass().await.unwrap();

    let before = group.storage.moved_bytes(&snapshot).await;
    ana.sync
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(pins(ben.database()).await.len(), wishes);
    let moved = group.storage.moved_bytes(&snapshot).await - before;
    assert_eq!(moved, size, "each of the snapshot's parts moved once");
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}
