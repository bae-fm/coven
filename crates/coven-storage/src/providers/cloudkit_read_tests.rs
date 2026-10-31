use super::*;
use std::sync::Mutex;

struct Parts {
    requested: Mutex<Vec<u64>>,
    total: usize,
    truncate: bool,
}
#[async_trait::async_trait]
impl AssetParts for Parts {
    async fn part(
        &self,
        root: &str,
        start: u64,
        progress: native::AssetProgress,
    ) -> Result<Vec<u8>, StorageError> {
        self.requested.lock().unwrap().push(start);
        // As CloudKit's per-record progress does: halfway, then the whole asset.
        let name = record::part_name(root, start);
        progress.heard(&name, 0.5);
        progress.heard(&name, 1.0);
        let length = (self.total - start as usize).min(PART_SIZE) - usize::from(self.truncate);
        Ok(vec![(start / PART_SIZE as u64) as u8; length])
    }
}

#[tokio::test]
async fn each_native_part_is_fetched_once_and_only_when_its_bytes_are_needed() {
    let source =
        Parts { requested: Mutex::new(Vec::new()), total: PART_SIZE * 2 + 3, truncate: false };
    let mut body = CloudKitBody {
        storage: &source,
        root: "upload".into(),
        size: source.total as u64,
        offset: 0,
        end: source.total as u64,
        part: Cursor::new(Vec::new()),
        received: ByteProgress::unread(),
    };
    assert!(source.requested.lock().unwrap().is_empty());
    let mut output = [0; 65_536];
    let mut through = 0;
    loop {
        let length = body.read(&mut output).await.unwrap();
        if length == 0 {
            break;
        }
        for (index, byte) in output[..length].iter().enumerate() {
            assert_eq!(*byte, ((through + index) / PART_SIZE) as u8);
        }
        through += length;
        assert_eq!(source.requested.lock().unwrap().len(), through.div_ceil(PART_SIZE));
        assert!(body.part.get_ref().len() <= PART_SIZE);
    }
    assert_eq!(through, source.total);
    assert_eq!(*source.requested.lock().unwrap(), [0, PART_SIZE as u64, (PART_SIZE * 2) as u64]);
}

#[tokio::test]
async fn ranged_assets_fetch_only_intersecting_parts_and_reject_incomplete_parts() {
    for truncate in [false, true] {
        let source = Parts { requested: Mutex::new(Vec::new()), total: PART_SIZE * 3, truncate };
        let mut body = CloudKitBody {
            storage: &source,
            root: "upload".into(),
            size: source.total as u64,
            offset: PART_SIZE as u64 + 1,
            end: PART_SIZE as u64 + 4,
            part: Cursor::new(Vec::new()),
            received: ByteProgress::unread(),
        };
        let result = body.read(&mut [0; 8]).await;
        if truncate {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::Protocol);
        } else {
            assert_eq!(result.unwrap(), 3);
            assert_eq!(body.read(&mut [0; 8]).await.unwrap(), 0);
        }
        assert_eq!(*source.requested.lock().unwrap(), [PART_SIZE as u64]);
    }
}

/// Each part's fetch is heard through the object as CloudKit reports its
/// asset arriving, from where the part starts, never past the range's end.
#[tokio::test]
async fn each_parts_fetch_is_heard_through_the_object() {
    let source =
        Parts { requested: Mutex::new(Vec::new()), total: PART_SIZE * 2 + 3, truncate: false };
    let heard = std::sync::Arc::new(Mutex::new(Vec::new()));
    let end = PART_SIZE as u64 + 3;
    let mut body = CloudKitBody {
        storage: &source,
        root: "upload".into(),
        size: source.total as u64,
        offset: 0,
        end,
        part: Cursor::new(Vec::new()),
        received: ByteProgress::new({
            let heard = heard.clone();
            move |count| heard.lock().unwrap().push(count)
        }),
    };
    let mut output = [0; 65_536];
    while body.read(&mut output).await.unwrap() != 0 {}
    let part = PART_SIZE as u64;
    assert_eq!(*heard.lock().unwrap(), [part / 2, part, end, end]);
}
