use super::*;
use crate::test_utils::MemoryStorage;
use crate::{ObjectPath, Storage};
use coven_foundation::clock::FixedClock;
use std::{sync::Arc, time::UNIX_EPOCH};

#[test]
fn counts_since_an_earlier_reading_name_each_kind_that_sent() {
    let mut earlier = RequestCounts::default();
    earlier.0[RequestKind::Read.index()] =
        RequestTotal { count: 2, bytes: 10, time: Duration::from_millis(5) };
    let mut now = earlier;
    now.0[RequestKind::Read.index()] =
        RequestTotal { count: 5, bytes: 40, time: Duration::from_millis(9) };
    now.0[RequestKind::Status.index()].count = 1;
    let since = now.since(&earlier);
    assert_eq!(
        since.kind(RequestKind::Read),
        RequestTotal { count: 3, bytes: 30, time: Duration::from_millis(4) }
    );
    assert_eq!(since.total().count, 4);
    assert_eq!(since.to_string(), "status 1 (0 B, 0ns), read 3 (30 B, 4ms)");
    assert_eq!(RequestCounts::default().to_string(), "none");
}

#[tokio::test]
async fn a_connection_counts_its_requests_and_the_bytes_they_carry() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = MemoryStorage::builder().clock(clock).build().unwrap();
    let path = ObjectPath::slot(crate::test_utils::STORE_ID, 1.try_into().unwrap());
    storage.create(&path, b"twelve bytes").await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"twelve bytes");
    let missing = ObjectPath::slot(crate::test_utils::STORE_ID, 2.try_into().unwrap());
    assert!(storage.status(&missing).await.unwrap().is_none());
    let counts = storage.requests();
    assert_eq!(
        counts.kind(RequestKind::Create),
        RequestTotal { count: 1, bytes: 12, time: Duration::ZERO }
    );
    assert_eq!(counts.kind(RequestKind::Read).count, 1);
    assert_eq!(counts.kind(RequestKind::Read).bytes, 12);
    assert_eq!(counts.kind(RequestKind::Status).count, 1);
    assert_eq!(counts.total().count, 3);
}
