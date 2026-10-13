use super::*;

#[test]
fn ranges_refuse_empty_or_reversed_bounds_and_keep_exclusive_ends() {
    for (start, end) in [(0, 0), (1, 0), (u64::MAX, u64::MAX)] {
        assert_eq!(
            ByteRange::new(start, end).unwrap_err().failure(),
            StorageFailure::InvalidRange
        );
    }
    let range = ByteRange::new(1, 4).unwrap();
    assert_eq!((range.start(), range.end(), range.len()), (1, 4, 3));
    assert!(!range.is_empty());
    assert_eq!(range.header(), "bytes=1-3");
    assert_eq!(range.select(b"abcde").unwrap(), b"bcd");
    assert_eq!(
        range.select(b"abc").unwrap_err().failure(),
        StorageFailure::InvalidRange
    );
    let range = ByteRange::new(0, u64::MAX).unwrap();
    assert_eq!(range.len(), u64::MAX);
    assert_eq!(range.header(), "bytes=0-18446744073709551614");
}
