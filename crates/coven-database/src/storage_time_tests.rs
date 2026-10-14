use super::*;

#[test]
fn storage_times_keep_every_nanosecond_and_reject_noncanonical_fractions() {
    for (time, expected) in [
        (StorageTime::UNIX_EPOCH, vec![0; 12]),
        // Not a multiple of 100 ns: a Windows SystemTime would lose it.
        (
            StorageTime::new(0x0102_0304_0506_0708, 123_456_789).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 91, 205, 21],
        ),
        (
            StorageTime::new(u64::MAX, 999_999_999).unwrap(),
            vec![255, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255],
        ),
    ] {
        assert_eq!(encode(time), expected);
        assert_eq!(decode(&expected).unwrap(), time);
    }
    for bytes in [
        vec![],
        vec![0; 11],
        vec![0; 13],
        [vec![0; 8], 1_000_000_000u32.to_be_bytes().to_vec()].concat(),
        [vec![0; 8], u32::MAX.to_be_bytes().to_vec()].concat(),
    ] {
        assert!(matches!(decode(&bytes), Err(DbError::DamagedDatabase)));
    }
}

#[test]
fn byte_order_is_time_order() {
    let times = [
        StorageTime::new(1, 999_999_999).unwrap(),
        StorageTime::new(2, 0).unwrap(),
        StorageTime::new(2, 1).unwrap(),
        StorageTime::new(256, 0).unwrap(),
    ];
    for pair in times.windows(2) {
        assert!(encode(pair[0]) < encode(pair[1]));
    }
}
