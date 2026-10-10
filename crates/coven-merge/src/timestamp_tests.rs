use crate::{MergeError, Timestamp};
use coven_foundation::id_source::DeviceId;

#[test]
fn clocks_counters_and_devices_stay_ordered() {
    let ana = Timestamp::new(60_000, 0, DeviceId(9)).unwrap();
    let ben = Timestamp::next(Some(ana), 30_000, DeviceId(1)).unwrap();
    assert_eq!(
        (ben.milliseconds(), ben.counter(), ben.device()),
        (60_000, 1, DeviceId(1))
    );
    assert!(ben > ana);
    assert_eq!(
        Timestamp::next(Some(ben), 60_001, DeviceId(1)).unwrap(),
        Timestamp::new(60_001, 0, DeviceId(1)).unwrap()
    );
    let full = Timestamp::new(70_000, u16::MAX, DeviceId(u64::MAX)).unwrap();
    assert_eq!(
        Timestamp::next(Some(full), 60_000, DeviceId(0)).unwrap(),
        Timestamp::new(70_001, 0, DeviceId(0)).unwrap()
    );
    assert_eq!(
        Timestamp::next(None, 0, DeviceId(0)).unwrap(),
        Timestamp::new(0, 0, DeviceId(0)).unwrap()
    );
    assert!(
        Timestamp::new(1, 0, DeviceId(2)).unwrap() > Timestamp::new(1, 0, DeviceId(1)).unwrap()
    );
    let future = Timestamp::new(31_536_000_000, u16::MAX, DeviceId(3)).unwrap();
    assert!(Timestamp::next(Some(future), 1, DeviceId(1)).unwrap() > future);
}

#[test]
fn representation_limits_are_typed_errors() {
    let max = Timestamp::MAX_MILLISECONDS;
    assert_eq!(
        Timestamp::new(max + 1, 0, DeviceId(0)),
        Err(MergeError::MillisecondsOutOfRange(max + 1))
    );
    assert_eq!(
        Timestamp::next(None, u64::MAX, DeviceId(0)),
        Err(MergeError::MillisecondsOutOfRange(u64::MAX))
    );
    assert_eq!(
        Timestamp::next(
            Some(Timestamp::new(max, u16::MAX, DeviceId(0)).unwrap()),
            0,
            DeviceId(1)
        ),
        Err(MergeError::TimestampExhausted)
    );
}
