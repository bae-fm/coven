use super::*;
use crate::test_utils as f;

#[test]
fn batch_checks_device_read_through_and_original_write_order() {
    let slot = f::slot_header(8, 4);
    let header = f::write().header;
    let mut other = header.clone();
    other.position.device.0 += 1;
    assert!(check_write(&slot, None, &other).is_err());
    let mut future = header.clone();
    future.read_through = 8;
    assert!(check_write(&slot, None, &future).is_err());
    check_write(&slot, None, &header).unwrap();
    assert!(check_write(&slot, Some(header.position.number), &header).is_err());
    assert!(check_write(&slot, Some(header.position.number + 1), &header).is_err());
    check_write(&slot, Some(header.position.number - 1), &header).unwrap();
    assert_eq!(
        identity(f::STORE_ID, header.position).unwrap(),
        format!("coven/write/{}/1/3", f::STORE_ID)
    );
}
