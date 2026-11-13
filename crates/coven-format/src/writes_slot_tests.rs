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
    // Every device applies slot 1 before its first write (D5.1).
    let mut unread = header.clone();
    unread.read_through = 0;
    assert!(check_write(&slot, None, &unread).is_err());
    assert!(check_write(&slot, Some(header.position.number), &header).is_err());
    assert!(check_write(&slot, Some(header.position.number + 1), &header).is_err());
    check_write(&slot, Some(header.position.number - 1), &header).unwrap();
    assert_eq!(
        identity(f::STORE_ID, header.position).unwrap(),
        format!("coven/write/{}/1/3", f::STORE_ID)
    );
}

#[test]
fn a_body_head_holds_its_slot_header_and_a_positive_write_count() {
    let path = f::slot_path(8);
    let slot = f::slot_header(8, 4);
    let head = encode_head(&slot, &path, 2).unwrap();
    assert_eq!(head.len(), HEAD_LEN);
    assert_eq!(&head[SlotHeader::ENCODED_LEN..], 2u64.to_be_bytes());
    assert_eq!(decode_head(&head, &path).unwrap(), (slot.clone(), 2));
    assert!(decode_head(&head, &f::slot_path(9)).is_err());
    assert!(decode_head(&head[..HEAD_LEN - 1], &path).is_err());
    assert!(encode_head(&slot, &path, 0).is_err());
    let mut empty = head;
    empty[SlotHeader::ENCODED_LEN..].copy_from_slice(&0u64.to_be_bytes());
    assert!(decode_head(&empty, &path).is_err());
}

#[test]
fn an_inner_write_length_is_positive_and_within_the_body() {
    let field = encode_write_length(5);
    assert_eq!(field, 5u64.to_be_bytes());
    assert_eq!(decode_write_length(field, 5).unwrap(), 5);
    assert!(decode_write_length(field, 4).is_err());
    assert!(decode_write_length(encode_write_length(0), 5).is_err());
}
